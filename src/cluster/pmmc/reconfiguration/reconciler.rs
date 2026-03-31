use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

use crate::{
    cluster::{
        pmmc::{
            control::types::{
                ConfigurationCommand, ConfigurationHandlerError, ConfigurationReplyOutcome,
            },
            process_manager::PmmcProcessManager,
            reconfiguration::{
                ClusterConfiguration, ConfigurationStrategy, reconfig_patch::ReconfigPatch,
                types::ReconcileError,
            },
        },
    },
    monitor::{Event, current_timestamp_millis},
    rsm::checkpoint::RsmCheckpoint,
};

#[async_trait]
trait ReconcileOps {
    async fn configuration_operation(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
        timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError>;

    async fn configuration_operation_batch(
        &self,
        endpoints: Vec<Uuid>,
        cmd: ConfigurationCommand,
        timeout: Duration,
    ) -> HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>;

    fn emit_reconfiguration_stop_completed(
        &self,
        _strategy: ConfigurationStrategy,
        _stopped_nodes: Vec<Uuid>,
    ) {
    }
}

#[async_trait]
impl ReconcileOps for PmmcProcessManager {
    async fn configuration_operation(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
        timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        PmmcProcessManager::configuration_operation(self, endpoint, cmd, timeout).await
    }

    async fn configuration_operation_batch(
        &self,
        endpoints: Vec<Uuid>,
        cmd: ConfigurationCommand,
        timeout: Duration,
    ) -> HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> {
        PmmcProcessManager::configuration_operation_batch(self, endpoints, |_| cmd.clone(), timeout)
            .await
    }

    fn emit_reconfiguration_stop_completed(
        &self,
        strategy: ConfigurationStrategy,
        stopped_nodes: Vec<Uuid>,
    ) {
        self.observe_event(Event::ReconfigurationStopCompleted {
            strategy,
            stopped_nodes,
            created_at: current_timestamp_millis(),
        });
    }
}

pub struct ClusterReconciler {
    prev: Arc<ClusterConfiguration>,
    next_config: ClusterConfiguration,
}

impl ClusterReconciler {
    pub fn next_config(&self) -> &ClusterConfiguration {
        &self.next_config
    }

    pub async fn execute(
        &mut self,
        process_manager: &PmmcProcessManager,
        timeout: Duration,
    ) -> Result<(), ReconcileError> {
        self.stop_all(process_manager, timeout).await?;
        let checkpoint = self.get_checkpoint(process_manager, timeout).await?;
        self.next_config.add_checkpoint(checkpoint);
        Ok(())
    }

    pub async fn stop_all(
        &self,
        process_manager: &PmmcProcessManager,
        timeout: Duration,
    ) -> Result<(), ReconcileError> {
        self.stop_all_with_ops(process_manager, timeout).await
    }

    async fn stop_all_with_ops<O: ReconcileOps + Sync>(
        &self,
        ops: &O,
        timeout: Duration,
    ) -> Result<(), ReconcileError> {
        let mut replicas = self.prev.replicas();
        replicas.sort_unstable();

        let mut failures = HashMap::new();
        let per_attempt_timeout = timeout.min(Duration::from_secs(5));

        let mut stop_accepted = false;
        for replica in replicas.iter().copied() {
            match ops
                .configuration_operation(replica, ConfigurationCommand::Stop, per_attempt_timeout)
                .await
            {
                Ok(ConfigurationReplyOutcome::Stopped) => {
                    stop_accepted = true;
                    break;
                }
                Ok(other) => {
                    failures.insert(
                        replica,
                        ConfigurationHandlerError::Internal {
                            reason: format!("unexpected stop outcome: {:?}", other),
                        },
                    );
                }
                Err(err) => {
                    failures.insert(replica, err);
                }
            }
        }

        if !stop_accepted {
            return Err(ReconcileError::StopFailed { per_node: failures });
        }

        self.wait_until_replicas_stopped(ops, timeout).await?;
        ops.emit_reconfiguration_stop_completed(self.next_config.strategy(), replicas);
        Ok(())
    }

    async fn wait_until_replicas_stopped<O: ReconcileOps + Sync>(
        &self,
        ops: &O,
        timeout: Duration,
    ) -> Result<(), ReconcileError> {
        let mut replicas = self.prev.replicas();
        replicas.sort_unstable();

        let deadline = Instant::now() + timeout;
        let mut last_failures = HashMap::new();

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(ReconcileError::StopFailed {
                    per_node: last_failures,
                });
            }

            let remaining = deadline - now;
            let poll_timeout = remaining.min(Duration::from_millis(750));
            let mut outcomes = ops
                .configuration_operation_batch(
                    replicas.clone(),
                    ConfigurationCommand::Status,
                    poll_timeout,
                )
                .await;

            let mut failures = HashMap::new();
            let mut all_stopped = true;
            for id in replicas.iter().copied() {
                match outcomes.remove(&id) {
                    Some(Ok(ConfigurationReplyOutcome::Stopped)) => {}
                    Some(Ok(ConfigurationReplyOutcome::Active)) => {
                        all_stopped = false;
                        failures.insert(
                            id,
                            ConfigurationHandlerError::Internal {
                                reason: "replica still active while waiting for stop".to_string(),
                            },
                        );
                    }
                    Some(Ok(other)) => {
                        all_stopped = false;
                        failures.insert(
                            id,
                            ConfigurationHandlerError::Internal {
                                reason: format!("unexpected status outcome: {:?}", other),
                            },
                        );
                    }
                    Some(Err(err)) => {
                        all_stopped = false;
                        failures.insert(id, err);
                    }
                    None => {
                        all_stopped = false;
                        failures.insert(
                            id,
                            ConfigurationHandlerError::Internal {
                                reason: "missing status result for replica endpoint".to_string(),
                            },
                        );
                    }
                }
            }

            if all_stopped {
                return Ok(());
            }

            last_failures = failures;
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn get_checkpoint(
        &self,
        process_manager: &PmmcProcessManager,
        timeout: Duration,
    ) -> Result<RsmCheckpoint, ReconcileError> {
        self.get_checkpoint_with_ops(process_manager, timeout).await
    }

    async fn get_checkpoint_with_ops<O: ReconcileOps + Sync>(
        &self,
        ops: &O,
        timeout: Duration,
    ) -> Result<RsmCheckpoint, ReconcileError> {
        let mut node_ids = self.prev.replicas();
        node_ids.sort_unstable();

        let mut outcomes = ops
            .configuration_operation_batch(
                node_ids.clone(),
                ConfigurationCommand::Checkpoint,
                timeout,
            )
            .await;

        let mut failures = HashMap::new();
        let mut checkpoint = None;
        for id in node_ids {
            match outcomes.remove(&id) {
                Some(response) => match response {
                    Ok(outcome) => match outcome {
                        ConfigurationReplyOutcome::Checkpoint(rsm_checkpoint) => match checkpoint {
                            None => checkpoint = Some(rsm_checkpoint),
                            Some(ref lhs) => {
                                if *lhs < rsm_checkpoint {
                                    checkpoint = Some(rsm_checkpoint)
                                }
                            }
                        },
                        other => {
                            failures.insert(
                                id,
                                ConfigurationHandlerError::Internal {
                                    reason: format!("unexpected stop outcome: {:?}", other),
                                },
                            );
                        }
                    },
                    Err(err) => {
                        failures.insert(
                            id,
                            ConfigurationHandlerError::Internal {
                                reason: format!("unexpected stop outcome: {:?}", err),
                            },
                        );
                    }
                },
                None => {
                    failures.insert(
                        id,
                        ConfigurationHandlerError::Internal {
                            reason: "missing checkpoint result for endpoint".to_string(),
                        },
                    );
                }
            }
        }

        if !failures.is_empty() {
            return Err(ReconcileError::CheckpointFailed { per_node: failures });
        }

        if let Some(checkpoint) = checkpoint {
            Ok(checkpoint)
        } else {
            Err(ReconcileError::NoCheckpointCandidate)
        }
    }
}
impl TryFrom<(Arc<ClusterConfiguration>, ReconfigPatch)> for ClusterReconciler {
    type Error = ReconcileError;

    fn try_from(
        (prev, patch): (Arc<ClusterConfiguration>, ReconfigPatch),
    ) -> Result<Self, Self::Error> {
        let next_config = ClusterConfiguration::try_from((Arc::clone(&prev), patch))?;

        Ok(ClusterReconciler { prev, next_config })
    }
}

#[cfg(test)]
#[path = "reconciler_tests.rs"]
mod reconciler_tests;

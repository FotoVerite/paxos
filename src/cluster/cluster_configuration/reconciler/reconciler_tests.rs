use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use uuid::Uuid;

use super::*;
use crate::cluster::cluster_configuration::{ConfigurationStatus, ConfigurationStrategy};
use crate::rsm::checkpoint::{CheckpointManifest, CheckpointState, RsmCheckpoint, RsmCheckpointState};

#[derive(Clone)]
struct MockOps {
    stop_results: Arc<Mutex<HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>>>,
    status_batches:
        Arc<Mutex<VecDeque<HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>>>>,
    checkpoint_batches:
        Arc<Mutex<VecDeque<HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>>>>,
    stop_calls: Arc<Mutex<Vec<Uuid>>>,
    status_default: ConfigurationReplyOutcome,
}

impl Default for MockOps {
    fn default() -> Self {
        Self {
            stop_results: Arc::new(Mutex::new(HashMap::new())),
            status_batches: Arc::new(Mutex::new(VecDeque::new())),
            checkpoint_batches: Arc::new(Mutex::new(VecDeque::new())),
            stop_calls: Arc::new(Mutex::new(Vec::new())),
            status_default: ConfigurationReplyOutcome::Stopped,
        }
    }
}

impl MockOps {
    fn with_stop_result(
        self,
        id: Uuid,
        result: Result<ConfigurationReplyOutcome, ConfigurationHandlerError>,
    ) -> Self {
        self.stop_results.lock().unwrap().insert(id, result);
        self
    }

    fn with_status_batch(
        self,
        batch: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>,
    ) -> Self {
        self.status_batches.lock().unwrap().push_back(batch);
        self
    }

    fn with_checkpoint_batch(
        self,
        batch: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>,
    ) -> Self {
        self.checkpoint_batches.lock().unwrap().push_back(batch);
        self
    }

    fn with_status_default(self, status_default: ConfigurationReplyOutcome) -> Self {
        Self {
            status_default,
            ..self
        }
    }

    fn stop_calls(&self) -> Vec<Uuid> {
        self.stop_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ReconcileOps for MockOps {
    async fn configuration_operation(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
        _timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        match cmd {
            ConfigurationCommand::Stop => {
                self.stop_calls.lock().unwrap().push(endpoint);
                self.stop_results
                    .lock()
                    .unwrap()
                    .remove(&endpoint)
                    .unwrap_or(Err(ConfigurationHandlerError::Timeout))
            }
            _ => Err(ConfigurationHandlerError::InvalidRequest {
                reason: "unexpected command in mock configuration_operation".to_string(),
            }),
        }
    }

    async fn configuration_operation_batch(
        &self,
        endpoints: Vec<Uuid>,
        cmd: ConfigurationCommand,
        _timeout: Duration,
    ) -> HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> {
        match cmd {
            ConfigurationCommand::Status => {
                if let Some(batch) = self.status_batches.lock().unwrap().pop_front() {
                    return batch;
                }
                endpoints
                    .into_iter()
                    .map(|id| (id, Ok(self.status_default.clone())))
                    .collect()
            }
            ConfigurationCommand::Checkpoint => {
                if let Some(batch) = self.checkpoint_batches.lock().unwrap().pop_front() {
                    return batch;
                }
                endpoints
                    .into_iter()
                    .map(|id| {
                        (
                            id,
                            Err(ConfigurationHandlerError::Internal {
                                reason: "no checkpoint batch configured".to_string(),
                            }),
                        )
                    })
                    .collect()
            }
            _ => endpoints
                .into_iter()
                .map(|id| {
                    (
                        id,
                        Err(ConfigurationHandlerError::InvalidRequest {
                            reason: "unexpected command in mock batch".to_string(),
                        }),
                    )
                })
                .collect(),
        }
    }
}

fn roles(proposer: bool, acceptor: bool, learner: bool) -> Roles {
    Roles {
        proposer,
        acceptor,
        learner,
    }
}

fn base_configuration(replica_ids: [Uuid; 2]) -> Arc<ClusterConfiguration> {
    Arc::new(ClusterConfiguration {
        id: 7,
        epoch: 1,
        status: ConfigurationStatus::ACTIVE,
        nodes: vec![
            (Uuid::from_u128(100), roles(true, false, false)),
            (Uuid::from_u128(200), roles(false, true, false)),
            (replica_ids[0], roles(false, false, true)),
            (replica_ids[1], roles(false, false, true)),
        ],
        strategy: ConfigurationStrategy::JointConsensus,
        alpha_max_inflight: 5,
        checkpoint: None,
    })
}

fn checkpoint(source_node: Uuid, config_id: u64, epoch: u64, last_applied_slot: usize) -> RsmCheckpoint {
    let manifest = CheckpointManifest::new(
        Uuid::new_v4(),
        source_node,
        config_id,
        epoch,
        Some(last_applied_slot),
        1_742_123_456_789,
    );
    let state = CheckpointState::new(RsmCheckpointState::new(HashMap::new()), HashMap::new());
    RsmCheckpoint::new(manifest, state)
}

#[tokio::test]
async fn stop_all_fails_over_to_second_replica_after_first_failure() {
    let r0 = Uuid::from_u128(1);
    let r1 = Uuid::from_u128(2);
    let prev = base_configuration([r0, r1]);
    let reconciler = ClusterReconciler::try_from((Arc::clone(&prev), ReconfigPatch::new()))
        .expect("reconciler should build");

    let status: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> = [
        (r0, Ok(ConfigurationReplyOutcome::Stopped)),
        (r1, Ok(ConfigurationReplyOutcome::Stopped)),
    ]
    .into_iter()
    .collect();

    let mock = MockOps::default()
        .with_stop_result(r0, Err(ConfigurationHandlerError::Timeout))
        .with_stop_result(r1, Ok(ConfigurationReplyOutcome::Stopped))
        .with_status_batch(status);

    reconciler
        .stop_all_with_ops(&mock, Duration::from_secs(2))
        .await
        .expect("stop_all should succeed via failover");

    let calls = mock.stop_calls();
    assert_eq!(calls, vec![r0, r1], "should fail over to second replica");
}

#[tokio::test]
async fn stop_all_returns_stop_failed_when_all_replicas_fail() {
    let r0 = Uuid::from_u128(10);
    let r1 = Uuid::from_u128(11);
    let prev = base_configuration([r0, r1]);
    let reconciler = ClusterReconciler::try_from((Arc::clone(&prev), ReconfigPatch::new()))
        .expect("reconciler should build");

    let mock = MockOps::default()
        .with_stop_result(r0, Err(ConfigurationHandlerError::Timeout))
        .with_stop_result(r1, Err(ConfigurationHandlerError::ResponseChannelClosed));

    let err = reconciler
        .stop_all_with_ops(&mock, Duration::from_secs(2))
        .await
        .expect_err("stop_all should fail when all replicas fail");

    match err {
        ReconcileError::StopFailed { per_node } => {
            assert_eq!(per_node.len(), 2);
            assert!(per_node.contains_key(&r0));
            assert!(per_node.contains_key(&r1));
        }
        other => panic!("expected StopFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn wait_until_replicas_stopped_succeeds_after_active_then_stopped() {
    let r0 = Uuid::from_u128(20);
    let r1 = Uuid::from_u128(21);
    let prev = base_configuration([r0, r1]);
    let reconciler = ClusterReconciler::try_from((Arc::clone(&prev), ReconfigPatch::new()))
        .expect("reconciler should build");

    let first_status: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> = [
        (r0, Ok(ConfigurationReplyOutcome::Active)),
        (r1, Ok(ConfigurationReplyOutcome::Stopped)),
    ]
    .into_iter()
    .collect();
    let second_status: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> = [
        (r0, Ok(ConfigurationReplyOutcome::Stopped)),
        (r1, Ok(ConfigurationReplyOutcome::Stopped)),
    ]
    .into_iter()
    .collect();

    let mock = MockOps::default()
        .with_status_batch(first_status)
        .with_status_batch(second_status);

    reconciler
        .wait_until_replicas_stopped(&mock, Duration::from_secs(1))
        .await
        .expect("should converge once all replicas report stopped");
}

#[tokio::test]
async fn wait_until_replicas_stopped_times_out_if_replicas_never_stop() {
    let r0 = Uuid::from_u128(30);
    let r1 = Uuid::from_u128(31);
    let prev = base_configuration([r0, r1]);
    let reconciler = ClusterReconciler::try_from((Arc::clone(&prev), ReconfigPatch::new()))
        .expect("reconciler should build");

    let mock = MockOps::default().with_status_default(ConfigurationReplyOutcome::Active);

    let err = reconciler
        .wait_until_replicas_stopped(&mock, Duration::from_millis(250))
        .await
        .expect_err("should timeout while replicas stay active");

    match err {
        ReconcileError::StopFailed { per_node } => {
            assert_eq!(per_node.len(), 2);
            assert!(per_node.contains_key(&r0));
            assert!(per_node.contains_key(&r1));
        }
        other => panic!("expected StopFailed timeout error, got {other:?}"),
    }
}

#[tokio::test]
async fn get_checkpoint_selects_highest_checkpoint() {
    let r0 = Uuid::from_u128(40);
    let r1 = Uuid::from_u128(41);
    let prev = base_configuration([r0, r1]);
    let reconciler = ClusterReconciler::try_from((Arc::clone(&prev), ReconfigPatch::new()))
        .expect("reconciler should build");

    let lower = checkpoint(r0, 7, 1, 10);
    let higher = checkpoint(r1, 7, 1, 22);
    let batch: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> = [
        (r0, Ok(ConfigurationReplyOutcome::Checkpoint(lower.clone()))),
        (r1, Ok(ConfigurationReplyOutcome::Checkpoint(higher.clone()))),
    ]
    .into_iter()
    .collect();

    let mock = MockOps::default().with_checkpoint_batch(batch);

    let selected = reconciler
        .get_checkpoint_with_ops(&mock, Duration::from_secs(1))
        .await
        .expect("checkpoint collection should succeed");

    assert_eq!(selected, higher);
}

#[tokio::test]
async fn get_checkpoint_returns_checkpoint_failed_on_non_checkpoint_outcome() {
    let r0 = Uuid::from_u128(50);
    let r1 = Uuid::from_u128(51);
    let prev = base_configuration([r0, r1]);
    let reconciler = ClusterReconciler::try_from((Arc::clone(&prev), ReconfigPatch::new()))
        .expect("reconciler should build");

    let good = checkpoint(r0, 7, 1, 10);
    let batch: HashMap<Uuid, Result<ConfigurationReplyOutcome, ConfigurationHandlerError>> = [
        (r0, Ok(ConfigurationReplyOutcome::Checkpoint(good))),
        (r1, Ok(ConfigurationReplyOutcome::Active)),
    ]
    .into_iter()
    .collect();

    let mock = MockOps::default().with_checkpoint_batch(batch);

    let err = reconciler
        .get_checkpoint_with_ops(&mock, Duration::from_secs(1))
        .await
        .expect_err("non-checkpoint outcome should fail checkpoint collection");

    match err {
        ReconcileError::CheckpointFailed { per_node } => {
            assert_eq!(per_node.len(), 1);
            assert!(per_node.contains_key(&r1));
        }
        other => panic!("expected CheckpointFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn get_checkpoint_returns_no_checkpoint_candidate_when_none_returned() {
    let prev = Arc::new(ClusterConfiguration {
        id: 9,
        epoch: 2,
        status: ConfigurationStatus::ACTIVE,
        nodes: vec![
            (Uuid::from_u128(1000), roles(true, false, false)),
            (Uuid::from_u128(2000), roles(false, true, false)),
        ],
        strategy: ConfigurationStrategy::JointConsensus,
        alpha_max_inflight: 5,
        checkpoint: None,
    });

    let reconciler = ClusterReconciler {
        prev: Arc::clone(&prev),
        next_config: (*prev).clone(),
        to_add: HashMap::new(),
        to_remove: std::collections::HashSet::new(),
    };

    let mock = MockOps::default();

    let err = reconciler
        .get_checkpoint_with_ops(&mock, Duration::from_secs(1))
        .await
        .expect_err("no checkpoint candidates should fail");

    match err {
        ReconcileError::NoCheckpointCandidate => {}
        other => panic!("expected NoCheckpointCandidate, got {other:?}"),
    }
}

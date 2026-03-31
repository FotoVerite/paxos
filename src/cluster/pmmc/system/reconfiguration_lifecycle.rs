use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::time::{Instant, sleep, timeout};

use crate::cluster::pmmc::control::types::{
    ConfigurationCommand, ConfigurationReplyOutcome,
};
use crate::cluster::pmmc::process_manager::PmmcProcessManager;
use crate::cluster::pmmc::reconfiguration::reconciler::ClusterReconciler;
use crate::cluster::pmmc::reconfiguration::reconfig_patch::ReconfigPatch;
use crate::monitor::{Event, current_timestamp_millis};

use super::PmmcCluster;

impl PmmcCluster {
    const UPDATE_CONFIGURATION_TIMEOUT: Duration = Duration::from_secs(30);

    pub async fn update_configuration(&mut self, patch: ReconfigPatch) -> anyhow::Result<()> {
        timeout(Self::UPDATE_CONFIGURATION_TIMEOUT, async {
            let strategy = patch.strategy.unwrap_or(self.configuration.strategy());
            let previous_nodes = self.configuration.member_uuids();
            let mut reconciler =
                ClusterReconciler::try_from((Arc::clone(&self.configuration), patch))?;
            reconciler
                .execute(&self.process_manager, Self::UPDATE_CONFIGURATION_TIMEOUT)
                .await?;

            self.process_manager.reset_for_reconfiguration().await;
            let configuration = Arc::new(reconciler.next_config().clone());
            let process_manager = PmmcProcessManager::init(
                configuration.node_configs(),
                Arc::clone(&self.fabric),
                Arc::clone(&configuration),
                Arc::clone(&self.persistence),
                Arc::clone(&self.observer),
            )
            .await?;
            self.total_number = configuration.node_configs().len();
            self.quorum_size = configuration.quorum();
            self.configuration = configuration;
            self.process_manager = process_manager;
            self.process_manager.start_for_reconfiguration().await;

            self.observer.on_event(Event::ReconfigurationApplied {
                strategy,
                previous_node_count: previous_nodes.len(),
                next_node_count: self.configuration.member_uuids().len(),
                created_at: current_timestamp_millis(),
            });

            Ok::<(), anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow!("update_configuration lifecycle timed out after 30 seconds"))??;

        Ok(())
    }

    pub async fn start_all_ready(&self, timeout: Duration) -> anyhow::Result<usize> {
        self.start_all().await;
        self.wait_ready(timeout).await
    }

    pub async fn update_configuration_ready(
        &mut self,
        patch: ReconfigPatch,
        timeout_duration: Duration,
    ) -> anyhow::Result<usize> {
        timeout(timeout_duration, self.update_configuration(patch))
            .await
            .map_err(|_| {
                anyhow!(
                    "update_configuration timed out after {:?}",
                    timeout_duration
                )
            })??;
        self.wait_ready(timeout_duration).await
    }

    pub async fn wait_ready(&self, timeout: Duration) -> anyhow::Result<usize> {
        let deadline = Instant::now() + timeout;
        let mut last_statuses: Vec<String> = Vec::new();
        let mut last_leader: Option<usize> = None;

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!(
                    "PMMC cluster did not become ready within {:?}; leader={:?}; statuses=[{}]",
                    timeout,
                    last_leader,
                    last_statuses.join(", ")
                ));
            }

            let per_node_timeout = (deadline - now).min(Duration::from_millis(400));
            let mut all_active = true;
            let mut statuses = Vec::new();

            for node_id in self.get_node_uuids() {
                match self
                    .process_manager
                    .configuration_operation(
                        node_id,
                        ConfigurationCommand::Status,
                        per_node_timeout,
                    )
                    .await
                {
                    Ok(ConfigurationReplyOutcome::Active) => {
                        statuses.push(format!("{node_id}=Active"));
                    }
                    Ok(other) => {
                        all_active = false;
                        statuses.push(format!("{node_id}={other:?}"));
                    }
                    Err(err) => {
                        all_active = false;
                        statuses.push(format!("{node_id}=Err({err})"));
                    }
                }
            }

            let leader_index = self.leader_index().await;
            if all_active {
                if let Some(idx) = leader_index {
                    return Ok(idx);
                }
            }

            last_statuses = statuses;
            last_leader = leader_index;
            sleep(Duration::from_millis(25)).await;
        }
    }
}

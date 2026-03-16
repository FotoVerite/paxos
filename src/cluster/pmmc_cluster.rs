use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

use crate::cluster::cluster_configuration::reconciler::ClusterReconciler;
use crate::cluster::cluster_configuration::reconfig_patch::ReconfigPatch;
use crate::cluster::cluster_configuration::{ClusterConfiguration, ConfigurationStrategy};
use crate::cluster::configuration_handler::types::{
    ConfigurationCommand, ConfigurationReplyOutcome,
};
use crate::cluster::runtime_member::RuntimeMember;
use crate::cluster::runtime_registry::RuntimeRegistry;
use crate::common::persistence::ClusterPersistence;
use crate::{
    common::persistence::Persistence,
    message::ClientMessage,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{
        config::PmmcNodeConfig,
        pmmc::transport::{PmmcFabric, new_pmmc_fabric},
    },
    paxos_command::PaxosCommand,
    rsm::kv_store::ReplyOutcome,
};

pub mod fixtures;
mod utility_ops;

pub struct PmmcCluster {
    configuration: Arc<ClusterConfiguration>,
    total_number: usize,
    quorum_size: usize,
    observer: Arc<dyn PaxosObserver>,
    pub runtime_registry: RuntimeRegistry,
    persistence: Arc<ClusterPersistence>,
    fabric: Arc<PmmcFabric>,
    cleanup_on_drop: bool,
}

impl PmmcCluster {
    const UPDATE_CONFIGURATION_TIMEOUT: Duration = Duration::from_secs(30);

    pub async fn new(
        ip: IpAddr,
        total_number: usize,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let configs = vec![PmmcNodeConfig::default(); total_number];
        Self::new_with_configs(ip, configs, observer).await
    }

    pub async fn update_configuration(&mut self, patch: ReconfigPatch) -> anyhow::Result<()> {
        timeout(Self::UPDATE_CONFIGURATION_TIMEOUT, async {
            let strategy = patch.strategy.unwrap_or(self.configuration.strategy());
            let previous_nodes = self.configuration.member_uuids();
            let mut reconciler =
                ClusterReconciler::try_from((Arc::clone(&self.configuration), patch))?;
            reconciler
                .execute(&self.runtime_registry, Self::UPDATE_CONFIGURATION_TIMEOUT)
                .await?;

            self.runtime_registry.reset_for_reconfiguration().await;
            let configuration = Arc::new(reconciler.next_config().clone());
            let runtime_registry = RuntimeRegistry::init(
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
            self.runtime_registry = runtime_registry;
            self.runtime_registry.start_for_reconfiguration().await;

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

    pub async fn new_with_configs(
        ip: IpAddr,
        configs: Vec<PmmcNodeConfig>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let configuration = ClusterConfiguration::bootstrap_pmmc(ip, configs)?;
        Self::new_with_configuration(ip, configuration, observer).await
    }

    pub async fn new_with_configuration(
        ip: IpAddr,
        configuration: ClusterConfiguration,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let persistence = Arc::new(Persistence::cluster(ip));
        let node_configs = configuration.node_configs();
        let total_number = node_configs.len();
        let quorum = configuration.quorum();
        let fabric = Arc::new(new_pmmc_fabric(Arc::clone(&observer)));
        let configuration = Arc::new(configuration);

        let runtime_registry = RuntimeRegistry::init(
            node_configs,
            Arc::clone(&fabric),
            Arc::clone(&configuration),
            Arc::clone(&persistence),
            Arc::clone(&observer),
        )
        .await?;
        Ok(Self {
            configuration,
            total_number,
            quorum_size: quorum,
            runtime_registry,
            observer: Arc::clone(&observer),
            fabric,
            persistence: Arc::clone(&persistence),
            cleanup_on_drop: false,
        })
    }

    pub async fn start_all(&self) {
        self.runtime_registry.start().await;
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
                    .runtime_registry
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

    pub fn num_nodes(&self) -> usize {
        self.total_number
    }

    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    pub fn get_node_uuids(&self) -> Vec<Uuid> {
        self.configuration.member_uuids()
    }

    pub fn strategy(&self) -> ConfigurationStrategy {
        self.configuration.strategy()
    }

    pub fn checkpoint_last_applied_slot(&self) -> Option<usize> {
        self.configuration.starting_slot().checked_sub(1)
    }

    pub async fn connect_client_to(
        &self,
        node: usize,
        client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        let uuid = self.configuration.member(node)?;
        self.runtime_registry
            .connect_client_to(uuid, client_id)
            .await
    }

    pub async fn request_replica(
        &self,
        replica_idx: usize,
        client_id: Uuid,
        request_id: u64,
        cmd: PaxosCommand,
        timeout_duration: Duration,
    ) -> anyhow::Result<ReplyOutcome> {
        let Some((tx, mut rx)) = self.connect_client_to(replica_idx, client_id).await else {
            return Err(anyhow!(
                "failed to attach client {} to replica {}",
                client_id,
                replica_idx
            ));
        };

        tx.send(ClientMessage::PROPOSE {
            cmd: cmd.with_client(client_id, request_id),
        })
        .await
        .map_err(|_| anyhow!("failed to send proposal to replica {replica_idx}"))?;

        match timeout(timeout_duration, rx.recv()).await {
            Ok(Some(ClientMessage::RESPONSE { response, .. })) => Ok(response),
            Ok(Some(other)) => Err(anyhow!(
                "unexpected client message from replica {}: {:?}",
                replica_idx,
                other
            )),
            Ok(None) => Err(anyhow!(
                "response channel closed while waiting on replica {}",
                replica_idx
            )),
            Err(_) => Err(anyhow!(
                "timed out waiting for replica {} response after {:?}",
                replica_idx,
                timeout_duration
            )),
        }
    }

    pub async fn request_any_replica(
        &self,
        request_id: u64,
        cmd: PaxosCommand,
        timeout_duration: Duration,
    ) -> anyhow::Result<ReplyOutcome> {
        if self.num_nodes() == 0 {
            return Err(anyhow!("request_any_replica called with empty cluster"));
        }

        let deadline = Instant::now() + timeout_duration;
        let mut last_errors: Vec<String> = Vec::new();

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!(
                    "client request timed out after {:?}; last_errors=[{}]",
                    timeout_duration,
                    last_errors.join(", ")
                ));
            }

            let mut round_errors = Vec::new();
            for replica_idx in 0..self.num_nodes() {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }

                let attempt_timeout = (deadline - now).min(Duration::from_secs(2));
                let client_id = Uuid::new_v4();
                match self
                    .request_replica(
                        replica_idx,
                        client_id,
                        request_id,
                        cmd.clone(),
                        attempt_timeout,
                    )
                    .await
                {
                    Ok(reply) => return Ok(reply),
                    Err(err) => {
                        round_errors.push(format!("replica {replica_idx}: {err}"));
                    }
                }
            }

            last_errors = round_errors;
            sleep(Duration::from_millis(75)).await;
        }
    }

    pub async fn leader(&self) -> Option<Arc<RuntimeMember>> {
        self.runtime_registry.current_leader().await
    }

    pub async fn leader_index(&self) -> Option<usize> {
        let leader = self.leader().await?;
        self.configuration
            .member_uuids()
            .iter()
            .position(|uuid| *uuid == leader.uuid())
    }
}

#[cfg(test)]
#[path = "pmmc_cluster_tests.rs"]
mod tests;

impl Drop for PmmcCluster {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }

        if let Err(err) = self.persistence.purge_cluster_dir_blocking() {
            tracing::warn!("failed to purge PMMC cluster persistence on drop: {}", err);
        }
    }
}

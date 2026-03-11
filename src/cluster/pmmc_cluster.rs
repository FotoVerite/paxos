use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;
use uuid::Uuid;

use crate::cluster::cluster_configuration::ClusterConfiguration;
use crate::cluster::cluster_configuration::reconciler::ClusterReconciler;
use crate::cluster::cluster_configuration::reconfig_patch::ReconfigPatch;
use crate::cluster::runtime_member::RuntimeMember;
use crate::cluster::runtime_registry::RuntimeRegistry;
use crate::common::persistence::ClusterPersistence;
use crate::{
    cluster::network_fabric::NetworkFabric, common::persistence::Persistence,
    message::ClientMessage, monitor::PaxosObserver, node::config::PmmcNodeConfig,
};

mod utility_ops;

pub struct PmmcCluster {
    configuration: Arc<ClusterConfiguration>,
    total_number: usize,
    quorum_size: usize,
    observer: Arc<dyn PaxosObserver>,
    pub runtime_registry: RuntimeRegistry,
    persistence: Arc<ClusterPersistence>,
    fabric: Arc<NetworkFabric>,
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
            let mut reconciler =
                ClusterReconciler::try_from((Arc::clone(&self.configuration), patch))?;
            reconciler
                .execute(&self.runtime_registry, Self::UPDATE_CONFIGURATION_TIMEOUT)
                .await?;
            self.runtime_registry.reset().await;
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
            self.runtime_registry.start().await;

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
        let fabric = Arc::new(NetworkFabric::new(Arc::clone(&observer)));
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

    pub fn num_nodes(&self) -> usize {
        self.total_number
    }

    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    pub fn get_node_uuids(&self) -> Vec<Uuid> {
        self.configuration.member_uuids()
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

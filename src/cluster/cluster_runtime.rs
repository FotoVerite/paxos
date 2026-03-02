use std::net::IpAddr;
use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;

use crate::cluster::cluster_configuration::ClusterConfiguration;
use crate::cluster::runtime_member::RuntimeMember;
use crate::cluster::runtime_registry::RuntimeRegistry;
use crate::common::persistence::ClusterPersistence;
use crate::node::peer_topology::PeerTopology;
use crate::{
    cluster::{network_fabric::NetworkFabric, network_simulator::NetworkFailure},
    common::persistence::Persistence,
    message::ClientMessage,
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::config::PmmcNodeConfig,
    paxos_command::PaxosCommand,
};

pub struct ClusterRuntime {
    configuration: ClusterConfiguration,
    total_number: usize,
    quorum_size: usize,
    observer: Arc<dyn PaxosObserver>,
    pub runtime_registry: RuntimeRegistry,
    persistence: Arc<ClusterPersistence>,
    fabric: Arc<NetworkFabric>,
}

impl ClusterRuntime {
    pub async fn new(
        ip: IpAddr,
        total_number: usize,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let configs = vec![PmmcNodeConfig::default(); total_number];
        Self::new_with_configs(ip, configs, observer).await
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
        let topology = PeerTopology::from(&configuration);
        let fabric = Arc::new(NetworkFabric::new(Arc::clone(&observer)));

        let runtime_registry = RuntimeRegistry::init(
            node_configs,
            quorum,
            Arc::clone(&fabric),
            Arc::clone(&persistence),
            Arc::clone(&observer),
            topology,
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

    pub async fn enable_failures(&self) {
        self.fabric.set_enabled(true).await;
    }

    pub async fn disable_failures(&self) {
        self.fabric.set_enabled(false).await;
    }

    pub async fn partition(&self, node1: usize, node2: usize) {
        if let (Some(node1), Some(node2)) = (
            self.configuration.member(node1),
            self.configuration.member(node2),
        ) {
            self.fabric
                .set_failure(node1, node2, NetworkFailure::Partition)
                .await;
            self.fabric
                .set_failure(node2, node1, NetworkFailure::Partition)
                .await;
        }
    }

    pub async fn heal_partition(&self, node1: usize, node2: usize) {
        if let (Some(node1), Some(node2)) = (
            self.configuration.member(node1),
            self.configuration.member(node2),
        ) {
            self.fabric.clear_failure(node1, node2).await;
            self.fabric.clear_failure(node2, node1).await;
        }
    }

    pub async fn propose_from(&self, node: usize, cmd: PaxosCommand) {
        if let Some(uuid) = self.configuration.member(node) {
            self.runtime_registry.propose_from(uuid, cmd).await;
        }
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
            .position(|uuid| *uuid == leader.uuid)
    }

    pub async fn crash_node(&self, node: usize) -> Option<Uuid> {
        let crashed_uuid = self.configuration.member(node)?;
        if !self.runtime_registry.crash_node(crashed_uuid).await {
            return None;
        }
        self.observer.on_event(Event::NodeCrashed {
            id: crashed_uuid,
            created_at: current_timestamp_millis(),
        });

        Some(crashed_uuid)
    }

    pub async fn isolate_node(&self, node: usize) -> Option<Uuid> {
        let isolated_uuid = self.configuration.member(node)?;
        if !self.runtime_registry.isolate_node(isolated_uuid).await {
            return None;
        }
        Some(isolated_uuid)
    }

    pub async fn heal_node(&self, node: usize) -> Option<Uuid> {
        let to_heal = self.configuration.member(node)?;
        if !self.runtime_registry.heal_node(to_heal).await {
            return None;
        }
        Some(to_heal)
    }

    pub fn node_uuid(ip: IpAddr, node_id: usize) -> Uuid {
        let namespace = Uuid::NAMESPACE_DNS;
        let name = format!("{}:pmmc:{}", ip, node_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }
}

#[cfg(test)]
#[path = "cluster_runtime_tests.rs"]
mod tests;

use rand::Rng;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::{
    cluster::{
        cluster_configuration::ClusterConfiguration,
        network_fabric::NetworkFabric,
        network_handle::{NetworkFailure, NetworkHandle},
    },
    common::persistence::{ClusterPersistence, Persistence},
    common::types::DecreeId,
    message::Message,
    monitor::PaxosObserver,
    node::{
        config::{ClassicNodeConfig, LearningStrategy},
        paxos_node::PaxosNode,
    },
    paxos_command::PaxosCommand,
};

pub trait NodeRef {
    fn resolve_uuid(self, cluster: &ClassicCluster) -> Option<Uuid>;
}

impl NodeRef for Uuid {
    fn resolve_uuid(self, _cluster: &ClassicCluster) -> Option<Uuid> {
        Some(self)
    }
}

impl NodeRef for usize {
    fn resolve_uuid(self, cluster: &ClassicCluster) -> Option<Uuid> {
        cluster.nodes.get(self).map(|n| n.uuid)
    }
}

pub struct ClassicCluster {
    _id: usize,
    total_number: usize,
    pub nodes: Vec<PaxosNode>,
    _observer: Arc<dyn PaxosObserver>,
    simulators: HashMap<Uuid, Arc<NetworkHandle>>,
    node_indices: HashMap<Uuid, usize>,
    quorum_size: usize,
    persistence: Arc<ClusterPersistence>,
    cleanup_on_drop: bool,
}

impl ClassicCluster {
    pub async fn new(
        id: usize,
        ip: IpAddr,
        total_number: usize,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        // Default: all nodes have all roles
        let configs = vec![ClassicNodeConfig::default(); total_number];
        Self::new_with_configs(id, ip, configs, observer).await
    }

    pub async fn new_with_configs(
        id: usize,
        ip: IpAddr,
        configs: Vec<ClassicNodeConfig>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let configuration = ClusterConfiguration::bootstrap_classic(ip, configs)?;
        Self::new_with_configuration(id, ip, configuration, observer).await
    }

    pub async fn new_with_configuration(
        id: usize,
        ip: IpAddr,
        configuration: ClusterConfiguration,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let total_number = configuration.len();
        let node_uuids = configuration.member_uuids();
        let persistence = Arc::new(Persistence::cluster(ip));

        use crate::node::peer_topology::PeerTopology;
        let topology = PeerTopology::from(&configuration);
        let quorum = configuration.quorum();
        let configs: Vec<ClassicNodeConfig> = configuration
            .members()
            .into_iter()
            .map(|(_, roles)| ClassicNodeConfig {
                roles,
                learning_strategy: LearningStrategy::default(),
            })
            .collect();

        let mut peers = Vec::<Sender<Message>>::with_capacity(total_number);
        let mut receivers = Vec::<Receiver<Message>>::with_capacity(total_number);
        for _ in 0..total_number {
            let (tx, rx) = mpsc::channel(1024);
            peers.push(tx);
            receivers.push(rx);
        }

        let fabric = Arc::new(NetworkFabric::new(Arc::clone(&observer)));
        for (idx, uuid) in node_uuids.iter().enumerate() {
            fabric.register(*uuid, peers[idx].clone()).await;
        }

        let mut nodes = Vec::new();
        let mut simulators = HashMap::new();
        let mut node_indices = HashMap::new();

        for (i, (rx, config)) in receivers.into_iter().zip(configs.into_iter()).enumerate() {
            let simulator = Arc::new(NetworkHandle::from_fabric(
                node_uuids[i],
                Arc::clone(&fabric),
            ));
            simulators.insert(node_uuids[i], Arc::clone(&simulator));
            node_indices.insert(node_uuids[i], i);

            let node_uuid = node_uuids[i];

            let node = PaxosNode::new(
                node_uuid,
                rx,
                Arc::clone(&observer),
                simulator,
                persistence.node(node_uuid),
                quorum,
                config,
                topology.clone(),
            )
            .await?;
            nodes.push(node);
        }

        Ok(Self {
            _id: id,
            total_number,
            nodes,
            _observer: Arc::clone(&observer),
            simulators,
            node_indices,
            quorum_size: quorum,
            persistence,
            cleanup_on_drop: false,
        })
    }

    pub fn num_nodes(&self) -> usize {
        return self.total_number;
    }

    pub fn quorum_size(&self) -> usize {
        return self.quorum_size;
    }

    pub fn get_simulator<N: NodeRef>(&self, node: N) -> Option<&Arc<NetworkHandle>> {
        let node_uuid = node.resolve_uuid(self)?;
        self.simulators.get(&node_uuid)
    }

    pub fn get_node_uuids(&self) -> Vec<Uuid> {
        self.nodes.iter().map(|node| node.uuid).collect()
    }

    pub fn uuid(&self, index: usize) -> Uuid {
        self.nodes[index].uuid
    }

    pub async fn propose(&mut self, cmd: PaxosCommand) {
        let node_id = random_node_idx(self.total_number);
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.propose(cmd, None).await;
        }
    }

    pub async fn propose_from<N: NodeRef>(&mut self, node: N, cmd: PaxosCommand) {
        let Some(node_uuid) = node.resolve_uuid(self) else {
            return;
        };
        self.propose_from_with_decree_num(node_uuid, None, cmd)
            .await;
    }

    pub async fn propose_from_with_decree_num<N: NodeRef>(
        &mut self,
        node: N,
        decree_num: Option<DecreeId>,
        cmd: PaxosCommand,
    ) {
        let Some(node_uuid) = node.resolve_uuid(self) else {
            return;
        };
        if let Some(node_idx) = self.node_indices.get(&node_uuid).copied() {
            let node = &mut self.nodes[node_idx];
            node.propose(cmd, decree_num).await;
        }
    }

    pub async fn enable_failures(&self) {
        for simulator in self.simulators.values() {
            simulator.set_enabled(true).await;
        }
    }

    pub async fn disable_failures(&self) {
        for simulator in self.simulators.values() {
            simulator.set_enabled(false).await;
        }
    }

    pub async fn partition<N1: NodeRef, N2: NodeRef>(&self, node1: N1, node2: N2) {
        let Some(node1) = node1.resolve_uuid(self) else {
            return;
        };
        let Some(node2) = node2.resolve_uuid(self) else {
            return;
        };
        if let (Some(sim1), Some(sim2)) = (self.simulators.get(&node1), self.simulators.get(&node2))
        {
            let failure = NetworkFailure::Partition;
            sim2.set_failure(node1, failure.clone()).await;

            let failure = NetworkFailure::Partition;
            sim1.set_failure(node2, failure).await;
        }
    }

    pub async fn heal_partition<N1: NodeRef, N2: NodeRef>(&self, node1: N1, node2: N2) {
        let Some(node1) = node1.resolve_uuid(self) else {
            return;
        };
        let Some(node2) = node2.resolve_uuid(self) else {
            return;
        };
        if let (Some(sim1), Some(sim2)) = (self.simulators.get(&node1), self.simulators.get(&node2))
        {
            sim1.clear_failure(node2).await;
            sim2.clear_failure(node1).await;
        }
    }

    pub async fn add_delay<N1: NodeRef, N2: NodeRef>(
        &self,
        from: N1,
        to: N2,
        delay: std::time::Duration,
    ) {
        let Some(from) = from.resolve_uuid(self) else {
            return;
        };
        let Some(to) = to.resolve_uuid(self) else {
            return;
        };
        if let Some(sim) = self.simulators.get(&from) {
            sim.set_failure(to, NetworkFailure::Delay(delay)).await;
        }
    }

    pub async fn add_packet_loss<N1: NodeRef, N2: NodeRef>(
        &self,
        from: N1,
        to: N2,
        drop_rate: f32,
    ) {
        let Some(from) = from.resolve_uuid(self) else {
            return;
        };
        let Some(to) = to.resolve_uuid(self) else {
            return;
        };
        if let Some(sim) = self.simulators.get(&from) {
            sim.set_failure(to, NetworkFailure::PacketLoss { drop_rate })
                .await;
        }
    }

    pub fn node_uuid(ip: IpAddr, node_id: usize) -> Uuid {
        let namespace = Uuid::NAMESPACE_DNS;
        let name = format!("{}:{}", ip, node_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }

    pub fn set_cleanup_on_drop(&mut self, enabled: bool) {
        self.cleanup_on_drop = enabled;
    }

    pub async fn cleanup(&self) -> anyhow::Result<()> {
        self.persistence.purge_cluster_dir().await?;
        Ok(())
    }
}

impl Drop for ClassicCluster {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }

        if let Err(err) = self.persistence.purge_cluster_dir_blocking() {
            tracing::warn!("failed to purge Classic cluster persistence on drop: {}", err);
        }
    }
}
fn random_node_idx(n: usize) -> usize {
    let mut rng = rand::rng();
    rng.random_range(0..n) as usize // inclusive range
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, sync::Arc};

    use crate::{
        cluster::cluster_configuration::ClusterConfiguration,
        monitor::NoOpObserver,
        node::config::{ClassicNodeConfig, Roles},
    };

    use super::ClassicCluster;

    #[tokio::test]
    async fn new_with_configuration_builds_classic_cluster() {
        let ip = IpAddr::V4([127, 0, 0, 11].into());
        let configs = vec![
            ClassicNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: true,
                    learner: true,
                },
                learning_strategy: Default::default(),
            },
            ClassicNodeConfig {
                roles: Roles {
                    proposer: false,
                    acceptor: true,
                    learner: true,
                },
                learning_strategy: Default::default(),
            },
            ClassicNodeConfig {
                roles: Roles {
                    proposer: true,
                    acceptor: true,
                    learner: false,
                },
                learning_strategy: Default::default(),
            },
        ];
        let configuration =
            ClusterConfiguration::bootstrap_classic(ip, configs).expect("config should build");

        let cluster = ClassicCluster::new_with_configuration(0, ip, configuration, Arc::new(NoOpObserver))
            .await
            .expect("classic cluster should build from configuration");

        assert_eq!(cluster.num_nodes(), 3);
        assert_eq!(cluster.quorum_size(), 2);
        assert_eq!(cluster.get_node_uuids().len(), 3);
    }
}

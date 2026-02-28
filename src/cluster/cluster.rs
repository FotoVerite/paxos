use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::{
    cluster::{
        network_fabric::NetworkFabric,
        network_simulator::{NetworkFailure, NetworkSimulator},
    },
    common::persistence::Persistence,
    common::types::DecreeId,
    message::Message,
    monitor::PaxosObserver,
    node::{config::ClassicNodeConfig, paxos_node::PaxosNode},
    paxos_command::PaxosCommand,
};

pub trait NodeRef {
    fn resolve_uuid(self, cluster: &Cluster) -> Option<Uuid>;
}

impl NodeRef for Uuid {
    fn resolve_uuid(self, _cluster: &Cluster) -> Option<Uuid> {
        Some(self)
    }
}

impl NodeRef for usize {
    fn resolve_uuid(self, cluster: &Cluster) -> Option<Uuid> {
        cluster.nodes.get(self).map(|n| n.uuid)
    }
}

pub struct Cluster {
    _id: usize,
    total_number: usize,
    pub nodes: Vec<PaxosNode>,
    _observer: Arc<dyn PaxosObserver>,
    simulators: HashMap<Uuid, Arc<NetworkSimulator>>,
    node_indices: HashMap<Uuid, usize>,
    quorum_size: usize,
}

impl Cluster {
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
        let total_number = configs.len();

        let node_uuids: Vec<Uuid> = (0..total_number).map(|i| Self::node_uuid(ip, i)).collect();
        let persistence = Persistence::cluster(ip);

        let proposer_ids: Vec<Uuid> = configs
            .iter()
            .enumerate()
            .filter(|(_, c)| c.roles.proposer)
            .map(|(i, _)| node_uuids[i])
            .collect();
        // Build role-aware peer lists in transport IDs (Uuid).
        let acceptor_ids: Vec<Uuid> = configs
            .iter()
            .enumerate()
            .filter(|(_, c)| c.roles.acceptor)
            .map(|(i, _)| node_uuids[i])
            .collect();

        let learner_ids: Vec<Uuid> = configs
            .iter()
            .enumerate()
            .filter(|(_, c)| c.roles.learner)
            .map(|(i, _)| node_uuids[i])
            .collect();

        use crate::node::peer_topology::PeerTopology;
        let topology = PeerTopology::new(
            acceptor_ids.clone(),
            learner_ids.clone(),
            proposer_ids.clone(),
        );

        // Calculate quorum based on acceptor count
        let quorum = acceptor_ids.len() / 2 + 1;

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
            let simulator = Arc::new(NetworkSimulator::from_fabric(
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
        })
    }

    pub fn num_nodes(&self) -> usize {
        return self.total_number;
    }

    pub fn quorum_size(&self) -> usize {
        return self.quorum_size;
    }

    pub fn get_simulator<N: NodeRef>(&self, node: N) -> Option<&Arc<NetworkSimulator>> {
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
            let mut partition_set = HashSet::new();
            partition_set.insert(node1);
            let failure = NetworkFailure::Partition {
                nodes: partition_set,
            };
            sim2.set_failure(node1, failure.clone()).await;

            let mut partition_set = HashSet::new();
            partition_set.insert(node2);
            let failure = NetworkFailure::Partition {
                nodes: partition_set,
            };
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
}
fn random_node_idx(n: usize) -> usize {
    let mut rng = rand::rng();
    rng.random_range(0..n) as usize // inclusive range
}

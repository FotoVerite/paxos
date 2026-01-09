use rand::Rng;
use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;
use std::net::IpAddr;

use std::sync::Arc;
use std::collections::HashSet;

use crate::{
    cluster::network_simulator::{NetworkFailure, NetworkSimulator},
    common::types::{DecreeId, NodeId}, message::Message, monitor::PaxosObserver, node::paxos_node::PaxosNode, paxos_command::PaxosCommand
};

pub struct Cluster {
    _id: NodeId,
    total_number: usize,
    pub nodes: Vec<PaxosNode>,
    _observer: Arc<dyn PaxosObserver>,
    simulators: Vec<Arc<NetworkSimulator>>,
}

impl Cluster {
    pub async fn new(id: usize, ip: IpAddr, total_number: usize, observer: Arc<dyn PaxosObserver>) -> anyhow::Result<Self> {
        let mut peers = Vec::<Sender<Message>>::with_capacity(total_number);
        let mut receivers = Vec::<Receiver<Message>>::with_capacity(total_number);
        for _ in 0..total_number {
            let (tx, rx) = mpsc::channel(1024);
            peers.push(tx);
            receivers.push(rx);
        }

        let mut nodes = Vec::new();
        let mut simulators = Vec::new();

        for (i, rx) in receivers.into_iter().enumerate() {
            let simulator = Arc::new(NetworkSimulator::new(NodeId(i), peers.clone()));
            simulators.push(Arc::clone(&simulator));

            // Generate deterministic UUID for this node
            let node_uuid = Self::node_uuid(ip, i);

            let node = PaxosNode::new(
                NodeId(i),
                node_uuid,
                rx,
                Arc::clone(&observer),
                simulator,
                total_number / 2 + 1
            ).await?;
            nodes.push(node);
        }

        Ok(Self {
            _id: NodeId(id),
            total_number,
            nodes,
            _observer: Arc::clone(&observer),
            simulators,
        })
    }

    pub fn num_nodes(&self) -> usize {
        return self.total_number;
    }

    pub fn quorum_size(&self) -> usize {
        return self.total_number / 2 + 1;
    }

    pub fn get_simulator(&self, node_id: NodeId) -> Option<&Arc<NetworkSimulator>> {
        self.simulators.get(usize::from(node_id))
    }

    pub fn get_node_uuids(&self) -> Vec<Uuid> {
        self.nodes.iter().map(|node| node.uuid).collect()
    }

    pub async fn propose(&mut self, cmd: PaxosCommand) {
        let node_id = random_node_idx(self.total_number);
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.propose(cmd, None).await;
        }
    }

    pub async fn propose_from(&mut self, node_id: NodeId, cmd: PaxosCommand) {
        self.propose_from_with_decree_num(node_id, None, cmd).await;
    }

    pub async fn propose_from_with_decree_num(&mut self, node_id: NodeId, decree_num: Option<DecreeId>, cmd: PaxosCommand) {
        if let Some(node) = self.nodes.get_mut(usize::from(node_id)) {
            node.propose(cmd, decree_num).await;
        }
    }

    pub async fn enable_failures(&self) {
        for simulator in &self.simulators {
            simulator.set_enabled(true).await;
        }
    }

    pub async fn disable_failures(&self) {
        for simulator in &self.simulators {
            simulator.set_enabled(false).await;
        }
    }

    pub async fn partition(&self, node1: NodeId, node2: NodeId) {
        let n1: usize = node1.into();
        let n2: usize = node2.into();
        if n1 < self.total_number && n2 < self.total_number {
            let mut partition_set = HashSet::new();
            partition_set.insert(node1);
            let failure = NetworkFailure::Partition { nodes: partition_set };
            self.simulators[n2].set_failure(node1, failure.clone()).await;

            let mut partition_set = HashSet::new();
            partition_set.insert(node2);
            let failure = NetworkFailure::Partition { nodes: partition_set };
            self.simulators[n1].set_failure(node2, failure).await;
        }
    }

    pub async fn heal_partition(&self, node1: NodeId, node2: NodeId) {
         let n1: usize = node1.into();
         let n2: usize = node2.into();
        if n1 < self.total_number && n2 < self.total_number {
            self.simulators[n1].clear_failure(node2).await;
            self.simulators[n2].clear_failure(node1).await;
        }
    }

    pub async fn add_delay(&self, from: NodeId, to: NodeId, delay: std::time::Duration) {
        let f: usize = from.into();
        let t: usize = to.into();

        if f < self.total_number && t < self.total_number {
            self.simulators[f].set_failure(to, NetworkFailure::Delay(delay)).await;
        }
    }

    pub async fn add_packet_loss(&self, from: NodeId, to: NodeId, drop_rate: f32) {
         let f: usize = from.into();
         let t: usize = to.into();
        if f < self.total_number && t < self.total_number {
            self.simulators[f].set_failure(to, NetworkFailure::PacketLoss { drop_rate }).await;
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

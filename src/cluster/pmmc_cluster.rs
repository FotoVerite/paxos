use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver, Sender};
use uuid::Uuid;

use crate::{
    cluster::network_simulator::{NetworkFailure, NetworkSimulator},
    message::{ClientMessage, Message},
    monitor::{Event, PaxosObserver, current_timestamp_millis},
    node::{config::PmmcNodeConfig, pmmc::pmmc_node::PmmcNode},
    paxos_command::PaxosCommand,
};

pub struct PmmcCluster {
    _id: usize,
    total_number: usize,
    quorum_size: usize,
    pub nodes: Vec<PmmcNode>,
    observer: Arc<dyn PaxosObserver>,
    simulators: HashMap<Uuid, Arc<NetworkSimulator>>,
}

impl PmmcCluster {
    pub async fn new(
        id: usize,
        ip: IpAddr,
        total_number: usize,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let configs = vec![PmmcNodeConfig::default(); total_number];
        Self::new_with_configs(id, ip, configs, observer).await
    }

    pub async fn new_with_configs(
        id: usize,
        ip: IpAddr,
        configs: Vec<PmmcNodeConfig>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let total_number = configs.len();
        let node_uuids: Vec<Uuid> = (0..total_number).map(|i| Self::node_uuid(ip, i)).collect();

        let proposer_ids: Vec<Uuid> = configs
            .iter()
            .enumerate()
            .filter(|(_, c)| c.roles.proposer)
            .map(|(i, _)| node_uuids[i])
            .collect();
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

        let topology = crate::node::peer_topology::PeerTopology::new(
            acceptor_ids.clone(),
            learner_ids.clone(),
            proposer_ids.clone(),
        );
        let quorum = acceptor_ids.len() / 2 + 1;

        let mut peers = Vec::<Sender<Message>>::with_capacity(total_number);
        let mut receivers = Vec::<Receiver<Message>>::with_capacity(total_number);
        for _ in 0..total_number {
            let (tx, rx) = mpsc::channel(1024);
            peers.push(tx);
            receivers.push(rx);
        }

        let mut nodes = Vec::new();
        let mut simulators = HashMap::new();
        for (i, (rx, config)) in receivers.into_iter().zip(configs.into_iter()).enumerate() {
            let peers_map: HashMap<Uuid, Sender<Message>> = node_uuids
                .iter()
                .enumerate()
                .map(|(idx, uuid)| (*uuid, peers[idx].clone()))
                .collect();

            let simulator = Arc::new(NetworkSimulator::new(
                node_uuids[i],
                peers_map,
                Arc::clone(&observer),
            ));
            simulators.insert(node_uuids[i], Arc::clone(&simulator));

            let node = PmmcNode::new(
                node_uuids[i],
                rx,
                Arc::clone(&observer),
                simulator,
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
            quorum_size: quorum,
            nodes,
            observer,
            simulators,
        })
    }

    pub fn num_nodes(&self) -> usize {
        self.total_number
    }

    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    pub fn get_node_uuids(&self) -> Vec<Uuid> {
        self.nodes.iter().map(|node| node.uuid).collect()
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

    pub async fn partition(&self, node1: usize, node2: usize) {
        let node1 = self.nodes[node1].uuid;
        let node2 = self.nodes[node2].uuid;
        if let (Some(sim1), Some(sim2)) = (self.simulators.get(&node1), self.simulators.get(&node2))
        {
            let mut partition_set = HashSet::new();
            partition_set.insert(node1);
            sim2.set_failure(node1, NetworkFailure::Partition { nodes: partition_set })
                .await;

            let mut partition_set = HashSet::new();
            partition_set.insert(node2);
            sim1.set_failure(node2, NetworkFailure::Partition { nodes: partition_set })
                .await;
        }
    }

    pub async fn heal_partition(&self, node1: usize, node2: usize) {
        let node1 = self.nodes[node1].uuid;
        let node2 = self.nodes[node2].uuid;
        if let (Some(sim1), Some(sim2)) = (self.simulators.get(&node1), self.simulators.get(&node2))
        {
            sim1.clear_failure(node2).await;
            sim2.clear_failure(node1).await;
        }
    }

    pub async fn propose_from(&self, node: usize, cmd: PaxosCommand) {
        if let Some(n) = self.nodes.get(node) {
            n.propose(cmd).await;
        }
    }

    pub async fn connect_client_to(
        &self,
        node: usize,
        client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        let n = self.nodes.get(node)?;
        n.connect_client(client_id).await
    }

    pub async fn leader_index(&self) -> Option<usize> {
        for (idx, node) in self.nodes.iter().enumerate() {
            if node.is_leader().await {
                return Some(idx);
            }
        }
        None
    }

    pub async fn crash_node(&self, node: usize) -> Option<Uuid> {
        let crashed_uuid = self.isolate_node(node).await?;

        self.observer.on_event(Event::NodeCrashed {
            id: crashed_uuid,
            created_at: current_timestamp_millis(),
        });

        Some(crashed_uuid)
    }

    pub async fn isolate_node(&self, node: usize) -> Option<Uuid> {
        let isolated_uuid = self.nodes.get(node)?.uuid;

        for peer in &self.nodes {
            if peer.uuid == isolated_uuid {
                continue;
            }
            if let Some(isolated_sim) = self.simulators.get(&isolated_uuid) {
                let mut partition_set = HashSet::new();
                partition_set.insert(peer.uuid);
                isolated_sim
                    .set_failure(peer.uuid, NetworkFailure::Partition { nodes: partition_set })
                    .await;
            }
            if let Some(peer_sim) = self.simulators.get(&peer.uuid) {
                let mut partition_set = HashSet::new();
                partition_set.insert(isolated_uuid);
                peer_sim
                    .set_failure(
                        isolated_uuid,
                        NetworkFailure::Partition { nodes: partition_set },
                    )
                    .await;
            }
        }

        Some(isolated_uuid)
    }

    pub async fn heal_node(&self, node: usize) -> Option<Uuid> {
        let healed_uuid = self.nodes.get(node)?.uuid;

        if let Some(healed_sim) = self.simulators.get(&healed_uuid) {
            for peer in &self.nodes {
                if peer.uuid == healed_uuid {
                    continue;
                }
                healed_sim.clear_failure(peer.uuid).await;
            }
        }

        for peer in &self.nodes {
            if peer.uuid == healed_uuid {
                continue;
            }
            if let Some(peer_sim) = self.simulators.get(&peer.uuid) {
                peer_sim.clear_failure(healed_uuid).await;
            }
        }

        Some(healed_uuid)
    }

    pub fn node_uuid(ip: IpAddr, node_id: usize) -> Uuid {
        let namespace = Uuid::NAMESPACE_DNS;
        let name = format!("{}:pmmc:{}", ip, node_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }
}

#[cfg(test)]
#[path = "pmmc_cluster_tests.rs"]
mod tests;

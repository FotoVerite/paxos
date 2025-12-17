use rand::Rng;
use tokio::sync::mpsc::{self, Receiver, Sender};

use std::sync::Arc;

use crate::{
    cluster::peer_sender::PeerSender, message::Message, monitor::PaxosObserver,
    node::{paxos_node::PaxosNode}, paxos_command::PaxosCommand,
};

pub struct Cluster {
    id: usize,
    total_number: usize,
    nodes: Vec<PaxosNode>,
    observer: Arc<dyn PaxosObserver>,
}

impl Cluster {
    pub async fn new(id: usize, total_number: usize, observer: Arc<dyn PaxosObserver>) -> anyhow::Result<Self> {
        let mut peers = Vec::<Sender<Message>>::with_capacity(total_number);
        let mut receivers = Vec::<Receiver<Message>>::with_capacity(total_number);
        for _ in 0..total_number {
            let (tx, rx) = mpsc::channel(1024);
            peers.push(tx);
            receivers.push(rx);
        }
        
        let mut nodes = Vec::new();
        for (i, rx) in receivers.into_iter().enumerate() {
            let node = PaxosNode::new(
                i,
                rx,
                Arc::clone(&observer),
                PeerSender::new(i, peers.clone()),   
                total_number / 2 + 1
            ).await?;
            nodes.push(node);
        }

        Ok(Self {
            id,
            total_number,
            nodes,
            observer: Arc::clone(&observer),
        })
    }

    pub fn num_nodes(&self) -> usize {
        return self.total_number;
    }

    pub fn quorum_size(&self) -> usize {
        return self.total_number / 2 + 1;
    }

    pub async fn propose(&mut self, cmd: PaxosCommand) {
        let node_id = random_node_idx(self.total_number);
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.propose(cmd).await;
        }
    }


}
fn random_node_idx(n: usize) -> usize {
    let mut rng = rand::rng();
    rng.random_range(0..n) as usize // inclusive range
}

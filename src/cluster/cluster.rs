use rand::Rng;
use tokio::sync::mpsc::{self, Receiver, Sender};

use std::{process::Command, sync::Arc};

use crate::{
    cluster::{ledger::Ledger, peer_sender::PeerSender}, message::Message, monitor::PaxosObserver,
    node::paxos_node::PaxosNode, paxos_command::PaxosCommand,
};

pub struct Cluster {
    id: usize,
    total_number: usize,
    nodes: Vec<PaxosNode>,
    ledger: Ledger,
    observer: Arc<dyn PaxosObserver>,
}

impl Cluster {
    pub fn new(id: usize, total_number: usize, observer: Arc<dyn PaxosObserver>) -> Self {
        let mut peers = Vec::<Sender<Message>>::with_capacity(total_number);
        let mut receivers = Vec::<Receiver<Message>>::with_capacity(total_number);
        for _ in 0..total_number {
            let (tx, rx) = mpsc::channel(1024);
            peers.push(tx);
            receivers.push(rx);
        }
        let (ledger, ledger_tx) = Ledger::init();
        let nodes = receivers
            .into_iter()
            .enumerate()
            .map(|(i, rx)| {
                PaxosNode::new(
                    i + 1,
                    rx,
                    Arc::clone(&observer),
                    PeerSender::new(i, peers.clone()),   
                )
            })
            .collect();

        Self {
            id,
            total_number,
            nodes,
            ledger,
            observer: Arc::clone(&observer),
        }
    }

    pub fn num_nodes(&self) -> usize {
        return self.total_number;
    }

    pub fn quorum_size(&self) -> usize {
        return self.total_number / 2 + 1;
    }

    pub async fn propose(&mut self, value: Command) {
        let node_id = random_node_idx(self.total_number);
        if let Some(node) = self.nodes.get_mut(node_id) {
            node.propose(Command).await;
        }
    }


}
fn random_node_idx(n: usize) -> usize {
    let mut rng = rand::rng();
    rng.random_range(0..n) as usize // inclusive range
}

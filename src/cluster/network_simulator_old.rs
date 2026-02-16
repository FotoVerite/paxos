use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::monitor::PaxosObserver;
use crate::{common::types::NodeId, message::Message};

#[derive(Debug, Clone)]
pub enum NetworkFailure {
    None,
    Delay(Duration),
    PacketLoss { drop_rate: f32 }, // 0.0 to 1.0
    Partition { nodes: HashSet<NodeId> },
}

pub struct NetworkSimulator {
    _me: NodeId,
    observer: Arc<dyn PaxosObserver>,
    peers: Vec<mpsc::Sender<Message>>,
    enabled: Arc<Mutex<bool>>,
    failures: Arc<Mutex<HashMap<NodeId, NetworkFailure>>>,
}

impl NetworkSimulator {
    pub fn new(
        me: NodeId,
        peers: Vec<mpsc::Sender<Message>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            _me: me,
            peers,
            observer,
            enabled: Arc::new(Mutex::new(false)),
            failures: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn set_enabled(&self, enabled: bool) {
        *self.enabled.lock().await = enabled;
    }

    pub async fn set_failure(&self, target: NodeId, failure: NetworkFailure) {
        self.failures.lock().await.insert(target, failure);
    }

    pub async fn clear_failure(&self, target: NodeId) {
        self.failures.lock().await.remove(&target);
    }

    pub async fn clear_all_failures(&self) {
        self.failures.lock().await.clear();
    }

    async fn should_fail(&self, target: NodeId) -> bool {
        let failures = self.failures.lock().await;

        match failures.get(&target) {
            Some(NetworkFailure::None) => false,
            Some(NetworkFailure::Partition { nodes }) => nodes.contains(&target),
            Some(NetworkFailure::PacketLoss { drop_rate }) => {
                let mut rng = rand::rng();
                use rand::Rng;
                rng.random::<f32>() < *drop_rate
            }
            Some(NetworkFailure::Delay(_)) => false, // Handled separately
            None => false,
        }
    }

    async fn get_delay(&self, target: NodeId) -> Option<Duration> {
        let failures = self.failures.lock().await;

        match failures.get(&target) {
            Some(NetworkFailure::Delay(duration)) => Some(*duration),
            _ => None,
        }
    }

    pub async fn send(&self, to: NodeId, msg: Message) {
        let enabled = *self.enabled.lock().await;
        let to_idx: usize = to.into();

        if !enabled {
            if to_idx < self.peers.len() {
                let _ = self.peers[to_idx].send(msg).await;
            }
            return;
        }

        if self.should_fail(to).await {
            return;
        }

        if let Some(delay) = self.get_delay(to).await {
            sleep(delay).await;
        }

        if to_idx < self.peers.len() {
            let _ = self.peers[to_idx].send(msg).await;
        }
    }

    pub async fn broadcast(&self, msg: Message)
    where
        Message: Clone,
    {
        let indexes: Vec<usize> = (0..self.peers.iter().len()).collect();
        for idx in indexes.iter() {
            self.send(NodeId(*idx), msg.clone()).await;
        }
        self.observer.on_message(&indexes, msg.clone());
    }

    pub async fn broadcast_to(&self, msg: &Message, peers: &HashSet<NodeId>)
    where
        Message: Clone,
    {

        for idx in peers.into_iter() {
            self.send(*idx, msg.clone()).await;
        }
    }

    pub async fn broadcast_to_uuids(&self, msg: &Message, peers: &HashSet<Uuid>)
    where
        Message: Clone,
    {

        for idx in peers.into_iter() {
            self.send(*idx, msg.clone()).await;
        }
    }
}

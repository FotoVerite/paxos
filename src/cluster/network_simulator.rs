use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::message::Message;
use crate::monitor::PaxosObserver;

#[derive(Debug, Clone)]
pub enum NetworkFailure {
    None,
    Delay(Duration),
    PacketLoss { drop_rate: f32 }, // 0.0 to 1.0
    Partition { nodes: HashSet<Uuid> },
}

pub struct NetworkSimulator {
    _me: Uuid,
    observer: Arc<dyn PaxosObserver>,
    peers: HashMap<Uuid, mpsc::Sender<Message>>,
    enabled: Arc<Mutex<bool>>,
    failures: Arc<Mutex<HashMap<Uuid, NetworkFailure>>>,
}

impl NetworkSimulator {
    pub fn new(
        me: Uuid,
        peers: HashMap<Uuid, mpsc::Sender<Message>>,
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

    pub async fn set_failure(&self, target: Uuid, failure: NetworkFailure) {
        self.failures.lock().await.insert(target, failure);
    }

    pub async fn clear_failure(&self, target: Uuid) {
        self.failures.lock().await.remove(&target);
    }

    pub async fn clear_all_failures(&self) {
        self.failures.lock().await.clear();
    }

    async fn should_fail(&self, target: Uuid) -> bool {
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

    async fn get_delay(&self, target: Uuid) -> Option<Duration> {
        let failures = self.failures.lock().await;

        match failures.get(&target) {
            Some(NetworkFailure::Delay(duration)) => Some(*duration),
            _ => None,
        }
    }

    pub async fn send(&self, to: Uuid, msg: Message) {
        let enabled = *self.enabled.lock().await;

        if !enabled {
            if let Some(peer) = self.peers.get(&to) {
                let _ = peer.send(msg).await;
            }

            return;
        }

        if self.should_fail(to).await {
            return;
        }

        if let Some(delay) = self.get_delay(to).await {
            sleep(delay).await;
        }

        if let Some(peer) = self.peers.get(&to) {
            let _ = peer.send(msg).await;
        }
    }

    pub async fn broadcast(&self, msg: Message)
    where
        Message: Clone,
    {
        let peers: Vec<Uuid> = self.peers.keys().cloned().collect();
        for to in peers.iter() {
            self.send(*to, msg.clone()).await;
        }
        self.observer.on_message(&peers, msg.clone());
    }

    pub async fn broadcast_to(&self, msg: &Message, peers: &HashSet<Uuid>)
    where
        Message: Clone,
    {
        let peers: Vec<Uuid> = peers.into_iter().cloned().collect();

        for &idx in peers.iter() {
            self.send(idx, msg.clone()).await;
        }
        self.observer.on_message(&peers, msg.clone());
    }
}

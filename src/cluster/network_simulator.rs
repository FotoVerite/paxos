use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::message::Message;

#[derive(Debug, Clone)]
pub enum NetworkFailure {
    None,
    Delay(Duration),
    PacketLoss { drop_rate: f32 }, // 0.0 to 1.0
    Partition { nodes: HashSet<usize> },
}

pub struct NetworkSimulator {
    me: usize,
    peers: Vec<mpsc::Sender<Message>>,
    enabled: Arc<Mutex<bool>>,
    failures: Arc<Mutex<HashMap<usize, NetworkFailure>>>,
}

impl NetworkSimulator {
    pub fn new(me: usize, peers: Vec<mpsc::Sender<Message>>) -> Self {
        Self {
            me,
            peers,
            enabled: Arc::new(Mutex::new(false)),
            failures: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn set_enabled(&self, enabled: bool) {
        *self.enabled.lock().await = enabled;
    }

    pub async fn set_failure(&self, target: usize, failure: NetworkFailure) {
        self.failures.lock().await.insert(target, failure);
    }

    pub async fn clear_failure(&self, target: usize) {
        self.failures.lock().await.remove(&target);
    }

    pub async fn clear_all_failures(&self) {
        self.failures.lock().await.clear();
    }

    async fn should_fail(&self, target: usize) -> bool {
        let failures = self.failures.lock().await;
        
        match failures.get(&target) {
            Some(NetworkFailure::None) => false,
            Some(NetworkFailure::Partition { nodes }) => nodes.contains(&self.me),
            Some(NetworkFailure::PacketLoss { drop_rate }) => {
                let mut rng = rand::rng();
                use rand::Rng;
                rng.random::<f32>() < *drop_rate
            }
            Some(NetworkFailure::Delay(_)) => false, // Handled separately
            None => false,
        }
    }

    async fn get_delay(&self, target: usize) -> Option<Duration> {
        let failures = self.failures.lock().await;
        
        match failures.get(&target) {
            Some(NetworkFailure::Delay(duration)) => Some(*duration),
            _ => None,
        }
    }

    pub async fn send(&self, to: usize, msg: Message) {
        if to == self.me {
            return;
        }

        let enabled = *self.enabled.lock().await;
        if !enabled {
            let _ = self.peers[to].send(msg).await;
            return;
        }

        if self.should_fail(to).await {
            return;
        }

        if let Some(delay) = self.get_delay(to).await {
            sleep(delay).await;
        }

        let _ = self.peers[to].send(msg).await;
    }

    pub async fn broadcast(&self, msg: Message)
    where
        Message: Clone,
    {
        for (idx, _) in self.peers.iter().enumerate() {
            if idx == self.me {
                continue;
            }
            self.send(idx, msg.clone()).await;
        }
    }

     pub async fn broadcast_to(&self, msg: &Message, peers: &HashSet<usize>)
    where
        Message: Clone,
    {
        if let Message::Accept { decree_num, .. } = msg {
            tracing::debug!("Broadcasting Accept for decree {} to quorum: {:?}", decree_num, peers);
        }
        for idx in peers.into_iter() {
            if *idx == self.me {
                continue;
            }
            self.send(*idx, msg.clone()).await;
        }
    }
}

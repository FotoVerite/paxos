use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rand::Rng;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::message::Message;
use crate::monitor::PaxosObserver;

#[derive(Debug, Clone)]
pub enum NetworkFailure {
    None,
    Delay(Duration),
    PacketLoss { drop_rate: f32 },
    Partition,
}

pub struct NetworkFabric {
    observer: Arc<dyn PaxosObserver>,
    peers: RwLock<HashMap<Uuid, mpsc::Sender<Message>>>,
    enabled: Mutex<bool>,
    failures: Mutex<HashMap<(Uuid, Uuid), NetworkFailure>>,
}

impl NetworkFabric {
    pub fn new(observer: Arc<dyn PaxosObserver>) -> Self {
        Self {
            observer,
            peers: RwLock::new(HashMap::new()),
            enabled: Mutex::new(false),
            failures: Mutex::new(HashMap::new()),
        }
    }

    pub async fn register(&self, uuid: Uuid, sender: mpsc::Sender<Message>) {
        self.peers.write().await.insert(uuid, sender);
    }

    pub async fn unregister(&self, uuid: Uuid) {
        self.peers.write().await.remove(&uuid);
        self.failures
            .lock()
            .await
            .retain(|(from, to), _| *from != uuid && *to != uuid);
    }

    pub async fn peers(&self) -> Vec<Uuid> {
        self.peers.read().await.keys().copied().collect()
    }

    pub async fn set_enabled(&self, enabled: bool) {
        *self.enabled.lock().await = enabled;
    }

    pub async fn set_failure(&self, from: Uuid, to: Uuid, failure: NetworkFailure) {
        self.failures.lock().await.insert((from, to), failure);
    }

    pub async fn clear_failure(&self, from: Uuid, to: Uuid) {
        self.failures.lock().await.remove(&(from, to));
    }

    pub async fn clear_failures_from(&self, from: Uuid) {
        self.failures
            .lock()
            .await
            .retain(|(src, _), _| *src != from);
    }

    async fn should_fail(&self, from: Uuid, to: Uuid) -> bool {
        let failures = self.failures.lock().await;
        match failures.get(&(from, to)) {
            Some(NetworkFailure::None) => false,
            Some(NetworkFailure::Partition) => true,
            Some(NetworkFailure::PacketLoss { drop_rate }) => {
                let mut rng = rand::rng();
                rng.random::<f32>() < *drop_rate
            }
            Some(NetworkFailure::Delay(_)) => false,
            None => false,
        }
    }

    async fn get_delay(&self, from: Uuid, to: Uuid) -> Option<Duration> {
        let failures = self.failures.lock().await;
        match failures.get(&(from, to)) {
            Some(NetworkFailure::Delay(duration)) => Some(*duration),
            _ => None,
        }
    }

    pub async fn send(&self, from: Uuid, to: Uuid, msg: Message) {
        let enabled = *self.enabled.lock().await;

        if enabled {
            if self.should_fail(from, to).await {
                return;
            }

            if let Some(delay) = self.get_delay(from, to).await {
                sleep(delay).await;
            }
        }

        if let Some(peer) = self.peers.read().await.get(&to).cloned() {
            let _ = peer.send(msg).await;
        }
    }

    pub async fn broadcast(&self, from: Uuid, msg: Message)
    where
        Message: Clone,
    {
        let peers = self.peers().await;
        for to in peers.iter().copied() {
            self.send(from, to, msg.clone()).await;
        }
        self.observer.on_message(&peers, msg);
    }

    pub async fn broadcast_to(&self, from: Uuid, msg: &Message, peers: &HashSet<Uuid>)
    where
        Message: Clone,
    {
        let peers: Vec<Uuid> = peers.iter().copied().collect();
        for to in peers.iter().copied() {
            self.send(from, to, msg.clone()).await;
        }
        self.observer.on_message(&peers, msg.clone());
    }
}

pub struct NetworkHandle {
    me: Uuid,
    fabric: Arc<NetworkFabric>,
}

impl NetworkHandle {
    pub async fn new(
        me: Uuid,
        peers: HashMap<Uuid, mpsc::Sender<Message>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        let fabric = Arc::new(NetworkFabric::new(observer));
        for (uuid, sender) in peers {
            fabric.register(uuid, sender).await;
        }
        Self { me, fabric }
    }

    pub fn from_fabric(me: Uuid, fabric: Arc<NetworkFabric>) -> Self {
        Self { me, fabric }
    }

    pub async fn set_enabled(&self, enabled: bool) {
        self.fabric.set_enabled(enabled).await;
    }

    pub async fn set_failure(&self, target: Uuid, failure: NetworkFailure) {
        self.fabric.set_failure(self.me, target, failure).await;
    }

    pub async fn clear_failure(&self, target: Uuid) {
        self.fabric.clear_failure(self.me, target).await;
    }

    pub async fn clear_all_failures(&self) {
        self.fabric.clear_failures_from(self.me).await;
    }

    pub async fn send(&self, to: Uuid, msg: Message) {
        self.fabric.send(self.me, to, msg).await;
    }

    pub async fn broadcast(&self, msg: Message)
    where
        Message: Clone,
    {
        self.fabric.broadcast(self.me, msg).await;
    }

    pub async fn broadcast_to(&self, msg: &Message, peers: &HashSet<Uuid>)
    where
        Message: Clone,
    {
        self.fabric.broadcast_to(self.me, msg, peers).await;
    }

    pub fn fabric(&self) -> Arc<NetworkFabric> {
        Arc::clone(&self.fabric)
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{cluster::network_fabric::NetworkFabric, message::Message, monitor::PaxosObserver};

pub use crate::cluster::network_fabric::NetworkFailure;

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

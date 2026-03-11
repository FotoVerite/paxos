use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::common::network_fabric::{NetworkFabric, NetworkFailure};

pub struct NetworkHandle<TMsg> {
    me: Uuid,
    fabric: Arc<NetworkFabric<TMsg>>,
}

impl<TMsg> NetworkHandle<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    pub async fn new(me: Uuid, peers: HashMap<Uuid, mpsc::Sender<TMsg>>) -> Self {
        let fabric = Arc::new(NetworkFabric::new());
        for (uuid, sender) in peers {
            fabric.register(uuid, sender).await;
        }
        Self { me, fabric }
    }

    pub fn from_fabric(me: Uuid, fabric: Arc<NetworkFabric<TMsg>>) -> Self {
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

    pub async fn send(&self, to: Uuid, msg: TMsg) {
        self.fabric.send(self.me, to, msg).await;
    }

    pub async fn broadcast(&self, msg: TMsg) {
        self.fabric.broadcast(self.me, msg).await;
    }

    pub async fn broadcast_to(&self, msg: TMsg, peers: &HashSet<Uuid>) {
        self.fabric.broadcast_to(self.me, msg, peers).await;
    }

    pub fn fabric(&self) -> Arc<NetworkFabric<TMsg>> {
        Arc::clone(&self.fabric)
    }
}

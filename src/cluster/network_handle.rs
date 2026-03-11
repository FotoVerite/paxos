use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::common::network_handle::NetworkHandle as GenericNetworkHandle;
use crate::message::Message;
use crate::monitor::PaxosObserver;

use crate::cluster::network_fabric::NetworkFabric;

pub use crate::common::network_fabric::NetworkFailure;

pub struct NetworkHandle {
    inner: GenericNetworkHandle<Message>,
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

        let inner = GenericNetworkHandle::from_fabric(me, fabric.inner());
        Self { inner, fabric }
    }

    pub fn from_fabric(me: Uuid, fabric: Arc<NetworkFabric>) -> Self {
        let inner = GenericNetworkHandle::from_fabric(me, fabric.inner());
        Self { inner, fabric }
    }

    pub async fn set_enabled(&self, enabled: bool) {
        self.inner.set_enabled(enabled).await;
    }

    pub async fn set_failure(&self, target: Uuid, failure: NetworkFailure) {
        self.inner.set_failure(target, failure).await;
    }

    pub async fn clear_failure(&self, target: Uuid) {
        self.inner.clear_failure(target).await;
    }

    pub async fn clear_all_failures(&self) {
        self.inner.clear_all_failures().await;
    }

    pub async fn send<M>(&self, to: Uuid, msg: M)
    where
        M: Into<Message>,
    {
        self.inner.send(to, msg.into()).await;
    }

    pub async fn broadcast<M>(&self, msg: M)
    where
        M: Into<Message>,
    {
        self.inner.broadcast(msg.into()).await;
    }

    pub async fn broadcast_to<M>(&self, msg: M, peers: &HashSet<Uuid>)
    where
        M: Into<Message>,
    {
        self.inner.broadcast_to(msg.into(), peers).await;
    }

    pub fn fabric(&self) -> Arc<NetworkFabric> {
        Arc::clone(&self.fabric)
    }
}

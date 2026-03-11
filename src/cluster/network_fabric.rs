use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::common::network_fabric::NetworkFabric as GenericNetworkFabric;
use crate::message::{Message, MessageTrace};
use crate::monitor::PaxosObserver;

pub struct NetworkFabric {
    inner: Arc<GenericNetworkFabric<Message>>,
}

impl NetworkFabric {
    pub fn new(observer: Arc<dyn PaxosObserver>) -> Self {
        let trace = Arc::new(move |targets: &[Uuid], msg: &Message| {
            observer.on_message_trace(MessageTrace::from_legacy(targets, msg));
        });

        Self {
            inner: Arc::new(GenericNetworkFabric::with_trace(trace)),
        }
    }

    pub(crate) fn inner(&self) -> Arc<GenericNetworkFabric<Message>> {
        Arc::clone(&self.inner)
    }

    pub async fn register(&self, uuid: Uuid, sender: mpsc::Sender<Message>) {
        self.inner.register(uuid, sender).await;
    }

    pub async fn unregister(&self, uuid: Uuid) {
        self.inner.unregister(uuid).await;
    }

    pub async fn peers(&self) -> Vec<Uuid> {
        self.inner.peers().await
    }

    pub async fn set_enabled(&self, enabled: bool) {
        self.inner.set_enabled(enabled).await;
    }

    pub async fn set_failure(&self, from: Uuid, to: Uuid, failure: NetworkFailure) {
        self.inner.set_failure(from, to, failure).await;
    }

    pub async fn clear_failure(&self, from: Uuid, to: Uuid) {
        self.inner.clear_failure(from, to).await;
    }

    pub async fn clear_failures_from(&self, from: Uuid) {
        self.inner.clear_failures_from(from).await;
    }

    pub async fn send<M>(&self, from: Uuid, to: Uuid, msg: M)
    where
        M: Into<Message>,
    {
        self.inner.send(from, to, msg.into()).await;
    }

    pub async fn broadcast<M>(&self, from: Uuid, msg: M)
    where
        M: Into<Message>,
    {
        self.inner.broadcast(from, msg.into()).await;
    }

    pub async fn broadcast_to<M>(&self, from: Uuid, msg: M, peers: &HashSet<Uuid>)
    where
        M: Into<Message>,
    {
        self.inner.broadcast_to(from, msg.into(), peers).await;
    }
}

pub use crate::common::network_fabric::NetworkFailure;

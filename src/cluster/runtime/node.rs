use std::sync::Arc;

use async_trait::async_trait;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::message::ClientMessage;

#[async_trait]
pub trait RuntimeNode<TMsg>: Send + Sync
where
    TMsg: Clone + Send + Sync + 'static,
{
    fn id(&self) -> Uuid;

    async fn start(&self, inbox: Receiver<TMsg>) -> JoinHandle<()>;

    async fn shutdown(&self);

    async fn connect_client(
        &self,
        _client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        None
    }
}

#[async_trait]
pub trait RuntimeNodeFactory<TMsg>: Send + Sync
where
    TMsg: Clone + Send + Sync + 'static,
{
    async fn build(&self, id: Uuid) -> anyhow::Result<Arc<dyn RuntimeNode<TMsg>>>;
}

use tokio::{
    sync::{
        Mutex, RwLock,
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    cluster::runtime::{state::RuntimeState, types::SharedRuntimeNode},
    message::ClientMessage,
};

pub struct RuntimeMember<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    uuid: Uuid,
    state: RwLock<RuntimeState>,
    node: SharedRuntimeNode<TMsg>,
    rx: Mutex<Option<Receiver<TMsg>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl<TMsg> RuntimeMember<TMsg>
where
    TMsg: Clone + Send + Sync + 'static,
{
    pub fn new(uuid: Uuid, node: SharedRuntimeNode<TMsg>, rx: Receiver<TMsg>) -> Self {
        Self {
            uuid,
            state: RwLock::new(RuntimeState::Starting),
            node,
            rx: Mutex::new(Some(rx)),
            task: Mutex::new(None),
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub async fn start(&self) {
        if self.state().await != RuntimeState::Starting {
            return;
        }

        let mut rx = self.rx.lock().await;
        let inbox = rx.take().expect("runtime already started");
        let handler = self.node.start(inbox).await;
        let mut task = self.task.lock().await;
        *task = Some(handler);
        drop(task);

        self.set_state(RuntimeState::Active).await;
    }

    pub async fn stop(&self) {
        if self.state().await != RuntimeState::Active {
            return;
        }

        self.abort_process().await;
        self.set_state(RuntimeState::Stopped).await;
    }

    pub async fn abort_process(&self) {
        self.node.shutdown().await;

        let mut task = self.task.lock().await;
        if let Some(runtime) = task.take() {
            runtime.abort();
            let _ = runtime.await;
        }
    }

    pub async fn state(&self) -> RuntimeState {
        self.state.read().await.clone()
    }

    pub async fn set_state(&self, next: RuntimeState) {
        let mut state = self.state.write().await;
        *state = next;
    }

    pub async fn transition_state(&self, expected: RuntimeState, next: RuntimeState) -> bool {
        let mut state = self.state.write().await;
        if *state != expected {
            return false;
        }
        *state = next;
        true
    }

    pub async fn connect_client(
        &self,
        client_id: Uuid,
    ) -> Option<(Sender<ClientMessage>, Receiver<ClientMessage>)> {
        self.node.connect_client(client_id).await
    }
}

use std::sync::Arc;
use tokio::{
    sync::{
        Mutex, RwLock,
        mpsc::{Receiver, Sender},
    },
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    cluster::{
        cluster_configuration::ClusterConfiguration,
        configuration_handler::types::ConfigurationHandlerMessage, runtime_state::RuntimeState,
    },
    common::persistence::NodePersistence,
    message::ClientMessage,
    monitor::PaxosObserver,
    node::{
        config::Roles,
        pmmc::{
            message::PmmcMessage,
            pmmc_node::PmmcNode,
            transport::{PmmcFabric, PmmcHandle},
        },
    },
};

pub struct RuntimeMember {
    uuid: Uuid,
    roles: Roles,
    state: RwLock<RuntimeState>,
    pub node: PmmcNode,
    rx: Mutex<Option<Receiver<PmmcMessage>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

pub enum RuneTimeSignal {
    Stopped,
}

impl RuntimeMember {
    pub async fn new(
        uuid: Uuid,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
        fabric: Arc<PmmcFabric>,
        persistence: NodePersistence,
        rx: Receiver<PmmcMessage>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let handle = Arc::new(PmmcHandle::from_fabric(uuid, Arc::clone(&fabric)));
        Ok(RuntimeMember {
            uuid,
            roles: roles.clone(),
            state: RwLock::new(RuntimeState::Starting),
            node: PmmcNode::new(
                uuid,
                observer,
                fabric,
                handle,
                persistence,
                roles,
                configuration,
            )
            .await?,
            rx: Mutex::new(Some(rx)),
            task: Mutex::new(None),
        })
    }

    pub async fn start(&self) {
        let should_start = self.state().await == RuntimeState::Starting;
        if !should_start {
            return;
        }

        let mut rx = self.rx.lock().await;
        let inbox = rx.take().expect("runtime already started");
        let handler = self.node.start(inbox);
        self.node.start_admin().await;
        let mut task = self.task.lock().await;
        *task = Some(handler);
        drop(task);

        self.set_state(RuntimeState::Active).await;
    }

    pub async fn stop(&self) {
        let can_stop = self.state().await == RuntimeState::Active;
        if !can_stop {
            return;
        }

        self.abort_process().await;
        self.set_state(RuntimeState::Stopped).await;
    }

    pub async fn abort_process(&self) {
        let mut task = self.task.lock().await;
        if let Some(runtime) = task.take() {
            runtime.abort();
            let _ = runtime.await;
        }
        drop(task);
        self.node.stop_admin().await;
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn roles(&self) -> &Roles {
        &self.roles
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

    pub async fn attach_configuration_transport(
        &self,
        endpoint_rx: Receiver<ConfigurationHandlerMessage>,
        endpoint_tx: Sender<ConfigurationHandlerMessage>,
    ) {
        self.node
            .attach_admin_transport(endpoint_rx, endpoint_tx)
            .await;
    }
}

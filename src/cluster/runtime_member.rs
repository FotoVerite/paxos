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
        configuration_handler::types::{
            ConfigurationCommand, ConfigurationHandlerError, ConfigurationReplyOutcome,
        },
        network_fabric::NetworkFabric,
        network_simulator::NetworkSimulator,
        runtime_state::RuntimeState,
    },
    common::persistence::NodePersistence,
    message::{ClientMessage, Message},
    monitor::PaxosObserver,
    node::{config::Roles, pmmc::pmmc_node::PmmcNode},
};

pub struct RuntimeMember {
    pub uuid: Uuid,
    pub roles: Roles,
    state: RwLock<RuntimeState>,
    pub node: PmmcNode,
    rx: Mutex<Option<Receiver<Message>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

pub enum RuneTimeSignal {
    Stopped 
}

impl RuntimeMember {
    pub async fn new(
        uuid: Uuid,
        roles: Roles,
        configuration: Arc<ClusterConfiguration>,
        fabric: Arc<NetworkFabric>,
        persistence: NodePersistence,
        rx: Receiver<Message>,
        observer: Arc<dyn PaxosObserver>,
    ) -> anyhow::Result<Self> {
        let handle = Arc::new(NetworkSimulator::from_fabric(uuid, Arc::clone(&fabric)));
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
        let should_start = {
            let state = self.state.read().await;
            *state == RuntimeState::Starting
        };
        if !should_start {
            return;
        }

        let mut rx = self.rx.lock().await;
        let inbox = rx.take().expect("runtime already started");
        let handler = self.node.start(inbox);
        let mut task = self.task.lock().await;
        *task = Some(handler);
        drop(task);

        let mut state = self.state.write().await;
        *state = RuntimeState::Active;
    }

    pub async fn stop(&self) {
        let can_stop = {
            let state = self.state.read().await;
            *state == RuntimeState::Active
        };
        if !can_stop {
            return;
        }

        // self.node.stop();
        // let mut task = self.task.lock().await;
        // if let Some(runtime) = task.take() {
        //     runtime.stop();
        //     runtime.abort();
        //     let _ = runtime.task.await;
        // }

        let mut state = self.state.write().await;
        *state = RuntimeState::Stopped;
    }

    pub async fn state(&self) -> RuntimeState {
        self.state.read().await.clone()
    }

    pub async fn transition_state(
        &self,
        expected: RuntimeState,
        next: RuntimeState,
    ) -> bool {
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

    pub async fn handle_configuration_command(
        &self,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        match cmd {
            ConfigurationCommand::Stop => {
                let state = self.state().await;
                match state {
                    RuntimeState::Active => {
                        self.stop().await;
                        Ok(ConfigurationReplyOutcome::Stopped)
                    }
                    RuntimeState::Stopped => Ok(ConfigurationReplyOutcome::Stopped),
                    RuntimeState::Crashed => Err(ConfigurationHandlerError::Conflict {
                        reason: "node is crashed".to_string(),
                    }),
                    _ => Err(ConfigurationHandlerError::Conflict {
                        reason: "node is not active".to_string(),
                    }),
                }
            }
            ConfigurationCommand::Emit => {
                let state = self.state().await;
                let outcome = match state {
                    RuntimeState::Active => ConfigurationReplyOutcome::Active,
                    RuntimeState::Stopped => ConfigurationReplyOutcome::Stopped,
                    _ => ConfigurationReplyOutcome::Data,
                };
                Ok(outcome)
            }
            ConfigurationCommand::Add { .. } | ConfigurationCommand::Remove { .. } => {
                Err(ConfigurationHandlerError::Rejected {
                    reason: "membership updates are not implemented on member endpoint".to_string(),
                })
            }
        }
    }

}

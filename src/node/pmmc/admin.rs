use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, timeout};
use uuid::Uuid;

use crate::{
    cluster::configuration_handler::types::{
        ConfigurationCommand, ConfigurationHandlerError, ConfigurationHandlerMessage,
        ConfigurationReplyOutcome,
    },
    message::ClientMessage,
    node::pmmc::{node_state::NodeState, replica::Replica},
    paxos_command::PaxosCommand,
};

const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_RETRY_POLL: Duration = Duration::from_millis(400);

pub struct NodeAdmin {
    client_id: Uuid,
    request_id: AtomicU64,
    state: Arc<NodeState>,
    runtime: Mutex<AdminRuntimeState>,
}

struct AdminRuntimeState {
    endpoint_rx: Option<mpsc::Receiver<ConfigurationHandlerMessage>>,
    endpoint_tx: Option<mpsc::Sender<ConfigurationHandlerMessage>>,
    task: Option<JoinHandle<()>>,
    stop_responses: Option<mpsc::Receiver<ClientMessage>>,
}

impl NodeAdmin {
    pub fn new(node_id: Uuid, state: Arc<NodeState>) -> Self {
        Self {
            client_id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("pmmc-admin-client:{}", node_id).as_bytes(),
            ),
            request_id: AtomicU64::new(0),
            state,
            runtime: Mutex::new(AdminRuntimeState {
                endpoint_rx: None,
                endpoint_tx: None,
                task: None,
                stop_responses: None,
            }),
        }
    }

    pub async fn attach_transport(
        &self,
        endpoint_rx: mpsc::Receiver<ConfigurationHandlerMessage>,
        endpoint_tx: mpsc::Sender<ConfigurationHandlerMessage>,
    ) {
        self.stop().await;

        let mut runtime = self.runtime.lock().await;
        runtime.endpoint_rx = Some(endpoint_rx);
        runtime.endpoint_tx = Some(endpoint_tx);
    }

    pub async fn start(self: Arc<Self>) {
        self.ensure_internal_client_registered().await;

        let mut runtime = self.runtime.lock().await;
        if runtime.task.is_some() {
            return;
        }

        let endpoint_rx = runtime
            .endpoint_rx
            .take()
            .expect("configuration receiver must be attached before starting node admin");
        let endpoint_tx =
            runtime.endpoint_tx.as_ref().cloned().expect(
                "configuration response sender must be attached before starting node admin",
            );

        let admin = Arc::clone(&self);
        runtime.task = Some(tokio::spawn(async move {
            admin.run_configuration_loop(endpoint_rx, endpoint_tx).await;
        }));
    }

    pub async fn stop(&self) {
        let (running, had_client) = {
            let mut runtime = self.runtime.lock().await;
            let running = runtime.task.take();
            let had_client = runtime.stop_responses.take().is_some();
            (running, had_client)
        };

        if let Some(task) = running {
            task.abort();
            let _ = task.await;
        }

        if had_client {
            self.state.unregister_internal_client(self.client_id).await;
        }
    }

    pub async fn handle_configuration_command(
        &self,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        match cmd {
            ConfigurationCommand::Stop => self.submit_stop().await,
            ConfigurationCommand::Emit => self.emit_status().await,
            ConfigurationCommand::Add { .. } | ConfigurationCommand::Remove { .. } => {
                Err(ConfigurationHandlerError::InvalidRequest {
                    reason: "membership commands are handled by node state".to_string(),
                })
            }
        }
    }

    async fn run_configuration_loop(
        self: Arc<Self>,
        mut endpoint_rx: mpsc::Receiver<ConfigurationHandlerMessage>,
        endpoint_tx: mpsc::Sender<ConfigurationHandlerMessage>,
    ) {
        while let Some(msg) = endpoint_rx.recv().await {
            let ConfigurationHandlerMessage::Reconfigure { request_id, cmd } = msg else {
                continue;
            };

            let response = match self.handle_configuration_command(cmd).await {
                Ok(outcome) => outcome,
                Err(err) => ConfigurationReplyOutcome::Err(err),
            };

            if endpoint_tx
                .send(ConfigurationHandlerMessage::RESPONSE {
                    request_id,
                    response,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    async fn ensure_internal_client_registered(&self) {
        let needs_registration = {
            let runtime = self.runtime.lock().await;
            runtime.stop_responses.is_none()
        };

        if !needs_registration {
            return;
        }

        let response_rx = self.state.register_internal_client(self.client_id, 8).await;
        let mut runtime = self.runtime.lock().await;
        if runtime.stop_responses.is_none() {
            runtime.stop_responses = Some(response_rx);
        } else {
            drop(runtime);
            self.state.unregister_internal_client(self.client_id).await;
        }
    }

    async fn emit_status(&self) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        if self.state.is_stopped().await {
            Ok(ConfigurationReplyOutcome::Stopped)
        } else {
            Ok(ConfigurationReplyOutcome::Active)
        }
    }

    async fn submit_stop(&self) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let replica = self.require_replica()?;
        self.ensure_internal_client_registered().await;

        let mut resp_rx = {
            let mut runtime = self.runtime.lock().await;
            runtime
                .stop_responses
                .take()
                .ok_or_else(|| ConfigurationHandlerError::Internal {
                    reason: "admin stop response receiver not registered".to_string(),
                })?
        };

        let result = self.propose_stop_until_ack(&replica, &mut resp_rx).await;

        let mut runtime = self.runtime.lock().await;
        if runtime.stop_responses.is_none() {
            runtime.stop_responses = Some(resp_rx);
        }
        result
    }

    fn require_replica(&self) -> Result<Arc<Replica>, ConfigurationHandlerError> {
        self.state
            .replica_handle()
            .ok_or_else(|| ConfigurationHandlerError::Rejected {
                reason: "node has no replica role for stop proposal".to_string(),
            })
    }

    async fn propose_stop_until_ack(
        &self,
        replica: &Replica,
        resp_rx: &mut mpsc::Receiver<ClientMessage>,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            let request_id = self.next_request_id();
            let command = PaxosCommand::STOP.with_client(self.client_id, request_id);
            replica.propose_from_client(command).await;
            if let Some(outcome) = self
                .wait_for_stop_response(resp_rx, deadline, request_id)
                .await?
            {
                return Ok(outcome);
            }
        }
    }

    async fn wait_for_stop_response(
        &self,
        resp_rx: &mut mpsc::Receiver<ClientMessage>,
        deadline: Instant,
        request_id: u64,
    ) -> Result<Option<ConfigurationReplyOutcome>, ConfigurationHandlerError> {
        let now = Instant::now();
        if now >= deadline {
            return Err(ConfigurationHandlerError::Timeout);
        }

        let poll = (deadline - now).min(STOP_RETRY_POLL);
        match timeout(poll, resp_rx.recv()).await {
            Ok(Some(ClientMessage::RESPONSE {
                request_id: response_id,
                ..
            })) if response_id == request_id => Ok(Some(ConfigurationReplyOutcome::Stopped)),
            Ok(Some(ClientMessage::RESPONSE { .. })) => Ok(None),
            Ok(Some(_)) => Ok(None),
            Ok(None) => Err(ConfigurationHandlerError::ResponseChannelClosed),
            Err(_) => Ok(None),
        }
    }
}

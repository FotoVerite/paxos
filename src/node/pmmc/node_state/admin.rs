use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, timeout};
use uuid::Uuid;

use crate::{
    cluster::configuration_handler::types::{
        ConfigurationCommand, ConfigurationHandlerError, ConfigurationReplyOutcome,
    },
    message::ClientMessage,
    node::pmmc::replica::Replica,
    paxos_command::PaxosCommand,
};

use super::NodeClientSink;

const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const STOP_RETRY_POLL: Duration = Duration::from_millis(400);

pub(super) struct NodeAdmin {
    client_id: Uuid,
    request_id: AtomicU64,
    replica: Option<Arc<Replica>>,
    client_sink: Arc<NodeClientSink>,
}

impl NodeAdmin {
    pub(super) fn new(
        node_id: Uuid,
        replica: Option<Arc<Replica>>,
        client_sink: Arc<NodeClientSink>,
    ) -> Self {
        Self {
            client_id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("pmmc-admin-client:{}", node_id).as_bytes(),
            ),
            request_id: AtomicU64::new(0),
            replica,
            client_sink,
        }
    }

    pub(super) async fn handle_configuration_command(
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

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    async fn emit_status(&self) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let Some(replica) = &self.replica else {
            return Ok(ConfigurationReplyOutcome::Stopped);
        };

        if replica.is_stopped().await {
            Ok(ConfigurationReplyOutcome::Stopped)
        } else {
            Ok(ConfigurationReplyOutcome::Active)
        }
    }

    async fn submit_stop(&self) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let replica = self.require_replica()?;
        let (request_id, command) = self.build_stop_submission();
        let mut resp_rx = self.register_response_sink().await;
        let result = self
            .propose_stop_until_ack(&replica, &command, request_id, &mut resp_rx)
            .await;
        self.unregister_response_sink().await;
        result
    }

    fn require_replica(&self) -> Result<Arc<Replica>, ConfigurationHandlerError> {
        self.replica
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| ConfigurationHandlerError::Rejected {
                reason: "node has no replica role for stop proposal".to_string(),
            })
    }

    fn build_stop_submission(&self) -> (u64, PaxosCommand) {
        let request_id = self.next_request_id();
        let command = PaxosCommand::STOP.with_client(self.client_id, request_id);
        (request_id, command)
    }

    async fn register_response_sink(&self) -> mpsc::Receiver<ClientMessage> {
        let (resp_tx, resp_rx) = mpsc::channel(1);
        self.client_sink.add_client(self.client_id, resp_tx).await;
        resp_rx
    }

    async fn unregister_response_sink(&self) {
        self.client_sink.remove_client(self.client_id).await;
    }

    async fn propose_stop_until_ack(
        &self,
        replica: &Replica,
        command: &PaxosCommand,
        request_id: u64,
        resp_rx: &mut mpsc::Receiver<ClientMessage>,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let deadline = Instant::now() + STOP_TIMEOUT;
        loop {
            self.propose_stop_once(replica, command).await;
            if let Some(outcome) = self
                .wait_for_stop_response(request_id, resp_rx, deadline)
                .await?
            {
                return Ok(outcome);
            }
        }
    }

    async fn propose_stop_once(&self, replica: &Replica, command: &PaxosCommand) {
        replica.propose_from_client(command.clone()).await;
    }

    async fn wait_for_stop_response(
        &self,
        request_id: u64,
        resp_rx: &mut mpsc::Receiver<ClientMessage>,
        deadline: Instant,
    ) -> Result<Option<ConfigurationReplyOutcome>, ConfigurationHandlerError> {
        let now = Instant::now();
        if now >= deadline {
            return Err(ConfigurationHandlerError::Timeout);
        }

        let poll = (deadline - now).min(STOP_RETRY_POLL);
        match timeout(poll, resp_rx.recv()).await {
            Ok(Some(ClientMessage::RESPONSE {
                request_id: response_id,
                response: _,
            })) if response_id == request_id => Ok(Some(ConfigurationReplyOutcome::Stopped)),
            Ok(Some(_)) => Ok(None),
            Ok(None) => Err(ConfigurationHandlerError::ResponseChannelClosed),
            Err(_) => Ok(None),
        }
    }
}

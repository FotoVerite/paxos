use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;
pub mod endpoint;
mod operation_record;
pub mod types;

use crate::cluster::configuration_handler::types::{
    ConfigurationCommand, ConfigurationHandlerError, ConfigurationHandlerMessage,
    ConfigurationOperationId, ConfigurationOperationStatus, ConfigurationReplyOutcome,
};
use crate::common::message_hub::{HubInbound, MessageHub};
use endpoint::ConfigurationEndpoint;
use operation_record::OperationRecord;

type OperationResult = Result<ConfigurationReplyOutcome, ConfigurationHandlerError>;

enum OperationWaiter {
    Receiver(oneshot::Receiver<OperationResult>),
    Terminal(OperationResult),
}

pub struct ConfigurationHandler {
    _client_id: Uuid,
    request_id: Mutex<u64>,
    operations: Mutex<HashMap<u64, OperationRecord>>,
    hub: Arc<MessageHub<ConfigurationHandlerMessage, ConfigurationHandlerInbound>>,
    hub_rx: Mutex<Option<mpsc::Receiver<ConfigurationHandlerInbound>>>,
}

#[derive(Debug, Clone)]
enum ConfigurationHandlerInbound {
    Endpoint {
        _from: Uuid,
        msg: ConfigurationHandlerMessage,
    },
}

impl From<HubInbound<ConfigurationHandlerMessage>> for ConfigurationHandlerInbound {
    fn from(value: HubInbound<ConfigurationHandlerMessage>) -> Self {
        match value {
            HubInbound::Client { from, msg } => Self::Endpoint { _from: from, msg },
        }
    }
}

impl ConfigurationHandler {
    pub fn new(client_id: Uuid) -> Arc<Self> {
        let (hub_tx, hub_rx) = mpsc::channel(1024);
        Arc::new(Self {
            _client_id: client_id,
            request_id: Mutex::new(0),
            operations: Mutex::new(HashMap::new()),
            hub: Arc::new(MessageHub::new(hub_tx)),
            hub_rx: Mutex::new(Some(hub_rx)),
        })
    }

    pub async fn register_endpoint(
        &self,
        endpoint_id: Uuid,
        sender: mpsc::Sender<ConfigurationHandlerMessage>,
    ) -> mpsc::Sender<ConfigurationHandlerMessage> {
        self.hub.register(endpoint_id, sender).await
    }

    pub async fn unregister_endpoint(&self, endpoint_id: Uuid) {
        self.hub.unregister(endpoint_id).await;
    }

    pub async fn request(
        &self,
        to: Uuid,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        let operation_id = self.submit(to, cmd).await?;
        self.await_outcome(operation_id, Duration::from_secs(12)).await
    }

    async fn submit_internal(
        &self,
        to: Uuid,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationOperationId, ConfigurationHandlerError> {
        if !self.hub.clients().await.contains(&to) {
            return Err(ConfigurationHandlerError::EndpointUnavailable { endpoint: to });
        }

        let rid = {
            let mut id = self.request_id.lock().await;
            *id += 1;
            *id
        };

        let token = CancellationToken::new();
        self.operations.lock().await.insert(
            rid,
            OperationRecord::submitted_with_retry_token(token.clone()),
        );

        // Initial request dispatch
        self.hub
            .send(
                to,
                ConfigurationHandlerMessage::Reconfigure {
                    request_id: rid,
                    cmd: cmd.clone(),
                },
            )
            .await;

        // Spawn retry loop
        self.spawn_retry(rid, to, cmd, token);
        Ok(rid)
    }

    fn spawn_retry(
        &self,
        rid: u64,
        to: Uuid,
        cmd: ConfigurationCommand,
        token: CancellationToken,
    ) {
        let hub = Arc::clone(&self.hub);
        let mut dur = Duration::from_millis(500);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(dur) => {
                        dur *= 2;
                        if dur > Duration::from_millis(10000) {
                            token.cancel();
                            break;
                        }
                        hub.send(
                            to,
                            ConfigurationHandlerMessage::Reconfigure {
                                request_id: rid,
                                cmd: cmd.clone(),
                            },
                        )
                        .await;
                    }
                    _ = token.cancelled() => break,
                }
            }
        });
    }

    async fn await_outcome_internal(
        &self,
        operation_id: ConfigurationOperationId,
        timeout_duration: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        match self.take_waiter(operation_id).await? {
            OperationWaiter::Terminal(result) => result,
            OperationWaiter::Receiver(rx) => {
                let result = Self::wait_for_result(timeout_duration, rx).await;
                self.record_terminal_result(operation_id, &result).await;
                result
            }
        }
    }

    async fn take_waiter(
        &self,
        operation_id: ConfigurationOperationId,
    ) -> Result<OperationWaiter, ConfigurationHandlerError> {
        let mut operations = self.operations.lock().await;
        let Some(operation) = operations.get_mut(&operation_id) else {
            return Err(ConfigurationHandlerError::UnknownOperationId { operation_id });
        };

        if let Some(rx) = operation.take_receiver() {
            return Ok(OperationWaiter::Receiver(rx));
        }

        match operation.status() {
            ConfigurationOperationStatus::Completed(outcome) => {
                Ok(OperationWaiter::Terminal(Ok(outcome)))
            }
            ConfigurationOperationStatus::Failed(err) => Ok(OperationWaiter::Terminal(Err(err))),
            ConfigurationOperationStatus::Submitted => Err(
                ConfigurationHandlerError::AlreadyAwaitingOperation { operation_id },
            ),
        }
    }

    async fn wait_for_result(
        timeout_duration: Duration,
        rx: oneshot::Receiver<OperationResult>,
    ) -> OperationResult {
        match timeout(timeout_duration, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConfigurationHandlerError::ResponseChannelClosed),
            Err(_) => Err(ConfigurationHandlerError::Timeout),
        }
    }

    async fn record_terminal_result(
        &self,
        operation_id: ConfigurationOperationId,
        result: &OperationResult,
    ) {
        let mut operations = self.operations.lock().await;
        if let Some(operation) = operations.get_mut(&operation_id) {
            if matches!(result, Err(ConfigurationHandlerError::Timeout)) {
                operation.cancel_retry();
            }
            operation.set_status(Self::status_from_result(result));
        }
    }

    fn status_from_result(result: &OperationResult) -> ConfigurationOperationStatus {
        match result {
            Ok(outcome) => ConfigurationOperationStatus::Completed(outcome.clone()),
            Err(err) => ConfigurationOperationStatus::Failed(err.clone()),
        }
    }

    async fn status_internal(
        &self,
        operation_id: ConfigurationOperationId,
    ) -> Result<ConfigurationOperationStatus, ConfigurationHandlerError> {
        self.operations
            .lock()
            .await
            .get(&operation_id)
            .map(|operation| operation.status())
            .ok_or(ConfigurationHandlerError::UnknownOperationId { operation_id })
    }

    pub async fn handle_response(
        &self,
        rid: u64,
        outcome: Result<ConfigurationReplyOutcome, ConfigurationHandlerError>,
    ) -> Result<(), ConfigurationHandlerError> {
        let mut operations = self.operations.lock().await;
        let Some(operation) = operations.get_mut(&rid) else {
            return Err(ConfigurationHandlerError::UnknownRequestId { request_id: rid });
        };

        operation.cancel_retry();
        operation.set_status(Self::status_from_result(&outcome));

        if let Some(tx) = operation.take_sender() {
            let _ = tx.send(outcome);
        }
        Ok(())
    }

    pub async fn start(self: Arc<Self>) {
        let hub_rx = {
            let mut hub_rx = self.hub_rx.lock().await;
            hub_rx
                .take()
                .expect("configuration handler hub loop already started")
        };

        tokio::spawn(async move {
            let mut hub_rx = hub_rx;
            while let Some(msg) = hub_rx.recv().await {
                match msg {
                    ConfigurationHandlerInbound::Endpoint {
                        msg:
                            ConfigurationHandlerMessage::RESPONSE {
                                request_id,
                                response,
                            },
                        ..
                    } => {
                        if let Err(err) = self.handle_response(request_id, Ok(response)).await {
                            debug!(
                                request_id,
                                error = ?err,
                                "received configuration response for unknown request"
                            );
                        }
                    }
                    _ => continue,
                }
            }
        });
    }
}

#[async_trait]
impl ConfigurationEndpoint for ConfigurationHandler {
    async fn submit(
        &self,
        endpoint: Uuid,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationOperationId, ConfigurationHandlerError> {
        self.submit_internal(endpoint, cmd).await
    }

    async fn await_outcome(
        &self,
        operation_id: ConfigurationOperationId,
        timeout: Duration,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
        self.await_outcome_internal(operation_id, timeout).await
    }

    async fn status(
        &self,
        operation_id: ConfigurationOperationId,
    ) -> Result<ConfigurationOperationStatus, ConfigurationHandlerError> {
        self.status_internal(operation_id).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;
    use uuid::Uuid;

    use crate::cluster::configuration_handler::endpoint::ConfigurationEndpoint;
    use crate::cluster::configuration_handler::types::{
        ConfigurationCommand, ConfigurationHandlerError, ConfigurationHandlerMessage,
        ConfigurationOperationStatus, ConfigurationReplyOutcome,
    };

    use super::ConfigurationHandler;

    #[tokio::test]
    async fn submit_tracks_status_and_awaits_completion() {
        let handler = ConfigurationHandler::new(Uuid::from_u128(0xC5));
        let endpoint = Uuid::new_v4();
        let (endpoint_tx, mut endpoint_rx) = mpsc::channel::<ConfigurationHandlerMessage>(8);
        handler.register_endpoint(endpoint, endpoint_tx).await;

        let op = handler
            .submit(endpoint, ConfigurationCommand::Emit)
            .await
            .expect("submit should succeed");

        let first_msg = endpoint_rx.recv().await.expect("endpoint should receive request");
        assert!(matches!(
            first_msg,
            ConfigurationHandlerMessage::Reconfigure {
                request_id,
                cmd: ConfigurationCommand::Emit
            } if request_id == op
        ));

        let status = handler.status(op).await.expect("status should exist");
        assert_eq!(status, ConfigurationOperationStatus::Submitted);

        handler
            .handle_response(op, Ok(ConfigurationReplyOutcome::Ok))
            .await
            .expect("response should be correlated");

        let outcome = handler
            .await_outcome(op, Duration::from_millis(50))
            .await
            .expect("outcome should complete");
        assert_eq!(outcome, ConfigurationReplyOutcome::Ok);

        let final_status = handler.status(op).await.expect("status should stay visible");
        assert_eq!(
            final_status,
            ConfigurationOperationStatus::Completed(ConfigurationReplyOutcome::Ok)
        );
    }

    #[tokio::test]
    async fn unknown_operation_status_returns_error() {
        let handler = ConfigurationHandler::new(Uuid::from_u128(0xC5));
        let err = handler.status(99).await.expect_err("missing op should error");
        assert!(matches!(
            err,
            ConfigurationHandlerError::UnknownOperationId { operation_id } if operation_id == 99
        ));
    }

    #[tokio::test]
    async fn second_await_on_submitted_operation_returns_already_awaiting() {
        let handler = ConfigurationHandler::new(Uuid::from_u128(0xC5));
        let endpoint = Uuid::new_v4();
        let (endpoint_tx, _endpoint_rx) = mpsc::channel::<ConfigurationHandlerMessage>(8);
        handler.register_endpoint(endpoint, endpoint_tx).await;

        let op = handler
            .submit(endpoint, ConfigurationCommand::Emit)
            .await
            .expect("submit should succeed");

        let first_handler = std::sync::Arc::clone(&handler);
        let first = tokio::spawn(async move {
            first_handler.await_outcome(op, Duration::from_millis(10)).await
        });
        tokio::task::yield_now().await;
        let second = handler.await_outcome(op, Duration::from_millis(10)).await;
        assert!(matches!(
            second,
            Err(ConfigurationHandlerError::AlreadyAwaitingOperation { operation_id }) if operation_id == op
        ));
        let _ = first.await;
    }
}

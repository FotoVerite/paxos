use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use uuid::Uuid;
pub mod types;

use crate::cluster::configuration_handler::types::{
    ConfigurationCommand, ConfigurationHandlerError, ConfigurationHandlerMessage,
    ConfigurationReplyOutcome,
};
use crate::common::message_hub::{HubInbound, MessageHub};

pub struct ConfigurationHandler {
    _client_id: Uuid,
    request_id: Mutex<u64>,
    inflight: Mutex<HashMap<u64, InflightRequest>>,
    hub: Arc<MessageHub<ConfigurationHandlerMessage, ConfigurationHandlerInbound>>,
    hub_rx: Mutex<Option<mpsc::Receiver<ConfigurationHandlerInbound>>>,
}

struct InflightRequest {
    tx: oneshot::Sender<Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>,
    token: CancellationToken,
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
            inflight: Mutex::new(HashMap::new()),
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
        if !self.hub.clients().await.contains(&to) {
            return Err(ConfigurationHandlerError::EndpointUnavailable { endpoint: to });
        }

        let rid = {
            let mut id = self.request_id.lock().await;
            *id += 1;
            *id
        };

        let (tx, rx) = oneshot::channel();
        let token = CancellationToken::new();
        {
            let mut inflight = self.inflight.lock().await;
            inflight.insert(
                rid,
                InflightRequest {
                    tx,
                    token: token.clone(),
                },
            );
        }

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

        // Bound request lifetime so stale inflight requests do not hang forever.
        match timeout(Duration::from_secs(12), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConfigurationHandlerError::ResponseChannelClosed),
            Err(_) => {
                if let Some(req) = self.inflight.lock().await.remove(&rid) {
                    req.token.cancel();
                }
                Err(ConfigurationHandlerError::Timeout)
            }
        }
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

    pub async fn handle_response(
        &self,
        rid: u64,
        outcome: Result<ConfigurationReplyOutcome, ConfigurationHandlerError>,
    ) -> Result<(), ConfigurationHandlerError> {
        let mut inflight = self.inflight.lock().await;
        if let Some(req) = inflight.remove(&rid) {
            // Cancel retry loop
            req.token.cancel();
            let _ = req.tx.send(outcome);
            return Ok(());
        }
        Err(ConfigurationHandlerError::UnknownRequestId { request_id: rid })
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

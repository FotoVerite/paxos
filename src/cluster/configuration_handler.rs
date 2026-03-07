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
use crate::cluster::network_fabric::NetworkFabric;
use crate::message::Message;

pub struct ConfigurationHandler {
    client_id: Uuid,
    request_id: Mutex<u64>,
    fabric: Arc<NetworkFabric>,
    inflight: Mutex<HashMap<u64, InflightRequest>>,
}

struct InflightRequest {
    tx: oneshot::Sender<Result<ConfigurationReplyOutcome, ConfigurationHandlerError>>,
    token: CancellationToken,
}

impl ConfigurationHandler {
    pub fn new(client_id: Uuid, fabric: Arc<NetworkFabric>) -> Arc<Self> {
        Arc::new(Self {
            client_id,
            request_id: Mutex::new(0),
            fabric,
            inflight: Mutex::new(HashMap::new()),
        })
    }

    pub async fn request(
        &self,
        to: Uuid,
        cmd: ConfigurationCommand,
    ) -> Result<ConfigurationReplyOutcome, ConfigurationHandlerError> {
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

        // Initial broadcast
        self.fabric
            .send(
                self.client_id,
                to,
                Message::RECONFIGURE {
                    from: self.client_id,
                    to,
                    request_id: rid,
                    cmd: cmd.clone(),
                },
            )
            .await;

        // Spawn retry loop
        self.spawn_retry(rid, self.client_id, to, cmd, token);

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
        from: Uuid,
        to: Uuid,
        cmd: ConfigurationCommand,
        token: CancellationToken,
    ) {
        let fabric = Arc::clone(&self.fabric);
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
                        fabric.send(
                            from,
                            to,
                            Message::RECONFIGURE {
                                from,
                                to,
                                request_id: rid,
                                cmd: cmd.clone(),
                            },
                        ).await;
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

    pub async fn start(self: Arc<Self>, mut rx: mpsc::Receiver<ConfigurationHandlerMessage>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ConfigurationHandlerMessage::RESPONSE {
                        request_id,
                        response,
                    } => {
                        if let Err(err) = self.handle_response(request_id, Ok(response)).await {
                            debug!(request_id, error = ?err, "received configuration response for unknown request");
                        }
                    }
                    _ => {}
                }
            }
        });
    }
}

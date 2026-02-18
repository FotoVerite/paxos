mod types;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    cluster::network_simulator::NetworkSimulator,
    message::{ClientMessage, Message},
    paxos_command::PaxosCommand,
    rsm::types::{ClerkResponseError, KVVersion},
    rsm::kv_store::ReplyOutcome,
};

pub struct Clerk {
    client_id: Uuid,
    request_id: Mutex<u64>,
    peers: Arc<NetworkSimulator>,
    inflight: Mutex<HashMap<u64, InflightRequest>>,
    kv_cache: Mutex<HashMap<String, KVVersion>>,
}

struct InflightRequest {
    tx: oneshot::Sender<Result<ReplyOutcome, ClerkResponseError>>,
    key: Option<String>,
    cmd: PaxosCommand,
    token: CancellationToken,
}

impl Clerk {
    pub fn new(client_id: Uuid, peers: Arc<NetworkSimulator>) -> Arc<Self> {
        Arc::new(Self {
            client_id,
            request_id: Mutex::new(0),
            peers,
            inflight: Mutex::new(HashMap::new()),
            kv_cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn request(&self, mut cmd: PaxosCommand) -> Result<ReplyOutcome, ClerkResponseError> {
        let rid = {
            let mut id = self.request_id.lock().await;
            *id += 1;
            *id
        };

        // Extract key for caching if applicable
        let key = match cmd {
            PaxosCommand::PUT { ref key, .. } => Some(key.clone()),
            PaxosCommand::ADD { ref key, .. } => Some(key.clone()),
            PaxosCommand::GET { ref key } => Some(key.clone()),
            _ => None,
        };

        let (tx, rx) = oneshot::channel();
        let token = CancellationToken::new();
        {
            let mut inflight = self.inflight.lock().await;
            inflight.insert(rid, InflightRequest { 
                tx, 
                key: key.clone(), 
                cmd: cmd.clone(),
                token: token.clone(),
            });
        }

        // Tag command with client identity
        cmd = cmd.with_client(self.client_id, rid);

        // Initial broadcast
        self.peers.broadcast(Message::PROPOSE {
            from: self.client_id,
            slot: 0, 
            cmd: cmd.clone(),
        }).await;

        // Spawn retry loop
        self.spawn_retry(self.client_id, rid, cmd, token);

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ClerkResponseError::Maybe),
        }
    }

    fn spawn_retry(&self, from: Uuid, rid: u64, cmd: PaxosCommand, token: CancellationToken) {
        let peers = Arc::clone(&self.peers);
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
                        peers.broadcast(Message::PROPOSE {
                            from,
                            slot: 0,
                            cmd: cmd.clone(),
                        }).await;
                    }
                    _ = token.cancelled() => break,
                }
            }
        });
    }

    pub async fn handle_response(&self, rid: u64, outcome: Result<ReplyOutcome, ClerkResponseError>) {
        let mut inflight = self.inflight.lock().await;
        if let Some(req) = inflight.remove(&rid) {
            // Cancel retry loop
            req.token.cancel();

            // Update local version cache on success
            if let Ok(ReplyOutcome::WriteOk { version }) = &outcome {
                if let Some(key) = req.key {
                    let mut cache = self.kv_cache.lock().await;
                    cache.insert(key, *version);
                }
            } else if let Ok(ReplyOutcome::GetOk { version, .. }) = &outcome {
                if let Some(key) = req.key {
                    let mut cache = self.kv_cache.lock().await;
                    cache.insert(key, *version);
                }
            }
            
            let _ = req.tx.send(outcome);
        }
    }

    pub async fn get_cached_version(&self, key: &str) -> Option<KVVersion> {
        let cache = self.kv_cache.lock().await;
        cache.get(key).cloned()
    }

    pub async fn start(self: Arc<Self>, mut rx: mpsc::Receiver<ClientMessage>) {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ClientMessage::RESPONSE { request_id, response } => {
                        self.handle_response(request_id, Ok(response)).await;
                    }
                    _ => {}
                }
            }
        });
    }
}

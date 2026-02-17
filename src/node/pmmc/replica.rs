use crate::common::persistence::Persistence;
use crate::common::types::DecreeId;
use crate::message::{ClientMessage, Message};
use crate::monitor::PaxosObserver;
use crate::node::classic_paxos::ballot::Ballot;
use crate::node::pmmc::replica::replica_state::ReplicaState;
use crate::node::pmmc::replica::replica_state::durable::ReplicaDurable;
use crate::node::pvalue::PValue;
use crate::paxos_command::PaxosCommand;
use crate::rsm::kv_store::KVStore;
use anyhow::Result;
use std::sync::Arc;
use tokio::net::unix::pipe::Receiver;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;
mod replica_state;

pub struct Replica {
    uuid: Uuid,
    store: Arc<KVStore>,
    state: Arc<ReplicaState>,
    observer: Arc<dyn PaxosObserver>,
}
impl Replica {
    pub async fn new(id: usize, uuid: Uuid, observer: Arc<dyn PaxosObserver>) -> Result<Self> {
        let data: ReplicaDurable = Persistence::load(&format!("replica_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = ReplicaDate::default();
        let replica = Self {
            uuid,
            store: Arc::new(KVStore::init(uuid).await?),
            state: Arc::new(ReplicaState::init(data)),
            observer: Arc::clone(&observer),
        };
        replica.spawn_applier().await;
        Ok(replica)
    }

    pub async fn accepted(&self, pvalue: PValue) -> Message {
        self.state.add_decision(pvalue.clone()).await;
        Message::ACK {
            from: self.uuid,
            to: pvalue.ballot().node_id,
            slot: pvalue.slot(),
        }
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::ACCEPTED { pvalue, .. } => self.accepted(pvalue).await,
            _ => Message::NACK,
        }
    }

    pub async fn spawn_client_handler(
        &self,
        client_id: Uuid,
        mut rx: mpsc::Receiver<ClientMessage>,
        tx: Sender<ClientMessage>,
    ) {
        let state = Arc::clone(&self.state);
        state.add_client(client_id, tx).await;
        tokio::spawn(async move {
            loop {
                select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            ClientMessage::PROPOSE { cmd } => {
                                state.proposal_handler(cmd).await;
                            },
                            _ => {},
                        }
                    }

                    else => break,
                }
            }
        });
    }

    pub async fn spawn_applier(&self) {
        let state = Arc::clone(&self.state);
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            loop {
                while let Some(cmd) = state.next_decision().await {
                    let slot = state.execution_slot().await;
                    let response = store.apply(cmd.operation().clone()).await;
                    match response {
                        Ok(response) => {
                            state.increment_execution_slot().await;
                            state.send_client_response(
                                cmd.client_id(),
                                ClientMessage::RESPONSE {
                                    request_id: cmd.request_id(),
                                    response,
                                },
                            );
                        }
                        _ => {}
                    }
                }
            }
        });
    }
}

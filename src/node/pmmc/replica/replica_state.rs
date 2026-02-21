use tokio::sync::{Mutex, Notify, mpsc::Sender};
use uuid::Uuid;

use crate::{
    message::ClientMessage,
    node::{
        pmmc::{
            proposal::ProposalsStore,
            replica::replica_state::{durable::ReplicaDurable, volatile::ReplicaVolatile},
        },
        pvalue::PValue,
    },
    paxos_command::{ClientId, PaxosCommand},
    rsm::kv_store::ReplyOutcome,
};

pub mod durable;
mod volatile;

pub struct ReplicaData {
    durable: ReplicaDurable,
    volatile: ReplicaVolatile,
}

impl Default for ReplicaData {
    fn default() -> Self {
        Self {
            durable: ReplicaDurable::default(),
            volatile: ReplicaVolatile::default(),
        }
    }
}

pub struct ReplicaState {
    data: Mutex<ReplicaData>,
    decision_notify: Notify,
}

impl ReplicaState {
    pub fn init(data: ReplicaDurable) -> Self {
        return Self {
            data: Mutex::new(ReplicaData {
                durable: data,
                volatile: ReplicaVolatile::default(),
            }),
            decision_notify: Notify::new(),
        };
    }

    pub async fn proposal(&self) -> ProposalsStore {
        let state = self.data.lock().await;
        state.durable.proposals()
    }

     pub async fn proposal_slot(&self) -> usize {
        let state = self.data.lock().await;
        state.durable.proposal_slot()
    }

    pub async fn execution_slot(&self) -> usize {
        let state = self.data.lock().await;
        state.durable.execution_slot()
    }

    pub async fn send_client_response(&self, client_id: ClientId, response: ClientMessage) {
        let tx = {
            let state = self.data.lock().await;
            state.volatile.sender(client_id)
        };

        if let Some(tx) = tx {
            let _ = tx.send(response).await;
        }
    }

    pub async fn wait_for_decision(&self) {
        self.decision_notify.notified().await;
    }

    pub async fn proposal_handler(&self, cmd: PaxosCommand) -> Option<usize> {
        let client_id = cmd.client_id();
        let request_id = cmd.request_id();

        let send = {
            let mut state = self.data.lock().await;
            let (cached, response) = state.durable.is_cached(&cmd);

            if !cached {
                let slot = state.durable.proposal_slot();
                state.durable.add_proposal(cmd);
                return Some(slot);
            }

            response.and_then(|response| {
                state.volatile.sender(client_id).map(|tx| {
                    (
                        tx,
                        ClientMessage::RESPONSE {
                            request_id,
                            response,
                        },
                    )
                })
            })
        };

        if let Some((tx, msg)) = send {
            let _ = tx.send(msg).await;
            return None
        }
        return None
    }

    pub async fn add_proposal(&self, cmd: PaxosCommand) -> usize {
        let mut state = self.data.lock().await;
        let slot = state.durable.proposal_slot();
        state.durable.add_proposal(cmd);
        slot
    }

    pub async fn add_decision(&self, pvalue: PValue) {
        {
            let mut state = self.data.lock().await;
            state.durable.add_decision(pvalue);
        }
        self.decision_notify.notify_one();
    }

    pub async fn increment_execution_slot(&self) {
        let mut state = self.data.lock().await;
        state.durable.increment_decisions();
    }

    pub async fn next_decision(&self) -> Option<PaxosCommand> {
        let mut state = self.data.lock().await;
        state.durable.next_decision()
    }

    pub async fn is_cached(&self, cmd: &PaxosCommand) -> (bool, Option<ReplyOutcome>) {
        let state = self.data.lock().await;
        state.durable.is_cached(cmd)
    }

    pub async fn update_cache(&self, cmd: &PaxosCommand, response: ReplyOutcome) {
        let mut state = self.data.lock().await;
        state.durable.update_cache(cmd, response);
    }

    pub async fn dump(&self) -> ReplicaDurable {
        let state = self.data.lock().await;
        state.durable.clone()
    }

    pub async fn add_client(&self, client_id: Uuid, tx: Sender<ClientMessage>) {
        let mut state = self.data.lock().await;
        state.volatile.add_client(client_id, tx);
    }
}

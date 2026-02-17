use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    os::macos::raw::stat,
    sync::Arc,
};

use tokio::{
    sync::{Mutex, mpsc::Sender},
    time::{Instant, Interval},
};
use uuid::Uuid;

use crate::{
    message::ClientMessage,
    node::{
        pmmc::{
            proposal::ProposalsStore,
            replica::{
                Replica,
                replica_state::{durable::ReplicaDurable, volatile::ReplicaVolatile},
            },
        },
        pvalue::PValue,
    },
    paxos_command::{ClientId, PaxosCommand},
    rsm::kv_store::ReplyOutcome,
};

pub mod durable;
mod volatile;

pub struct ReplicaDate {
    durable: ReplicaDurable,
    volatile: ReplicaVolatile,
}

impl Default for ReplicaDate {
    fn default() -> Self {
        Self {
            durable: ReplicaDurable::default(),
            volatile: ReplicaVolatile::default(),
        }
    }
}

pub struct ReplicaState {
    data: Mutex<ReplicaDate>,
}

impl ReplicaState {
    pub fn init(data: ReplicaDurable) -> Self {
        return Self {
            data: Mutex::new(ReplicaDate {
                durable: data,
                volatile: ReplicaVolatile::default(),
            }),
        };
    }

    pub async fn proposal(&self) -> ProposalsStore {
        let state = self.data.lock().await;
        state.durable.proposals()
    }

    pub async fn execution_slot(&self) -> usize {
        let state = self.data.lock().await;
        state.durable.execution_slot()
    }

    pub async fn send_client_response(&self, client_id: ClientId, response: ClientMessage) {
        let state = self.data.lock().await;
        state.volatile.send_client_response(client_id, response);
    }

    pub async fn proposal_handler(&self, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        let (cached, response) = state.durable.is_cached(&cmd);
        if cached {
            if let Some(response) = response {
                state.volatile.send_client_response(
                    cmd.client_id(),
                    ClientMessage::RESPONSE {
                        request_id: cmd.request_id(),
                        response,
                    },
                );
            }
            return
        }
        state.durable.add_proposal(cmd);
    }

    pub async fn add_proposal(&self, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        state.durable.add_proposal(cmd);
    }

    pub async fn add_decision(&self, pvalue: PValue) {
        let mut state = self.data.lock().await;
        state.durable.add_decision(pvalue);
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

    pub async fn dump(&self) -> ReplicaDurable {
        let state = self.data.lock().await;
        state.durable.clone()
    }

    pub async fn add_client(&self, client_id: Uuid, tx: Sender<ClientMessage>) {
        let mut state = self.data.lock().await;
        state.volatile.add_client(client_id, tx);
    }

}

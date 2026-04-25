use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    cluster::vertical::activation::ActivationSnapshot,
    node::vertical_paxos::{message::VerticalPaxosMessage, proposal::ProposalsStore},
    paxos_command::PaxosCommand,
};

use super::{LeaderProgress, commander::Commander, sync::LeaderSynchronizer};

struct LeaderData {
    proposals: ProposalsStore,
    synchronizer: Option<LeaderSynchronizer>,
    commander: Option<Commander>,
}

pub struct LeaderState {
    data: Mutex<LeaderData>,
}

impl LeaderState {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(LeaderData {
                proposals: ProposalsStore::default(),
                synchronizer: None,
                commander: None,
            }),
        }
    }

    pub async fn install_activation(&self, synchronizer: LeaderSynchronizer) {
        let mut state = self.data.lock().await;
        if let Some(existing) = state.synchronizer.as_mut() {
            existing.shutdown().await;
        }
        state.synchronizer = Some(synchronizer);
        state.commander = None;
    }

    pub async fn start_synchronization(&self) -> Option<Arc<ActivationSnapshot>> {
        let mut state = self.data.lock().await;
        if let Some(synchronizer) = state.synchronizer.as_mut() {
            synchronizer.start().await;
            return Some(Arc::clone(synchronizer.snapshot()));
        }
        None
    }

    pub async fn record_proposal(&self, slot: usize, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        if let Some(commander) = state.commander.as_ref() {
            commander.add_pending(slot, cmd).await;
            return;
        }
        state.proposals.entry(slot).or_insert(cmd);
    }

    pub async fn is_ready(&self) -> bool {
        let state = self.data.lock().await;
        state
            .synchronizer
            .as_ref()
            .is_some_and(LeaderSynchronizer::is_ready)
    }

    pub async fn handle_p1b(&self, msg: VerticalPaxosMessage) -> LeaderProgress {
        let mut state = self.data.lock().await;
        let mut commander_inputs = None;
        if let Some(synchronizer) = state.synchronizer.as_mut() {
            if synchronizer.is_in_progress() {
                synchronizer.handle_message(msg).await;
                if synchronizer.is_ready() {
                    commander_inputs =
                        Some((Arc::clone(synchronizer.snapshot()), synchronizer.peers()));
                }
                let progress = LeaderProgress::from_synchronizer(synchronizer);
                if state.commander.is_none() {
                    if let Some((snapshot, peers)) = commander_inputs {
                        state.commander = Some(Commander::new(
                            snapshot.configuration().leader(),
                            snapshot.configuration().ballot(),
                            snapshot.configuration().write_quorum().members().to_vec(),
                            snapshot.configuration().replicas().to_vec(),
                            state.proposals.clone(),
                            peers,
                        ));
                    }
                }
                return progress;
            }
        }
        LeaderProgress::None
    }

    pub async fn handle_message(&self, msg: VerticalPaxosMessage) -> Option<VerticalPaxosMessage> {
        let state = self.data.lock().await;
        if let Some(commander) = state.commander.as_ref() {
            return commander.handle_message(msg).await;
        }
        None
    }

    pub async fn handle_ack(&self, from: Uuid, slot: usize) {
        let state = self.data.lock().await;
        if let Some(commander) = state.commander.as_ref() {
            commander.record_replica_ack(from, slot).await;
        }
    }

    pub async fn shutdown(&self) {
        let mut state = self.data.lock().await;
        if let Some(synchronizer) = state.synchronizer.as_mut() {
            synchronizer.shutdown().await;
        }
        state.synchronizer = None;
        state.commander = None;
    }
}

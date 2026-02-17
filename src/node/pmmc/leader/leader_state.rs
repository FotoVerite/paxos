use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    os::macos::raw::stat,
    sync::Arc,
};

use tokio::{
    sync::Mutex,
    time::{Instant, Interval},
};
use uuid::Uuid;

use crate::{
    message::Message,
    monitor::PaxosObserver,
    node::{
        classic_paxos::ballot::Ballot,
        pmmc::{
            leader::{
                commander::Commander,
                leader_state::{durable::LeaderDurable, volatile::LeaderVolatile},
                scout::Scout,
            },
            proposal::ProposalsStore,
        },
        pvalue::PValue,
    },
    paxos_command::PaxosCommand,
};
pub mod durable;
mod volatile;

pub struct LeaderData {
    durable: LeaderDurable,
    volatile: LeaderVolatile,
}

pub struct LeaderState {
    data: Mutex<LeaderData>,
}

impl LeaderState {
    pub fn init(id: Uuid, data: LeaderDurable) -> Self {
        let durable = LeaderDurable::init(id, data);
        let volatile = LeaderVolatile::default();
        return Self {
            data: Mutex::new(LeaderData { durable, volatile }),
        };
    }

    pub async fn start_scout(
        &self,
        uuid: Uuid,
        quorum: usize,
        observer: Arc<dyn PaxosObserver>,
    ) -> Ballot {
        let mut state = self.data.lock().await;
        //TODO bump ballot
        let highest_seen = state.volatile.highest_seen;
        let ballot = state.durable.bump_ballot(highest_seen);
        state
            .volatile
            .start_scout(uuid, ballot, quorum, observer)
            .await;
        ballot
    }

    pub async fn ballot(&self) -> Ballot {
        let state = self.data.lock().await;
        state.durable.ballot()
    }

    pub async fn is_active(&self) -> bool {
        let state = self.data.lock().await;
        state.durable.is_active()
    }

    pub async fn set_as_active(&self) {
        let mut state = self.data.lock().await;
        state.durable.set_as_active();
    }

    pub async fn set_as_passive(&self) {
        let mut state = self.data.lock().await;
        state.durable.set_as_passive();
    }

    pub async fn is_stale_ballot(&self, ballot: Ballot) -> bool {
        let state = self.data.lock().await;
        state.durable.is_stale_ballot(ballot)
    }

    pub async fn pmax(&self, pvalues: Vec<PValue>) {
        let mut state = self.data.lock().await;
        state.durable.pmax(pvalues);
    }

    pub async fn add(&self, slot: usize, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        state.durable.add(slot, cmd);
    }

    pub async fn proposal(&self) -> ProposalsStore {
        let data = self.data.lock().await;
        data.durable.proposal()
    }

    pub async fn heartbeat_handler(&self, ballot: Ballot) {
        let mut state = self.data.lock().await;
        if state.durable.is_active() || state.durable.is_stale_ballot(ballot) {
            return;
        }
        state.durable.set_as_passive();
        state.volatile.set_highest_seen(ballot);
        state.volatile.drop_scount();
        state.volatile.drop_commander();
        state.volatile.reset_election_deadline();
    }

    pub async fn election_deadline(&self) -> Instant {
        let state = self.data.lock().await;
        state.volatile.election_deadline()
    }

    pub async fn preempt(&self, ballot: Ballot) {
        let mut state = self.data.lock().await;
        if state.durable.is_stale_ballot(ballot) {
            return;
        }
        state.durable.set_as_passive();
        state.volatile.set_highest_seen(ballot);
        state.volatile.drop_scount();
        state.volatile.drop_commander();
        state.volatile.aimd_backoff();
        state.volatile.reset_election_deadline();
    }

    pub async fn handle_p1b(&self, msg: Message) -> Message {
        let mut state = self.data.lock().await;
        state.volatile.p1b(msg).await
    }

    pub async fn handle_p2b(&self, msg: Message) -> Message {
        let mut state = self.data.lock().await;
        state.volatile.p2b(msg).await
    }

    pub async fn compact(&self, slots: &[usize]) {
        let mut state = self.data.lock().await;
        state.durable.compact(slots);
    }

    pub async fn dump(&self) -> LeaderDurable {
        let state = self.data.lock().await;
        state.durable.clone()
    }
}

use std::sync::Arc;

use tokio::{sync::Mutex, time::Instant};
use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    monitor::PaxosObserver,
    node::pmmc::{
        leader::leader_state::{durable::LeaderDurable, volatile::LeaderVolatile},
        message::PmmcMessage,
        transport::PmmcHandle,
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
    id: Uuid,
    epoch: usize,
}

impl LeaderState {
    pub fn init(data: LeaderDurable, id: Uuid, epoch: usize) -> Self {
        let volatile = LeaderVolatile::default();
        return Self {
            data: Mutex::new(LeaderData {
                durable: data,
                volatile,
            }),
            id,
            epoch,
        };
    }

    pub async fn start_scout(&self, quorum: usize, observer: Arc<dyn PaxosObserver>) -> Ballot {
        let mut state = self.data.lock().await;
        //TODO bump ballot
        let highest_seen = state.volatile.highest_seen;
        let ballot = state.durable.bump_ballot(self.id, self.epoch, highest_seen);
        state
            .volatile
            .start_scout(self.id, ballot, quorum, observer)
            .await;
        ballot
    }

    pub async fn ballot(&self) -> Ballot {
        let state = self.data.lock().await;
        state.durable.ballot(self.id, self.epoch)
    }

    pub async fn is_active(&self) -> bool {
        let state = self.data.lock().await;
        state.durable.is_active()
    }

    pub async fn set_as_active(
        &self,
        quorum: usize,
        ballot: Ballot,
        replicas: Vec<Uuid>,
        observer: Arc<dyn PaxosObserver>,
        peers: Arc<PmmcHandle>,
    ) -> bool {
        let mut state = self.data.lock().await;
        if ballot != state.durable.ballot(self.id, self.epoch) {
            return false;
        }
        if state.volatile.highest_seen > ballot {
            return false;
        }
        if state.durable.is_active() {
            return false;
        }
        let pvalues = state.volatile.scout_pvalues();
        state.durable.set_as_active(pvalues);
        let proposals = state.durable.proposal();
        state.volatile.start_commander(
            self.id, quorum, ballot, replicas, proposals, observer, peers,
        );
        true
    }

    pub async fn add(&self, slot: usize, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        if state.durable.add(slot, cmd.clone()) {
            state.volatile.add_to_commander(slot, cmd).await;
        }
    }

    pub async fn heartbeat_handler(&self, ballot: Ballot) {
        let mut state = self.data.lock().await;
        if state.durable.is_stale_ballot(self.id, self.epoch, ballot) {
            return;
        }
        state.durable.set_as_passive();
        state.volatile.set_highest_seen(ballot);
        state.volatile.drop_scout();
        state.volatile.drop_commander().await;
        state.volatile.reset_election_deadline();
    }

    pub async fn election_deadline(&self) -> Instant {
        let state = self.data.lock().await;
        state.volatile.election_deadline()
    }

    pub async fn preempt(&self, ballot: Ballot) {
        let mut state = self.data.lock().await;
        if state.durable.is_stale_ballot(self.id, self.epoch, ballot) {
            return;
        }
        state.durable.set_as_passive();
        state.volatile.set_highest_seen(ballot);
        state.volatile.drop_scout();
        state.volatile.drop_commander().await;
        state.volatile.aimd_backoff();
        state.volatile.reset_election_deadline();
    }

    pub async fn dump(&self) -> LeaderDurable {
        let state = self.data.lock().await;
        state.durable.dump().await
    }

    pub async fn handle_p1b(&self, msg: PmmcMessage) -> Option<PmmcMessage> {
        let mut state = self.data.lock().await;
        state.volatile.p1b(msg).await
    }

    pub async fn handle_p2b(&self, msg: PmmcMessage) -> Option<PmmcMessage> {
        let mut state = self.data.lock().await;
        state.volatile.p2b(msg).await
    }

    pub async fn handle_ack(&self, from: Uuid, slot: usize) {
        let mut state = self.data.lock().await;
        state.volatile.ack(from, slot).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use uuid::Uuid;

    use crate::{common::ballot::Ballot, monitor::NoOpObserver};

    use super::{LeaderData, LeaderState, durable::LeaderDurable, volatile::LeaderVolatile};

    fn mk_state(active: bool, ballot: Ballot) -> LeaderState {
        let id = if ballot.node_id == Uuid::nil() {
            Uuid::from_u128(1)
        } else {
            ballot.node_id
        };
        LeaderState {
            data: tokio::sync::Mutex::new(LeaderData {
                durable: LeaderDurable::for_test(active, ballot.number, BTreeMap::new()),
                volatile: LeaderVolatile::default(),
            }),
            id,
            epoch: ballot.epoch,
        }
    }

    #[tokio::test]
    async fn higher_preempt_updates_state_and_drops_scout() {
        let current = Ballot::new(5, Uuid::nil());
        let higher = Ballot::new(6, Uuid::new_v4());
        let state = mk_state(false, current);
        let leader_id = Uuid::new_v4();
        let before = {
            let mut guard = state.data.lock().await;
            let d = guard.volatile.election_deadline();
            guard
                .volatile
                .start_scout(
                    leader_id,
                    Ballot::new(5, leader_id),
                    2,
                    Arc::new(NoOpObserver),
                )
                .await;
            d
        };

        state.preempt(higher).await;

        let guard = state.data.lock().await;
        assert_eq!(guard.volatile.highest_seen, higher);
        assert!(!guard.volatile.has_scout());
        assert!(guard.volatile.election_deadline() > before);
    }

    #[tokio::test]
    async fn stale_preempt_is_ignored() {
        let current = Ballot::new(9, Uuid::nil());
        let stale = Ballot::new(8, Uuid::new_v4());
        let state = mk_state(false, current);
        let leader_id = Uuid::new_v4();
        let (before_deadline, before_seen) = {
            let mut guard = state.data.lock().await;
            guard
                .volatile
                .start_scout(
                    leader_id,
                    Ballot::new(9, leader_id),
                    2,
                    Arc::new(NoOpObserver),
                )
                .await;
            (
                guard.volatile.election_deadline(),
                guard.volatile.highest_seen,
            )
        };

        state.preempt(stale).await;

        let guard = state.data.lock().await;
        assert_eq!(guard.volatile.highest_seen, before_seen);
        assert!(guard.volatile.has_scout());
        assert_eq!(guard.volatile.election_deadline(), before_deadline);
    }

    #[tokio::test]
    async fn heartbeat_with_higher_ballot_drops_scout_and_resets_deadline() {
        let state = mk_state(false, Ballot::new(3, Uuid::nil()));
        let higher = Ballot::new(4, Uuid::new_v4());
        let leader_id = Uuid::new_v4();
        let before = {
            let mut guard = state.data.lock().await;
            let d = guard.volatile.election_deadline();
            guard
                .volatile
                .start_scout(
                    leader_id,
                    Ballot::new(3, leader_id),
                    2,
                    Arc::new(NoOpObserver),
                )
                .await;
            d
        };

        state.heartbeat_handler(higher).await;

        let guard = state.data.lock().await;
        assert_eq!(guard.volatile.highest_seen, higher);
        assert!(!guard.volatile.has_scout());
        assert!(guard.volatile.election_deadline() > before);
    }

    #[tokio::test]
    async fn heartbeat_is_ignored_for_stale_ballot_or_active_leader() {
        let active = mk_state(true, Ballot::new(7, Uuid::nil()));
        let stale = Ballot::new(6, Uuid::new_v4());
        let active_before = {
            let guard = active.data.lock().await;
            (
                guard.volatile.election_deadline(),
                guard.volatile.highest_seen,
            )
        };

        active.heartbeat_handler(stale).await;
        let active_after = active.data.lock().await;
        assert_eq!(active_after.volatile.election_deadline(), active_before.0);
        assert_eq!(active_after.volatile.highest_seen, active_before.1);

        let passive = mk_state(false, Ballot::new(7, Uuid::nil()));
        let passive_before = {
            let guard = passive.data.lock().await;
            (
                guard.volatile.election_deadline(),
                guard.volatile.highest_seen,
            )
        };

        passive.heartbeat_handler(stale).await;
        let passive_after = passive.data.lock().await;
        assert_eq!(passive_after.volatile.election_deadline(), passive_before.0);
        assert_eq!(passive_after.volatile.highest_seen, passive_before.1);
    }
}

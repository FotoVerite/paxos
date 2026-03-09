use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use rand::Rng;
use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    common::aimd_timeout::AimdTimeout,
    message::Message,
    node::{classic_paxos::ballot::Ballot, pmmc::proposal::ProposalsStore, pvalue::PValue},
    paxos_command::PaxosCommand,
};

#[derive(Clone)]
pub struct PendingSlot {
    pub cmd: PaxosCommand,
    pub acceptor_acks: HashSet<Uuid>,
    pub decided: bool,
    pub replica_acks: HashSet<Uuid>,
}
pub type CommanderPendingProposals = HashMap<usize, PendingSlot>;
pub struct CommanderData {
    ballot: Ballot,
    quorum: usize,
    id: Uuid,
    replicas: HashSet<Uuid>,
    proposals: CommanderPendingProposals,
    aimd_timeout: AimdTimeout,
    deadline: Instant,
}

pub struct CommanderState {
    data: Mutex<CommanderData>,
    stopped: AtomicBool,
    stop_notify: Notify,
}

impl CommanderState {
    pub fn new(
        id: Uuid,
        ballot: Ballot,
        quorum: usize,
        replicas: Vec<Uuid>,
        store: ProposalsStore,
    ) -> Self {
        let initial = store
            .iter()
            .map(|(&slot, cmd)| {
                (
                    slot,
                    PendingSlot {
                        cmd: cmd.clone(),
                        acceptor_acks: HashSet::new(),
                        decided: false,
                        replica_acks: HashSet::new(),
                    },
                )
            })
            .collect();

        let data = CommanderData {
            ballot,
            id,
            quorum,
            replicas: replicas.into_iter().collect(),
            proposals: initial,
            aimd_timeout: AimdTimeout::default(),
            deadline: Instant::now(),
        };
        Self {
            data: Mutex::new(data),
            stopped: AtomicBool::new(false),
            stop_notify: Notify::new(),
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.stop_notify.notify_waiters();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub async fn wait_for_stop(&self) {
        if self.is_stopped() {
            return;
        }
        self.stop_notify.notified().await;
    }

    pub async fn process(&self, acceptor: Uuid, pvalue: PValue) -> Message {
        let mut state = self.data.lock().await;

        let (cmd, ack_count, already_decided) = match state.proposals.get_mut(&pvalue.slot()) {
            Some(pending) => {
                pending.acceptor_acks.insert(acceptor);
                (
                    pending.cmd.clone(),
                    pending.acceptor_acks.len(),
                    pending.decided,
                )
            }
            None => return Message::NACK,
        };
        state.aimd_timeout.success();
        if !already_decided && ack_count >= state.quorum {
            if let Some(pending) = state.proposals.get_mut(&pvalue.slot()) {
                pending.decided = true;
            }
            return Message::ACCEPTED {
                from: state.id,
                pvalue: PValue::new(pvalue.slot(), state.ballot, cmd),
            };
        }
        return Message::NACK;
    }

    #[cfg(test)]
    pub async fn proposals(&self) -> CommanderPendingProposals {
        let state = self.data.lock().await;
        state.proposals.clone()
    }

    pub async fn tick_snapshot(&self) -> (CommanderPendingProposals, HashSet<Uuid>) {
        let state = self.data.lock().await;
        (state.proposals.clone(), state.replicas.clone())
    }

    pub async fn record_replica_ack(&self, from: Uuid, slot: usize) {
        let mut state = self.data.lock().await;
        let replicas = state.replicas.clone();
        if let Some(pending) = state.proposals.get_mut(&slot) {
            if !pending.decided {
                return;
            }
            pending.replica_acks.insert(from);
            if pending.replica_acks.is_superset(&replicas) {
                state.proposals.remove(&slot);
            }
        }
    }

    pub async fn deadline(&self) -> Instant {
        let state = self.data.lock().await;
        state.deadline
    }

    pub async fn add_pending(&self, slot: usize, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        if state.proposals.contains_key(&slot) {
            return;
        }
        state.proposals.insert(
            slot,
            PendingSlot {
                cmd,
                acceptor_acks: HashSet::new(),
                decided: false,
                replica_acks: HashSet::new(),
            },
        );
    }

    pub async fn reset_deadline(&self) {
        let mut state = self.data.lock().await;
        let base = state.aimd_timeout.interval();
        let jitter = rand::rng().random_range(0..=((base.as_millis() as u64 / 5).max(1)));
        state.deadline = Instant::now() + base + Duration::from_millis(jitter);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use crate::{
        message::Message,
        node::{classic_paxos::ballot::Ballot, pvalue::PValue},
        paxos_command::PaxosCommand,
    };

    use super::CommanderState;

    fn cmd(v: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value: v,
        }
    }

    #[tokio::test]
    async fn add_pending_for_same_slot_should_keep_first_command_regression() {
        let id = Uuid::new_v4();
        let ballot = Ballot::new(1, id);
        let mut initial = BTreeMap::new();
        initial.insert(0, cmd(10));
        let state = CommanderState::new(id, ballot, 2, vec![], initial);

        state.add_pending(0, cmd(99)).await;
        let proposals = state.proposals().await;

        assert_eq!(
            proposals.get(&0).map(|p| p.cmd.clone()),
            Some(cmd(10)),
            "regression: same-slot add_pending should not overwrite existing command"
        );
    }

    #[tokio::test]
    async fn process_ignores_p2b_for_unknown_slot() {
        let id = Uuid::new_v4();
        let ballot = Ballot::new(2, id);
        let state = CommanderState::new(id, ballot, 2, vec![], BTreeMap::new());

        let reply = state
            .process(Uuid::new_v4(), PValue::new(7, ballot, cmd(7)))
            .await;
        assert!(matches!(reply, Message::NACK));
    }
}

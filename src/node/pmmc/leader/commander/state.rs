use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use rand::Rng;
use tokio::{sync::Mutex, time::Instant};
use uuid::Uuid;

use crate::{
    common::aimd_timeout::AimdTimeout,
    message::Message,
    node::{classic_paxos::ballot::Ballot, pmmc::proposal::ProposalsStore, pvalue::PValue},
    paxos_command::PaxosCommand,
};

pub type CommanderPendingProposals = HashMap<usize, (PaxosCommand, HashSet<Uuid>)>;
pub struct CommanderData {
    ballot: Ballot,
    quorum: usize, 
    id: Uuid,
    proposals: CommanderPendingProposals,
    aimd_timeout: AimdTimeout,
    deadline: Instant,
}

pub struct CommanderState {
    data: Mutex<CommanderData>,
}

impl CommanderState {
    pub fn new(id: Uuid, ballot: Ballot, quorum: usize, store: ProposalsStore) -> Self {
        let initial = store
            .iter()
            .map(|(&slot, cmd)| (slot, (cmd.clone(), HashSet::new())))
            .collect();

        let data = CommanderData {
            ballot,
            id, 
            quorum,
            proposals: initial,
            aimd_timeout: AimdTimeout::default(),
            deadline: Instant::now(),
        };
        Self {
            data: Mutex::new(data),
        }
    }

    pub async fn process(&self, acceptor: Uuid, pvalue: PValue) -> Message {
        let mut state = self.data.lock().await;

        let (cmd, ack_count) = match state.proposals.get_mut(&pvalue.slot()) {
            Some((cmd, quorum)) => {
                quorum.insert(acceptor);
                (cmd.clone(), quorum.len())
            }
            None => return Message::NACK,
        };
        state.aimd_timeout.success();
        if ack_count >= state.quorum {
            return Message::ACCEPTED {
                from: state.id,
                pvalue: PValue::new(pvalue.slot(), state.ballot, cmd),
            };
        }
        return Message::NACK;
    }

    pub async fn proposals(&self) -> CommanderPendingProposals {
        let state = self.data.lock().await;
        state.proposals.clone()
    }

    pub async fn deadline(&self) -> Instant {
        let state = self.data.lock().await;
        state.deadline
    }

    pub async fn add_pending(&self, slot: usize, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        state.proposals.insert(slot, (cmd, HashSet::new()));
    }

    pub async fn reset_deadline(&self) {
        let mut state = self.data.lock().await;
        let base = state.aimd_timeout.interval();
        let jitter = rand::rng().random_range(0..=((base.as_millis() as u64 / 5).max(1)));
        state.deadline = Instant::now() + base + Duration::from_millis(jitter);
    }

    pub async fn success(&self) {
        let mut state = self.data.lock().await;
        state.aimd_timeout.success();
    }

    pub async fn backoff(&self) {
        let mut state = self.data.lock().await;
        state.aimd_timeout.backoff();
    }
}

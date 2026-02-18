// This module will handle the visualization/monitoring hooks.
// It will likely communicate with the web server part later.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::message::Message;
use crate::node::classic_paxos::ballot::Ballot;
use crate::node::pvalue::PValue;
use crate::{common::types::DecreeId, paxos_command::PaxosCommand};

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    Proposal {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Promise {
        id: Uuid,
        from: Uuid, // Destination proposer UUID
        decree_num: DecreeId,
        ballot: usize, // Keeping ballot as usize for now inside event if complex, but Ballot struct uses usize. Wait, Ballot::number is usize. Event usually flattens.
        created_at: u64,
    },
    Accept {
        id: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        quorum: HashSet<Uuid>,
        value: PaxosCommand,
        created_at: u64,
    },
    Accepted {
        id: Uuid,
        from: Uuid, // Destination learner/proposer UUID
        decree_num: DecreeId,
        ballot: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Learn {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Success {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    LearnedValue {
        // New event for local learning
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    InitialDecree {
        // Event for pre-populated ledger decrees at node startup
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    BatchInitialDecrees {
        // Batch event for multiple initial decrees
        id: Uuid,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    LedgerDump {
        // Full ledger dump event
        id: Uuid,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    NodeState {
        id: Uuid,
        role: String,
        ballot: usize,
        learned_count: usize,
    },
    NodeCapabilities {
        id: Uuid,
        roles: Vec<String>,
        learning_strategy: String,
    },
    // Message passing events for visualization
    MessageSent {
        from: Uuid,
        to: Uuid,
        message_type: String,
    },
    BallotAdopted {
        id: Uuid,
        ballot: Ballot,
    },
    ProposalAccepted {
        id: Uuid,
        pvalue: PValue,
    },
    PmmcPropose {
        id: Uuid,
        slot: usize,
        cmd: PaxosCommand,
        created_at: u64,
    },
    PmmcP1A {
        from: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcP1B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcP2A {
        from: Uuid,
        pvalue: PValue,
        created_at: u64,
    },
    PmmcP2B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        pvalue: PValue,
        created_at: u64,
    },
    PmmcAdopted {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcPreempted {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcHeartbeat {
        from: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcAck {
        from: Uuid,
        to: Uuid,
        slot: usize,
        created_at: u64,
    },
    // Network partition events
    PartitionCreated {
        partition_a: Vec<Uuid>,
        partition_b: Vec<Uuid>,
        created_at: u64,
    },
    PartitionHealed {
        partition_a: Vec<Uuid>,
        partition_b: Vec<Uuid>,
        created_at: u64,
    },
    // Leadership event
    LeaderElected {
        id: Uuid,
        created_at: u64,
    },
    LeaderSteppedDown {
        id: Uuid,
        created_at: u64,
    },
}

pub trait PaxosObserver: Send + Sync {
    fn on_event(&self, event: Event);

    fn on_message(&self, indexes: &[Uuid], message: Message);
}

pub struct NoOpObserver;

impl PaxosObserver for NoOpObserver {
    fn on_event(&self, _event: Event) {}
    fn on_message(&self, _indexes: &[Uuid], _message: Message) {}
}

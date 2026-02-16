// This module will handle the visualization/monitoring hooks.
// It will likely communicate with the web server part later.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::message::Message;
use crate::node::paxos_state::ballot::Ballot;
use crate::{
    common::types::{DecreeId, NodeId},
    paxos_command::PaxosCommand,
};

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    Proposal {
        id: NodeId,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Promise {
        id: NodeId,
        from: NodeId, // The acceptor that sent the promise
        decree_num: DecreeId,
        ballot: usize, // Keeping ballot as usize for now inside event if complex, but Ballot struct uses NodeId. Wait, Ballot::number is usize. Event usually flattens.
        created_at: u64,
    },
    Accept {
        id: NodeId,
        decree_num: DecreeId,
        ballot: usize,
        quorum: HashSet<NodeId>,
        value: PaxosCommand,
        created_at: u64,
    },
    Accepted {
        id: NodeId,
        from: NodeId, // The acceptor that accepted
        decree_num: DecreeId,
        ballot: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Learn {
        id: NodeId,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Success {
        id: NodeId,
        from: NodeId,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    LearnedValue {
        // New event for local learning
        id: NodeId,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    InitialDecree {
        // Event for pre-populated ledger decrees at node startup
        id: NodeId,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    BatchInitialDecrees {
        // Batch event for multiple initial decrees
        id: NodeId,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    LedgerDump {
        // Full ledger dump event
        id: NodeId,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    NodeState {
        id: NodeId,
        role: String,
        ballot: usize,
        learned_count: usize,
    },
    NodeCapabilities {
        id: NodeId,
        roles: Vec<String>,
        learning_strategy: String,
    },
    // Message passing events for visualization
    MessageSent {
        from: NodeId,
        to: NodeId,
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
    // Network partition events
    PartitionCreated {
        partition_a: Vec<NodeId>,
        partition_b: Vec<NodeId>,
        created_at: u64,
    },
    PartitionHealed {
        partition_a: Vec<NodeId>,
        partition_b: Vec<NodeId>,
        created_at: u64,
    },
    // Leadership event
    LeaderElected {
        id: NodeId,
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

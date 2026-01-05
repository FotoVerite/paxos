// This module will handle the visualization/monitoring hooks.
// It will likely communicate with the web server part later.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::paxos_command::PaxosCommand;

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    Proposal {
        id: usize,
        decree_num: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Promise {
        id: usize,
        from: usize, // The acceptor that sent the promise
        decree_num: usize,
        ballot: usize,
        created_at: u64,
    },
    Accept {
        id: usize,
        decree_num: usize,
        ballot: usize,
        quorum: HashSet<usize>,
        value: PaxosCommand,
        created_at: u64,
    },
    Accepted {
        id: usize,
        from: usize, // The acceptor that accepted
        decree_num: usize,
        ballot: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Learn {
        id: usize,
        decree_num: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Success {
        id: usize,
        from: usize,
        decree_num: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    LearnedValue { // New event for local learning
        id: usize,
        decree_num: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    InitialDecree { // Event for pre-populated ledger decrees at node startup
        id: usize,
        decree_num: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    NodeState {
        id: usize,
        role: String,
        ballot: usize,
        learned_count: usize,
    },
    // Message passing events for visualization
    MessageSent {
        from: usize,
        to: usize,
        message_type: String,
    },
    // Network partition events
    PartitionCreated {
        partition_a: Vec<usize>,
        partition_b: Vec<usize>,
        created_at: u64,
    },
    PartitionHealed {
        created_at: u64,
    },
}

pub trait PaxosObserver: Send + Sync {
    fn on_event(&self, event: Event);
}

pub struct NoOpObserver;

impl PaxosObserver for NoOpObserver {
    fn on_event(&self, _event: Event) {}
}
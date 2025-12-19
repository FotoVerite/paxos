// This module will handle the visualization/monitoring hooks.
// It will likely communicate with the web server part later.

use crate::paxos_command::PaxosCommand;

#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    Proposal {
        id: usize,
        decree_num: usize,
        value: PaxosCommand,
    },
    Promise {
        id: usize,
        decree_num: usize,
        ballot: usize,
    },
    Accept {
        id: usize,
        decree_num: usize,
        ballot: usize,
        value: PaxosCommand,
    },
    Learn {
        id: usize,
        decree_num: usize,
        value: PaxosCommand,
    },
    NodeState {
        id: usize,
        role: String,
        ballot: usize,
        learned_count: usize,
    },
}

pub trait PaxosObserver: Send + Sync {
    fn on_event(&self, event: Event);
}

pub struct NoOpObserver;

impl PaxosObserver for NoOpObserver {
    fn on_event(&self, _event: Event) {}
}

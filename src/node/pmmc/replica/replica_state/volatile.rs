use std::collections::VecDeque;

use crate::paxos_command::PaxosCommand;

pub struct ReplicaVolatile {
    pub queued: VecDeque<PaxosCommand>,
}

impl Default for ReplicaVolatile {
    fn default() -> Self {
        Self {
            queued: VecDeque::new(),
        }
    }
}

impl ReplicaVolatile {}

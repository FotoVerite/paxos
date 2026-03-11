use std::{cmp::Ordering, fmt};

use serde::{Deserialize, Serialize};

use crate::{common::ballot::Ballot, paxos_command::PaxosCommand};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PValue {
    slot: usize,
    ballot: Ballot,
    cmd: PaxosCommand,
}

impl PValue {
    pub fn new(slot: usize, ballot: Ballot, cmd: PaxosCommand) -> Self {
        Self { slot, ballot, cmd }
    }

    pub fn compare(&self, other: &Self) -> Result<Ordering, PValueError> {
        if self.slot != other.slot {
            return Err(PValueError::DifferentSlot);
        }
        Ok(self.cmp(other))
    }

    pub fn slot(&self) -> usize {
        self.slot
    }

    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    pub fn cmd(&self) -> PaxosCommand {
        self.cmd.clone()
    }
}

impl PartialOrd for PValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ballot.cmp(&other.ballot)
    }
}

impl fmt::Display for PValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}-{}-{} }}", self.slot, self.ballot, self.cmd)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PValueError {
    DifferentSlot,
    InvalidOperation(String),
}

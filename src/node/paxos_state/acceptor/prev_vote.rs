use crate::{node::paxos_state::ballot::Ballot, paxos_command::PaxosCommand};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PrevVote {
    pub ballot: Ballot,
    pub value: PaxosCommand,
}

impl PartialOrd for PrevVote {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrevVote {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ballot.cmp(&other.ballot)
    }
}

impl Default for PrevVote {
    fn default() -> PrevVote {
        PrevVote {
            ballot: Ballot::default(),
            value: PaxosCommand::BLANK,
        }
    }
}

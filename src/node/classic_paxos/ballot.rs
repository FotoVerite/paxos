use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::{self},
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ballot {
    pub number: usize,
    pub node_id: Uuid,
}

impl Ballot {
    pub fn new(number: usize, node_id: Uuid) -> Self {
        Self { number, node_id }
    }

    pub fn init(&self, node_id: Uuid) -> Self {
        if self.node_id != Uuid::nil() && self.node_id != node_id {
            panic!("Trying to init a non owned ballot");
        }
        Self {
            node_id,
            number: self.number,
        }
    }

    pub fn next(&self) -> Self {
        if self.node_id == Uuid::nil() {
            panic!("Trying to Increment a sentry Ballot")
        }
        Self {
            node_id: self.node_id,
            number: self.number + 1,
        }
    }

    pub fn bump(&self, ballot: Ballot) -> Self {
        if self.node_id == Uuid::nil() {
            panic!("Trying to Increment a sentry Ballot")
        }
        if ballot <= *self {
            return self.next();
        }
        Self {
            node_id: self.node_id,
            number: ballot.number + 1,
        }
    }
}

// Lexicographical ordering: compare number first, then node_id
impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.number.cmp(&other.number) {
            Ordering::Equal => self.node_id.cmp(&other.node_id),
            ord => ord,
        }
    }
}

impl Default for Ballot {
    fn default() -> Ballot {
        Ballot {
            number: 0,
            node_id: Uuid::nil(),
        }
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}-{} }}", self.node_id, self.number)
    }
}

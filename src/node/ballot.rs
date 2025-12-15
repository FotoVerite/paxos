use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ballot {
    pub number: usize,
    pub node_id: usize,
}

impl Ballot {
    pub fn new(number: usize, node_id: usize) -> Self {
        Self { number, node_id }
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

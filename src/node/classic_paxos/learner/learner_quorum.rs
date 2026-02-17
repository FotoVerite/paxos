use std::{collections::HashSet, usize::MAX};
use uuid::Uuid;

use crate::node::classic_paxos::ballot::Ballot;

pub struct LearnerQuorum {
    votes: HashSet<Uuid>,
    quorum_size: usize,
    cached_last_tried: Ballot,
}

impl Default for LearnerQuorum {
    fn default() -> LearnerQuorum {
        LearnerQuorum {
            cached_last_tried: Ballot::default(),
            votes: HashSet::new(),
            quorum_size: MAX,
        }
    }
}

impl LearnerQuorum {
    pub fn new(quorum_size: usize, ballot: Ballot) -> Self {
        Self {
            cached_last_tried: ballot,
            votes: HashSet::new(),
            quorum_size,
        }
    }

    pub fn add_vote(&mut self, node_id: Uuid, cached_last_tried: Ballot) {
        if self.cached_last_tried != cached_last_tried {
            self.clear();
        }
        self.cached_last_tried = cached_last_tried;
        self.votes.insert(node_id);
    }

    pub fn clear(&mut self) {
        self.votes.clear();
    }

    pub fn has_met_quorum(&self) -> bool {
        self.votes.len() >= self.quorum_size
    }

    pub fn quorum_set(&self) -> HashSet<Uuid> {
        self.votes.clone()
    }
}

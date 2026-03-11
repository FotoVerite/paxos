use std::{collections::HashSet, usize::MAX};
use uuid::Uuid;

use crate::common::ballot::Ballot;

pub struct Quorum {
    promises: HashSet<Uuid>,
    highest_accepted: Ballot,
    pub quorum_size: usize,
}

impl Default for Quorum {
    fn default() -> Quorum {
        Quorum {
            promises: HashSet::new(),
            highest_accepted: Ballot::default(),
            quorum_size: MAX,
        }
    }
}

impl Quorum {
    pub fn init(quorum_size: usize) -> Self {
        Self {
            promises: HashSet::new(),
            highest_accepted: Ballot::default(),
            quorum_size,
        }
    }

    pub fn reset(&mut self) {
        self.promises.clear();
    }

    pub fn has_met_quorum(&self) -> bool {
        self.promises.len() >= self.quorum_size
    }

    pub fn highest_accepted(&self) -> Ballot {
        self.highest_accepted
    }

    pub fn update(&mut self, id: Uuid, ballot: Ballot) -> bool {
        self.promises.insert(id);
        if self.highest_accepted >= ballot {
            return false;
        }
        self.highest_accepted = ballot;
        return true;
    }
    pub fn quorum_set(&self) -> HashSet<Uuid> {
        self.promises.clone()
    }
}

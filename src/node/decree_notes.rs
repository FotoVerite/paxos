use std::{cmp::max, collections::HashMap};

use crate::node::ballot::Ballot;

pub struct DecreeNotes {
    pub state: HashMap<usize, DecreeNote>,
}

pub struct DecreeNote {
    pub last_tried: Ballot,
}

impl DecreeNotes {
    pub fn new() -> Self {
        return Self {
            state: HashMap::new(),
        };
    }
}
impl DecreeNote {
    pub fn new(node_id: usize) -> Self {
        return Self {
            last_tried: Ballot { number: 0, node_id },
        };
    }

    pub fn next_ballot(&mut self, highest_accepted: usize) -> Ballot {
        self.last_tried.number = max(self.last_tried.number + 1, highest_accepted + 1);
        self.last_tried
    }
}

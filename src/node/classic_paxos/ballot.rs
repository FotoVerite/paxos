use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::{self},
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Ballot {
    pub epoch: usize,
    pub number: usize,
    pub node_id: Uuid,
}

impl Ballot {
    pub fn new(number: usize, node_id: Uuid) -> Self {
        Self {
            epoch: 0,
            number,
            node_id,
        }
    }

    pub fn with_epoch(number: usize, node_id: Uuid, epoch: usize) -> Self {
        Self {
            epoch,
            number,
            node_id,
        }
    }

    pub fn init(&self, node_id: Uuid, epoch: usize) -> Self {
        if self.node_id != Uuid::nil() && self.node_id != node_id {
            panic!("Trying to init a non owned ballot");
        }
        Self {
            node_id,
            number: self.number,
            epoch: epoch,
        }
    }

    pub fn next(&self) -> Self {
        if self.node_id == Uuid::nil() {
            panic!("Trying to Increment a sentry Ballot")
        }
        Self {
            epoch: self.epoch,
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
            epoch: self.epoch,
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
        match self.epoch.cmp(&other.epoch) {
            Ordering::Equal => match self.number.cmp(&other.number) {
                Ordering::Equal => self.node_id.cmp(&other.node_id),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl Default for Ballot {
    fn default() -> Ballot {
        Ballot {
            number: 0,
            node_id: Uuid::nil(),
            epoch: 0,
        }
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}-{} }}", self.node_id, self.number)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::Ballot;

    #[test]
    fn new_defaults_to_epoch_zero() {
        let b = Ballot::new(4, Uuid::new_v4());
        assert_eq!(b.epoch, 0);
    }

    #[test]
    fn epoch_orders_before_number_and_node() {
        let id = Uuid::new_v4();
        let low_epoch = Ballot::with_epoch(999, id, 0);
        let high_epoch = Ballot::with_epoch(0, id, 1);
        assert!(high_epoch > low_epoch);
    }

    #[test]
    fn with_epoch_sets_explicit_epoch() {
        let b = Ballot::with_epoch(1, Uuid::new_v4(), 7);
        assert_eq!(b.epoch, 7);
    }
}

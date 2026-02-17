use std::collections::{BTreeMap, btree_map::Entry};

use tokio::sync::Mutex;

use crate::node::{classic_paxos::ballot::Ballot, pvalue::PValue};

pub type AcceptedMap = BTreeMap<usize, PValue>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AcceptedData {
    ballot_num: Ballot,
    accepted: AcceptedMap,
}

impl Default for AcceptedData {
    fn default() -> Self {
        Self {
            accepted: BTreeMap::new(),
            ballot_num: Ballot::default(),
        }
    }
}

pub struct AcceptorState {
    data: Mutex<AcceptedData>,
}

impl AcceptorState {
    pub fn init(data: AcceptedData) -> Self {
        return Self {
            data: Mutex::new(data),
        };
    }

    pub async fn update_ballot(&self, ballot: Ballot) {
        let mut state = self.data.lock().await;
        if state.ballot_num < ballot {
            state.ballot_num = ballot
        }
    }

    pub async fn p1a(&self, ballot: Ballot, start_index: usize) -> (Vec<PValue>, Ballot, bool) {
        let mut state = self.data.lock().await;
        let mut updated: bool = false;
        if state.ballot_num < ballot {
            state.ballot_num = ballot;
            updated = true;
        }
        let map = state
            .accepted
            .iter()
            .filter_map(|(&k, v)| {
                if k >= start_index {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect();
        return (map, state.ballot_num, updated);
    }

    pub async fn p2a(&self, value: PValue) -> bool {
        let mut state = self.data.lock().await;
        let slot = value.slot();

        let ballot = value.ballot();
        if state.ballot_num > ballot {
            return false;
        }
        state.ballot_num = ballot;
        match state.accepted.entry(slot) {
            Entry::Vacant(v) => {
                v.insert(value);
                return true;
            }
            Entry::Occupied(mut o) => {
                if o.get() < &value {
                    o.insert(value);
                    return true;
                }
                return false;
            }
        }
    }

    pub async fn get(&self, slot: &usize) -> Option<PValue> {
        let state = self.data.lock().await;
        state.accepted.get(slot).cloned()
    }

    pub async fn accept(&self, slot: usize, value: PValue) {
        let mut state = self.data.lock().await;
        match state.accepted.entry(slot) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => {
                if o.get() < &value {
                    o.insert(value);
                }
            }
        }
    }

    pub async fn ballot_num(&self) -> Ballot {
        let state = self.data.lock().await;
        state.ballot_num.clone()
    }

    pub async fn compact(&self, slots: &[usize]) {
        let mut state = self.data.lock().await;
        for s in slots {
            state.accepted.remove(s);
        }
    }

    pub async fn dump(&self) -> AcceptedData {
        let data = self.data.lock().await;
        data.clone()
    }
}

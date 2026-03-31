use std::collections::{BTreeMap, btree_map::Entry};

use tokio::sync::Mutex;

use crate::{common::ballot::Ballot, node::pvalue::PValue};

pub type AcceptedMap = BTreeMap<usize, PValue>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct AcceptorRecord {
    promised_ballot: Ballot,
    accepted: AcceptedMap,
}

impl Default for AcceptorRecord {
    fn default() -> Self {
        Self {
            accepted: BTreeMap::new(),
            promised_ballot: Ballot::default(),
        }
    }
}

pub struct AcceptorState {
    record: Mutex<AcceptorRecord>,
}

impl AcceptorState {
    pub fn init(record: AcceptorRecord) -> Self {
        Self {
            record: Mutex::new(record),
        }
    }

    pub async fn update_ballot(&self, ballot: Ballot) {
        let mut state = self.record.lock().await;
        if state.promised_ballot < ballot {
            state.promised_ballot = ballot
        }
    }

    pub async fn p1a(&self, ballot: Ballot, start_index: usize) -> (Vec<PValue>, Ballot, bool) {
        let mut state = self.record.lock().await;
        let mut updated = false;
        if state.promised_ballot < ballot {
            state.promised_ballot = ballot;
            updated = true;
        }
        let accepted = state
            .accepted
            .iter()
            .filter_map(|(&slot, pvalue)| (slot >= start_index).then_some(pvalue.clone()))
            .collect();
        (accepted, state.promised_ballot, updated)
    }

    pub async fn p2a(&self, value: PValue) -> bool {
        let mut state = self.record.lock().await;
        let slot = value.slot();
        let ballot = value.ballot();

        if state.promised_ballot != ballot {
            return false;
        }

        match state.accepted.entry(slot) {
            Entry::Vacant(v) => {
                v.insert(value);
                true
            }
            Entry::Occupied(mut o) => {
                if o.get() < &value {
                    o.insert(value);
                    return true;
                }
                false
            }
        }
    }

    pub async fn get(&self, slot: &usize) -> Option<PValue> {
        let state = self.record.lock().await;
        state.accepted.get(slot).cloned()
    }

    pub async fn accept(&self, slot: usize, value: PValue) {
        let mut state = self.record.lock().await;
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

    pub async fn promised_ballot(&self) -> Ballot {
        let state = self.record.lock().await;
        state.promised_ballot
    }

    pub async fn compact(&self, slots: &[usize]) {
        let mut state = self.record.lock().await;
        for slot in slots {
            state.accepted.remove(slot);
        }
    }

    pub async fn dump(&self) -> AcceptorRecord {
        let record = self.record.lock().await;
        record.clone()
    }
}

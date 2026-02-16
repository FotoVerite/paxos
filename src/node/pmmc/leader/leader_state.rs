use std::collections::{BTreeMap, HashMap, hash_map::Entry};

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    node::{
        paxos_state::ballot::Ballot, pmmc::acceptor::accepted_state::AcceptedMap, pvalue::PValue,
    },
    paxos_command::PaxosCommand,
};

pub type ProposalsMap = HashMap<usize, PaxosCommand>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct LeaderData {
    active: bool,
    proposals: ProposalsMap,
    ballot_num: Ballot,
}

impl Default for LeaderData {
    fn default() -> Self {
        Self {
            active: false,
            ballot_num: Ballot::default(),
            proposals: HashMap::new(),
        }
    }
}

pub struct LeaderState {
    data: Mutex<LeaderData>,
}

impl LeaderState {
    pub fn init(id: Uuid, mut data: LeaderData) -> Self {
        data.ballot_num = data.ballot_num.init(id);
        return Self {
            data: Mutex::new(data),
        };
    }

    pub async fn is_active(&self) -> bool {
        let state = self.data.lock().await;
        state.active
    }

    pub async fn set_as_active(&self) {
        let mut state = self.data.lock().await;
        state.active = true;
    }

    pub async fn set_as_passive(&self) {
        let mut state = self.data.lock().await;
        state.active = false;
    }

    pub async fn ballot(&self) -> Ballot {
        let state = self.data.lock().await;
        state.ballot_num
    }

    pub async fn bump_ballot(&self, ballot: Ballot) -> Ballot {
        let mut state = self.data.lock().await;
        state.ballot_num = state.ballot_num.bump(ballot);
        state.ballot_num
    }
    pub async fn is_stale_ballot(&self, ballot: Ballot) -> bool {
        let state = self.data.lock().await;
        state.ballot_num > ballot
    }

    pub async fn pmax(&self, pvalues: Vec<PValue>) {
        let mut state = self.data.lock().await;
        for pvalue in pvalues.iter() {
            let ballot = state.ballot_num;

            match state.proposals.entry(pvalue.slot()) {
                Entry::Vacant(v) => {
                    v.insert(pvalue.cmd());
                }
                Entry::Occupied(mut o) => {
                    if ballot < pvalue.ballot() {
                        o.insert(pvalue.cmd());
                    }
                }
            }
        }
    }

    pub async fn add(&self, slot: usize, cmd: PaxosCommand) {
        let mut state = self.data.lock().await;
        match state.proposals.entry(slot) {
            Entry::Vacant(v) => {
                v.insert(cmd);
            }
            Entry::Occupied(_) => {}
        }
    }

     pub async fn proposal(&self) -> ProposalsMap {
        let data = self.data.lock().await;
        data.proposals.clone()
    }

    pub async fn compact(&self, slots: &[usize]) {
        let mut state = self.data.lock().await;
        for s in slots {
            state.proposals.remove(s);
        }
    }

    pub async fn dump(&self) -> LeaderData {
        let data = self.data.lock().await;
        data.clone()
    }
}

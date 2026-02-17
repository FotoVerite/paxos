use std::collections::{BTreeMap, btree_map::Entry};

use uuid::Uuid;

use crate::{
    node::{
        classic_paxos::ballot::Ballot, pmmc::proposal::ProposalsStore, pvalue::PValue
    }, paxos_command::PaxosCommand,
};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct LeaderDurable {
    active: bool,
    proposals: ProposalsStore,
    ballot_num: Ballot,
}

impl Default for LeaderDurable {
    fn default() -> Self {
        Self {
            active: false,
            ballot_num: Ballot::default(),
            proposals: BTreeMap::new(),
        }
    }
}

impl LeaderDurable {
    pub fn init(id: Uuid, mut data: LeaderDurable) -> Self {
        data.ballot_num = data.ballot_num.init(id);
        data
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn set_as_active(&mut self) {
        self.active = true;
    }

    pub fn set_as_passive(&mut self) {
        self.active = false;
    }

    pub fn ballot(&self) -> Ballot {
        self.ballot_num
    }

    pub fn bump_ballot(&mut self, ballot: Ballot) -> Ballot {
        self.ballot_num = self.ballot_num.bump(ballot);
        self.ballot_num
    }

    pub fn is_stale_ballot(&self, ballot: Ballot) -> bool {
        self.ballot_num > ballot
    }

    pub fn pmax(&mut self, pvalues: Vec<PValue>) {
        for pvalue in pvalues.iter() {
            let ballot: Ballot = self.ballot_num;

            match self.proposals.entry(pvalue.slot()) {
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

    pub  fn add(&mut self, slot: usize, cmd: PaxosCommand) {
        match self.proposals.entry(slot) {
            Entry::Vacant(v) => {
                v.insert(cmd);
            }
            Entry::Occupied(_) => {}
        }
    }

    pub  fn proposal(&self) -> ProposalsStore {
        self.proposals.clone()
    }

    pub fn compact(&mut self, slots: &[usize]) {
        for s in slots {
            self.proposals.remove(s);
        }
    }

    pub async fn dump(&self) -> LeaderDurable {
        self.clone()
    }
}

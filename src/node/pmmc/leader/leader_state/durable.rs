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

    pub fn set_as_active(&mut self, pvalues: Vec<PValue>) {
        self.active = true;
        self.pmax(pvalues);
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
        // Scout already computes max-ballot pvalues per slot. Durable state only
        // needs to project those decisions into slot -> command.
        for pvalue in pvalues.iter() {
            self.proposals.insert(pvalue.slot(), pvalue.cmd());
        }
    }

    pub fn add(&mut self, slot: usize, cmd: PaxosCommand) -> bool {
        match self.proposals.entry(slot) {
            Entry::Vacant(v) => {
                v.insert(cmd);
                true
            }
            Entry::Occupied(_) => {false}
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

#[cfg(test)]
impl LeaderDurable {
    pub(crate) fn for_test(active: bool, ballot_num: Ballot, proposals: ProposalsStore) -> Self {
        Self {
            active,
            ballot_num,
            proposals,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use crate::{node::{classic_paxos::ballot::Ballot, pvalue::PValue}, paxos_command::PaxosCommand};

    use super::LeaderDurable;

    fn cmd(value: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: "k".to_string(),
            version: 1,
            value,
        }
    }

    #[test]
    fn pmax_overwrites_same_slot_without_leader_ballot_gating() {
        let leader_id = Uuid::new_v4();
        let a1 = Uuid::new_v4();
        let mut state = LeaderDurable {
            active: false,
            proposals: BTreeMap::new(),
            ballot_num: Ballot::new(100, leader_id),
        };

        state.add(3, cmd(10));
        let adopted = PValue::new(3, Ballot::new(5, a1), cmd(20));
        state.pmax(vec![adopted]);

        let proposals = state.proposal();
        assert_eq!(
            proposals.get(&3),
            Some(&cmd(20)),
            "PMMC leader durable merge should honor scout-selected pmax values"
        );
    }
}

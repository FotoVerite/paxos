use std::collections::{BTreeMap, btree_map::Entry};

use uuid::Uuid;

use crate::{
    common::ballot::Ballot,
    node::{pmmc::proposal::ProposalsStore, pvalue::PValue},
    paxos_command::PaxosCommand,
};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct LeaderDurable {
    active: bool,
    proposals: ProposalsStore,
    ballot_counter: usize,
}

impl Default for LeaderDurable {
    fn default() -> Self {
        Self {
            active: false,
            ballot_counter: 0,
            proposals: BTreeMap::new(),
        }
    }
}

impl LeaderDurable {
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

    pub fn ballot(&self, id: Uuid, epoch: usize) -> Ballot {
        Ballot::with_epoch(self.ballot_counter, id, epoch)
    }

    pub fn bump_ballot(&mut self, id: Uuid, epoch: usize, ballot: Ballot) -> Ballot {
        let current = self.ballot(id, epoch);
        let next = current.bump(ballot);
        self.ballot_counter = next.number;
        next
    }

    pub fn is_stale_ballot(&self, id: Uuid, epoch: usize, ballot: Ballot) -> bool {
        self.ballot(id, epoch) > ballot
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
            Entry::Occupied(_) => false,
        }
    }

    pub fn proposal(&self) -> ProposalsStore {
        self.proposals.clone()
    }

    pub async fn dump(&self) -> LeaderDurable {
        self.clone()
    }
}

#[cfg(test)]
impl LeaderDurable {
    pub(crate) fn for_test(active: bool, ballot_counter: usize, proposals: ProposalsStore) -> Self {
        Self {
            active,
            ballot_counter,
            proposals,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use crate::{common::ballot::Ballot, node::pvalue::PValue, paxos_command::PaxosCommand};

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
        let a1 = Uuid::new_v4();
        let mut state = LeaderDurable {
            active: false,
            proposals: BTreeMap::new(),
            ballot_counter: 100,
        };

        state.add(3, cmd(10));
        let adopted = PValue::new(3, Ballot::with_epoch(5, a1, 0), cmd(20));
        state.pmax(vec![adopted]);

        let proposals = state.proposal();
        assert_eq!(
            proposals.get(&3),
            Some(&cmd(20)),
            "PMMC leader durable merge should honor scout-selected pmax values"
        );
    }
}

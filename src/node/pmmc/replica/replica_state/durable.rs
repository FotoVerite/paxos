use std::collections::{btree_map::Entry, BTreeMap, HashMap};

use crate::{
    node::{
        pmmc::proposal::ProposalsStore,
        pvalue::PValue,
    },
    paxos_command::{ClientId, PaxosCommand, RequestId},
    rsm::kv_store::ReplyOutcome,
};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ReplicaDurable {
    next_slot: usize,
    next_execute_slot: usize,
    cache: HashMap<(ClientId, RequestId), Option<ReplyOutcome>>,
    proposals: ProposalsStore,
    decisions: ProposalsStore,
}

impl Default for ReplicaDurable {
    fn default() -> Self {
        Self {
            next_slot: 0,
            next_execute_slot: 0,
            cache: HashMap::new(),
            decisions: BTreeMap::new(),
            proposals: BTreeMap::new(),
        }
    }
}

impl ReplicaDurable {
    pub fn proposals(&self) -> ProposalsStore {
        self.proposals.clone()
    }

    pub fn proposal_slot(&self) -> usize {
        self.next_slot
    }

    pub fn increment_proposals(&mut self) -> usize {
        self.next_slot += 1;
        self.next_slot
    }

    pub fn execution_slot(&self) -> usize {
        self.next_execute_slot
    }

    pub fn increment_decisions(&mut self) -> usize {
        self.next_execute_slot += 1;
        self.next_execute_slot
    }

    pub fn add_proposal(&mut self, cmd: PaxosCommand) {
        self.proposals.insert(self.proposal_slot(), cmd.clone());
        if let Some(client_key) = cmd.client_identity() {
            self.cache.insert(client_key, None);
        }
        self.increment_proposals();
    }

    pub fn add_decision(&mut self, pvalue: PValue) {
        if self.next_execute_slot > pvalue.slot() {
            return;
        }
        if let Some(proposal) = self.proposals.remove(&pvalue.slot()) {
            if proposal != pvalue.cmd() {
                self.add_proposal(proposal);
            }
        }
        self.decisions.insert(pvalue.slot(), pvalue.cmd());
    }

    pub fn next_decision(&mut self) -> Option<PaxosCommand> {
        match self.decisions.entry(self.next_execute_slot) {
            Entry::Occupied(o) => Some(o.get().clone()),
            Entry::Vacant(_) => None,
        }
    }

    pub fn is_cached(&self, cmd: &PaxosCommand) -> (bool, Option<ReplyOutcome>) {
        let Some(client_key) = cmd.client_identity() else {
            return (false, None);
        };
        if self.cache.contains_key(&client_key) {
            (true, self.cache.get(&client_key).and_then(|v| v.clone()))
        } else {
            (false, None)
        }
    }

    pub async fn dump(&self) -> ReplicaDurable {
        self.clone()
    }
}

use std::{
    any,
    collections::{BTreeMap, HashMap, VecDeque, btree_map::Entry},
    sync::Arc,
};

use uuid::Uuid;

use crate::{
    node::{
        pmmc::proposal::{Proposal, ProposalsStore},
        pvalue::{self, PValue},
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
        self.cache.insert(cmd.client_identity().unwrap(), None);
        self.increment_proposals();
    }

    pub fn add_decision(&mut self, pvalue: PValue) {
        if self.next_execute_slot > pvalue.slot() {
            return;
        }
        self.decisions.insert(pvalue.slot(), pvalue.cmd());
    }

    pub fn next_decision(&mut self) -> Option<PaxosCommand> {
        match self.decisions.entry(self.next_execute_slot) {
            Entry::Occupied(o) => Some(o.get().clone()),
            Entry::Vacant(v) => return None,
        }
    }

    pub fn is_cached(&self, cmd: &PaxosCommand) -> (bool, Option<ReplyOutcome>) {
        if self.cache.contains_key(&cmd.client_identity().unwrap()) {
            (
                true,
                self.cache
                    .get(&cmd.client_identity().unwrap())
                    .unwrap()
                    .clone(),
            )
        } else {
            (false, None)
        }
    }

    pub async fn dump(&self) -> ReplicaDurable {
        self.clone()
    }
}

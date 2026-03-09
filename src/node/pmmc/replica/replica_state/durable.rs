use std::collections::{BTreeMap, HashMap, btree_map::Entry};

use crate::{
    cluster::cluster_configuration::ConfigurationStrategy,
    node::{pmmc::proposal::ProposalsStore, pvalue::PValue},
    paxos_command::{ClientId, PaxosCommand, RequestId},
    rsm::kv_store::ReplyOutcome,
};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ReplicaDurable {
    next_slot: usize,
    next_execute_slot: usize,
    cache: HashMap<(ClientId, RequestId), Option<ReplyOutcome>>,
    alpha_max_inflight: usize,
    proposals: ProposalsStore,
    decisions: ProposalsStore,
    pub strategy: ConfigurationStrategy,
    pub stop_slot: Option<usize>,
}

impl Default for ReplicaDurable {
    fn default() -> Self {
        Self {
            next_slot: 0,
            next_execute_slot: 0,
            strategy: ConfigurationStrategy::JointConsensus,
            cache: HashMap::new(),
            decisions: BTreeMap::new(),
            proposals: BTreeMap::new(),
            alpha_max_inflight: 0,
            stop_slot: None,
        }
    }
}

pub enum ReconfigurationStrategyInEffect {
    None,
    StopSign { slot: usize, delayed: usize },
    Padding,
    BrickWall,
}

impl ReplicaDurable {
    pub fn set_alpha(&mut self, alpha_max_inflight: usize) {
        self.alpha_max_inflight = alpha_max_inflight;
    }

    pub fn set_starting_slots(&mut self, slot: usize) {
        if self.next_slot != 0 || self.next_execute_slot != 0 {
            panic!("Cannot update starting on non init calls")
        }
        if !self.proposals.is_empty() || !self.decisions.is_empty() {
            panic!("Cannot update starting on non init calls")
        }
        self.next_slot = slot;
        self.next_execute_slot = slot;
    }

    pub fn set_strategy(&mut self, strategy: ConfigurationStrategy) {
        self.strategy = strategy;
    }

    pub fn reconfiguration_strategy_in_effect(&self) -> ReconfigurationStrategyInEffect {
        if let Some(slot) = self.stop_slot {
            return match self.strategy {
                // Control-plane STOP can be issued before strategy-specific
                // reconfiguration paths are wired. Treat it as an immediate stop
                // instead of panicking the replica task.
                ConfigurationStrategy::JointConsensus => {
                    ReconfigurationStrategyInEffect::StopSign { slot, delayed: 0 }
                }
                ConfigurationStrategy::StopSign => {
                    ReconfigurationStrategyInEffect::StopSign { slot, delayed: 0 }
                }
                ConfigurationStrategy::DelayedStopSign => {
                    ReconfigurationStrategyInEffect::StopSign { slot, delayed: 0 }
                }
                ConfigurationStrategy::Padding => ReconfigurationStrategyInEffect::Padding,
                ConfigurationStrategy::BrickWall => ReconfigurationStrategyInEffect::BrickWall,
            };
        }
        return ReconfigurationStrategyInEffect::None;
    }

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

    pub fn max_inflight_exceeded(&self) -> bool {
        self.alpha_max_inflight < self.proposals.len()
    }

    pub fn inflight(&self) -> usize {
        self.proposals.len()
    }

    pub fn execution_slot(&self) -> usize {
        self.next_execute_slot
    }

    pub fn increment_decisions(&mut self) -> usize {
        self.next_execute_slot += 1;
        self.next_execute_slot
    }

    pub fn add_proposal(&mut self, cmd: PaxosCommand) -> PaxosCommand {
        let cmd = {
            match self.reconfiguration_strategy_in_effect() {
                ReconfigurationStrategyInEffect::Padding => PaxosCommand::NOOP,
                _ => cmd,
            }
        };
        self.proposals.insert(self.proposal_slot(), cmd.clone());
        if let Some(client_key) = cmd.client_identity() {
            self.cache.insert(client_key, None);
        }
        self.increment_proposals();
        cmd
    }

    pub fn add_decision(&mut self, pvalue: PValue) -> bool {
        if self.next_execute_slot > pvalue.slot() {
            return false;
        }
        // Learned decisions advance the proposal watermark so a failover replica
        // does not repropose an already-decided slot.
        self.next_slot = self.next_slot.max(pvalue.slot() + 1);
        if let Some(proposal) = self.proposals.remove(&pvalue.slot()) {
            if proposal != pvalue.cmd() {
                self.add_proposal(proposal);
            }
        }
        self.decisions.insert(pvalue.slot(), pvalue.cmd());
        if pvalue.cmd().operation() == &PaxosCommand::STOP {
            self.stop_slot = Some(pvalue.slot());
        }
        return true;
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

    pub fn pending_slot_for(&self, cmd: &PaxosCommand) -> Option<usize> {
        let target_identity = cmd.client_identity()?;
        self.proposals.iter().find_map(|(slot, proposal)| {
            (proposal.client_identity() == Some(target_identity)).then_some(*slot)
        })
    }

    pub fn add_to_cache(&mut self, cmd: &PaxosCommand) {
        if let Some(identity) = cmd.client_identity() {
            self.cache.insert(identity, None);
        }
    }

    pub fn update_cache(&mut self, cmd: &PaxosCommand, response: ReplyOutcome) {
        if let Some(identity) = cmd.client_identity() {
            self.cache.insert(identity, Some(response));
        }
    }

    pub fn client_dedup_watermark(&self) -> HashMap<ClientId, RequestId> {
        let mut dedup: HashMap<ClientId, RequestId> = HashMap::new();
        for ((client_id, request_id), response) in &self.cache {
            if response.is_none() {
                continue;
            }
            dedup
                .entry(*client_id)
                .and_modify(|current| *current = (*current).max(*request_id))
                .or_insert(*request_id);
        }
        dedup
    }

    pub async fn dump(&self) -> ReplicaDurable {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{paxos_command::PaxosCommand, rsm::kv_store::ReplyOutcome};

    use super::ReplicaDurable;

    #[test]
    fn dedup_watermark_ignores_pending_cache_entries() {
        let mut durable = ReplicaDurable::default();
        let client_id = Uuid::new_v4();
        let req1 = PaxosCommand::NOOP.with_client(client_id, 1);
        let req2 = PaxosCommand::NOOP.with_client(client_id, 2);

        durable.add_to_cache(&req1);
        assert!(durable.client_dedup_watermark().is_empty());

        durable.update_cache(&req1, ReplyOutcome::Ok);
        assert_eq!(durable.client_dedup_watermark().get(&client_id), Some(&1));

        durable.add_to_cache(&req2);
        assert_eq!(durable.client_dedup_watermark().get(&client_id), Some(&1));
    }
}

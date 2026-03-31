use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use crate::{
    cluster::vertical::{
        activation::ActivationSnapshot,
        configuration::{VerticalClusterConfiguration, VerticalPaxosVariant},
    },
    common::ballot::Ballot,
    node::{pvalue::PValue, vertical_paxos::message::VerticalPaxosMessage},
};

use super::scout_round::ScoutRound;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveredState {
    settled_values: HashMap<usize, PValue>,
    start_index: usize,
}

impl RecoveredState {
    pub fn new(settled_values: HashMap<usize, PValue>, start_index: usize) -> Self {
        Self {
            settled_values,
            start_index,
        }
    }

    pub fn settled_values(&self) -> &HashMap<usize, PValue> {
        &self.settled_values
    }

    pub fn start_index(&self) -> usize {
        self.start_index
    }
}

pub struct RecoverySynchronizer {
    leader_id: uuid::Uuid,
    peers: Arc<crate::node::vertical_paxos::transport::VerticalPaxosHandle>,
    snapshot: Arc<ActivationSnapshot>,
    recovery_configs: Vec<Arc<VerticalClusterConfiguration>>,
    next_round: usize,
    active_round: Option<ScoutRound>,
    settled_values: HashMap<usize, PValue>,
    recovered_state: Option<RecoveredState>,
    superseded_by: Option<Ballot>,
}

impl RecoverySynchronizer {
    pub fn new(
        leader_id: uuid::Uuid,
        peers: Arc<crate::node::vertical_paxos::transport::VerticalPaxosHandle>,
        snapshot: Arc<ActivationSnapshot>,
    ) -> Self {
        Self {
            leader_id,
            peers,
            recovery_configs: recovery_configs(&snapshot),
            snapshot,
            next_round: 0,
            active_round: None,
            settled_values: HashMap::new(),
            recovered_state: None,
            superseded_by: None,
        }
    }

    pub async fn start(&mut self) {
        self.next_round = 0;
        self.settled_values.clear();
        self.recovered_state = None;
        self.superseded_by = None;
        self.shutdown_active_round().await;
        self.start_next_round().await;
    }

    pub fn is_complete(&self) -> bool {
        self.recovered_state.is_some()
    }

    pub fn superseded_by(&self) -> Option<Ballot> {
        self.superseded_by
    }

    pub fn recovered_state(&self) -> Option<&RecoveredState> {
        self.recovered_state.as_ref()
    }

    pub async fn handle_message(&mut self, msg: &VerticalPaxosMessage) {
        let Some(round) = self.active_round.as_ref() else {
            return;
        };

        round.handle_message(msg).await;

        if let Some(superseded_by) = round.superseded_by().await {
            self.superseded_by = Some(superseded_by);
            self.shutdown_active_round().await;
            return;
        }

        if round.is_complete().await {
            self.merge_pvalues(round.settled_values().await);
            self.shutdown_active_round().await;
            self.next_round += 1;
            self.start_next_round().await;
        }
    }

    pub async fn shutdown(&mut self) {
        self.shutdown_active_round().await;
    }

    async fn start_next_round(&mut self) {
        if let Some(configuration) = self.recovery_configs.get(self.next_round).cloned() {
            self.active_round = Some(ScoutRound::new(
                self.leader_id,
                Arc::clone(&self.peers),
                self.snapshot.configuration().ballot(),
                effective_start_index(self.snapshot.configuration()),
                configuration
                    .read_quorum()
                    .members()
                    .iter()
                    .copied()
                    .collect(),
            ));
            return;
        }

        self.recovered_state = Some(RecoveredState::new(
            self.settled_values.clone(),
            effective_start_index(self.snapshot.configuration()),
        ));
    }

    async fn shutdown_active_round(&mut self) {
        if let Some(round) = self.active_round.take() {
            round.shutdown().await;
        }
    }

    fn merge_pvalues(&mut self, pvalues: HashMap<usize, PValue>) {
        for (slot, pvalue) in pvalues {
            match self.settled_values.entry(slot) {
                Entry::Vacant(entry) => {
                    entry.insert(pvalue);
                }
                Entry::Occupied(mut entry) => {
                    if entry.get().ballot() < pvalue.ballot() {
                        entry.insert(pvalue);
                    }
                }
            }
        }
    }
}

fn recovery_configs(snapshot: &ActivationSnapshot) -> Vec<Arc<VerticalClusterConfiguration>> {
    let mut configs = vec![Arc::clone(snapshot.configuration())];
    let complete_bal = snapshot.configuration().complete_bal();

    for configuration in snapshot.predecessor_chain().entries().iter().cloned() {
        if let Some(complete_bal) = complete_bal {
            if configuration.ballot() <= complete_bal {
                break;
            }
        }
        configs.push(configuration);
    }

    configs
}

fn effective_start_index(configuration: &VerticalClusterConfiguration) -> usize {
    match configuration.variant() {
        VerticalPaxosVariant::V1 => 0,
        VerticalPaxosVariant::V2 => configuration.start_index(),
    }
}

#[cfg(test)]
mod tests;

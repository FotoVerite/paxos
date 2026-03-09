use std::sync::Arc;

use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::{
    cluster::cluster_configuration::ClusterConfiguration,
    node::{
        pmmc::{
            proposal::ProposalsStore,
            replica::replica_state::{
                durable::{ReconfigurationStrategyInEffect, ReplicaDurable},
                volatile::ReplicaVolatile,
            },
        },
        pvalue::PValue,
    },
    paxos_command::PaxosCommand,
    rsm::kv_store::ReplyOutcome,
};

pub mod durable;
mod volatile;

pub enum ReplicaAdmissionOutcome {
    Admitted {
        slot: usize,
        cmd: PaxosCommand,
    },
    Queued,
    Stopped,
    CachedResponse {
        client_id: Uuid,
        request_id: u64,
        response: Option<ReplyOutcome>,
    },
}

pub struct ReplicaStateData {
    durable: ReplicaDurable,
    volatile: ReplicaVolatile,
}

impl Default for ReplicaStateData {
    fn default() -> Self {
        Self {
            durable: ReplicaDurable::default(),
            volatile: ReplicaVolatile::default(),
        }
    }
}

pub struct ReplicaState {
    data: Mutex<ReplicaStateData>,
    decision_notify: Notify,
}

impl ReplicaState {
    pub fn init(mut data: ReplicaDurable, configuration: Arc<ClusterConfiguration>) -> Self {
        data.set_alpha(configuration.alpha_max_inflight());
        data.set_starting_slots(configuration.starting_slot());
        data.set_strategy(configuration.strategy());
        return Self {
            data: Mutex::new(ReplicaStateData {
                durable: data,
                volatile: ReplicaVolatile::default(),
            }),
            decision_notify: Notify::new(),
        };
    }

    pub async fn proposal(&self) -> ProposalsStore {
        let state = self.data.lock().await;
        state.durable.proposals()
    }

    pub async fn proposal_slot(&self) -> usize {
        let state = self.data.lock().await;
        state.durable.proposal_slot()
    }

    pub async fn execution_slot(&self) -> usize {
        let state = self.data.lock().await;
        state.durable.execution_slot()
    }

    pub async fn max_inflight_exceeded(&self) -> bool {
        let state = self.data.lock().await;
        state.durable.max_inflight_exceeded()
    }

    pub async fn is_stopped(&self) -> bool {
        let state = self.data.lock().await;
        match state.durable.reconfiguration_strategy_in_effect() {
            ReconfigurationStrategyInEffect::None => false,
            ReconfigurationStrategyInEffect::StopSign { slot, delayed } => true,
            ReconfigurationStrategyInEffect::Padding => true,
            ReconfigurationStrategyInEffect::BrickWall => true,
        }
    }

    pub async fn wait_for_decision(&self) {
        self.decision_notify.notified().await;
    }

    pub async fn proposal_handler(&self, cmd: PaxosCommand) -> ReplicaAdmissionOutcome {
        let client_id = cmd.client_id();
        let request_id = cmd.request_id();

        let mut state = self.data.lock().await;
        let (cached, response) = state.durable.is_cached(&cmd);

        if !cached {
            match state.durable.reconfiguration_strategy_in_effect() {
                ReconfigurationStrategyInEffect::BrickWall => {
                    return ReplicaAdmissionOutcome::Stopped;
                }
                _ => {}
            }
            let slot = state.durable.proposal_slot();
            if state.durable.max_inflight_exceeded() {
                state.volatile.queued.push_back(cmd.clone());
                state.durable.add_to_cache(&cmd);
                return ReplicaAdmissionOutcome::Queued;
            }
            let cmd = state.durable.add_proposal(cmd.clone());
            return ReplicaAdmissionOutcome::Admitted { slot, cmd };
        }

        return ReplicaAdmissionOutcome::CachedResponse {
            client_id,
            request_id,
            response,
        };
    }

    pub async fn add_decision(&self, pvalue: PValue) -> Option<ReplicaAdmissionOutcome> {
        let admitted = {
            let mut state = self.data.lock().await;
            match state.durable.reconfiguration_strategy_in_effect() {
                ReconfigurationStrategyInEffect::BrickWall => {
                    return Some(ReplicaAdmissionOutcome::Stopped);
                }
                _ => {}
            }

            if state.durable.add_decision(pvalue) {
                if let Some(cmd) = state.volatile.queued.pop_front() {
                    let slot = state.durable.proposal_slot();
                    let cmd = state.durable.add_proposal(cmd.clone());
                    return Some(ReplicaAdmissionOutcome::Admitted { slot, cmd });
                }
            }
            None
        };
        self.decision_notify.notify_one();
        return admitted;
    }

    pub async fn increment_execution_slot(&self) {
        let mut state = self.data.lock().await;
        state.durable.increment_decisions();
    }

    pub async fn next_decision(&self) -> Option<PaxosCommand> {
        let mut state = self.data.lock().await;
        state.durable.next_decision()
    }

    pub async fn is_cached(&self, cmd: &PaxosCommand) -> (bool, Option<ReplyOutcome>) {
        let state = self.data.lock().await;
        state.durable.is_cached(cmd)
    }

    pub async fn update_cache(&self, cmd: &PaxosCommand, response: ReplyOutcome) {
        let mut state = self.data.lock().await;
        state.durable.update_cache(cmd, response);
    }

    pub async fn client_dedup_watermark(&self) -> std::collections::HashMap<Uuid, u64> {
        let state = self.data.lock().await;
        state.durable.client_dedup_watermark()
    }

    pub async fn dump(&self) -> ReplicaDurable {
        let state = self.data.lock().await;
        state.durable.clone()
    }
}

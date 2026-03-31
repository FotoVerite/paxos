use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::info;
use uuid::Uuid;

use crate::cluster::pmmc::PmmcCluster;
use crate::monitor::{Event, PaxosObserver, current_timestamp_millis};
use crate::web::websocket_observer::WebSocketObserver;

use super::client_pool::ClientPool;
use super::runner::{PmmcScenarioContext, PmmcScenarioRunState};
use super::spec::{PmmcAction, PmmcTrigger};

#[derive(Default)]
pub(crate) struct RunnerMemory {
    pub current_leader_target: Option<(usize, Uuid)>,
}

#[derive(Clone, Copy)]
pub(crate) enum PmmcTriggerKind {
    OnStart,
    AfterSuccesses,
    AfterAttempts,
    AfterTimeouts,
}

pub(crate) async fn run_triggered_actions(
    context: &PmmcScenarioContext,
    cluster_for_runner: &Arc<Mutex<PmmcCluster>>,
    observer_for_runner: &Arc<WebSocketObserver>,
    pool: &mut ClientPool,
    executed_actions: &mut HashSet<usize>,
    memory: &mut RunnerMemory,
    state: &PmmcScenarioRunState,
    trigger_kind: PmmcTriggerKind,
) -> anyhow::Result<()> {
    for (index, rule) in context.spec.actions.iter().enumerate() {
        if executed_actions.contains(&index) || !trigger_matches(&rule.when, trigger_kind, state) {
            continue;
        }

        apply_action(
            cluster_for_runner,
            observer_for_runner,
            pool,
            memory,
            &rule.action,
        )
        .await?;

        executed_actions.insert(index);
    }

    Ok(())
}

async fn apply_action(
    cluster_for_runner: &Arc<Mutex<PmmcCluster>>,
    observer_for_runner: &Arc<WebSocketObserver>,
    pool: &mut ClientPool,
    memory: &mut RunnerMemory,
    action: &PmmcAction,
) -> anyhow::Result<()> {
    match action {
        PmmcAction::CrashNode { node_index } => {
            if let Some(uuid) = cluster_for_runner
                .lock()
                .await
                .crash_node(*node_index)
                .await
            {
                info!(node_index, %uuid, "PMMC action: crashed node");
            }
        }
        PmmcAction::CrashCurrentLeader => {
            if let Some(idx) = current_leader_index(cluster_for_runner).await {
                if let Some(uuid) = cluster_for_runner.lock().await.crash_node(idx).await {
                    info!(node_index = idx, %uuid, "PMMC action: crashed current leader");
                }
            }
        }
        PmmcAction::IsolateNode { node_index } => {
            if let Some((uuid, all_uuids)) = isolate_node(cluster_for_runner, *node_index).await {
                emit_partition_created(observer_for_runner, all_uuids, vec![uuid]);
                info!(node_index, %uuid, "PMMC action: isolated node");
            }
        }
        PmmcAction::IsolateCurrentLeader => {
            if let Some(idx) = current_leader_index(cluster_for_runner).await {
                if let Some((uuid, all_uuids)) = isolate_node(cluster_for_runner, idx).await {
                    emit_partition_created(observer_for_runner, all_uuids, vec![uuid]);
                    memory.current_leader_target = Some((idx, uuid));
                    info!(node_index = idx, %uuid, "PMMC action: isolated current leader");
                }
            }
        }
        PmmcAction::HealNode { node_index } => {
            if let Some(uuid) = cluster_for_runner.lock().await.heal_node(*node_index).await {
                emit_partition_healed(observer_for_runner, vec![uuid]);
                info!(node_index, %uuid, "PMMC action: healed node");
            }
        }
        PmmcAction::HealCurrentLeader => {
            if let Some((idx, uuid)) = memory.current_leader_target.take() {
                if let Some(healed) = cluster_for_runner.lock().await.heal_node(idx).await {
                    emit_partition_healed(observer_for_runner, vec![uuid]);
                    info!(node_index = idx, %healed, "PMMC action: healed current leader");
                }
            }
        }
        PmmcAction::MoveClient {
            client_id,
            replica_index,
        } => {
            if pool.move_client(client_id, *replica_index) {
                info!(client_id, replica_index, "PMMC action: moved client");
            }
        }
        PmmcAction::SleepMs { millis } => {
            sleep(Duration::from_millis(*millis)).await;
        }
        PmmcAction::EmitNote { note } => {
            info!(note, "PMMC action note");
        }
    }

    Ok(())
}

fn trigger_matches(
    trigger: &PmmcTrigger,
    trigger_kind: PmmcTriggerKind,
    state: &PmmcScenarioRunState,
) -> bool {
    match (trigger_kind, trigger) {
        (PmmcTriggerKind::OnStart, PmmcTrigger::OnStart) => true,
        (PmmcTriggerKind::AfterSuccesses, PmmcTrigger::AfterSuccesses { count }) => {
            state.success_count >= *count
        }
        (PmmcTriggerKind::AfterAttempts, PmmcTrigger::AfterAttempts { count }) => {
            state.attempt_count >= *count
        }
        (PmmcTriggerKind::AfterTimeouts, PmmcTrigger::AfterTimeouts { count }) => {
            state.timeout_count >= *count
        }
        _ => false,
    }
}

async fn current_leader_index(cluster_for_runner: &Arc<Mutex<PmmcCluster>>) -> Option<usize> {
    let cluster = cluster_for_runner.lock().await;
    cluster.leader_index().await
}

async fn isolate_node(
    cluster_for_runner: &Arc<Mutex<PmmcCluster>>,
    node_index: usize,
) -> Option<(Uuid, Vec<Uuid>)> {
    let cluster = cluster_for_runner.lock().await;
    let isolated = cluster.isolate_node(node_index).await?;
    Some((isolated, cluster.get_node_uuids()))
}

fn emit_partition_created(
    observer_for_runner: &Arc<WebSocketObserver>,
    all_uuids: Vec<Uuid>,
    partition_b: Vec<Uuid>,
) {
    let partition_a: Vec<Uuid> = all_uuids
        .into_iter()
        .filter(|uuid| !partition_b.contains(uuid))
        .collect();
    observer_for_runner.on_event(Event::PartitionCreated {
        partition_a,
        partition_b,
        created_at: current_timestamp_millis(),
    });
}

fn emit_partition_healed(observer_for_runner: &Arc<WebSocketObserver>, partition_b: Vec<Uuid>) {
    observer_for_runner.on_event(Event::PartitionHealed {
        partition_a: vec![],
        partition_b,
        created_at: current_timestamp_millis(),
    });
}

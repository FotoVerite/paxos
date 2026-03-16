use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Mutex, broadcast, mpsc::error::TryRecvError};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use uuid::Uuid;

use crate::cluster::cluster_configuration::reconfig_patch::ReconfigPatch;
use crate::cluster::pmmc_cluster::PmmcCluster;
use crate::message::ClientMessage;
use crate::monitor::{Event, PaxosObserver, ReconfigurationPhase, current_timestamp_millis};
use crate::paxos_command::PaxosCommand;
use crate::web::websocket_observer::WebSocketObserver;

use super::actions::{PmmcTriggerKind, RunnerMemory, run_triggered_actions};
use super::client_pool::{ClientPool, ClientSession};
use super::completion::{CompletionReason, log_completion};
use super::reconfiguration::PmmcReconfigurationSpec;
use super::spec::{PmmcRequestPlanKind, PmmcScenarioSpec};

struct PendingRequest {
    session_index: usize,
    client_id: String,
    replica_index: usize,
    request_id: u64,
    command: PaxosCommand,
}

enum AwaitOutcome {
    Message(ClientMessage),
    Timeout,
}

struct ReconfigurationTaskResult {
    strategy: crate::cluster::cluster_configuration::ConfigurationStrategy,
    leader_index: usize,
    checkpoint_last_applied_slot: Option<usize>,
    active_nodes: Vec<Uuid>,
    node_count: usize,
}

#[derive(Debug, Clone)]
pub struct PmmcScenarioContext {
    pub scenario_run_id: Uuid,
    pub duration_secs: u64,
    pub spec: PmmcScenarioSpec,
}

#[derive(Debug, Clone, Default)]
pub struct PmmcScenarioRunState {
    pub success_count: usize,
    pub attempt_count: usize,
    pub timeout_count: usize,
}

pub struct PmmcScenarioExecution {
    context: PmmcScenarioContext,
    cluster: Arc<Mutex<PmmcCluster>>,
    observer: Arc<WebSocketObserver>,
    pool: ClientPool,
    executed_actions: HashSet<usize>,
    memory: RunnerMemory,
    state: PmmcScenarioRunState,
    start: Instant,
}

impl PmmcScenarioExecution {
    const RECONFIG_TIMEOUT: Duration = Duration::from_secs(30);

    pub fn new(
        scenario_run_id: Uuid,
        duration_secs: u64,
        spec: PmmcScenarioSpec,
        cluster: Arc<Mutex<PmmcCluster>>,
        observer: Arc<WebSocketObserver>,
    ) -> Self {
        Self {
            pool: ClientPool::new(&spec.clients),
            context: PmmcScenarioContext {
                scenario_run_id,
                duration_secs,
                spec,
            },
            cluster,
            observer,
            executed_actions: HashSet::new(),
            memory: RunnerMemory::default(),
            state: PmmcScenarioRunState::default(),
            start: Instant::now(),
        }
    }

    pub async fn run(mut self, mut stop_rx: broadcast::Receiver<()>) {
        let timings = self.context.spec.timings.clone();
        let target_successes = self.context.spec.completion.target_successes();
        let wait_for_reply = self.context.spec.request_plan.wait_for_reply;
        let mut completion_reason: Option<CompletionReason> = None;
        let mut reconfiguration_pending = self.context.spec.reconfiguration.is_some();
        let mut reconfiguration_task: Option<
            JoinHandle<anyhow::Result<ReconfigurationTaskResult>>,
        > = None;
        let mut reconfiguration_strategy_inflight: Option<
            crate::cluster::cluster_configuration::ConfigurationStrategy,
        > = None;

        if self.pool.is_empty() {
            warn!(%self.context.scenario_run_id, "PMMC scenario has no clients");
            return;
        }

        if let Err(err) = self.run_actions(PmmcTriggerKind::OnStart, "on_start").await {
            warn!(%self.context.scenario_run_id, error = %err, "Failed to start PMMC scenario");
            return;
        }

        {
            let ready_timeout = Duration::from_millis(timings.initial_settle_ms.max(2_000));
            let cluster = self.cluster.lock().await;
            if let Err(err) = cluster.wait_ready(ready_timeout).await {
                warn!(
                    %self.context.scenario_run_id,
                    error = %err,
                    "PMMC cluster did not reach ready state before scenario loop"
                );
                return;
            }
        }

        if timings.initial_settle_ms > 0 {
            sleep(Duration::from_millis(timings.initial_settle_ms)).await;
        }

        loop {
            if let Some(reason) = self.completion_check(&mut stop_rx, target_successes) {
                completion_reason = Some(reason);
                break;
            }

            if !wait_for_reply {
                if let Some(task) = reconfiguration_task.as_mut() {
                    if task.is_finished() {
                        let completed = reconfiguration_task
                            .take()
                            .expect("reconfiguration task should exist when finished")
                            .await;
                        match completed {
                            Ok(Ok(result)) => {
                                self.emit_reconfiguration_event(
                                    Event::ReconfigurationCheckpointSelected {
                                        strategy: result.strategy,
                                        source_node: None,
                                        last_applied_slot: result.checkpoint_last_applied_slot,
                                        created_at: current_timestamp_millis(),
                                    },
                                );
                                let leader = result.active_nodes.get(result.leader_index).copied();
                                self.emit_reconfiguration_event(Event::ReconfigurationReady {
                                    strategy: result.strategy,
                                    leader,
                                    active_nodes: result.active_nodes,
                                    created_at: current_timestamp_millis(),
                                });
                                self.pool.normalize_replica_indexes(result.node_count);
                                self.pool.detach_all();
                                reconfiguration_strategy_inflight = None;
                            }
                            Ok(Err(err)) => {
                                self.emit_reconfiguration_event(Event::ReconfigurationFailed {
                                    strategy: reconfiguration_strategy_inflight,
                                    phase: ReconfigurationPhase::Stop,
                                    reason: err.to_string(),
                                    created_at: current_timestamp_millis(),
                                });
                                warn!(
                                    %self.context.scenario_run_id,
                                    scenario = self.context.spec.name,
                                    error = %err,
                                    "Failed to apply PMMC scenario reconfiguration"
                                );
                                return;
                            }
                            Err(err) => {
                                self.emit_reconfiguration_event(Event::ReconfigurationFailed {
                                    strategy: reconfiguration_strategy_inflight,
                                    phase: ReconfigurationPhase::Stop,
                                    reason: err.to_string(),
                                    created_at: current_timestamp_millis(),
                                });
                                warn!(
                                    %self.context.scenario_run_id,
                                    scenario = self.context.spec.name,
                                    error = %err,
                                    "PMMC scenario reconfiguration task join failed"
                                );
                                return;
                            }
                        }
                    }
                }

                if let Err(reason) = self.drain_ready_responses().await {
                    completion_reason = Some(reason);
                    break;
                }

                if reconfiguration_pending
                    && self.state.success_count > 0
                    && reconfiguration_task.is_none()
                {
                    match self.begin_reconfiguration_task().await {
                        Ok((strategy, task)) => {
                            reconfiguration_strategy_inflight = Some(strategy);
                            reconfiguration_task = Some(task);
                            reconfiguration_pending = false;
                        }
                        Err(err) => {
                            self.emit_reconfiguration_event(Event::ReconfigurationFailed {
                                strategy: reconfiguration_strategy_inflight,
                                phase: ReconfigurationPhase::Stop,
                                reason: err.to_string(),
                                created_at: current_timestamp_millis(),
                            });
                            warn!(
                                %self.context.scenario_run_id,
                                scenario = self.context.spec.name,
                                error = %err,
                                "Failed to apply PMMC scenario reconfiguration"
                            );
                            return;
                        }
                    }
                }

                if self.state.success_count < target_successes {
                    let request = match self.prepare_request().await {
                        Ok(request) => request,
                        Err(reason) => {
                            completion_reason = Some(reason);
                            break;
                        }
                    };

                    if let Err(reason) = self.dispatch_request(&request).await {
                        if reconfiguration_task.is_some() {
                            if matches!(reason, CompletionReason::ClientChannelClosed(_)) {
                                self.pool.session_mut(request.session_index).detach();
                                sleep(Duration::from_millis(timings.retry_backoff_ms)).await;
                                continue;
                            }
                        }
                        completion_reason = Some(reason);
                        break;
                    }
                }

                sleep(Duration::from_millis(timings.loop_interval_ms)).await;
                continue;
            }

            let request = match self.prepare_request().await {
                Ok(request) => request,
                Err(reason) => {
                    completion_reason = Some(reason);
                    break;
                }
            };

            if let Err(reason) = self.dispatch_request(&request).await {
                completion_reason = Some(reason);
                break;
            }

            match self
                .await_response(&mut stop_rx, &request, timings.response_timeout_ms)
                .await
            {
                Ok(AwaitOutcome::Message(client_message)) => {
                    if let Err(reason) = self.handle_response(&request, client_message).await {
                        completion_reason = Some(reason);
                        break;
                    }

                    // Reconfiguration demos should show at least one successful
                    // command before control-plane stop/restart is issued.
                    if reconfiguration_pending && self.state.success_count > 0 {
                        if let Err(err) = self.apply_reconfiguration_if_configured().await {
                            warn!(
                                %self.context.scenario_run_id,
                                scenario = self.context.spec.name,
                                error = %err,
                                "Failed to apply PMMC scenario reconfiguration"
                            );
                            return;
                        }
                        reconfiguration_pending = false;
                    }
                }
                Ok(AwaitOutcome::Timeout) => {
                    self.state.timeout_count += 1;

                    if let Err(err) = self
                        .run_actions(PmmcTriggerKind::AfterTimeouts, "timeout")
                        .await
                    {
                        warn!(
                            %self.context.scenario_run_id,
                            error = %err,
                            "Failed to execute PMMC timeout actions"
                        );
                        break;
                    }

                    sleep(Duration::from_millis(timings.retry_backoff_ms)).await;
                    continue;
                }
                Err(reason) => {
                    completion_reason = Some(reason);
                    break;
                }
            }

            sleep(Duration::from_millis(timings.loop_interval_ms)).await;
        }

        log_completion(&self.context, &self.state, self.start, completion_reason);
    }

    async fn begin_reconfiguration_task(
        &self,
    ) -> anyhow::Result<(
        crate::cluster::cluster_configuration::ConfigurationStrategy,
        JoinHandle<anyhow::Result<ReconfigurationTaskResult>>,
    )> {
        let Some(spec) = self.context.spec.reconfiguration.clone() else {
            anyhow::bail!("missing reconfiguration spec");
        };

        let patch = Self::build_reconfiguration_patch(&self.context.spec.name, &spec)?;
        let mut add_nodes: Vec<Uuid> = patch.add.keys().copied().collect();
        let mut remove_nodes: Vec<Uuid> = patch.remove.iter().copied().collect();
        add_nodes.sort_unstable();
        remove_nodes.sort_unstable();

        let (strategy, previous_nodes) = {
            let cluster = self.cluster.lock().await;
            let strategy = patch.strategy.unwrap_or(cluster.strategy());
            let previous_nodes = cluster.get_node_uuids();
            (strategy, previous_nodes)
        };

        self.emit_reconfiguration_event(Event::ReconfigurationRequested {
            strategy,
            add_nodes,
            remove_nodes,
            created_at: current_timestamp_millis(),
        });
        self.emit_reconfiguration_event(Event::ReconfigurationStopStarted {
            strategy,
            target_nodes: previous_nodes.clone(),
            created_at: current_timestamp_millis(),
        });
        for to in previous_nodes.iter().copied() {
            self.emit_reconfiguration_event(Event::ReconfigurationStopCommandSent {
                to,
                created_at: current_timestamp_millis(),
            });
        }

        let cluster_for_task = Arc::clone(&self.cluster);
        let task = tokio::spawn(async move {
            let mut cluster = cluster_for_task.lock().await;
            let leader_index = cluster
                .update_configuration_ready(patch, Self::RECONFIG_TIMEOUT)
                .await?;
            Ok(ReconfigurationTaskResult {
                strategy,
                leader_index,
                checkpoint_last_applied_slot: cluster.checkpoint_last_applied_slot(),
                active_nodes: cluster.get_node_uuids(),
                node_count: cluster.num_nodes(),
            })
        });

        Ok((strategy, task))
    }

    async fn apply_reconfiguration_if_configured(&mut self) -> anyhow::Result<()> {
        let Some(spec) = self.context.spec.reconfiguration.clone() else {
            return Ok(());
        };

        let patch = Self::build_reconfiguration_patch(&self.context.spec.name, &spec)?;
        let mut add_nodes: Vec<Uuid> = patch.add.keys().copied().collect();
        let mut remove_nodes: Vec<Uuid> = patch.remove.iter().copied().collect();
        add_nodes.sort_unstable();
        remove_nodes.sort_unstable();

        let mut cluster = self.cluster.lock().await;
        let strategy = patch.strategy.unwrap_or(cluster.strategy());
        let previous_nodes = cluster.get_node_uuids();

        self.emit_reconfiguration_event(Event::ReconfigurationRequested {
            strategy,
            add_nodes,
            remove_nodes,
            created_at: current_timestamp_millis(),
        });
        self.emit_reconfiguration_event(Event::ReconfigurationStopStarted {
            strategy,
            target_nodes: previous_nodes.clone(),
            created_at: current_timestamp_millis(),
        });
        for to in previous_nodes.iter().copied() {
            self.emit_reconfiguration_event(Event::ReconfigurationStopCommandSent {
                to,
                created_at: current_timestamp_millis(),
            });
        }

        let leader_result = cluster
            .update_configuration_ready(patch, Self::RECONFIG_TIMEOUT)
            .await;
        let leader_index = match leader_result {
            Ok(index) => index,
            Err(err) => {
                self.emit_reconfiguration_event(Event::ReconfigurationFailed {
                    strategy: Some(strategy),
                    phase: ReconfigurationPhase::Stop,
                    reason: err.to_string(),
                    created_at: current_timestamp_millis(),
                });
                return Err(err);
            }
        };

        self.emit_reconfiguration_event(Event::ReconfigurationCheckpointSelected {
            strategy,
            source_node: None,
            last_applied_slot: cluster.checkpoint_last_applied_slot(),
            created_at: current_timestamp_millis(),
        });

        let active_nodes = cluster.get_node_uuids();
        let leader = active_nodes.get(leader_index).copied();
        self.emit_reconfiguration_event(Event::ReconfigurationReady {
            strategy,
            leader,
            active_nodes,
            created_at: current_timestamp_millis(),
        });
        let node_count = cluster.num_nodes();
        self.pool.normalize_replica_indexes(node_count);
        self.pool.detach_all();
        Ok(())
    }

    fn build_reconfiguration_patch(
        scenario_name: &str,
        spec: &PmmcReconfigurationSpec,
    ) -> anyhow::Result<ReconfigPatch> {
        spec.to_runtime_patch(scenario_name).map_err(Into::into)
    }

    fn emit_reconfiguration_event(&self, event: Event) {
        self.observer.on_event(event);
    }

    fn completion_check(
        &mut self,
        stop_rx: &mut broadcast::Receiver<()>,
        target_successes: usize,
    ) -> Option<CompletionReason> {
        if stop_rx.try_recv().is_ok() {
            return Some(CompletionReason::StopSignal);
        }
        if self.start.elapsed().as_secs() >= self.context.duration_secs {
            return Some(CompletionReason::DurationElapsed);
        }
        if self.state.success_count >= target_successes {
            return Some(CompletionReason::TargetReached);
        }
        None
    }

    async fn run_actions(
        &mut self,
        trigger: PmmcTriggerKind,
        trigger_name: &str,
    ) -> anyhow::Result<()> {
        run_triggered_actions(
            &self.context,
            &self.cluster,
            &self.observer,
            &mut self.pool,
            &mut self.executed_actions,
            &mut self.memory,
            &self.state,
            trigger,
        )
        .await
        .map_err(|err| {
            warn!(
                %self.context.scenario_run_id,
                trigger = trigger_name,
                error = %err,
                "Failed to execute PMMC actions"
            );
            err
        })
    }

    async fn prepare_request(&mut self) -> Result<PendingRequest, CompletionReason> {
        let session_index = self.pool.next_session_index();
        self.pool.session_mut(session_index).record_attempt();
        self.state.attempt_count += 1;

        self.run_actions(PmmcTriggerKind::AfterAttempts, "attempt")
            .await
            .map_err(|_| CompletionReason::DurationElapsed)?;

        let session = self.pool.session_mut(session_index);
        Self::ensure_client_attached(&self.context, &self.cluster, session).await?;

        let request_id = if self.context.spec.request_plan.wait_for_reply {
            session.current_request_id()
        } else {
            let request_id = session.current_request_id();
            session.advance_request_id();
            request_id
        };
        let command = Self::build_command(&self.context, &self.state, session, request_id);

        Ok(PendingRequest {
            session_index,
            client_id: session.id.clone(),
            replica_index: session.replica_index,
            request_id,
            command,
        })
    }

    async fn ensure_client_attached(
        context: &PmmcScenarioContext,
        cluster_for_runner: &Arc<Mutex<PmmcCluster>>,
        session: &mut ClientSession,
    ) -> Result<(), CompletionReason> {
        if session.is_attached() {
            return Ok(());
        }

        let cluster = cluster_for_runner.lock().await;
        let _ = cluster.wait_ready(Duration::from_secs(2)).await;
        match cluster
            .connect_client_to(session.replica_index, session.uuid)
            .await
        {
            Some((tx, rx)) => {
                info!(
                    %context.scenario_run_id,
                    client_id = %session.id,
                    replica_index = session.replica_index,
                    "Attached PMMC client to replica"
                );
                session.attach(tx, rx);
                Ok(())
            }
            None => Err(CompletionReason::ClientAttachFailed(
                session.id.clone(),
                session.replica_index,
            )),
        }
    }

    async fn dispatch_request(&mut self, request: &PendingRequest) -> Result<(), CompletionReason> {
        info!(
            %self.context.scenario_run_id,
            client_id = %request.client_id,
            request_id = request.request_id,
            replica_index = request.replica_index,
            "Dispatching PMMC client request"
        );

        let tx = {
            let session = self.pool.session_mut(request.session_index);
            session.sender().cloned()
        };
        let Some(tx) = tx else {
            return Err(CompletionReason::ClientChannelClosed(
                request.client_id.clone(),
            ));
        };

        tx.send(ClientMessage::PROPOSE {
            cmd: request.command.clone(),
        })
        .await
        .map_err(|_| CompletionReason::ClientChannelClosed(request.client_id.clone()))
    }

    async fn await_response(
        &mut self,
        stop_rx: &mut broadcast::Receiver<()>,
        request: &PendingRequest,
        response_timeout_ms: u64,
    ) -> Result<AwaitOutcome, CompletionReason> {
        let recv_future = async {
            let session = self.pool.session_mut(request.session_index);
            let Some(rx) = session.receiver_mut() else {
                return None;
            };
            rx.recv().await
        };

        let recv_result = tokio::select! {
            _ = stop_rx.recv() => return Err(CompletionReason::StopSignal),
            result = tokio::time::timeout(Duration::from_millis(response_timeout_ms), recv_future) => result,
        };

        match recv_result {
            Ok(Some(message)) => Ok(AwaitOutcome::Message(message)),
            Ok(None) => Err(CompletionReason::ResponseChannelClosed(
                request.client_id.clone(),
            )),
            Err(_) => Ok(AwaitOutcome::Timeout),
        }
    }

    async fn handle_response(
        &mut self,
        request: &PendingRequest,
        client_message: ClientMessage,
    ) -> Result<(), CompletionReason> {
        self.handle_response_for_session(
            request.session_index,
            request.client_id.clone(),
            client_message,
        )
        .await
    }

    async fn handle_response_for_session(
        &mut self,
        session_index: usize,
        client_id: String,
        client_message: ClientMessage,
    ) -> Result<(), CompletionReason> {
        match client_message {
            ClientMessage::RESPONSE { request_id, .. } => {
                {
                    let session = self.pool.session_mut(session_index);
                    session.record_success();
                    if self.context.spec.request_plan.wait_for_reply {
                        session.advance_request_id();
                    }
                }
                self.state.success_count += 1;

                info!(
                    %self.context.scenario_run_id,
                    client_id = %client_id,
                    request_id,
                    success_count = self.state.success_count,
                    "Received PMMC client response"
                );

                self.run_actions(PmmcTriggerKind::AfterSuccesses, "success")
                    .await
                    .map_err(|_| CompletionReason::DurationElapsed)?;

                Ok(())
            }
            ClientMessage::PROPOSE { .. } => {
                Err(CompletionReason::UnexpectedClientMessage(client_id))
            }
        }
    }

    async fn drain_ready_responses(&mut self) -> Result<(), CompletionReason> {
        let session_count = self.pool.len();
        for session_index in 0..session_count {
            loop {
                let (client_id, recv_result) = {
                    let session = self.pool.session_mut(session_index);
                    let client_id = session.id.clone();
                    let recv_result = match session.receiver_mut() {
                        Some(rx) => match rx.try_recv() {
                            Ok(message) => Ok(Some(message)),
                            Err(TryRecvError::Empty) => Ok(None),
                            Err(TryRecvError::Disconnected) => Err(()),
                        },
                        None => Ok(None),
                    };
                    (client_id, recv_result)
                };

                match recv_result {
                    Ok(Some(message)) => {
                        self.handle_response_for_session(session_index, client_id, message)
                            .await?;
                    }
                    Ok(None) => break,
                    Err(()) => return Err(CompletionReason::ResponseChannelClosed(client_id)),
                }
            }
        }

        Ok(())
    }

    fn build_command(
        context: &PmmcScenarioContext,
        state: &PmmcScenarioRunState,
        session: &ClientSession,
        request_id: u64,
    ) -> PaxosCommand {
        match &context.spec.request_plan.kind {
            PmmcRequestPlanKind::SequentialKvPuts {
                key,
                value_prefix: _,
                start_at,
            } => {
                let ordinal = if context.spec.request_plan.wait_for_reply {
                    start_at + state.success_count
                } else {
                    start_at + state.attempt_count.saturating_sub(1)
                };
                let key = if context.spec.request_plan.multi_key {
                    format!("{}-{}", key, ordinal)
                } else {
                    key.clone()
                };
                PaxosCommand::PUT {
                    key,
                    version: 1,
                    value: ordinal,
                }
                .with_client(session.uuid, request_id)
            }
        }
    }
}

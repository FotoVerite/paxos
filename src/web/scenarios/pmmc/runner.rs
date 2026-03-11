use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Mutex, broadcast};
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use uuid::Uuid;

use crate::cluster::pmmc_cluster::PmmcCluster;
use crate::message::ClientMessage;
use crate::paxos_command::PaxosCommand;
use crate::web::websocket_observer::WebSocketObserver;

use super::actions::{PmmcTriggerKind, RunnerMemory, run_triggered_actions};
use super::client_pool::{ClientPool, ClientSession};
use super::completion::{CompletionReason, log_completion};
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
        let mut completion_reason: Option<CompletionReason> = None;

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

        let request_id = session.request_id();
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

        let session = self.pool.session_mut(request.session_index);
        let Some(tx) = session.sender() else {
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
        match client_message {
            ClientMessage::RESPONSE { request_id, .. } => {
                self.pool
                    .session_mut(request.session_index)
                    .record_success();
                self.state.success_count += 1;

                info!(
                    %self.context.scenario_run_id,
                    client_id = %request.client_id,
                    request_id,
                    success_count = self.state.success_count,
                    "Received PMMC client response"
                );

                self.run_actions(PmmcTriggerKind::AfterSuccesses, "success")
                    .await
                    .map_err(|_| CompletionReason::DurationElapsed)?;

                Ok(())
            }
            ClientMessage::PROPOSE { .. } => Err(CompletionReason::UnexpectedClientMessage(
                request.client_id.clone(),
            )),
        }
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
                let ordinal = start_at + state.success_count;
                PaxosCommand::PUT {
                    key: key.clone(),
                    version: 1,
                    value: ordinal,
                }
                .with_client(session.uuid, request_id)
            }
        }
    }
}

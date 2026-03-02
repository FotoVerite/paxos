use std::time::Instant;

use tracing::{info, warn};

use super::runner::{PmmcScenarioContext, PmmcScenarioRunState};

pub(crate) enum CompletionReason {
    StopSignal,
    DurationElapsed,
    TargetReached,
    ClientAttachFailed(String, usize),
    ClientChannelClosed(String),
    ResponseChannelClosed(String),
    UnexpectedClientMessage(String),
}

impl CompletionReason {
    fn label(&self) -> &'static str {
        match self {
            Self::StopSignal => "stop_signal",
            Self::DurationElapsed => "duration_elapsed",
            Self::TargetReached => "target_reached",
            Self::ClientAttachFailed(_, _) => "client_attach_failed",
            Self::ClientChannelClosed(_) => "client_channel_closed",
            Self::ResponseChannelClosed(_) => "response_channel_closed",
            Self::UnexpectedClientMessage(_) => "unexpected_client_message",
        }
    }

    fn client_id(&self) -> Option<&str> {
        match self {
            Self::ClientAttachFailed(client_id, _)
            | Self::ClientChannelClosed(client_id)
            | Self::ResponseChannelClosed(client_id)
            | Self::UnexpectedClientMessage(client_id) => Some(client_id.as_str()),
            _ => None,
        }
    }

    fn replica_index(&self) -> Option<usize> {
        match self {
            Self::ClientAttachFailed(_, replica_index) => Some(*replica_index),
            _ => None,
        }
    }
}

pub(crate) fn log_completion(
    context: &PmmcScenarioContext,
    state: &PmmcScenarioRunState,
    start: Instant,
    completion_reason: Option<CompletionReason>,
) {
    let elapsed_ms = start.elapsed().as_millis() as u64;
    let reason = completion_reason.unwrap_or(CompletionReason::DurationElapsed);

    if matches!(reason, CompletionReason::TargetReached) {
        info!(
            %context.scenario_run_id,
            scenario = context.spec.name,
            proposal_count = state.success_count,
            client_attempt_count = state.attempt_count,
            elapsed_ms,
            "Scenario runner completed"
        );
        return;
    }

    if let Some(client_id) = reason.client_id() {
        if let Some(replica_index) = reason.replica_index() {
            warn!(
                %context.scenario_run_id,
                completion_reason = reason.label(),
                client_id,
                replica_index,
                proposal_count = state.success_count,
                client_attempt_count = state.attempt_count,
                elapsed_ms,
                "Scenario runner stopped"
            );
        } else {
            warn!(
                %context.scenario_run_id,
                completion_reason = reason.label(),
                client_id,
                proposal_count = state.success_count,
                client_attempt_count = state.attempt_count,
                elapsed_ms,
                "Scenario runner stopped"
            );
        }
        return;
    }

    info!(
        %context.scenario_run_id,
        completion_reason = reason.label(),
        proposal_count = state.success_count,
        client_attempt_count = state.attempt_count,
        elapsed_ms,
        "Scenario runner stopped"
    );
}

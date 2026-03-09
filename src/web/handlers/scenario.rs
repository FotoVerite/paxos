use axum::{Json, extract::ConnectInfo, extract::State, http::HeaderMap, response::IntoResponse};
use std::net::SocketAddr;
use tracing::{Instrument, error, info, info_span};

use super::super::{ProposalRequest, ScenarioRequest};
use super::utils::{AppState, get_client_ip, get_cm};
use crate::paxos_command::PaxosCommand;
use crate::web::scenarios::ScenarioType;

/// Start a new scenario
pub async fn start_scenario_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ScenarioRequest>,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    let scenario_type = ScenarioType::parse(&req.scenario_type);
    let learning_strategy = if req.learning_strategy.is_empty() {
        "ProposerManaged"
    } else {
        &req.learning_strategy
    };
    let span = info_span!(
        "start_scenario",
        client_ip = %ip,
        scenario_type = scenario_type.as_str(),
        node_count = req.node_count,
        duration_secs = req.duration_secs,
        learning_strategy,
        leader_node = ?req.leader_node
    );

    async move {
        info!("Starting scenario");

        let cm = get_cm(state, addr, headers).await;
        let cm = cm.lock().await;
        match cm
            .start_scenario(
                ip,
                req.node_count,
                req.duration_secs,
                scenario_type,
                learning_strategy,
                req.leader_node,
            )
            .await
        {
            Ok(_) => {
                info!("Scenario started successfully");
                (axum::http::StatusCode::OK, "Scenario started".to_string())
            }
            Err(e) => {
                error!("Failed to start scenario: {}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error: {}", e),
                )
            }
        }
    }
    .instrument(span)
    .await
}

/// Propose a decree
pub async fn propose_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ProposalRequest>,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    let span = info_span!(
        "classic_proposal",
        client_ip = %ip,
        author = %req.author,
        decree = %req.decree
    );

    async move {
        info!("Submitting decree proposal");

        let cmd = PaxosCommand::EnactDecree {
            author: req.author,
            law: req.decree,
        };

        let cm = get_cm(state, addr, headers).await;
        let cm = cm.lock().await;
        match cm.propose(cmd).await {
            Ok(_) => {
                info!("Proposal accepted");
                (axum::http::StatusCode::OK, "Proposal submitted".to_string())
            }
            Err(e) => {
                error!("Proposal failed: {}", e);
                let status = if e.to_string().contains("not supported for PMMC") {
                    axum::http::StatusCode::BAD_REQUEST
                } else {
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR
                };
                (status, format!("Error: {}", e))
            }
        }
    }
    .instrument(span)
    .await
}

/// Stop the currently running scenario
pub async fn stop_scenario_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    let span = info_span!("stop_scenario", client_ip = %ip);

    async move {
        info!("Stopping scenario");

        let cm = get_cm(state, addr, headers).await;
        let cm = match cm.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                info!("Stop already in progress, returning early");
                return (
                    axum::http::StatusCode::OK,
                    "Scenario already stopping".to_string(),
                );
            }
        };
        match cm.reset(ip).await {
            Ok(_) => {
                info!("Scenario stopped");
                (axum::http::StatusCode::OK, "Scenario stopped".to_string())
            }
            Err(e) => {
                error!("Failed to stop scenario: {}", e);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error: {}", e),
                )
            }
        }
    }
    .instrument(span)
    .await
}

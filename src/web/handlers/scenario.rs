use axum::{Json, extract::ConnectInfo, extract::State, http::HeaderMap, response::IntoResponse};
use std::net::SocketAddr;
use tracing::{error, info};

use super::super::{ProposalRequest, ScenarioRequest};
use super::utils::{AppState, get_client_ip, get_cm};
use crate::paxos_command::PaxosCommand;

/// Start a new scenario
pub async fn start_scenario_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ScenarioRequest>,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    let scenario_type = if req.scenario_type.is_empty() {
        "happy_path"
    } else {
        &req.scenario_type
    };

    info!(
        "Starting scenario '{}' with {} nodes for {} seconds from {}",
        scenario_type, req.node_count, req.duration_secs, ip
    );

    let cm = get_cm(state, addr, headers).await;
    let cm = cm.lock().await;
    let learning_strategy = if req.learning_strategy.is_empty() {
        "ProposerManaged"
    } else {
        &req.learning_strategy
    };
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
            info!("Scenario started successfully for {}", ip);
            (axum::http::StatusCode::OK, "Scenario started".to_string())
        }
        Err(e) => {
            error!("Failed to start scenario for {}: {}", ip, e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error: {}", e),
            )
        }
    }
}

/// Propose a decree
pub async fn propose_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<ProposalRequest>,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    info!("Proposal from {}: {} by {}", ip, req.decree, req.author);

    let cmd = PaxosCommand::EnactDecree {
        author: req.author,
        law: req.decree,
    };

    let cm = get_cm(state, addr, headers).await;
    let cm = cm.lock().await;
    match cm.propose(cmd).await {
        Ok(_) => {
            info!("Proposal accepted for {}", ip);
            (axum::http::StatusCode::OK, "Proposal submitted".to_string())
        }
        Err(e) => {
            error!("Proposal failed for {}: {}", ip, e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error: {}", e),
            )
        }
    }
}

/// Stop the currently running scenario
pub async fn stop_scenario_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    info!("Stopping scenario for {}", ip);

    let cm = get_cm(state, addr, headers).await;
    let cm = match cm.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            info!("Stop already in progress for {}, returning early", ip);
            return (
                axum::http::StatusCode::OK,
                "Scenario already stopping".to_string(),
            );
        }
    };
    match cm.reset(ip).await {
        Ok(_) => {
            info!("Scenario stopped for {}", ip);
            (axum::http::StatusCode::OK, "Scenario stopped".to_string())
        }
        Err(e) => {
            error!("Failed to stop scenario for {}: {}", ip, e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error: {}", e),
            )
        }
    }
}

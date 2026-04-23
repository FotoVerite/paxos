use axum::{
    extract::{ConnectInfo, Query, State},
    http::HeaderMap,
    response::IntoResponse,
};
use serde::Deserialize;
use std::net::SocketAddr;
use tracing::{Instrument, error, info, info_span};

use super::utils::{AppState, get_client_ip, get_cm};
use crate::web::scenarios::VerticalScenarioKey;

#[derive(Debug, Deserialize)]
pub struct VerticalDemoRunParams {
    scenario: Option<String>,
}

pub async fn run_vertical_demo_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(params): Query<VerticalDemoRunParams>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    let span = info_span!("run_vertical_demo", client_ip = %ip);
    let scenario_key = VerticalScenarioKey::parse(params.scenario.as_deref().unwrap_or_default());

    async move {
        info!(scenario = scenario_key.as_str(), "Starting vertical demo");

        let cm = get_cm(state, addr, headers).await;
        let cm = cm.lock().await;
        match cm.start_vertical_demo(ip, scenario_key).await {
            Ok(_) => (
                axum::http::StatusCode::OK,
                "Vertical demo started".to_string(),
            ),
            Err(err) => {
                error!("Failed to start vertical demo: {}", err);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error: {}", err),
                )
            }
        }
    }
    .instrument(span)
    .await
}

pub async fn reset_vertical_demo_handler(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    let span = info_span!("reset_vertical_demo", client_ip = %ip);

    async move {
        info!("Resetting vertical demo");

        let cm = get_cm(state, addr, headers).await;
        let cm = match cm.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                info!("Vertical demo reset already in progress, returning early");
                return (
                    axum::http::StatusCode::OK,
                    "Vertical demo already resetting".to_string(),
                );
            }
        };

        match cm.reset_vertical_demo(ip).await {
            Ok(_) => (
                axum::http::StatusCode::OK,
                "Vertical demo reset".to_string(),
            ),
            Err(err) => {
                error!("Failed to reset vertical demo: {}", err);
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error: {}", err),
                )
            }
        }
    }
    .instrument(span)
    .await
}

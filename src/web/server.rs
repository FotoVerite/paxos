use std::sync::Arc;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
    http::StatusCode,
};
use tower_http::services::ServeDir;

use super::websocket_observer::WebSocketObserver;
use super::cluster_manager::ClusterManager;
use super::{ScenarioRequest, ProposalRequest};
use crate::paxos_command::PaxosCommand;

/// Shared state for the WebSocketObserver and ClusterManager.
#[derive(Clone)]
struct AppState {
    websocket_observer: Arc<WebSocketObserver>,
    cluster_manager: Arc<ClusterManager>,
}

pub async fn run_web_server(observer: Arc<WebSocketObserver>) {
    // Serve static files from the "static" directory
    let static_files_service = ServeDir::new("static");
    let cluster_manager = Arc::new(ClusterManager::new(observer.clone()));

    let app_state = AppState {
        websocket_observer: observer,
        cluster_manager,
    };

    let app = Router::new()
        .route("/", get(landing_handler))
        .route("/ws", get(websocket_handler))
        .route("/api/start-scenario", post(start_scenario_handler))
        .route("/api/propose", post(propose_handler))
        .route("/visualizer", get(visualizer_handler))
        .route("/senate", get(senate_handler))
        .route("/decree", get(decree_handler))
        .route("/leslie", get(leslie_handler))

        .fallback_service(static_files_service)
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Web server listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}

/// Handles the WebSocket upgrade request.
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handles a single WebSocket connection, sending events to the client.
async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let mut receiver = state.websocket_observer.subscribe().await;

    while let Ok(event_json) = receiver.recv().await {
        if socket
            .send(Message::Text(event_json))
            .await
            .is_err()
        {
            // Client disconnected
            break;
        }
    }
}

/// Start a new scenario
async fn start_scenario_handler(
    State(state): State<AppState>,
    Json(req): Json<ScenarioRequest>,
) -> impl IntoResponse {
    match state.cluster_manager.start_scenario(req.node_count, req.duration_secs).await {
        Ok(_) => (axum::http::StatusCode::OK, "Scenario started".to_string()),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)),
    }
}

/// Propose a decree
async fn propose_handler(
    State(state): State<AppState>,
    Json(req): Json<ProposalRequest>,
) -> impl IntoResponse {
    let cmd = PaxosCommand::EnactDecree {
        author: req.author,
        law: req.decree,
    };
    
    match state.cluster_manager.propose(cmd).await {
        Ok(_) => (axum::http::StatusCode::OK, "Proposal submitted".to_string()),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)),
    }
}

/// Serve the Senate visualizer page
async fn senate_handler() -> impl IntoResponse {
    let html = include_str!("../../static/senate.html");
    (StatusCode::OK, [("Content-Type", "text/html")], html)
}

/// Serve the Decree lifecycle visualizer page
async fn decree_handler() -> impl IntoResponse {
    let html = include_str!("../../static/decree.html");
    (StatusCode::OK, [("Content-Type", "text/html")], html)
}

/// Serve the Landing page
async fn landing_handler() -> impl IntoResponse {
    let html = include_str!("../../static/landing.html");
    (StatusCode::OK, [("Content-Type", "text/html")], html)
}

async fn leslie_handler() -> impl IntoResponse {
    let html = include_str!("../../static/leslie.html");
    (StatusCode::OK, [("Content-Type", "text/html")], html)
}

/// Serve the Visualizer page
async fn visualizer_handler() -> impl IntoResponse {
    let html = include_str!("../../static/visualizer.html");
    (StatusCode::OK, [("Content-Type", "text/html")], html)
}
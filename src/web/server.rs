use askama::Template;
use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
    routing::{get, post},
};
use std::sync::Arc;
use tower_http::services::ServeDir;

use super::cluster_manager::ClusterManager;
use super::websocket_observer::WebSocketObserver;
use super::{ProposalRequest, ScenarioRequest};
use crate::paxos_command::PaxosCommand;

// ============================================================================
// Template Handler Macro
// ============================================================================

/// Macro to define a template struct and its handler in one invocation
/// Generates: template struct with #[derive(Template)], #[template(path = ...)]
/// and async handler that renders the template
macro_rules! page_template {
    ($handler_name:ident, $template_name:ident, $template_path:expr) => {
        #[derive(Template)]
        #[template(path = $template_path)]
        struct $template_name;

        async fn $handler_name() -> impl IntoResponse {
            Html($template_name.render().unwrap_or_default())
        }
    };
}

// Define pages
page_template!(landing_handler, LandingTemplate, "landing.html");
page_template!(leslie_handler, LeslieTemplate, "leslie.html");
page_template!(visualizer_handler, VisualizerTemplate, "visualizer.html");
page_template!(senate_handler, SenateTemplate, "senate.html");
page_template!(
    setting_of_paper_handler,
    SettingOfPaper,
    "setting_of_paper.html"
);
page_template!(terms_handler, TermTemplate, "terms.html");
page_template!(the_problem_handler, TheProblem, "the_problem.html");
page_template!(decree_handler, DecreeTemplate, "decree.html");
page_template!(
    synod_definitions_handler,
    SynodDefinitions,
    "./synod/definitions.html"
);
page_template!(synod_ballot_handler, SynodBallot, "./synod/ballot.html");

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
        .route("/leslie", get(leslie_handler))
        .route("/visualizer", get(visualizer_handler))
        .route("/senate", get(senate_handler))
        .route("/setting_of_paper", get(setting_of_paper_handler))
        .route("/terms", get(terms_handler))
        .route("/the_problem", get(the_problem_handler))
        .route("/decree", get(decree_handler))
        .route("/ws", get(websocket_handler))
        .route("/api/start-scenario", post(start_scenario_handler))
        //synod
        .route("/synod/definitions", get(synod_definitions_handler))
        .route("/synod/ballot", get(synod_ballot_handler))
        .route("/api/propose", post(propose_handler))
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
        if socket.send(Message::Text(event_json)).await.is_err() {
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
    match state
        .cluster_manager
        .start_scenario(req.node_count, req.duration_secs)
        .await
    {
        Ok(_) => (axum::http::StatusCode::OK, "Scenario started".to_string()),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", e),
        ),
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
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {}", e),
        ),
    }
}

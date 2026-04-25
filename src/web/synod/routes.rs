use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    cluster::synod::{ClientId, RUST_EMOJI_POOL, SynodProposalError},
    web::handlers::AppState,
};

const DEFAULT_ROOM: &str = "main";

#[derive(Debug, Deserialize)]
pub struct JoinRequest {
    pub client_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JoinResponse {
    pub room: &'static str,
    pub client_id: String,
    pub assigned_node: String,
    pub active_nodes: usize,
    pub emoji_pool: Vec<&'static str>,
}

#[derive(Debug, Deserialize)]
pub struct ProposalRequest {
    pub client_id: String,
    pub request_id: u64,
    pub emoji: String,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub room: &'static str,
    pub active_nodes: usize,
    pub bootstrap_node: Option<Uuid>,
    pub active_configuration: Option<ConfigurationStatus>,
    pub emoji_pool: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct ConfigurationStatus {
    pub configuration_id: Uuid,
    pub leader: Uuid,
    pub acceptors: Vec<Uuid>,
    pub replicas: Vec<Uuid>,
    pub start_index: usize,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(mobile_page))
        .route("/dashboard", get(dashboard_page))
        .route("/api/join", post(join))
        .route("/api/status", get(status))
        .route("/api/proposals", post(propose))
        .route("/ws", get(ws_placeholder))
}

async fn mobile_page(State(state): State<AppState>) -> Response {
    render_synod_template(&state, "synod/mobile.html")
}

async fn dashboard_page(State(state): State<AppState>) -> Response {
    render_synod_template(&state, "synod/dashboard.html")
}

async fn join(State(state): State<AppState>, Json(request): Json<JoinRequest>) -> Response {
    let client_id = request.client_id.map(ClientId::from_existing);
    let mut synod = state.synod.lock().await;
    let assignment = match synod.assign_client(client_id).await {
        Ok(assignment) => assignment,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to assign synod client: {err}"),
            )
                .into_response();
        }
    };
    let membership = synod.membership();

    Json(JoinResponse {
        room: DEFAULT_ROOM,
        client_id: assignment.client_id.to_string(),
        assigned_node: assignment.node_id.to_string(),
        active_nodes: membership.node_count(),
        emoji_pool: RUST_EMOJI_POOL.to_vec(),
    })
    .into_response()
}

async fn propose(State(state): State<AppState>, Json(request): Json<ProposalRequest>) -> Response {
    let synod = state.synod.lock().await;
    match synod
        .propose_emoji(
            ClientId::from_existing(request.client_id),
            request.request_id,
            request.emoji,
        )
        .await
    {
        Ok(receipt) => Json(receipt).into_response(),
        Err(SynodProposalError::InvalidEmoji(err)) => (
            StatusCode::BAD_REQUEST,
            format!("invalid synod emoji: {err}"),
        )
            .into_response(),
        Err(SynodProposalError::UnknownClient(err)) => (
            StatusCode::NOT_FOUND,
            format!("unknown synod client: {err}"),
        )
            .into_response(),
        Err(SynodProposalError::Submit(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to submit synod proposal: {err}"),
        )
            .into_response(),
    }
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    let synod = state.synod.lock().await;
    let membership = synod.membership();
    let active_configuration =
        synod
            .active_configuration()
            .await
            .map(|configuration| ConfigurationStatus {
                configuration_id: configuration.id(),
                leader: configuration.leader(),
                acceptors: configuration.acceptors().to_vec(),
                replicas: configuration.replicas().to_vec(),
                start_index: configuration.start_index(),
            });

    Json(StatusResponse {
        room: DEFAULT_ROOM,
        active_nodes: membership.node_count(),
        bootstrap_node: membership.bootstrap_node,
        active_configuration,
        emoji_pool: RUST_EMOJI_POOL.to_vec(),
    })
}

async fn ws_placeholder() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "Synod websocket will be wired after the room API is in place",
    )
}

fn render_synod_template(state: &AppState, template_name: &str) -> Response {
    match state.tera.render(template_name, &tera::Context::new()) {
        Ok(html) => Html(html).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to render {template_name}: {err}"),
        )
            .into_response(),
    }
}

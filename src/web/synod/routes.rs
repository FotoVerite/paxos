use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::Response,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(mobile_page))
        .route("/dashboard", get(dashboard_page))
        .route("/api/join", post(join))
        .route("/api/proposals", post(propose))
        .route("/ws", get(ws_placeholder))
}

async fn mobile_page() -> Html<&'static str> {
    Html("<!doctype html><title>Synod</title><h1>Synod</h1>")
}

async fn dashboard_page() -> Html<&'static str> {
    Html("<!doctype html><title>Synod Dashboard</title><h1>Synod Dashboard</h1>")
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

async fn ws_placeholder() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "Synod websocket will be wired after the room API is in place",
    )
}

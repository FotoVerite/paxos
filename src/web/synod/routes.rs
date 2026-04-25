use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::{cluster::synod::ClientId, web::handlers::AppState};

const DEFAULT_ROOM: &str = "main";
const EMOJI_POOL: &[&str] = &["🦀", "📦", "🔒", "🧵", "⚙️", "🧪"];

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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(mobile_page))
        .route("/dashboard", get(dashboard_page))
        .route("/api/join", post(join))
        .route("/ws", get(ws_placeholder))
}

async fn mobile_page() -> Html<&'static str> {
    Html("<!doctype html><title>Synod</title><h1>Synod</h1>")
}

async fn dashboard_page() -> Html<&'static str> {
    Html("<!doctype html><title>Synod Dashboard</title><h1>Synod Dashboard</h1>")
}

async fn join(
    State(state): State<AppState>,
    Json(request): Json<JoinRequest>,
) -> impl IntoResponse {
    let client_id = request.client_id.map(ClientId::from_existing);
    let mut synod = state.synod.lock().await;
    let assignment = synod.assign_client(client_id);
    let membership = synod.membership();

    Json(JoinResponse {
        room: DEFAULT_ROOM,
        client_id: assignment.client_id.to_string(),
        assigned_node: assignment.node_id.to_string(),
        active_nodes: membership.node_count(),
        emoji_pool: EMOJI_POOL.to_vec(),
    })
}

async fn ws_placeholder() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        "Synod websocket will be wired after the room API is in place",
    )
}

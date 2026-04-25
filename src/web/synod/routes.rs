use axum::{
    Json, Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, interval};
use uuid::Uuid;

use crate::{
    cluster::synod::{
        ClientId, RUST_EMOJI_POOL, SYNOD_CLIENT_TTL, SynodProposalError, SynodRequestStatus,
        SynodRoomState,
    },
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
    pub state: SynodRoomState,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    pub client_id: String,
}

#[derive(Debug, Serialize)]
pub struct HeartbeatResponse {
    pub room: &'static str,
    pub client_id: String,
    pub assigned_node: String,
    pub active_nodes: usize,
    pub ttl_ms: u64,
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
    pub state: SynodRoomState,
}

#[derive(Debug, Deserialize)]
pub struct RequestStatusQuery {
    pub client_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SocketQuery {
    pub client_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SynodSocketClientMessage {
    Heartbeat,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SynodSocketServerMessage {
    Joined { session: JoinResponse },
    Heartbeat { heartbeat: HeartbeatResponse },
    RoomState { status: StatusResponse },
    Error { message: String },
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
        .route("/api/heartbeat", post(heartbeat))
        .route("/api/status", get(status))
        .route("/api/requests/:request_id", get(request_status))
        .route("/api/proposals", post(propose))
        .route("/ws", get(ws))
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
        state: synod.room_state(),
    })
    .into_response()
}

async fn heartbeat(
    State(state): State<AppState>,
    Json(request): Json<HeartbeatRequest>,
) -> Response {
    let mut synod = state.synod.lock().await;
    let client_id = ClientId::from_existing(request.client_id);
    let Some(assignment) = synod.heartbeat_client(&client_id) else {
        return (StatusCode::NOT_FOUND, "unknown synod client").into_response();
    };
    let membership = synod.membership();

    Json(HeartbeatResponse {
        room: DEFAULT_ROOM,
        client_id: assignment.client_id.to_string(),
        assigned_node: assignment.node_id.to_string(),
        active_nodes: membership.node_count(),
        ttl_ms: SYNOD_CLIENT_TTL.as_millis() as u64,
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
        state: synod.room_state(),
    })
}

async fn request_status(
    State(state): State<AppState>,
    Path(request_id): Path<u64>,
    Query(query): Query<RequestStatusQuery>,
) -> Response {
    let synod = state.synod.lock().await;
    match synod.request_status(&ClientId::from_existing(query.client_id), request_id) {
        Some(status) => Json::<SynodRequestStatus>(status).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown synod request").into_response(),
    }
}

async fn ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<SocketQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, query.client_id))
}

async fn handle_socket(mut socket: WebSocket, state: AppState, client_id: Option<String>) {
    let joined = {
        let mut synod = state.synod.lock().await;
        let assignment = match synod.assign_client(client_id.map(ClientId::from_existing)).await {
            Ok(assignment) => assignment,
            Err(err) => {
                let _ = send_socket_message(
                    &mut socket,
                    SynodSocketServerMessage::Error {
                        message: format!("failed to join synod room: {err}"),
                    },
                )
                .await;
                return;
            }
        };
        let membership = synod.membership();
        JoinResponse {
            room: DEFAULT_ROOM,
            client_id: assignment.client_id.to_string(),
            assigned_node: assignment.node_id.to_string(),
            active_nodes: membership.node_count(),
            emoji_pool: RUST_EMOJI_POOL.to_vec(),
            state: synod.room_state(),
        }
    };
    let socket_client_id = ClientId::from_existing(joined.client_id.clone());

    if send_socket_message(
        &mut socket,
        SynodSocketServerMessage::Joined { session: joined },
    )
        .await
        .is_err()
    {
        return;
    }

    let mut ticker = interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if send_room_state(&mut socket, &state).await.is_err() {
                    break;
                }
            }
            message = socket.recv() => {
                let Some(Ok(message)) = message else {
                    break;
                };
                match message {
                    Message::Text(payload) => {
                        if handle_socket_message(&mut socket, &state, &socket_client_id, &payload).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn handle_socket_message(
    socket: &mut WebSocket,
    state: &AppState,
    client_id: &ClientId,
    payload: &str,
) -> Result<(), axum::Error> {
    let Ok(message) = serde_json::from_str::<SynodSocketClientMessage>(payload) else {
        return Ok(());
    };

    match message {
        SynodSocketClientMessage::Heartbeat => {
            let heartbeat = {
                let mut synod = state.synod.lock().await;
                let Some(assignment) = synod.heartbeat_client(client_id) else {
                    send_socket_message(
                        socket,
                        SynodSocketServerMessage::Error {
                            message: "unknown synod client".to_string(),
                        },
                    )
                    .await?;
                    return Ok(());
                };
                let membership = synod.membership();
                HeartbeatResponse {
                    room: DEFAULT_ROOM,
                    client_id: assignment.client_id.to_string(),
                    assigned_node: assignment.node_id.to_string(),
                    active_nodes: membership.node_count(),
                    ttl_ms: SYNOD_CLIENT_TTL.as_millis() as u64,
                }
            };
            send_socket_message(socket, SynodSocketServerMessage::Heartbeat { heartbeat }).await?;
        }
    }

    Ok(())
}

async fn send_room_state(socket: &mut WebSocket, state: &AppState) -> Result<(), axum::Error> {
    let status = {
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

        StatusResponse {
            room: DEFAULT_ROOM,
            active_nodes: membership.node_count(),
            bootstrap_node: membership.bootstrap_node,
            active_configuration,
            emoji_pool: RUST_EMOJI_POOL.to_vec(),
            state: synod.room_state(),
        }
    };

    send_socket_message(socket, SynodSocketServerMessage::RoomState { status }).await
}

async fn send_socket_message(
    socket: &mut WebSocket,
    message: SynodSocketServerMessage,
) -> Result<(), axum::Error> {
    socket
        .send(Message::Text(
            serde_json::to_string(&message).expect("synod socket message should serialize"),
        ))
        .await
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

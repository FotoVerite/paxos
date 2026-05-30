use axum::{
    Json, Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    cluster::synod::{
        ClientId, RUST_EMOJI_POOL, SYNOD_CLIENT_TTL, SynodActivityEvent, SynodAppliedEmoji,
        SynodCluster, SynodProposalError, SynodRequestStatus, SynodRoomState, SynodRoomUpdate,
    },
    web::handlers::AppState,
};

#[derive(Debug, Deserialize)]
pub struct JoinRequest {
    pub client_id: Option<String>,
    pub client_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JoinResponse {
    pub room: String,
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
    pub room: String,
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
    pub room: String,
    pub active_nodes: usize,
    pub bootstrap_node: Option<Uuid>,
    pub active_configuration: Option<ConfigurationStatus>,
    pub clients: Vec<ClientStatus>,
    pub emoji_pool: Vec<&'static str>,
    pub state: SynodRoomState,
}

#[derive(Debug, Deserialize)]
pub struct RequestStatusQuery {
    pub client_id: String,
    pub room: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SocketQuery {
    pub room: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RoomQuery {
    pub room: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SynodSocketClientMessage {
    JoinClient {
        client_id: Option<String>,
        client_name: Option<String>,
    },
    SubscribeObserver,
    Heartbeat,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SynodSocketServerMessage {
    Joined { session: JoinResponse },
    Heartbeat { heartbeat: HeartbeatResponse },
    RequestState { request: SynodRequestStatus },
    Applied { entry: SynodAppliedEmoji },
    Activity { event: SynodActivityEvent },
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
    pub checkpoint_slot: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ClientStatus {
    pub client_id: String,
    pub client_name: Option<String>,
    pub node_id: Uuid,
}

fn room_name(room: Option<&String>) -> &str {
    room.map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("main")
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

async fn join(
    State(state): State<AppState>,
    Query(room_query): Query<RoomQuery>,
    Json(request): Json<JoinRequest>,
) -> Response {
    let room = room_name(room_query.room.as_ref()).to_string();
    let synod = state.get_or_create_room(&room).await;
    let requested_client_id = request.client_id.map(ClientId::from_existing);
    let mut synod = synod.lock().await;
    let already_assigned = requested_client_id
        .as_ref()
        .is_some_and(|client_id| synod.has_assignment(client_id));
    let assignment = match synod.assign_client(requested_client_id).await {
        Ok(assignment) => assignment,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to assign synod client: {err}"),
            )
                .into_response();
        }
    };
    if let Some(client_name) = request.client_name {
        synod.record_client_name(&assignment.client_id, client_name);
    }
    if !already_assigned {
        synod.emit_client_joined(&assignment);
    }
    let membership = synod.membership();

    Json(JoinResponse {
        room,
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
    Query(room_query): Query<RoomQuery>,
    Json(request): Json<HeartbeatRequest>,
) -> Response {
    let room = room_name(room_query.room.as_ref()).to_string();
    let synod = state.get_or_create_room(&room).await;
    let mut synod = synod.lock().await;
    let client_id = ClientId::from_existing(request.client_id);
    let Some(assignment) = synod.heartbeat_client(&client_id) else {
        return (StatusCode::NOT_FOUND, "unknown synod client").into_response();
    };
    let membership = synod.membership();

    Json(HeartbeatResponse {
        room,
        client_id: assignment.client_id.to_string(),
        assigned_node: assignment.node_id.to_string(),
        active_nodes: membership.node_count(),
        ttl_ms: SYNOD_CLIENT_TTL.as_millis() as u64,
    })
    .into_response()
}

async fn propose(
    State(state): State<AppState>,
    Query(room_query): Query<RoomQuery>,
    Json(request): Json<ProposalRequest>,
) -> Response {
    let room = room_name(room_query.room.as_ref()).to_string();
    let synod = state.get_or_create_room(&room).await;
    let plan = {
        let synod = synod.lock().await;
        synod.plan_emoji_proposal(
            ClientId::from_existing(request.client_id),
            request.request_id,
            request.emoji,
        )
    };

    match plan {
        Ok(plan) => match plan.submit().await {
            Ok(receipt) => Json(receipt).into_response(),
            Err(err) => proposal_error_response(err),
        },
        Err(err) => proposal_error_response(err),
    }
}

fn proposal_error_response(err: SynodProposalError) -> Response {
    match err {
        SynodProposalError::InvalidEmoji(err) => (
            StatusCode::BAD_REQUEST,
            format!("invalid synod emoji: {err}"),
        )
            .into_response(),
        SynodProposalError::UnknownClient(err) => (
            StatusCode::NOT_FOUND,
            format!("unknown synod client: {err}"),
        )
            .into_response(),
        SynodProposalError::Submit(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to submit synod proposal: {err}"),
        )
            .into_response(),
    }
}

async fn status(
    State(state): State<AppState>,
    Query(room_query): Query<RoomQuery>,
) -> impl IntoResponse {
    let room = room_name(room_query.room.as_ref()).to_string();
    let synod = state.get_or_create_room(&room).await;
    let synod = synod.lock().await;
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
                checkpoint_slot: configuration
                    .checkpoint()
                    .and_then(|checkpoint| checkpoint.manifest().last_applied_slot()),
            });

    Json(StatusResponse {
        room,
        active_nodes: membership.node_count(),
        bootstrap_node: membership.bootstrap_node,
        active_configuration,
        clients: membership
            .assignments
            .iter()
            .map(|assignment| ClientStatus {
                client_id: assignment.client_id.to_string(),
                client_name: synod.client_name(&assignment.client_id),
                node_id: assignment.node_id,
            })
            .collect(),
        emoji_pool: RUST_EMOJI_POOL.to_vec(),
        state: synod.room_state(),
    })
}

async fn request_status(
    State(state): State<AppState>,
    Path(request_id): Path<u64>,
    Query(query): Query<RequestStatusQuery>,
) -> Response {
    let room = room_name(query.room.as_ref()).to_string();
    let synod = state.get_or_create_room(&room).await;
    let synod = synod.lock().await;
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
    let room = room_name(query.room.as_ref()).to_string();
    ws.on_upgrade(move |socket| async move {
        let room_cluster = state.get_or_create_room(&room).await;
        handle_socket(socket, room, room_cluster).await;
    })
}

async fn handle_socket(
    mut socket: WebSocket,
    room: String,
    room_cluster: Arc<Mutex<SynodCluster>>,
) {
    let mut updates = {
        let synod = room_cluster.lock().await;
        synod.subscribe_updates()
    };
    let mut socket_client_id: Option<ClientId> = None;
    let mut subscribed = false;

    loop {
        tokio::select! {
            update = updates.recv() => {
                if !subscribed {
                    continue;
                }
                let Ok(update) = update else {
                    continue;
                };
                if send_synod_update(&mut socket, &room, &room_cluster, update).await.is_err() {
                    break;
                }
            }
            message = socket.recv() => {
                let Some(Ok(message)) = message else {
                    break;
                };
                match message {
                    Message::Text(payload) => {
                        if handle_socket_message(
                            &mut socket,
                            &room,
                            &room_cluster,
                            &mut socket_client_id,
                            &mut subscribed,
                            &payload,
                        ).await.is_err() {
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

async fn send_synod_update(
    socket: &mut WebSocket,
    room: &str,
    room_cluster: &Arc<Mutex<SynodCluster>>,
    update: SynodRoomUpdate,
) -> Result<(), axum::Error> {
    match update {
        SynodRoomUpdate::RequestState(request) => {
            send_socket_message(socket, SynodSocketServerMessage::RequestState { request }).await
        }
        SynodRoomUpdate::Applied(entry) => {
            send_socket_message(socket, SynodSocketServerMessage::Applied { entry }).await
        }
        SynodRoomUpdate::Activity(event) => {
            send_socket_message(socket, SynodSocketServerMessage::Activity { event }).await
        }
        SynodRoomUpdate::RoomChanged => send_room_state(socket, room, room_cluster).await,
    }
}

async fn handle_socket_message(
    socket: &mut WebSocket,
    room: &str,
    room_cluster: &Arc<Mutex<SynodCluster>>,
    socket_client_id: &mut Option<ClientId>,
    subscribed: &mut bool,
    payload: &str,
) -> Result<(), axum::Error> {
    let Ok(message) = serde_json::from_str::<SynodSocketClientMessage>(payload) else {
        return Ok(());
    };

    match message {
        SynodSocketClientMessage::JoinClient {
            client_id,
            client_name,
        } => {
            let joined = {
                let mut synod = room_cluster.lock().await;
                let requested_client_id = client_id.map(ClientId::from_existing);
                let already_assigned = requested_client_id
                    .as_ref()
                    .is_some_and(|client_id| synod.has_assignment(client_id));
                let assignment = match synod.assign_client(requested_client_id).await {
                    Ok(assignment) => assignment,
                    Err(err) => {
                        send_socket_message(
                            socket,
                            SynodSocketServerMessage::Error {
                                message: format!("failed to join synod room: {err}"),
                            },
                        )
                        .await?;
                        return Ok(());
                    }
                };
                if let Some(client_name) = client_name {
                    synod.record_client_name(&assignment.client_id, client_name);
                }
                if !already_assigned {
                    synod.emit_client_joined(&assignment);
                }
                let membership = synod.membership();
                JoinResponse {
                    room: room.to_string(),
                    client_id: assignment.client_id.to_string(),
                    assigned_node: assignment.node_id.to_string(),
                    active_nodes: membership.node_count(),
                    emoji_pool: RUST_EMOJI_POOL.to_vec(),
                    state: synod.room_state(),
                }
            };
            *socket_client_id = Some(ClientId::from_existing(joined.client_id.clone()));
            *subscribed = true;
            send_socket_message(socket, SynodSocketServerMessage::Joined { session: joined })
                .await?;
        }
        SynodSocketClientMessage::SubscribeObserver => {
            *subscribed = true;
            send_room_state(socket, room, room_cluster).await?;
        }
        SynodSocketClientMessage::Heartbeat => {
            let Some(client_id) = socket_client_id.as_ref() else {
                send_socket_message(
                    socket,
                    SynodSocketServerMessage::Error {
                        message: "socket has not joined as a synod client".to_string(),
                    },
                )
                .await?;
                return Ok(());
            };
            let heartbeat = {
                let mut synod = room_cluster.lock().await;
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
                    room: room.to_string(),
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

async fn send_room_state(
    socket: &mut WebSocket,
    room: &str,
    room_cluster: &Arc<Mutex<SynodCluster>>,
) -> Result<(), axum::Error> {
    let status = {
        let synod = room_cluster.lock().await;
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
                    checkpoint_slot: configuration
                        .checkpoint()
                        .and_then(|checkpoint| checkpoint.manifest().last_applied_slot()),
                });

        StatusResponse {
            room: room.to_string(),
            active_nodes: membership.node_count(),
            bootstrap_node: membership.bootstrap_node,
            active_configuration,
            clients: membership
                .assignments
                .iter()
                .map(|assignment| ClientStatus {
                    client_id: assignment.client_id.to_string(),
                    client_name: synod.client_name(&assignment.client_id),
                    node_id: assignment.node_id,
                })
                .collect(),
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

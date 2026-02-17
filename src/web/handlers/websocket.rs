use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::IntoResponse,
};
use std::net::SocketAddr;
use tracing::info;

use super::utils::{AppState, get_client_ip, get_cm};

/// Handles the WebSocket upgrade request.
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let ip = get_client_ip(&headers, addr);
    info!("WebSocket connection from {}", ip);
    ws.on_upgrade(move |socket| handle_socket(socket, state, addr, headers))
}

/// Handles a single WebSocket connection, sending events to the client.
async fn handle_socket(
    mut socket: WebSocket,
    state: AppState,
    addr: SocketAddr,
    headers: HeaderMap,
) {
    let ip = get_client_ip(&headers, addr);
    let cm = get_cm(state, addr, headers).await;
    let cm = cm.lock().await;
    let mut receiver = cm.get_observer().subscribe().await;
    info!("WebSocket subscribed for {}", ip);
    drop(cm); // Release lock before waiting for events

    loop {
        match receiver.recv().await {
            Ok(event_json) => {
                info!(
                    "Broadcasting event to {}: {}",
                    ip,
                    &event_json[..50.min(event_json.len())]
                );
                if socket.send(Message::Text(event_json)).await.is_err() {
                    // Client disconnected
                    info!("Client {} disconnected", ip);
                    break;
                }
            }
            Err(e) => {
                info!("Receiver error for {}: {}", ip, e);
                break;
            }
        }
    }
}

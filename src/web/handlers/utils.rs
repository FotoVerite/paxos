use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use axum::http::HeaderMap;
use dashmap::DashMap;
use tokio::sync::Mutex;

use super::super::cluster_manager::ClusterManager;

/// Shared state for the WebSocketObserver and ClusterManager.
#[derive(Clone)]
pub struct AppState {
    pub clusters: Arc<DashMap<IpAddr, Arc<Mutex<ClusterManager>>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clusters: Arc::new(DashMap::new()),
        }
    }
}

/// Extract or create a ClusterManager for a given client IP
pub async fn get_cm(
    state: AppState,
    addr: SocketAddr,
    headers: HeaderMap,
) -> Arc<Mutex<ClusterManager>> {
    let ip = get_client_ip(&headers, addr);
    let cluster = state
        .clusters
        .entry(ip)
        .or_insert_with(|| Arc::new(Mutex::new(ClusterManager::new())));
    cluster.clone()
}

/// Extract client IP from X-Forwarded-For header or socket address
pub fn get_client_ip(headers: &HeaderMap, addr: SocketAddr) -> IpAddr {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next()?.trim().parse().ok())
        .unwrap_or_else(|| addr.ip())
}

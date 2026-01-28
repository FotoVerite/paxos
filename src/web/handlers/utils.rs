use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use axum::http::HeaderMap;
use dashmap::DashMap;
use tokio::sync::Mutex;

use super::super::cluster_manager::ClusterManager;

use tera::Tera;

/// Shared state for the WebSocketObserver and ClusterManager.
#[derive(Clone)]
pub struct AppState {
    pub clusters: Arc<DashMap<IpAddr, Arc<Mutex<ClusterManager>>>>,
    pub tera: Arc<Tera>,
}

impl AppState {
    pub fn new() -> Self {
        // Load templates from the templates directory
        // This will find all .html files in templates/ and subdirectories
        let tera = match Tera::new("templates/**/*.html") {
            Ok(t) => t,
            Err(e) => {
                println!("Parsing error(s): {}", e);
                ::std::process::exit(1);
            }
        };

        Self {
            clusters: Arc::new(DashMap::new()),
            tera: Arc::new(tera),
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

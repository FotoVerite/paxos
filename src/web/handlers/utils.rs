use axum::http::HeaderMap;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use super::super::cluster_manager::ClusterManager;
use crate::{
    cluster::synod::{SYNOD_CLIENT_TTL, SynodCluster},
    common::persistence::ClusterPersistence,
    monitor::{NoOpObserver, PaxosObserver},
};

use tera::Tera;

/// Shared state for the WebSocketObserver and ClusterManager.
#[derive(Clone)]
pub struct AppState {
    pub clusters: Arc<DashMap<IpAddr, Arc<Mutex<ClusterManager>>>>,
    pub synod: Arc<Mutex<SynodCluster>>,
    pub tera: Arc<Tera>,
}

impl AppState {
    pub async fn new() -> anyhow::Result<Self> {
        // Load templates from the templates directory
        // This will find all .html files in templates/ and subdirectories
        let tera = match Tera::new("templates/**/*.html") {
            Ok(t) => t,
            Err(e) => {
                println!("Parsing error(s): {}", e);
                ::std::process::exit(1);
            }
        };

        let synod_observer: Arc<dyn PaxosObserver> = Arc::new(NoOpObserver);
        let synod = SynodCluster::new(
            Arc::new(ClusterPersistence::for_test("synod-main")),
            synod_observer,
        )
        .await?;
        let synod = Arc::new(Mutex::new(synod));
        spawn_synod_liveness_sweeper(Arc::clone(&synod));

        Ok(Self {
            clusters: Arc::new(DashMap::new()),
            synod,
            tera: Arc::new(tera),
        })
    }
}

fn spawn_synod_liveness_sweeper(synod: Arc<Mutex<SynodCluster>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let mut synod = synod.lock().await;
            if let Err(err) = synod.decommission_idle_clients(SYNOD_CLIENT_TTL).await {
                tracing::warn!("failed to decommission idle synod clients: {err}");
            }
        }
    });
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

pub mod handlers;
pub mod server;
pub mod websocket_observer;
pub mod cluster_manager;
pub mod scenarios;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub total_nodes: usize,
    pub quorum_size: usize,
}

#[derive(Debug, Clone, Serialize)]
pub enum VisualizerMessage {
    #[serde(rename = "ClusterInitialized")]
    ClusterInitialized(ClusterInfo),
    #[serde(rename = "Event")]
    Event(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioRequest {
    pub node_count: usize,
    pub duration_secs: u64,
    #[serde(default)]
    pub scenario_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalRequest {
    pub author: String,
    pub decree: String,
}
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub total_nodes: usize,
    pub quorum_size: usize,
    pub node_uuids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize)]
pub enum VisualizerMessage {
    #[serde(rename = "ClusterInitialized")]
    ClusterInitialized(ClusterInfo),
    #[serde(rename = "Event")]
    Event(serde_json::Value),
    #[serde(rename = "Message")]
    Message(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioRequest {
    pub node_count: usize,
    pub duration_secs: u64,
    #[serde(default)]
    pub scenario_type: String,
    #[serde(default)]
    pub learning_strategy: String,
    #[serde(default)]
    pub leader_node: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalRequest {
    pub author: String,
    pub decree: String,
}

use crate::node::config::Roles;
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ConfigurationReplyOutcome {
    Ok, // commands with no payload
    Err(ConfigurationHandlerError),
    Stopped,
    Active,
    Data,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, thiserror::Error, PartialEq, Eq)]
pub enum ConfigurationHandlerError {
    #[error("configuration request timed out")]
    Timeout,
    #[error("configuration request was canceled")]
    Canceled,
    #[error("configuration response channel closed")]
    ResponseChannelClosed,
    #[error("configuration response for unknown request id {request_id}")]
    UnknownRequestId { request_id: u64 },
    #[error("configuration endpoint {endpoint} is unavailable")]
    EndpointUnavailable { endpoint: Uuid },
    #[error("request sent to a non-leader")]
    NotLeader { leader_hint: Option<Uuid> },
    #[error("configuration request rejected: {reason}")]
    Rejected { reason: String },
    #[error("invalid configuration request: {reason}")]
    InvalidRequest { reason: String },
    #[error("configuration conflict: {reason}")]
    Conflict { reason: String },
    #[error("internal configuration handler error: {reason}")]
    Internal { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConfigurationCommand {
    Stop,
    Add { roles: Roles },
    Remove { roles: Roles },
    Emit,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ConfigurationHandlerMessage {
    Reconfigure {
        request_id: u64,
        cmd: ConfigurationCommand,
    },
    RESPONSE {
        request_id: u64,
        response: ConfigurationReplyOutcome,
    },
}

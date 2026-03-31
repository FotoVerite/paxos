use std::collections::HashMap;

use uuid::Uuid;

use crate::cluster::pmmc::control::types::ConfigurationHandlerError;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Reconfig(#[from] ReconfigError),
    #[error("stop_all failed for one or more nodes")]
    StopFailed {
        per_node: HashMap<Uuid, ConfigurationHandlerError>,
    },
    #[error("checkpoint collection failed for one or more nodes")]
    CheckpointFailed {
        per_node: HashMap<Uuid, ConfigurationHandlerError>,
    },
    #[error("no checkpoint candidate returned by replicas")]
    NoCheckpointCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
pub enum ReconfigError {
    #[error("config id must increase")]
    NonMonotonicConfigId,
    #[error("invalid membership change: {0}")]
    InvalidMembership(&'static str),
    #[error("configuration must include at least one leader")]
    NoLeaders,
    #[error("configuration must include at least one acceptor")]
    NoAcceptors,
    #[error("configuration must include at least one learner")]
    NoLearners,
    #[error("configuration strategy needs alpha assignment")]
    NoAlpha,
}

#[derive(Debug, thiserror::Error)]
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

use crate::common::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LearningStrategy {
    /// Acceptors broadcast Accepted messages to all learners directly.
    Direct,
    /// Acceptors send Accepted messages only to a specific set of distinguished learners.
    DistinguishedLearners(Vec<NodeId>),
    /// Acceptors send Accepted messages back to the Proposer (sender of Accept),
    /// who then broadcasts Success if quorum is reached.
    ProposerManaged,
}

impl Default for LearningStrategy {
    fn default() -> Self {
        Self::ProposerManaged
    }
}

#[derive(Debug, Clone)]
pub struct Roles {
    pub proposer: bool,
    pub acceptor: bool,
    pub learner: bool,
}

impl Default for Roles {
    fn default() -> Self {
        Self {
            proposer: true,
            acceptor: true,
            learner: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub roles: Roles,
    pub learning_strategy: LearningStrategy,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            roles: Roles::default(),
            learning_strategy: LearningStrategy::default(),
        }
    }
}

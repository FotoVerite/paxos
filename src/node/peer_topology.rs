use uuid::Uuid;

use crate::cluster::pmmc::reconfiguration::ClusterConfiguration;

/// Topology information about which peers have which roles.
/// This is built by the Cluster and passed to each node.
#[derive(Debug, Clone)]
pub struct PeerTopology {
    /// List of all nodes with the Acceptor role
    pub proposers: Vec<Uuid>,
    pub acceptors: Vec<Uuid>,
    /// List of all nodes with the Learner role
    pub learners: Vec<Uuid>,
}

impl PeerTopology {
    pub fn new(acceptors: Vec<Uuid>, learners: Vec<Uuid>, proposers: Vec<Uuid>) -> Self {
        Self {
            proposers,
            acceptors,
            learners,
        }
    }
}

impl From<&ClusterConfiguration> for PeerTopology {
    fn from(config: &ClusterConfiguration) -> Self {
        Self::new(config.acceptors(), config.replicas(), config.leaders())
    }
}

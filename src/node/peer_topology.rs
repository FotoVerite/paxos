use crate::common::types::NodeId;

/// Topology information about which peers have which roles.
/// This is built by the Cluster and passed to each node.
#[derive(Debug, Clone)]
pub struct PeerTopology {
    /// List of all nodes with the Acceptor role
    pub acceptors: Vec<NodeId>,
    /// List of all nodes with the Learner role
    pub learners: Vec<NodeId>,
}

impl PeerTopology {
    pub fn new(acceptors: Vec<NodeId>, learners: Vec<NodeId>) -> Self {
        Self { acceptors, learners }
    }
}

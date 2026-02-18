use crate::{
    message::Message,
    node::{config::LearningStrategy, peer_topology::PeerTopology},
};
use uuid::Uuid;

/// Represents a routing decision for a message
#[derive(Debug, Clone)]
pub enum RoutingDecision {
    /// Broadcast to all peers
    Broadcast,
    /// Send to a specific node
    SendTo(Uuid),
    /// Send to multiple specific nodes
    SendToMany(Vec<Uuid>),
    /// Drop the message (don't send)
    Drop,
}

/// Routes messages based on learning strategy and peer topology
pub struct MessageRouter {
    learning_strategy: LearningStrategy,
    topology: PeerTopology,
}

impl MessageRouter {
    pub fn new(learning_strategy: LearningStrategy, topology: PeerTopology) -> Self {
        Self {
            learning_strategy,
            topology,
        }
    }

    /// Determine how to route a response message based on its type
    pub fn route_response(&self, msg: &Message, from: Uuid) -> RoutingDecision {
        match msg {
            // Prepare and Accept messages go to acceptors only
            Message::Prepare { .. } | Message::PrepareBatch { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            }
            Message::Accept { .. } => RoutingDecision::SendToMany(self.topology.acceptors.clone()),

            // Accepted messages routing depends on learning strategy
            Message::Accepted { .. } => match &self.learning_strategy {
                LearningStrategy::ProposerManaged => {
                    // Send back to the proposer who sent the Accept
                    RoutingDecision::SendTo(from)
                }
                LearningStrategy::Direct => {
                    // Broadcast to all learners
                    RoutingDecision::SendToMany(self.topology.learners.clone())
                }
                LearningStrategy::DistinguishedLearners(learners) => {
                    // Send to specific distinguished learners
                    RoutingDecision::SendToMany(learners.clone())
                }
            },

            // Promise and Success messages are broadcast to all
            Message::Promise { .. } | Message::Success { .. } => RoutingDecision::Broadcast,

            // NACK messages are dropped
            Message::NACK => RoutingDecision::Drop,

            _ => RoutingDecision::Drop,
        }
    }

    pub fn pmmc_route_response(&self, msg: &Message) -> RoutingDecision {
        match msg {
            Message::HEARTBEAT { .. } | Message::PROPOSE { .. } => {
                RoutingDecision::SendToMany(self.topology.proposers.clone())
            }
            Message::P1A { .. } | Message::P2A { .. } | Message::ACCEPTED { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            } // Prepare and Accept messages go to acceptors only
            Message::ACK { to, .. }
            | Message::ADOPTED { to, .. }
            | Message::P1B { to, .. }
            | Message::P2B { to, .. }
            | Message::PREEMPT { to, .. } => RoutingDecision::SendTo(*to),

            _ => RoutingDecision::Drop,
        }
    }
}

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
            Message::P1A { .. } | Message::P2A { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            } // Prepare and Accept messages go to acceptors only
            Message::ACCEPTED { .. } => RoutingDecision::SendToMany(self.topology.learners.clone()),
            Message::ACK { to, .. }
            | Message::ADOPTED { to, .. }
            | Message::P1B { to, .. }
            | Message::P2B { to, .. }
            | Message::PREEMPT { to, .. } => RoutingDecision::SendTo(*to),

            _ => RoutingDecision::Drop,
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        message::Message,
        node::{
            classic_paxos::ballot::Ballot, config::LearningStrategy, peer_topology::PeerTopology,
            pvalue::PValue,
        },
        paxos_command::PaxosCommand,
    };

    use super::{MessageRouter, RoutingDecision};

    fn topo() -> PeerTopology {
        PeerTopology::new(
            vec![Uuid::from_u128(0xA1), Uuid::from_u128(0xA2)],
            vec![Uuid::from_u128(0xB1), Uuid::from_u128(0xB2)],
            vec![Uuid::from_u128(0xC1), Uuid::from_u128(0xC2)],
        )
    }

    #[test]
    fn pmmc_routes_accepted_to_learners() {
        let router = MessageRouter::new(LearningStrategy::default(), topo());
        let msg = Message::ACCEPTED {
            from: Uuid::new_v4(),
            pvalue: PValue::new(0, Ballot::new(1, Uuid::new_v4()), PaxosCommand::NOOP),
        };
        let decision = router.pmmc_route_response(&msg);
        assert!(
            matches!(decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xB1), Uuid::from_u128(0xB2)]),
            "PMMC ACCEPTED should route to learner/replica nodes"
        );
    }

    #[test]
    fn pmmc_routes_phase_messages_to_acceptors_and_leader_msgs_to_proposers() {
        let router = MessageRouter::new(LearningStrategy::default(), topo());
        let p1a = Message::P1A {
            from: Uuid::new_v4(),
            ballot: Ballot::new(1, Uuid::new_v4()),
            start_index: 0,
        };
        let propose = Message::PROPOSE {
            from: Uuid::new_v4(),
            slot: 0,
            cmd: PaxosCommand::NOOP,
        };

        let p1a_decision = router.pmmc_route_response(&p1a);
        let propose_decision = router.pmmc_route_response(&propose);

        assert!(matches!(p1a_decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xA1), Uuid::from_u128(0xA2)]));
        assert!(matches!(propose_decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xC1), Uuid::from_u128(0xC2)]));
    }

    #[test]
    fn pmmc_routes_reply_messages_to_explicit_target() {
        let router = MessageRouter::new(LearningStrategy::default(), topo());
        let to = Uuid::new_v4();
        let msg = Message::ACK {
            from: Uuid::new_v4(),
            to,
            slot: 7,
        };
        let decision = router.pmmc_route_response(&msg);
        assert!(matches!(decision, RoutingDecision::SendTo(t) if t == to));
    }
}

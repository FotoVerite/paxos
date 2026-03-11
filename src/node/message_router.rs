use crate::node::{
    classic_paxos::message::ClassicMessage, config::LearningStrategy, peer_topology::PeerTopology,
    pmmc::message::PmmcMessage,
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

    pub fn route_classic(&self, msg: &ClassicMessage, from: Uuid) -> RoutingDecision {
        match msg {
            // Prepare and Accept messages go to acceptors only
            ClassicMessage::Prepare { .. } | ClassicMessage::PrepareBatch { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            }
            ClassicMessage::Accept { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            }

            // Accepted messages routing depends on learning strategy
            ClassicMessage::Accepted { .. } => match &self.learning_strategy {
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
            ClassicMessage::Promise { .. } | ClassicMessage::Success { .. } => {
                RoutingDecision::Broadcast
            }
        }
    }

    pub fn route_pmmc(&self, msg: &PmmcMessage) -> RoutingDecision {
        match msg {
            PmmcMessage::HEARTBEAT { .. } | PmmcMessage::PROPOSE { .. } => {
                RoutingDecision::SendToMany(self.topology.proposers.clone())
            }
            PmmcMessage::P1A { .. } | PmmcMessage::P2A { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            } // Prepare and Accept messages go to acceptors only
            PmmcMessage::ACCEPTED { .. } => {
                RoutingDecision::SendToMany(self.topology.learners.clone())
            }
            PmmcMessage::ACK { to, .. }
            | PmmcMessage::ADOPTED { to, .. }
            | PmmcMessage::P1B { to, .. }
            | PmmcMessage::P2B { to, .. }
            | PmmcMessage::PREEMPT { to, .. } => RoutingDecision::SendTo(*to),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        common::ballot::Ballot,
        node::{
            classic_paxos::message::ClassicMessage, config::LearningStrategy,
            peer_topology::PeerTopology, pmmc::message::PmmcMessage, pvalue::PValue,
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
        let msg = PmmcMessage::ACCEPTED {
            from: Uuid::new_v4(),
            pvalue: PValue::new(0, Ballot::new(1, Uuid::new_v4()), PaxosCommand::NOOP),
        };
        let decision = router.route_pmmc(&msg);
        assert!(
            matches!(decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xB1), Uuid::from_u128(0xB2)]),
            "PMMC ACCEPTED should route to learner/replica nodes"
        );
    }

    #[test]
    fn pmmc_routes_phase_messages_to_acceptors_and_leader_msgs_to_proposers() {
        let router = MessageRouter::new(LearningStrategy::default(), topo());
        let p1a = PmmcMessage::P1A {
            from: Uuid::new_v4(),
            ballot: Ballot::new(1, Uuid::new_v4()),
            start_index: 0,
        };
        let propose = PmmcMessage::PROPOSE {
            from: Uuid::new_v4(),
            slot: 0,
            cmd: PaxosCommand::NOOP,
        };

        let p1a_decision = router.route_pmmc(&p1a);
        let propose_decision = router.route_pmmc(&propose);

        assert!(
            matches!(p1a_decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xA1), Uuid::from_u128(0xA2)])
        );
        assert!(
            matches!(propose_decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xC1), Uuid::from_u128(0xC2)])
        );
    }

    #[test]
    fn pmmc_routes_reply_messages_to_explicit_target() {
        let router = MessageRouter::new(LearningStrategy::default(), topo());
        let to = Uuid::new_v4();
        let msg = PmmcMessage::ACK {
            from: Uuid::new_v4(),
            to,
            slot: 7,
        };
        let decision = router.route_pmmc(&msg);
        assert!(matches!(decision, RoutingDecision::SendTo(t) if t == to));
    }

    #[test]
    fn classic_routes_accepted_to_proposer_in_proposer_managed_mode() {
        let router = MessageRouter::new(LearningStrategy::ProposerManaged, topo());
        let accept_sender = Uuid::from_u128(0xDEADBEEF);
        let msg = ClassicMessage::Accepted {
            from: Uuid::new_v4(),
            decree_num: crate::common::types::DecreeId(1),
            ballot: Ballot::new(1, Uuid::new_v4()),
            value: PaxosCommand::NOOP,
        };

        let decision = router.route_classic(&msg, accept_sender);
        assert!(matches!(decision, RoutingDecision::SendTo(node) if node == accept_sender));
    }
}

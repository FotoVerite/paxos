use crate::node::{
    peer_topology::PeerTopology,
    pmmc::message::PmmcMessage,
    routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PmmcComponent {
    Leader,
    Acceptor,
    Replica,
}

pub struct PmmcRouter {
    topology: PeerTopology,
}

impl PmmcRouter {
    pub fn new(topology: PeerTopology) -> Self {
        Self { topology }
    }
}

impl ProtocolRouter<PmmcMessage> for PmmcRouter {
    type Context = ();

    fn route(&self, msg: &PmmcMessage, _context: &Self::Context) -> RoutingDecision {
        match msg {
            PmmcMessage::HEARTBEAT { .. } | PmmcMessage::PROPOSE { .. } => {
                RoutingDecision::SendToMany(self.topology.proposers.clone())
            }
            PmmcMessage::P1A { .. } | PmmcMessage::P2A { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            }
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

impl ProtocolInboxRouter<PmmcMessage> for PmmcRouter {
    type Context = ();
    type Target = PmmcComponent;

    fn classify(
        &self,
        msg: &PmmcMessage,
        _context: &Self::Context,
    ) -> LocalRoutingDecision<Self::Target> {
        match msg {
            PmmcMessage::ACK { .. }
            | PmmcMessage::ADOPTED { .. }
            | PmmcMessage::HEARTBEAT { .. }
            | PmmcMessage::P1B { .. }
            | PmmcMessage::P2B { .. }
            | PmmcMessage::PROPOSE { .. }
            | PmmcMessage::PREEMPT { .. } => LocalRoutingDecision::DeliverTo(PmmcComponent::Leader),
            PmmcMessage::P1A { .. } | PmmcMessage::P2A { .. } => {
                LocalRoutingDecision::DeliverTo(PmmcComponent::Acceptor)
            }
            PmmcMessage::ACCEPTED { .. } => LocalRoutingDecision::DeliverTo(PmmcComponent::Replica),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        common::ballot::Ballot,
        node::{
            peer_topology::PeerTopology,
            pmmc::{
                message::PmmcMessage,
                router::{PmmcComponent, PmmcRouter},
            },
            pvalue::PValue,
            routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
        },
        paxos_command::PaxosCommand,
    };

    fn topo() -> PeerTopology {
        PeerTopology::new(
            vec![Uuid::from_u128(0xA1), Uuid::from_u128(0xA2)],
            vec![Uuid::from_u128(0xB1), Uuid::from_u128(0xB2)],
            vec![Uuid::from_u128(0xC1), Uuid::from_u128(0xC2)],
        )
    }

    #[test]
    fn routes_accepted_to_learners() {
        let router = PmmcRouter::new(topo());
        let msg = PmmcMessage::ACCEPTED {
            from: Uuid::new_v4(),
            pvalue: PValue::new(0, Ballot::new(1, Uuid::new_v4()), PaxosCommand::NOOP),
        };
        let decision = router.route(&msg, &());
        assert!(
            matches!(decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xB1), Uuid::from_u128(0xB2)])
        );
    }

    #[test]
    fn routes_phase_messages_to_acceptors_and_leader_msgs_to_proposers() {
        let router = PmmcRouter::new(topo());
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

        let p1a_decision = router.route(&p1a, &());
        let propose_decision = router.route(&propose, &());

        assert!(
            matches!(p1a_decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xA1), Uuid::from_u128(0xA2)])
        );
        assert!(
            matches!(propose_decision, RoutingDecision::SendToMany(nodes) if nodes == vec![Uuid::from_u128(0xC1), Uuid::from_u128(0xC2)])
        );
    }

    #[test]
    fn routes_reply_messages_to_explicit_target() {
        let router = PmmcRouter::new(topo());
        let to = Uuid::new_v4();
        let msg = PmmcMessage::ACK {
            from: Uuid::new_v4(),
            to,
            slot: 7,
        };
        let decision = router.route(&msg, &());
        assert!(matches!(decision, RoutingDecision::SendTo(t) if t == to));
    }

    #[test]
    fn classifies_accepted_to_the_replica() {
        let router = PmmcRouter::new(topo());
        let msg = PmmcMessage::ACCEPTED {
            from: Uuid::new_v4(),
            pvalue: PValue::new(0, Ballot::new(1, Uuid::new_v4()), PaxosCommand::NOOP),
        };

        assert_eq!(
            router.classify(&msg, &()),
            LocalRoutingDecision::DeliverTo(PmmcComponent::Replica)
        );
    }
}

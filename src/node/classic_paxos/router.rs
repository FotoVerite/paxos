use uuid::Uuid;

use crate::node::{
    classic_paxos::message::ClassicMessage,
    config::LearningStrategy,
    peer_topology::PeerTopology,
    routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassicComponent {
    Proposer,
    Acceptor,
    Learner,
}

pub struct ClassicRouter {
    learning_strategy: LearningStrategy,
    topology: PeerTopology,
}

impl ClassicRouter {
    pub fn new(learning_strategy: LearningStrategy, topology: PeerTopology) -> Self {
        Self {
            learning_strategy,
            topology,
        }
    }
}

impl ProtocolRouter<ClassicMessage> for ClassicRouter {
    type Context = Uuid;

    fn route(&self, msg: &ClassicMessage, from: &Self::Context) -> RoutingDecision {
        match msg {
            ClassicMessage::Prepare { .. } | ClassicMessage::PrepareBatch { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            }
            ClassicMessage::Accept { .. } => {
                RoutingDecision::SendToMany(self.topology.acceptors.clone())
            }
            ClassicMessage::Accepted { .. } => match &self.learning_strategy {
                LearningStrategy::ProposerManaged => RoutingDecision::SendTo(*from),
                LearningStrategy::Direct => {
                    RoutingDecision::SendToMany(self.topology.learners.clone())
                }
                LearningStrategy::DistinguishedLearners(learners) => {
                    RoutingDecision::SendToMany(learners.clone())
                }
            },
            ClassicMessage::Promise { .. } | ClassicMessage::Success { .. } => {
                RoutingDecision::Broadcast
            }
        }
    }
}

impl ProtocolInboxRouter<ClassicMessage> for ClassicRouter {
    type Context = ();
    type Target = ClassicComponent;

    fn classify(
        &self,
        msg: &ClassicMessage,
        _context: &Self::Context,
    ) -> LocalRoutingDecision<Self::Target> {
        match msg {
            ClassicMessage::Promise { .. } => {
                LocalRoutingDecision::DeliverTo(ClassicComponent::Proposer)
            }
            ClassicMessage::Prepare { .. }
            | ClassicMessage::PrepareBatch { .. }
            | ClassicMessage::Accept { .. } => {
                LocalRoutingDecision::DeliverTo(ClassicComponent::Acceptor)
            }
            ClassicMessage::Accepted { .. } | ClassicMessage::Success { .. } => {
                LocalRoutingDecision::DeliverTo(ClassicComponent::Learner)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        common::ballot::Ballot,
        node::{
            classic_paxos::{
                message::ClassicMessage,
                router::{ClassicComponent, ClassicRouter},
            },
            config::LearningStrategy,
            peer_topology::PeerTopology,
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
    fn routes_accepted_to_proposer_in_proposer_managed_mode() {
        let router = ClassicRouter::new(LearningStrategy::ProposerManaged, topo());
        let accept_sender = Uuid::from_u128(0xDEADBEEF);
        let msg = ClassicMessage::Accepted {
            from: Uuid::new_v4(),
            decree_num: crate::common::types::DecreeId(1),
            ballot: Ballot::new(1, Uuid::new_v4()),
            value: PaxosCommand::NOOP,
        };

        let decision = router.route(&msg, &accept_sender);
        assert!(matches!(decision, RoutingDecision::SendTo(node) if node == accept_sender));
    }

    #[test]
    fn classifies_promises_to_the_proposer() {
        let router = ClassicRouter::new(LearningStrategy::ProposerManaged, topo());
        let msg = ClassicMessage::Promise {
            from: Uuid::new_v4(),
            decree_num: crate::common::types::DecreeId(1),
            ballot: Ballot::new(1, Uuid::new_v4()),
            accepted_ballot: Ballot::new(0, Uuid::nil()),
            accepted_value: PaxosCommand::NOOP,
        };

        assert_eq!(
            router.classify(&msg, &()),
            LocalRoutingDecision::DeliverTo(ClassicComponent::Proposer)
        );
    }
}

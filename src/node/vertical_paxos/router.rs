use crate::node::{
    routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
    vertical_paxos::message::VerticalPaxosMessage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerticalPaxosComponent {
    Acceptor,
    Leader,
    Replica,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerticalInboxContext {
    pub acceptor_online: bool,
}

pub struct VerticalPaxosRouter;

impl ProtocolRouter<VerticalPaxosMessage> for VerticalPaxosRouter {
    type Context = ();

    fn route(&self, msg: &VerticalPaxosMessage, _context: &Self::Context) -> RoutingDecision {
        match msg {
            VerticalPaxosMessage::ACK { to, .. }
            | VerticalPaxosMessage::ADOPTED { to, .. }
            | VerticalPaxosMessage::PREEMPT { to, .. }
            | VerticalPaxosMessage::P1B { to, .. }
            | VerticalPaxosMessage::P2B { to, .. } => RoutingDecision::SendTo(*to),
            _ => RoutingDecision::Drop,
        }
    }
}

impl ProtocolInboxRouter<VerticalPaxosMessage> for VerticalPaxosRouter {
    type Context = VerticalInboxContext;
    type Target = VerticalPaxosComponent;

    fn classify(
        &self,
        msg: &VerticalPaxosMessage,
        context: &Self::Context,
    ) -> LocalRoutingDecision<Self::Target> {
        match msg {
            VerticalPaxosMessage::P1A { .. } | VerticalPaxosMessage::P2A { .. } => {
                if context.acceptor_online {
                    LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Acceptor)
                } else {
                    LocalRoutingDecision::Drop
                }
            }
            VerticalPaxosMessage::P1B { .. }
            | VerticalPaxosMessage::P2B { .. }
            | VerticalPaxosMessage::PROPOSE { .. }
            | VerticalPaxosMessage::ACK { .. }
            | VerticalPaxosMessage::PREEMPT { .. } => {
                LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Leader)
            }
            VerticalPaxosMessage::ACCEPTED { .. } => {
                LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Replica)
            }
            _ => LocalRoutingDecision::Drop,
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        common::ballot::Ballot,
        node::{
            routing::{LocalRoutingDecision, ProtocolInboxRouter, ProtocolRouter, RoutingDecision},
            vertical_paxos::{
                message::VerticalPaxosMessage,
                router::{VerticalInboxContext, VerticalPaxosComponent, VerticalPaxosRouter},
            },
        },
    };

    #[test]
    fn routes_reply_messages_to_their_explicit_target() {
        let router = VerticalPaxosRouter;
        let to = Uuid::new_v4();
        let msg = VerticalPaxosMessage::P1B {
            from: Uuid::new_v4(),
            to,
            ballot: Ballot::new(1, Uuid::new_v4()),
            pvalues: vec![],
        };

        let decision = router.route(&msg, &());
        assert!(matches!(decision, RoutingDecision::SendTo(target) if target == to));
    }

    #[test]
    fn drops_non_reply_messages() {
        let router = VerticalPaxosRouter;
        let msg = VerticalPaxosMessage::P1A {
            from: Uuid::new_v4(),
            ballot: Ballot::new(1, Uuid::new_v4()),
            start_index: 0,
        };

        assert_eq!(router.route(&msg, &()), RoutingDecision::Drop);
    }

    #[test]
    fn classifies_accept_phase_messages_to_acceptor_when_online() {
        let router = VerticalPaxosRouter;
        let msg = VerticalPaxosMessage::P2A {
            from: Uuid::new_v4(),
            pvalue: crate::node::pvalue::PValue::new(
                0,
                Ballot::new(1, Uuid::new_v4()),
                crate::paxos_command::PaxosCommand::NOOP,
            ),
        };

        assert_eq!(
            router.classify(
                &msg,
                &VerticalInboxContext {
                    acceptor_online: true,
                }
            ),
            LocalRoutingDecision::DeliverTo(VerticalPaxosComponent::Acceptor)
        );
    }
}

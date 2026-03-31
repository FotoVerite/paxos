//! Observer-facing message payloads.
//!
//! This is the serialization boundary for traces and websocket output.
//! It is intentionally separate from protocol-domain message types.

use std::collections::HashSet;

use uuid::Uuid;

use crate::{
    common::{ballot::Ballot, types::DecreeId},
    node::pvalue::PValue,
    paxos_command::PaxosCommand,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum MessageProtocol {
    Classic,
    Pmmc,
    Vertical,
    System,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize)]
pub enum ObservedMessage {
    Prepare {
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
    },
    Promise {
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
    },
    Accept {
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
        value: PaxosCommand,
        quorum: HashSet<Uuid>,
    },
    Accepted {
        from: Uuid,
        decree_num: DecreeId,
        ballot: Ballot,
        value: PaxosCommand,
    },
    NACK,
    Success {
        from: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        ballot_proposer: Uuid,
    },
    PrepareBatch {
        from: Uuid,
        decrees_to: DecreeId,
        ballot: Ballot,
    },
    ACK {
        from: Uuid,
        to: Uuid,
        slot: usize,
    },
    ACCEPTED {
        from: Uuid,
        pvalue: PValue,
    },
    ADOPTED {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        pvalues: Vec<PValue>,
    },
    HEARTBEAT {
        from: Uuid,
        ballot: Ballot,
    },
    PROPOSE {
        from: Uuid,
        slot: usize,
        cmd: PaxosCommand,
    },
    PREEMPT {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
    },
    P1A {
        from: Uuid,
        ballot: Ballot,
        start_index: usize,
    },
    P1B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        pvalues: Vec<PValue>,
    },
    P2A {
        from: Uuid,
        pvalue: PValue,
    },
    P2B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        pvalue: PValue,
    },
}

impl ObservedMessage {
    pub fn protocol(&self) -> MessageProtocol {
        match self {
            Self::Prepare { .. }
            | Self::Promise { .. }
            | Self::Accept { .. }
            | Self::Accepted { .. }
            | Self::Success { .. }
            | Self::PrepareBatch { .. } => MessageProtocol::Classic,
            Self::ACK { .. }
            | Self::ACCEPTED { .. }
            | Self::ADOPTED { .. }
            | Self::HEARTBEAT { .. }
            | Self::PROPOSE { .. }
            | Self::PREEMPT { .. }
            | Self::P1A { .. }
            | Self::P1B { .. }
            | Self::P2A { .. }
            | Self::P2B { .. } => MessageProtocol::Pmmc,
            Self::NACK => MessageProtocol::System,
        }
    }

    pub fn should_broadcast_to_visualizer(&self) -> bool {
        !matches!(self, Self::NACK)
    }
}

pub trait TraceableMessage {
    fn protocol(&self) -> MessageProtocol;
    fn to_observed_message(&self) -> ObservedMessage;
}

impl TraceableMessage for ObservedMessage {
    fn protocol(&self) -> MessageProtocol {
        self.protocol()
    }

    fn to_observed_message(&self) -> ObservedMessage {
        self.clone()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MessageTrace {
    indexes: Vec<Uuid>,
    protocol: MessageProtocol,
    message: ObservedMessage,
}

impl MessageTrace {
    pub fn new(indexes: Vec<Uuid>, protocol: MessageProtocol, message: ObservedMessage) -> Self {
        Self {
            indexes,
            protocol,
            message,
        }
    }

    pub fn from_message<T>(indexes: &[Uuid], message: &T) -> Self
    where
        T: TraceableMessage,
    {
        Self::new(
            indexes.to_vec(),
            message.protocol(),
            message.to_observed_message(),
        )
    }

    pub fn indexes(&self) -> &[Uuid] {
        &self.indexes
    }

    pub fn protocol(&self) -> MessageProtocol {
        self.protocol
    }

    pub fn message(&self) -> &ObservedMessage {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{MessageProtocol, MessageTrace, ObservedMessage};
    use crate::common::ballot::Ballot;
    use crate::common::types::DecreeId;
    use crate::paxos_command::PaxosCommand;

    #[test]
    fn classifies_classic_message_trace() {
        let id = Uuid::new_v4();
        let message = ObservedMessage::Prepare {
            from: id,
            decree_num: DecreeId(1),
            ballot: Ballot::new(1, id),
        };
        let trace = MessageTrace::from_message(&[id], &message);
        assert_eq!(trace.protocol(), MessageProtocol::Classic);
        assert_eq!(trace.message(), &message);
    }

    #[test]
    fn classifies_pmmc_message_trace() {
        let id = Uuid::new_v4();
        let message = ObservedMessage::PROPOSE {
            from: id,
            slot: 7,
            cmd: PaxosCommand::NOOP,
        };
        let trace = MessageTrace::from_message(&[id], &message);
        assert_eq!(trace.protocol(), MessageProtocol::Pmmc);
        assert_eq!(trace.message(), &message);
    }

    #[test]
    fn classifies_system_message_trace() {
        let message = ObservedMessage::NACK;
        let trace = MessageTrace::from_message(&[], &message);
        assert_eq!(trace.protocol(), MessageProtocol::System);
        assert_eq!(trace.message(), &message);
    }
}

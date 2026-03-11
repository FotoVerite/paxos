use uuid::Uuid;

use crate::message::MessageProtocol;
use crate::node::classic_paxos::message::ClassicMessage;
use crate::node::pmmc::message::PmmcMessage;

use super::Message;

#[derive(Debug, Clone, serde::Serialize)]
pub enum MessageTracePayload {
    Classic(ClassicMessage),
    Pmmc(PmmcMessage),
    System(Message),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MessageTrace {
    indexes: Vec<Uuid>,
    payload: MessageTracePayload,
}

impl MessageTrace {
    pub fn new(indexes: Vec<Uuid>, payload: MessageTracePayload) -> Self {
        Self { indexes, payload }
    }

    pub fn from_legacy(indexes: &[Uuid], message: &Message) -> Self {
        if let Ok(classic) = ClassicMessage::try_from(message) {
            return Self::new(indexes.to_vec(), MessageTracePayload::Classic(classic));
        }
        if let Ok(pmmc) = PmmcMessage::try_from(message) {
            return Self::new(indexes.to_vec(), MessageTracePayload::Pmmc(pmmc));
        }
        Self::new(
            indexes.to_vec(),
            MessageTracePayload::System(message.clone()),
        )
    }

    pub fn indexes(&self) -> &[Uuid] {
        &self.indexes
    }

    pub fn payload(&self) -> &MessageTracePayload {
        &self.payload
    }

    pub fn protocol(&self) -> MessageProtocol {
        match self.payload {
            MessageTracePayload::Classic(_) => MessageProtocol::Classic,
            MessageTracePayload::Pmmc(_) => MessageProtocol::Pmmc,
            MessageTracePayload::System(_) => MessageProtocol::System,
        }
    }

    pub fn to_legacy_message(&self) -> Message {
        match &self.payload {
            MessageTracePayload::Classic(msg) => msg.clone().into(),
            MessageTracePayload::Pmmc(msg) => msg.clone().into(),
            MessageTracePayload::System(msg) => msg.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{MessageTrace, MessageTracePayload};
    use crate::common::ballot::Ballot;
    use crate::common::types::DecreeId;
    use crate::message::Message;
    use crate::paxos_command::PaxosCommand;

    #[test]
    fn classifies_classic_message_trace() {
        let id = Uuid::new_v4();
        let message = Message::Prepare {
            from: id,
            decree_num: DecreeId(1),
            ballot: Ballot::new(1, id),
        };
        let trace = MessageTrace::from_legacy(&[id], &message);
        assert!(matches!(trace.payload(), MessageTracePayload::Classic(_)));
        assert_eq!(trace.to_legacy_message(), message);
    }

    #[test]
    fn classifies_pmmc_message_trace() {
        let id = Uuid::new_v4();
        let message = Message::PROPOSE {
            from: id,
            slot: 7,
            cmd: PaxosCommand::NOOP,
        };
        let trace = MessageTrace::from_legacy(&[id], &message);
        assert!(matches!(trace.payload(), MessageTracePayload::Pmmc(_)));
        assert_eq!(trace.to_legacy_message(), message);
    }

    #[test]
    fn classifies_system_message_trace() {
        let message = Message::NACK;
        let trace = MessageTrace::from_legacy(&[], &message);
        assert!(matches!(trace.payload(), MessageTracePayload::System(_)));
        assert_eq!(trace.to_legacy_message(), message);
    }
}

use std::collections::HashSet;

use uuid::Uuid;

use crate::common::{ballot::Ballot, types::DecreeId};
use crate::monitor::{MessageProtocol, ObservedMessage, TraceableMessage};
use crate::paxos_command::PaxosCommand;

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize)]
pub enum ClassicMessage {
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
}

impl From<ClassicMessage> for ObservedMessage {
    fn from(value: ClassicMessage) -> Self {
        match value {
            ClassicMessage::Prepare {
                from,
                decree_num,
                ballot,
            } => Self::Prepare {
                from,
                decree_num,
                ballot,
            },
            ClassicMessage::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            } => Self::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            },
            ClassicMessage::Accept {
                from,
                decree_num,
                ballot,
                value,
                quorum,
            } => Self::Accept {
                from,
                decree_num,
                ballot,
                value,
                quorum,
            },
            ClassicMessage::Accepted {
                from,
                decree_num,
                ballot,
                value,
            } => Self::Accepted {
                from,
                decree_num,
                ballot,
                value,
            },
            ClassicMessage::Success {
                from,
                decree_num,
                value,
                ballot_proposer,
            } => Self::Success {
                from,
                decree_num,
                value,
                ballot_proposer,
            },
            ClassicMessage::PrepareBatch {
                from,
                decrees_to,
                ballot,
            } => Self::PrepareBatch {
                from,
                decrees_to,
                ballot,
            },
        }
    }
}

impl From<&ClassicMessage> for ObservedMessage {
    fn from(value: &ClassicMessage) -> Self {
        value.clone().into()
    }
}

impl TraceableMessage for ClassicMessage {
    fn protocol(&self) -> MessageProtocol {
        MessageProtocol::Classic
    }

    fn to_observed_message(&self) -> ObservedMessage {
        self.into()
    }
}

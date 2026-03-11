use std::collections::HashSet;

use uuid::Uuid;

use crate::common::{ballot::Ballot, types::DecreeId};
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

impl TryFrom<&crate::message::Message> for ClassicMessage {
    type Error = ();

    fn try_from(value: &crate::message::Message) -> Result<Self, Self::Error> {
        match value {
            crate::message::Message::Prepare {
                from,
                decree_num,
                ballot,
            } => Ok(Self::Prepare {
                from: *from,
                decree_num: *decree_num,
                ballot: *ballot,
            }),
            crate::message::Message::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            } => Ok(Self::Promise {
                from: *from,
                decree_num: *decree_num,
                ballot: *ballot,
                accepted_ballot: *accepted_ballot,
                accepted_value: accepted_value.clone(),
            }),
            crate::message::Message::Accept {
                from,
                decree_num,
                ballot,
                value,
                quorum,
            } => Ok(Self::Accept {
                from: *from,
                decree_num: *decree_num,
                ballot: *ballot,
                value: value.clone(),
                quorum: quorum.clone(),
            }),
            crate::message::Message::Accepted {
                from,
                decree_num,
                ballot,
                value,
            } => Ok(Self::Accepted {
                from: *from,
                decree_num: *decree_num,
                ballot: *ballot,
                value: value.clone(),
            }),
            crate::message::Message::Success {
                from,
                decree_num,
                value,
                ballot_proposer,
            } => Ok(Self::Success {
                from: *from,
                decree_num: *decree_num,
                value: value.clone(),
                ballot_proposer: *ballot_proposer,
            }),
            crate::message::Message::PrepareBatch {
                from,
                decrees_to,
                ballot,
            } => Ok(Self::PrepareBatch {
                from: *from,
                decrees_to: *decrees_to,
                ballot: *ballot,
            }),
            _ => Err(()),
        }
    }
}

impl From<ClassicMessage> for crate::message::Message {
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

use uuid::Uuid;

use crate::common::ballot::Ballot;
use crate::node::pvalue::PValue;
use crate::paxos_command::PaxosCommand;

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize)]
pub enum PmmcMessage {
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

impl TryFrom<&crate::message::Message> for PmmcMessage {
    type Error = ();

    fn try_from(value: &crate::message::Message) -> Result<Self, Self::Error> {
        match value {
            crate::message::Message::ACK { from, to, slot } => Ok(Self::ACK {
                from: *from,
                to: *to,
                slot: *slot,
            }),
            crate::message::Message::ACCEPTED { from, pvalue } => Ok(Self::ACCEPTED {
                from: *from,
                pvalue: pvalue.clone(),
            }),
            crate::message::Message::ADOPTED {
                from,
                to,
                ballot,
                pvalues,
            } => Ok(Self::ADOPTED {
                from: *from,
                to: *to,
                ballot: *ballot,
                pvalues: pvalues.clone(),
            }),
            crate::message::Message::HEARTBEAT { from, ballot } => Ok(Self::HEARTBEAT {
                from: *from,
                ballot: *ballot,
            }),
            crate::message::Message::PROPOSE { from, slot, cmd } => Ok(Self::PROPOSE {
                from: *from,
                slot: *slot,
                cmd: cmd.clone(),
            }),
            crate::message::Message::PREEMPT { from, to, ballot } => Ok(Self::PREEMPT {
                from: *from,
                to: *to,
                ballot: *ballot,
            }),
            crate::message::Message::P1A {
                from,
                ballot,
                start_index,
            } => Ok(Self::P1A {
                from: *from,
                ballot: *ballot,
                start_index: *start_index,
            }),
            crate::message::Message::P1B {
                from,
                to,
                ballot,
                pvalues,
            } => Ok(Self::P1B {
                from: *from,
                to: *to,
                ballot: *ballot,
                pvalues: pvalues.clone(),
            }),
            crate::message::Message::P2A { from, pvalue } => Ok(Self::P2A {
                from: *from,
                pvalue: pvalue.clone(),
            }),
            crate::message::Message::P2B {
                from,
                to,
                ballot,
                pvalue,
            } => Ok(Self::P2B {
                from: *from,
                to: *to,
                ballot: *ballot,
                pvalue: pvalue.clone(),
            }),
            _ => Err(()),
        }
    }
}

impl TryFrom<crate::message::Message> for PmmcMessage {
    type Error = ();

    fn try_from(value: crate::message::Message) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl From<PmmcMessage> for crate::message::Message {
    fn from(value: PmmcMessage) -> Self {
        match value {
            PmmcMessage::ACK { from, to, slot } => Self::ACK { from, to, slot },
            PmmcMessage::ACCEPTED { from, pvalue } => Self::ACCEPTED { from, pvalue },
            PmmcMessage::ADOPTED {
                from,
                to,
                ballot,
                pvalues,
            } => Self::ADOPTED {
                from,
                to,
                ballot,
                pvalues,
            },
            PmmcMessage::HEARTBEAT { from, ballot } => Self::HEARTBEAT { from, ballot },
            PmmcMessage::PROPOSE { from, slot, cmd } => Self::PROPOSE { from, slot, cmd },
            PmmcMessage::PREEMPT { from, to, ballot } => Self::PREEMPT { from, to, ballot },
            PmmcMessage::P1A {
                from,
                ballot,
                start_index,
            } => Self::P1A {
                from,
                ballot,
                start_index,
            },
            PmmcMessage::P1B {
                from,
                to,
                ballot,
                pvalues,
            } => Self::P1B {
                from,
                to,
                ballot,
                pvalues,
            },
            PmmcMessage::P2A { from, pvalue } => Self::P2A { from, pvalue },
            PmmcMessage::P2B {
                from,
                to,
                ballot,
                pvalue,
            } => Self::P2B {
                from,
                to,
                ballot,
                pvalue,
            },
        }
    }
}

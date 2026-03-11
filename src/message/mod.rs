pub mod trace;
pub use trace::MessageTrace;

use std::collections::HashSet;

use uuid::Uuid;

use crate::{
    common::ballot::Ballot, common::types::DecreeId, node::pvalue::PValue,
    paxos_command::PaxosCommand, rsm::kv_store::ReplyOutcome,
};

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize)]
pub enum Message {
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
        ballot_proposer: Uuid, // Track which node originated this proposal
    },

    PrepareBatch {
        from: Uuid,
        decrees_to: DecreeId,
        ballot: Ballot,
    },

    //PMMC
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum MessageProtocol {
    Classic,
    Pmmc,
    System,
}

impl Message {
    pub fn protocol(&self) -> MessageProtocol {
        match self {
            Message::Prepare { .. }
            | Message::Promise { .. }
            | Message::Accept { .. }
            | Message::Accepted { .. }
            | Message::Success { .. }
            | Message::PrepareBatch { .. } => MessageProtocol::Classic,

            Message::ACK { .. }
            | Message::ACCEPTED { .. }
            | Message::ADOPTED { .. }
            | Message::HEARTBEAT { .. }
            | Message::PROPOSE { .. }
            | Message::PREEMPT { .. }
            | Message::P1A { .. }
            | Message::P1B { .. }
            | Message::P2A { .. }
            | Message::P2B { .. } => MessageProtocol::Pmmc,

            Message::NACK => MessageProtocol::System,
        }
    }

    pub fn should_broadcast_to_visualizer(&self) -> bool {
        !matches!(self, Message::NACK)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum ClientMessage {
    PROPOSE {
        cmd: PaxosCommand,
    },
    RESPONSE {
        request_id: u64,
        response: ReplyOutcome,
    },
}

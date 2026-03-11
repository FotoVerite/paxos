use std::collections::HashSet;

use uuid::Uuid;

use crate::common::ballot::Ballot;
use crate::node::pvalue::PValue;
use crate::{common::types::DecreeId, paxos_command::PaxosCommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum EventProtocol {
    Classic,
    Pmmc,
    System,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Event {
    Proposal {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Promise {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        created_at: u64,
    },
    Accept {
        id: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        quorum: HashSet<Uuid>,
        value: PaxosCommand,
        created_at: u64,
    },
    Accepted {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        ballot: usize,
        value: PaxosCommand,
        created_at: u64,
    },
    Learn {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    Success {
        id: Uuid,
        from: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    LearnedValue {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    InitialDecree {
        id: Uuid,
        decree_num: DecreeId,
        value: PaxosCommand,
        created_at: u64,
    },
    BatchInitialDecrees {
        id: Uuid,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    LedgerDump {
        id: Uuid,
        decrees: Vec<(DecreeId, PaxosCommand)>,
        created_at: u64,
    },
    NodeState {
        id: Uuid,
        role: String,
        ballot: usize,
        learned_count: usize,
    },
    NodeCapabilities {
        id: Uuid,
        roles: Vec<String>,
        learning_strategy: String,
    },
    MessageSent {
        from: Uuid,
        to: Uuid,
        message_type: String,
    },
    BallotAdopted {
        id: Uuid,
        ballot: Ballot,
    },
    ProposalAccepted {
        id: Uuid,
        pvalue: PValue,
    },
    PmmcPropose {
        id: Uuid,
        slot: usize,
        cmd: PaxosCommand,
        created_at: u64,
    },
    PmmcP1A {
        from: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcP1B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcP2A {
        from: Uuid,
        pvalue: PValue,
        created_at: u64,
    },
    PmmcP2B {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        pvalue: PValue,
        created_at: u64,
    },
    PmmcAdopted {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcPreempted {
        from: Uuid,
        to: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcHeartbeat {
        from: Uuid,
        ballot: Ballot,
        created_at: u64,
    },
    PmmcAck {
        from: Uuid,
        to: Uuid,
        slot: usize,
        created_at: u64,
    },
    PartitionCreated {
        partition_a: Vec<Uuid>,
        partition_b: Vec<Uuid>,
        created_at: u64,
    },
    PartitionHealed {
        partition_a: Vec<Uuid>,
        partition_b: Vec<Uuid>,
        created_at: u64,
    },
    LeaderElected {
        id: Uuid,
        created_at: u64,
    },
    LeaderSteppedDown {
        id: Uuid,
        created_at: u64,
    },
    NodeCrashed {
        id: Uuid,
        created_at: u64,
    },
}

impl Event {
    pub fn protocol(&self) -> EventProtocol {
        match self {
            Event::Proposal { .. }
            | Event::Promise { .. }
            | Event::Accept { .. }
            | Event::Accepted { .. }
            | Event::Learn { .. }
            | Event::Success { .. }
            | Event::LearnedValue { .. }
            | Event::InitialDecree { .. }
            | Event::BatchInitialDecrees { .. }
            | Event::LedgerDump { .. }
            | Event::NodeState { .. } => EventProtocol::Classic,

            Event::BallotAdopted { .. }
            | Event::ProposalAccepted { .. }
            | Event::PmmcPropose { .. }
            | Event::PmmcP1A { .. }
            | Event::PmmcP1B { .. }
            | Event::PmmcP2A { .. }
            | Event::PmmcP2B { .. }
            | Event::PmmcAdopted { .. }
            | Event::PmmcPreempted { .. }
            | Event::PmmcHeartbeat { .. }
            | Event::PmmcAck { .. } => EventProtocol::Pmmc,

            Event::NodeCapabilities { .. }
            | Event::MessageSent { .. }
            | Event::PartitionCreated { .. }
            | Event::PartitionHealed { .. }
            | Event::LeaderElected { .. }
            | Event::LeaderSteppedDown { .. }
            | Event::NodeCrashed { .. } => EventProtocol::System,
        }
    }
}

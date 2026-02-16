use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::{
    common::types::{DecreeId, NodeId},
    node::{
        paxos_state::ballot::Ballot, pmmc::acceptor::accepted_state::AcceptedMap, pvalue::PValue,
    },
    paxos_command::PaxosCommand,
};

#[derive(Debug, Clone, serde::Serialize)]
pub enum Message {
    Prepare {
        from: NodeId,
        decree_num: DecreeId,
        ballot: Ballot,
    },
    Promise {
        from: NodeId,
        decree_num: DecreeId,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
    },
    Accept {
        from: NodeId,
        decree_num: DecreeId,
        ballot: Ballot,
        value: PaxosCommand,
        quorum: HashSet<NodeId>,
    },
    Accepted {
        from: NodeId,
        decree_num: DecreeId,
        ballot: Ballot,
        value: PaxosCommand,
    },
    NACK,
    Success {
        from: NodeId,
        decree_num: DecreeId,
        value: PaxosCommand,
        ballot_proposer: NodeId, // Track which node originated this proposal
    },

    PrepareBatch {
        from: NodeId,
        decrees_to: DecreeId,
        ballot: Ballot,
    },

    //PMMC
    ACK {
        from: Uuid,
        slot: usize,
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
    ACCEPTED {
        from: Uuid,
        pvalue: PValue,
    }
}

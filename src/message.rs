use std::collections::HashSet;

use uuid::Uuid;

use crate::{
    cluster::configuration_handler::types::ConfigurationCommand,
    common::types::DecreeId,
    node::{classic_paxos::ballot::Ballot, pvalue::PValue},
    paxos_command::PaxosCommand,
    rsm::kv_store::ReplyOutcome,
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
    RECONFIGURE {
        cmd: ConfigurationCommand,
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
    CATCHUP_REQUEST {
        from: Uuid,
        to: Uuid,
        from_slot: usize,
        epoch: usize,
    },
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

use std::collections::HashSet;

use crate::{common::types::{NodeId, DecreeId}, node::paxos_state::ballot::Ballot, paxos_command::PaxosCommand};

#[derive(Debug, Clone)]
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
}

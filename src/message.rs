use std::collections::HashSet;

use crate::{node::ballot::Ballot, paxos_command::PaxosCommand};

#[derive(Debug, Clone)]
pub enum Message {
    Prepare {
        from: usize,
        decree_num: usize,
        ballot: Ballot,
    },
    Promise {
        from: usize,
        decree_num: usize,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
    },
    Accept {
        from: usize,
        decree_num: usize,
        ballot: Ballot,
        value: PaxosCommand,
        quorum: HashSet<usize>,
    },
    Accepted {
        from: usize,
        decree_num: usize,
        ballot: Ballot,
        value: PaxosCommand,
    },
    NACK,
    Success {
        from: usize,
        decree_num: usize,
        value: PaxosCommand,
        ballot_proposer: usize,  // Track which node originated this proposal
    },
}

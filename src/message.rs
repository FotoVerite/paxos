use std::collections::HashSet;

use crate::{node::{acceptor::PromiseInfo, ballot::Ballot}, paxos_command::PaxosCommand};

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

    PrepareBatch {
        from: usize,
        decrees_to: usize,
        ballot: Ballot,
    },
    PromiseBatch {
        from: usize,
        promises:  Vec<PromiseInfo>
    }
}

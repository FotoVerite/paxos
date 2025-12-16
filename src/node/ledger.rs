use std::collections::HashMap;

use tokio::sync::Mutex;

use crate::{node::ballot::Ballot, paxos_command::PaxosCommand};

struct Decree {
    chosen: bool,
    votes: HashMap<usize, HashMap<usize, PaxosCommand>>,
}

impl Decree {
    fn default() -> Self {
        return Self {
            chosen: false,
            votes: HashMap::new(),
        };
    }
}

struct LedgerState {
    log: Vec<PaxosCommand>,
    decrees: HashMap<usize, Decree>,
}

impl LedgerState {
    fn init() -> Self {
        return Self {
            log: Vec::new(),
            decrees: HashMap::<usize, Decree>::new(),
        };
    }
}

pub struct Ledger {
    quorum: usize,
    state: Mutex<LedgerState>,
}

impl Ledger {
    pub fn init(quorum: usize) -> Self {
        return Self {
            quorum,
            state: Mutex::new(LedgerState::init()),
        };
    }

    pub async fn next(&self) -> usize {
        let state = self.state.lock().await;
        return state.log.len();
    }

    pub async fn vote(&self, decree_num: usize, ballot: Ballot, cmd: PaxosCommand) {
        let mut state = self.state.lock().await;

        if state.log.len() <= decree_num {
            state.log.resize(decree_num + 1, PaxosCommand::NOOP);
        }

        let decree = state.decrees.entry(decree_num).or_insert(Decree::default());

        if decree.chosen {
            return;
        }

        let ballot_votes = decree.votes.entry(ballot.number).or_default();

        match ballot_votes.get(&ballot.node_id) {
            Some(existing) if existing != &cmd => {
                // protocol violation — ignore or panic in debug
                debug_assert!(existing == &cmd);
                return;
            }
            _ => {
                ballot_votes.insert(ballot.node_id, cmd.clone());
            }
        }

        if ballot_votes.len() >= self.quorum {
            decree.chosen = true;
            state.log[decree_num] = cmd;
        }
    }
}

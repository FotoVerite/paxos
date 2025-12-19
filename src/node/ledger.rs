use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use tokio::sync::Mutex;

use crate::{node::ballot::Ballot, paxos_command::PaxosCommand};

const DATA_DIR: &str = ".paxos";

#[derive(serde::Serialize, serde::Deserialize, Clone)]
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

#[derive(serde::Serialize, serde::Deserialize)]
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
    id: usize,
    quorum: usize,
    state: Mutex<LedgerState>,
}

impl Ledger {
    pub async fn init(id: usize, quorum: usize) -> Result<Self> {
        let state = Ledger::load_or_init(id).await.unwrap_or_else(|_| LedgerState::init());
        Ok(Self {
            id,
            quorum,
            state: Mutex::new(state),
        })
    }

    fn state_path(&self) -> String {
        format!("{}/ledger_state_{}.bin", DATA_DIR, self.id)
    }

    async fn ensure_dir_exists() -> Result<()> {
        tokio::fs::create_dir_all(DATA_DIR).await?;
        Ok(())
    }

    async fn save(&self) -> Result<()> {
        Self::ensure_dir_exists().await?;
        let state = self.state.lock().await;
        let encoded = bincode::serialize(&*state)?;
        tokio::fs::write(self.state_path(), encoded).await?;
        Ok(())
    }

    async fn load_or_init(node_id: usize) -> Result<LedgerState> {
        let path_str = format!("{}/ledger_state_{}.bin", DATA_DIR, node_id);
        let path = Path::new(&path_str);
        
        if !path.exists() {
            return Ok(LedgerState::init());
        }

        let data = tokio::fs::read(&path).await?;
        if data.is_empty() {
            return Ok(LedgerState::init());
        }

        Ok(bincode::deserialize(&data)?)
    }

    pub async fn next(&self) -> usize {
        let state = self.state.lock().await;
        
        // Find the first decree that hasn't been chosen yet
        let mut decree_num = 0;
        loop {
            match state.decrees.get(&decree_num) {
                Some(decree) if decree.chosen => {
                    // This decree is chosen, move to next
                    decree_num += 1;
                }
                _ => {
                    // Either not in decrees map (not chosen) or not chosen yet
                    return decree_num;
                }
            }
        }
    }

    pub async fn vote(
        &self,
        decree_num: usize,
        ballot: Ballot,
        cmd: PaxosCommand,
        acceptor_id: usize,
    ) -> Option<PaxosCommand> {
        let mut state = self.state.lock().await;

        if state.log.len() <= decree_num {
            state.log.resize(decree_num + 1, PaxosCommand::NOOP);
        }

        let decree = state.decrees.entry(decree_num).or_insert_with(Decree::default);

        if decree.chosen {
            return None; // Already chosen, this vote doesn't trigger a learn event
        }

        let ballot_votes = decree.votes.entry(ballot.number).or_default();

        match ballot_votes.get(&acceptor_id) {
            Some(existing) if existing != &cmd => {
                // protocol violation — ignore or panic in debug
                debug_assert!(
                    existing == &cmd,
                    "Ledger vote conflict: existing {:?} vs new {:?}",
                    existing,
                    cmd
                );
                return None;
            }
            _ => {
                ballot_votes.insert(acceptor_id, cmd.clone());
            }
        }

        if ballot_votes.len() >= self.quorum {
            decree.chosen = true;
            state.log[decree_num] = cmd.clone();
            drop(state);
            let _ = self.save().await;
            return Some(cmd); // This vote caused the decree to be chosen
        }

        None
    }
}

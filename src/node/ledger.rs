use std::path::Path;

use anyhow::Result;
use tokio::sync::Mutex;

use crate::{
    paxos_command::{PaxosCommand},
};

const DATA_DIR: &str = ".paxos";

#[derive(serde::Serialize, serde::Deserialize)]
struct LedgerState {
    log: Vec<PaxosCommand>,
    decrees: Vec<Option<PaxosCommand>>,
}

impl LedgerState {
    fn init() -> Self {
        return Self {
            log: Vec::new(),
            decrees: Vec::new(),
        };
    }
}

pub struct Ledger {
    id: usize,
    state: Mutex<LedgerState>,
}

impl Ledger {
    pub async fn init(id: usize) -> Result<Self> {
        let state = Ledger::load_or_init(id)
            .await
            .unwrap_or_else(|_| LedgerState::init());
        Ok(Self {
            id,
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

    pub async fn insert(&self, slot: usize, value: PaxosCommand) -> bool {
        let inserted = {
            let mut state = self.state.lock().await;
            if state.decrees.len() <= slot {
                state.decrees.resize(slot + 1, None);
            }
            if state.decrees[slot].is_some() {
                return false;
            }
            state.decrees[slot] = Some(value);
            true
        }; // Lock released here
        
        if inserted {
            // Handle Result from save()
            return self.save().await.is_ok();
        }
        inserted
    }

    pub async fn get(&self, slot: usize) -> Option<PaxosCommand> {
        let state = self.state.lock().await;
        state.decrees.get(slot).cloned().flatten()
    }

    pub async fn next(&self) -> usize {
        let state = self.state.lock().await;

        // Find the first decree that hasn't been chosen yet
        return state.decrees.len();
    }
}
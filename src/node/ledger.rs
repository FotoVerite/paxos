use std::path::Path;
use uuid::Uuid;

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
    uuid: Uuid,
    state: Mutex<LedgerState>,
}

impl Ledger {
    pub async fn init(id: usize, uuid: Uuid) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let state = Ledger::load_or_init(uuid)
            .await
            .unwrap_or_else(|_| LedgerState::init());
        
        #[cfg(not(feature = "persistence"))]
        let state = LedgerState::init();
        
        Ok(Self {
            id,
            uuid,
            state: Mutex::new(state),
        })
    }

    /// Create a ledger with fresh empty state (for tests)
    pub fn new(id: usize, uuid: Uuid) -> Self {
        Self {
            id,
            uuid,
            state: Mutex::new(LedgerState::init()),
        }
    }

    fn state_path(&self) -> String {
        format!("{}/ledger_{}.bin", DATA_DIR, self.uuid)
    }

    async fn ensure_dir_exists() -> Result<()> {
        tokio::fs::create_dir_all(DATA_DIR).await?;
        Ok(())
    }

    /// Atomically save ledger state using temp file + rename pattern
    /// This ensures the file is never left in a corrupted state if process crashes mid-write
    async fn save(&self) -> Result<()> {
        Self::ensure_dir_exists().await?;
        
        let state = self.state.lock().await;
        let encoded = bincode::serialize(&*state)?;
        
        let path = self.state_path();
        let temp_path = format!("{}.tmp", path);
        
        // Write to temp file (safe to be incomplete if crash)
        tokio::fs::write(&temp_path, encoded).await?;
        
        // Atomic rename (either completes fully or fails, no partial state)
        tokio::fs::rename(&temp_path, &path).await?;
        
        Ok(())
    }

    async fn load_or_init(uuid: Uuid) -> Result<LedgerState> {
        let path_str = format!("{}/ledger_{}.bin", DATA_DIR, uuid);
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
            // Persist the ledger (if persistence is enabled)
            #[cfg(feature = "persistence")]
            {
                return self.save().await.is_ok();
            }
            #[cfg(not(feature = "persistence"))]
            {
                return true;  // In tests without persistence, always succeed
            }
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

    pub async fn next_gap(&self) -> Option<usize> {
        let state = self.state.lock().await;

        // Find the first slot with a gap (None value)
        for (idx, cmd_opt) in state.decrees.iter().enumerate() {
            if cmd_opt.is_none() {
                return Some(idx);
            }
        }
        
        // If there are no gaps but there are Some values, return the length to extend
        if state.decrees.iter().any(|c| c.is_some()) && state.decrees.iter().all(|c| c.is_some()) {
            return Some(state.decrees.len());
        }
        
        None
    }

    pub async fn get_initial_decrees(&self) -> Vec<(usize, PaxosCommand)> {
         let state = self.state.lock().await;
         state.decrees
             .iter()
             .enumerate()
             .filter_map(|(idx, cmd_opt)| cmd_opt.as_ref().map(|cmd| (idx, cmd.clone())))
             .collect()
     }

    /// Pre-populate a ledger file with initial decrees (used for scenario setup)
    pub async fn prepopulate(uuid: Uuid, decrees: Vec<Option<PaxosCommand>>) -> Result<()> {
        Self::ensure_dir_exists().await?;
        
        let state = LedgerState {
            log: Vec::new(),
            decrees,
        };
        
        let encoded = bincode::serialize(&state)?;
        let path = format!("{}/ledger_{}.bin", DATA_DIR, uuid);
        let temp_path = format!("{}.tmp", path);
        
        // Write to temp file
        tokio::fs::write(&temp_path, encoded).await?;
        
        // Atomic rename
        tokio::fs::rename(&temp_path, &path).await?;
        
        Ok(())
    }
}
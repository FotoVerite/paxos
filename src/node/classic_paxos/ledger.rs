use crate::{
    common::{persistence::NodePersistence, types::DecreeId},
    paxos_command::PaxosCommand,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Default)]
struct LedgerState {
    log: Vec<PaxosCommand>,
    decrees: Vec<Option<PaxosCommand>>,
}

impl LedgerState {
    fn init() -> Self {
        Self {
            log: Vec::new(),
            decrees: Vec::new(),
        }
    }
}

pub struct Ledger {
    persistence: NodePersistence,
    state: Mutex<LedgerState>,
}

impl Ledger {
    pub async fn init(_uuid: Uuid, persistence: NodePersistence) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let state = persistence
            .load("ledger.bin")
            .await
            .unwrap_or_else(|_| LedgerState::init());

        #[cfg(not(feature = "persistence"))]
        let state = LedgerState::init();

        Ok(Self {
            persistence,
            state: Mutex::new(state),
        })
    }

    /// Create a ledger with fresh empty state (for tests)
    pub fn new(uuid: Uuid) -> Self {
        Self {
            persistence: crate::common::persistence::ClusterPersistence::for_test("ledger_tests")
                .node(uuid),
            state: Mutex::new(LedgerState::init()),
        }
    }

    async fn save(&self) -> Result<()> {
        let state = self.state.lock().await;
        self.persistence.save("ledger.bin", &*state).await
    }

    pub async fn insert(&self, slot: DecreeId, value: PaxosCommand) -> bool {
        let idx: usize = slot.into();
        let inserted = {
            let mut state = self.state.lock().await;
            if state.decrees.len() <= idx {
                state.decrees.resize(idx + 1, None);
            }
            if state.decrees[idx].is_some() {
                return false;
            }
            state.decrees[idx] = Some(value);
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
                return true; // In tests without persistence, always succeed
            }
        }
        inserted
    }

    pub async fn get(&self, slot: DecreeId) -> Option<PaxosCommand> {
        let state = self.state.lock().await;
        state.decrees.get(usize::from(slot)).cloned().flatten()
    }

    pub async fn next(&self) -> DecreeId {
        let state = self.state.lock().await;

        // Find the first decree that hasn't been chosen yet
        DecreeId(state.decrees.len())
    }

    pub async fn next_gap(&self) -> Option<DecreeId> {
        let state = self.state.lock().await;

        // Find the first slot with a gap (None value)
        for (idx, cmd_opt) in state.decrees.iter().enumerate() {
            if cmd_opt.is_none() {
                return Some(DecreeId(idx));
            }
        }

        // If there are no gaps but there are Some values, return the length to extend
        if state.decrees.iter().any(|c| c.is_some()) && state.decrees.iter().all(|c| c.is_some()) {
            return Some(DecreeId(state.decrees.len()));
        }

        None
    }

    pub async fn get_initial_decrees(&self) -> Vec<(DecreeId, PaxosCommand)> {
        let state = self.state.lock().await;
        state
            .decrees
            .iter()
            .enumerate()
            .filter_map(|(idx, cmd_opt)| cmd_opt.as_ref().map(|cmd| (DecreeId(idx), cmd.clone())))
            .collect()
    }

    pub async fn get_all_decrees(&self) -> Vec<(DecreeId, PaxosCommand)> {
        self.get_initial_decrees().await
    }

    /// Pre-populate a ledger file with initial decrees (used for scenario setup)
    pub async fn prepopulate(
        persistence: &NodePersistence,
        decrees: Vec<Option<PaxosCommand>>,
    ) -> Result<()> {
        let state = LedgerState {
            log: Vec::new(),
            decrees,
        };

        persistence.save("ledger.bin", &state).await
    }
}

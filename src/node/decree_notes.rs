use crate::common::persistence::Persistence;
use crate::node::ballot::Ballot;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{cmp::max, collections::HashMap};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct DecreeNotes {
    pub state: HashMap<usize, DecreeNote>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DecreeNote {
    pub last_tried: Ballot,
}

impl DecreeNotes {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
        }
    }

    pub async fn save(&self, uuid: Uuid) -> Result<()> {
        Persistence::save(&format!("decree_notes_{}.bin", uuid), self).await
    }

    pub async fn load_or_init(uuid: Uuid) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let notes = Persistence::load(&format!("decree_notes_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let notes = DecreeNotes::new();

        Ok(notes)
    }
}

impl DecreeNote {
    pub fn new(node_id: usize) -> Self {
        Self {
            last_tried: Ballot {
                number: 0,
                node_id,
            },
        }
    }

    pub fn next_ballot(&mut self, highest_accepted: usize) -> Ballot {
        self.last_tried.number = max(self.last_tried.number + 1, highest_accepted + 1);
        self.last_tried
    }
}

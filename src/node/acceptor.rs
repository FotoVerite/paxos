use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const DATA_DIR: &str = ".paxos";

pub struct Acceptor {
    id: usize,
    uuid: Uuid,
    state: Mutex<HashMap<usize, AcceptedDecree>>,
    observer: Arc<dyn PaxosObserver>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AcceptedDecree {
    next_bal: Ballot,
    prev_vote: (Ballot, PaxosCommand),
}

impl Default for AcceptedDecree {
    fn default() -> AcceptedDecree {
        AcceptedDecree {
            next_bal: Ballot {
                number: usize::MIN,
                node_id: 0,
            },
            prev_vote: (
                Ballot {
                    number: usize::MIN,
                    node_id: 0,
                },
                PaxosCommand::BLANK,
            ),
        }
    }
}

impl Acceptor {
    pub async fn new(id: usize, uuid: Uuid, observer: Arc<dyn PaxosObserver>) -> Result<Self> {
        #[cfg(feature = "persistence")]
        let state = Acceptor::load_or_init(uuid).await?;

        #[cfg(not(feature = "persistence"))]
        let state = HashMap::new();

        return Ok(Self {
            id,
            uuid,
            state: Mutex::new(state),
            observer,
        });
    }

    async fn load_or_init(uuid: Uuid) -> Result<HashMap<usize, AcceptedDecree>> {
        let path_str = format!("{}/acceptor_{}.bin", DATA_DIR, uuid);
        let path = Path::new(&path_str);

        if !path.exists() {
            return Ok(HashMap::new());
        }

        let data = tokio::fs::read(&path).await?;
        if data.is_empty() {
            return Ok(HashMap::new());
        }

        Ok(bincode::deserialize(&data)?)
    }

    /// Atomically save acceptor state using temp file + rename pattern
    /// This ensures the file is never left in a corrupted state if process crashes mid-write
    async fn save(&self) -> Result<()> {
        let path = format!("{}/acceptor_{}.bin", DATA_DIR, self.uuid);
        let temp_path = format!("{}.tmp", path);

        // Ensure directory exists
        tokio::fs::create_dir_all(DATA_DIR).await?;

        // Serialize state
        let state = self.state.lock().await;
        let encoded = bincode::serialize(&*state)?;

        // Write to temp file (safe to be incomplete if crash)
        tokio::fs::write(&temp_path, encoded).await?;

        // Atomic rename (either completes fully or fails, no partial state)
        tokio::fs::rename(&temp_path, &path).await?;

        Ok(())
    }

    async fn prepare(&self, decree_num: usize, ballot: Ballot, from: usize) -> Message {
        let mut state = self.state.lock().await;

        let decree = state.entry(decree_num).or_default();
        if ballot > decree.next_bal {
            tracing::info!(
                "[Node {}] Prepare: ballot ({}, {}) > next_bal ({}, {}) - PROMISE",
                self.id,
                ballot.number,
                ballot.node_id,
                decree.next_bal.number,
                decree.next_bal.node_id
            );
            decree.next_bal = ballot;

            self.observer.on_event(Event::Promise {
                decree_num,
                id: self.id,
                from, // Track who initiated the prepare
                ballot: ballot.number,
                created_at: crate::monitor::current_timestamp_millis(),
            });

            let (b, v) = &decree.prev_vote;
            return Message::Promise {
                from: self.id,
                decree_num,
                ballot,
                accepted_ballot: b.clone(),
                accepted_value: v.clone(),
            };
        }
        tracing::info!(
            "[Node {}] Prepare: ballot ({}, {}) <= next_bal ({}, {}) - NACK",
            self.id,
            ballot.number,
            ballot.node_id,
            decree.next_bal.number,
            decree.next_bal.node_id
        );
        return Message::NACK;
    }

    async fn accept(
        &self,
        decree_num: usize,
        ballot: Ballot,
        cmd: PaxosCommand,
        from: usize,
    ) -> Message {
        let mut state = self.state.lock().await;
        let decree = state.entry(decree_num).or_default();
        if ballot == decree.next_bal {
            decree.prev_vote = (ballot, cmd.clone());

            tracing::info!(
                "[Node {}] Accept: ballot ({}, {}) == next_bal - ACCEPTED decree {} from node {}: {:?}",
                self.id,
                ballot.number,
                ballot.node_id,
                decree_num,
                from,
                cmd
            );

            self.observer.on_event(Event::Accepted {
                decree_num,
                id: self.id,
                from, // Track who initiated the accept
                ballot: ballot.number,
                value: cmd.clone(),
                created_at: crate::monitor::current_timestamp_millis(),
            });

            // Release lock before saving to avoid holding it during I/O
            drop(state);

            // Persist the updated state atomically (if persistence is enabled)
            #[cfg(feature = "persistence")]
            {
                if let Err(e) = self.save().await {
                    tracing::error!("[Node {}] Failed to persist acceptor state: {}", self.id, e);
                    // In production, might want to crash here to ensure durability
                    // For now, continue (data will be lost on crash)
                }
            }

            return Message::Accepted {
                from: self.id,
                decree_num,
                ballot,
                value: cmd,
            };
        }
        tracing::info!(
            "[Node {}] Accept: ballot ({}, {}) != next_bal ({}, {}) - NACK",
            self.id,
            ballot.number,
            ballot.node_id,
            decree.next_bal.number,
            decree.next_bal.node_id
        );
        return Message::NACK;
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::Prepare {
                decree_num,
                ballot,
                from,
            } => return self.prepare(decree_num, ballot, from).await,
            Message::Accept {
                decree_num,
                ballot,
                value,
                from,
                ..
            } => return self.accept(decree_num, ballot, value, from).await,
            _ => Message::NACK,
        }
    }

    /// Pre-populate acceptor state with initial votes (for scenario setup)
    pub async fn prepopulate(
        uuid: Uuid,
        initial_decrees: Vec<(usize, PaxosCommand)>,
    ) -> Result<()> {
        let mut state = HashMap::new();

        // For each initial decree, create an AcceptedDecree with a high ballot number
        // This simulates that these decrees were previously accepted in a high ballot
        let high_ballot = Ballot {
            number: 1,
            node_id: 0,
        };

        for (decree_num, cmd) in initial_decrees {
            state.insert(
                decree_num,
                AcceptedDecree {
                    next_bal: high_ballot.clone(),
                    prev_vote: (high_ballot.clone(), cmd),
                },
            );
        }

        let path = format!("{}/acceptor_{}.bin", DATA_DIR, uuid);
        let temp_path = format!("{}.tmp", path);

        // Ensure directory exists
        tokio::fs::create_dir_all(DATA_DIR).await?;

        let encoded = bincode::serialize(&state)?;

        // Write to temp file
        tokio::fs::write(&temp_path, encoded).await?;

        // Atomic rename
        tokio::fs::rename(&temp_path, &path).await?;

        Ok(())
    }
}

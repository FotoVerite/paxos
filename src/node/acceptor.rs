use crate::common::persistence::Persistence;
use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

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
                number: 0,
                node_id: 0,
            },
            prev_vote: (
                Ballot {
                    number: 0,
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
        let state = Persistence::load(&format!("acceptor_{}.bin", uuid)).await?;

        #[cfg(not(feature = "persistence"))]
        let state = HashMap::new();

        Ok(Self {
            id,
            uuid,
            state: Mutex::new(state),
            observer,
        })
    }

    async fn save(&self) -> Result<()> {
        let state = self.state.lock().await;
        Persistence::save(&format!("acceptor_{}.bin", self.uuid), &*state).await
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
                from,
                ballot: ballot.number,
                created_at: crate::monitor::current_timestamp_millis(),
            });

            let (b, v) = &decree.prev_vote;
            Message::Promise {
                from: self.id,
                decree_num,
                ballot,
                accepted_ballot: b.clone(),
                accepted_value: v.clone(),
            }
        } else {
            tracing::info!(
                "[Node {}] Prepare: ballot ({}, {}) <= next_bal ({}, {}) - NACK",
                self.id,
                ballot.number,
                ballot.node_id,
                decree.next_bal.number,
                decree.next_bal.node_id
            );
            Message::NACK
        }
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
                from,
                ballot: ballot.number,
                value: cmd.clone(),
                created_at: crate::monitor::current_timestamp_millis(),
            });

            // Release lock before saving to avoid holding it during I/O
            drop(state);

            #[cfg(feature = "persistence")]
            {
                if let Err(e) = self.save().await {
                    tracing::error!("[Node {}] Failed to persist acceptor state: {}", self.id, e);
                }
            }

            Message::Accepted {
                from: self.id,
                decree_num,
                ballot,
                value: cmd,
            }
        } else {
            tracing::info!(
                "[Node {}] Accept: ballot ({}, {}) != next_bal ({}, {}) - NACK",
                self.id,
                ballot.number,
                ballot.node_id,
                decree.next_bal.number,
                decree.next_bal.node_id
            );
            Message::NACK
        }
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::Prepare {
                decree_num,
                ballot,
                from,
            } => self.prepare(decree_num, ballot, from).await,
            Message::Accept {
                decree_num,
                ballot,
                value,
                from,
                ..
            } => self.accept(decree_num, ballot, value, from).await,
            _ => Message::NACK,
        }
    }

    pub async fn prepopulate(
        uuid: Uuid,
        initial_decrees: Vec<(usize, PaxosCommand)>,
    ) -> Result<()> {
        let mut state = HashMap::new();

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

        Persistence::save(&format!("acceptor_{}.bin", uuid), &state).await
    }
}


use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const DATA_DIR: &str = ".paxos";

pub struct Acceptor {
    id: usize,
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
    pub async fn new(id: usize, observer: Arc<dyn PaxosObserver>) -> Result<Self> {
        let state = Acceptor::load_or_init(id).await?;
        return Ok(Self {
            id,
            state: Mutex::new(state),
            observer,
        });
    }

    async fn load_or_init(node_id: usize) -> Result<HashMap<usize, AcceptedDecree>> {
        let path_str = format!("{}/state_{}.bin", DATA_DIR, node_id);
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

    async fn prepare(&self, decree_num: usize, ballot: Ballot, from: usize) -> Message {
        let mut state = self.state.lock().await;

        let decree = state.entry(decree_num).or_default();
        if ballot > decree.next_bal {
            tracing::info!("[Node {}] Prepare: ballot ({}, {}) > next_bal ({}, {}) - PROMISE", 
                self.id, ballot.number, ballot.node_id, decree.next_bal.number, decree.next_bal.node_id);
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
        tracing::info!("[Node {}] Prepare: ballot ({}, {}) <= next_bal ({}, {}) - NACK", 
            self.id, ballot.number, ballot.node_id, decree.next_bal.number, decree.next_bal.node_id);
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

            tracing::info!("[Node {}] Accept: ballot ({}, {}) == next_bal - ACCEPTED decree {} from node {}: {:?}", 
                self.id, ballot.number, ballot.node_id, decree_num, from, cmd);

            self.observer.on_event(Event::Accepted {
                decree_num,
                id: self.id,
                from, // Track who initiated the accept
                ballot: ballot.number,
                value: cmd.clone(),
                created_at: crate::monitor::current_timestamp_millis(),
            });

            return Message::Accepted {
                from: self.id,
                decree_num,
                ballot,
                value: cmd,
            };
        }
        tracing::info!("[Node {}] Accept: ballot ({}, {}) != next_bal ({}, {}) - NACK", 
            self.id, ballot.number, ballot.node_id, decree.next_bal.number, decree.next_bal.node_id);
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
}

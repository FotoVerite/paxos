use crate::message::Message;
use crate::monitor::{Event, PaxosObserver};
use crate::node::ballot::Ballot;
use crate::node::decree_notes::{DecreeNote, DecreeNotes};
use crate::paxos_command::PaxosCommand;
use anyhow::Result;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

pub struct Proposer {
    id: usize,
    uuid: Uuid,
    quorum_size: usize,
    decree_notes: Arc<Mutex<DecreeNotes>>,
    state: Mutex<HashMap<usize, ProposedDecree>>,
    observer: Arc<dyn PaxosObserver>,
}

impl Proposer {
    pub async fn new(
        id: usize,
        uuid: Uuid,
        quorum_size: usize,
        decree_notes: Arc<Mutex<DecreeNotes>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            uuid,
            quorum_size,
            decree_notes,
            state: Mutex::new(HashMap::new()),
            observer,
        })
    }

    pub async fn propose(&self, decree_num: usize, cmd: PaxosCommand) -> Message {
        // Increment the ballot number for every new proposal attempt.
        let mut state = self.state.lock().await;

        let entry = state.entry(decree_num).or_default();
        let highest_accepted = entry.quorum.highest_accepted.number;

        let mut decree_notes = self.decree_notes.lock().await;
        let notes = decree_notes
            .state
            .entry(decree_num)
            .or_insert(DecreeNote::new(self.id));
        let next_ballot = notes.next_ballot(highest_accepted);

        // Persist the updated ballot number
        #[cfg(feature = "persistence")]
        {
            if let Err(e) = decree_notes.save(self.uuid).await {
                tracing::error!("[Node {}] Failed to persist decree notes: {}", self.id, e);
            }
        }

        entry.quorum.promises.clear();
        if entry.proposed_value == PaxosCommand::BLANK {
            entry.proposed_value = cmd
        }

        tracing::info!(
            "[Node {}] Proposing decree {} with ballot ({}, {}) value: {:?}",
            self.id,
            decree_num,
            next_ballot.number,
            next_ballot.node_id,
            entry.proposed_value
        );

        self.observer.on_event(Event::Proposal {
            decree_num,
            id: self.id,
            value: entry.proposed_value.clone(),
            created_at: crate::monitor::current_timestamp_millis(),
        });

        Message::Prepare {
            from: self.id,
            decree_num,
            ballot: next_ballot,
        }
    }

    pub async fn promise(
        &self,
        decree_num: usize,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
        from_node: usize,
    ) -> Message {
        let mut state = self.state.lock().await;
        let Some(entry) = state.get_mut(&decree_num) else {
            return Message::NACK;
        };
        let mut decree_notes = self.decree_notes.lock().await;
        let notes = decree_notes
            .state
            .entry(decree_num)
            .or_insert(DecreeNote::new(self.id));
        match ballot.cmp(&notes.last_tried) {
            Ordering::Less => {
                // stale, ignore
                return Message::NACK;
            }
            Ordering::Greater => {
                // preempted — future work: backoff / leader detection
                return Message::NACK;
            }
            Ordering::Equal => {
                return self
                    .prepare(
                        entry,
                        decree_num,
                        ballot,
                        accepted_ballot,
                        accepted_value,
                        from_node,
                    )
                    .await;
            }
        }
    }

    async fn prepare(
        &self,
        proposed_decree: &mut ProposedDecree,
        decree_num: usize,
        ballot: Ballot,
        accepted_ballot: Ballot,
        accepted_value: PaxosCommand,
        from_node: usize,
    ) -> Message {
        let quorum = &mut proposed_decree.quorum;

        if accepted_ballot > quorum.highest_accepted {
            quorum.highest_accepted = accepted_ballot;
            proposed_decree.proposed_value = accepted_value.clone();
        }

        quorum.promises.insert(from_node);

        if quorum.promises.len() >= self.quorum_size {
            self.observer.on_event(Event::Accept {
                decree_num,
                id: self.id,
                ballot: ballot.number,
                quorum: quorum.promises.clone(),
                value: proposed_decree.proposed_value.clone(),
                created_at: crate::monitor::current_timestamp_millis(),
            });

            return Message::Accept {
                from: self.id,
                decree_num,
                ballot,
                value: proposed_decree.proposed_value.clone(),
                quorum: quorum.promises.clone(),
            };
        }
        return Message::NACK;
    }

    pub async fn handle_message(&self, msg: Message) -> Message {
        match msg {
            Message::Promise {
                from,
                decree_num,
                ballot,
                accepted_ballot,
                accepted_value,
            } => {
                self.promise(decree_num, ballot, accepted_ballot, accepted_value, from)
                    .await
            }
            _ => Message::NACK,
        }
    }
}

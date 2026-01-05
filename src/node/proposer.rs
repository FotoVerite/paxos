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

pub struct Proposer {
    id: usize,
    quorum_size: usize,
    decree_notes: Arc<Mutex<DecreeNotes>>,
    state: Mutex<HashMap<usize, ProposedDecree>>,
    observer: Arc<dyn PaxosObserver>,
}

/// Represents the full in-memory runtime state of a proposed decree managed by the Proposer.
/// The `votes` field is transient and not persisted, as it represents promises received
/// during a specific proposal round which become invalid upon a Proposer restart.
struct ProposedDecree {
    proposed_value: PaxosCommand,
    quorum: Quorum,
    #[allow(dead_code)]
    promise_notifier: Arc<Notify>
}

struct Quorum {
    promises: HashSet<usize>,
    highest_accepted: Ballot,
}

impl Default for ProposedDecree {
    fn default() -> ProposedDecree {
        ProposedDecree {
            proposed_value: PaxosCommand::BLANK,
            promise_notifier: Arc::new(Notify::new()),
            quorum: Quorum {
                promises: HashSet::new(),
                highest_accepted: Ballot::default(),
            },
        }
    }
}

impl Proposer {
    pub async fn new(
        id: usize,
        quorum_size: usize,
        decree_notes: Arc<Mutex<DecreeNotes>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Result<Self> {
        let state = Self::load_or_init(id).await?;
        Ok(Self {
            id,
            quorum_size,
            decree_notes,
            state: Mutex::new(state),
            observer,
        })
    }

    /// Loads the Proposer's state from disk or initializes it if no state is found.
    /// When loading, it reconstructs the in-memory `ProposedDecree` from the
    /// `ProposedDecreeState`, re-initializing transient fields (like `votes`) to their default
    /// or empty values, as they are not persisted.
    async fn load_or_init(_node_id: usize) -> Result<HashMap<usize, ProposedDecree>> {
        return Ok(HashMap::new());
    }

    pub async fn propose(&self, decree_num: usize, cmd: PaxosCommand) -> Message {
        // Increment the ballot number for every new proposal attempt.
        let mut state = self.state.lock().await;
        let mut decree_notes = self.decree_notes.lock().await;
        let notes = decree_notes
            .state
            .entry(decree_num)
            .or_insert(DecreeNote::new(self.id));

        let entry = state.entry(decree_num).or_default();
        let highest_accepted = entry.quorum.highest_accepted.number;
        let next_ballot = notes.next_ballot(highest_accepted);
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

    // pub async fn propose_with_retry(
    //     &self,
    //     decree_num: usize,
    //     cmd: PaxosCommand,
    // ) -> Result<(), String> {
    //     let max_retries = 3;
    //     let mut delay_ms = 100;

    //     for attempt in 0..max_retries {
    //         // Send prepare
    //         let msg = self.propose(decree_num, cmd.clone()).await;
    //         self.peers.broadcast(msg).await;

    //         // Wait for quorum of promises (with timeout)
    //         let promise_count = self
    //             .wait_for_promises(decree_num, Duration::from_millis(delay_ms))
    //             .await;

    //         if promise_count >= self.quorum_size {
    //             return Ok(()); // Success
    //         }

    //         if attempt < max_retries - 1 {
    //             tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    //             delay_ms *= 2; // Exponential backoff: 100, 200, 400
    //         }
    //     }

    //     Err("Failed to reach quorum after retries".to_string())
    // }

    

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

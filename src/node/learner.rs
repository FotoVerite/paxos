use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{
        ballot::Ballot,
        decree_notes::{DecreeNote, DecreeNotes},
        ledger::Ledger,
    },
};

pub struct Learner {
    id: usize,
    quorum_number: usize,
    decree_notes: Arc<Mutex<DecreeNotes>>,
    state: Mutex<HashMap<usize, Quorum>>,

    observer: Arc<dyn PaxosObserver>,
}

struct Quorum {
    votes: HashSet<usize>,
    cached_last_tried: Ballot,
}

impl Learner {
    pub fn new(
        id: usize,
        quorum_number: usize,
        decree_notes: Arc<Mutex<DecreeNotes>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            id: id,
            decree_notes,
            state: Mutex::new(HashMap::new()),
            quorum_number,
            observer,
        }
    }

    pub async fn handle_message(&self, msg: Message, ledger: &Ledger) -> Message {
        match msg {
            Message::Accepted {
                from,
                decree_num,
                ballot,
                value,
            } => {
                let mut decree_notes = self.decree_notes.lock().await;
                let notes = decree_notes
                    .state
                    .entry(decree_num)
                    .or_insert(DecreeNote::new(self.id));
                  if ballot != notes.last_tried {
                    return Message::NACK;
                }
                let proposer_id = ballot.node_id;
                drop(decree_notes);

                let mut state = self.state.lock().await;
                let quorum = state.entry(decree_num).or_insert_with(|| Quorum {
                    votes: HashSet::new(),
                    cached_last_tried: ballot,
                });
                if quorum.cached_last_tried != ballot {
                    quorum.votes.clear();
                    quorum.cached_last_tried = ballot;
                }
                quorum.votes.insert(from);
                if quorum.votes.len() >= self.quorum_number {
                    if ledger.insert(decree_num, value.clone()).await {
                        // Emit LearnedValue event when the Learner itself reaches quorum
                        self.observer.on_event(Event::LearnedValue {
                            decree_num,
                            id: self.id,
                            value: value.clone(),
                            created_at: crate::monitor::current_timestamp_millis(),
                        });
                        tracing::info!(
                            "[Node {}] Learner reached quorum for decree {}: {:?} from proposer {} (votes: {:?})",
                            self.id,
                            decree_num,
                            value,
                            proposer_id,
                            quorum.votes
                        );
                        return Message::Success {
                            from: self.id,
                            decree_num,
                            value: value.clone(),
                            ballot_proposer: proposer_id,
                        };
                    }
                }
                return Message::NACK;
            }
            _ => return Message::NACK,
        }
    }
    pub async fn learn_decree(&self, msg: Message, ledger: &Ledger) {
        match msg {
            Message::Success {
                from,
                decree_num,
                value,
                ballot_proposer,
            } => {
                // This handles a *received* Message::Success
                if ledger.insert(decree_num, value.clone()).await {
                    if self.id != from { // Emit Event::Success only if it's from another node
                        self.observer.on_event(Event::Success {
                            decree_num,
                            from, // 'from' is the sender of the Message::Success, not necessarily this learner
                            id: self.id, // This is the ID of the learner that is processing the message
                            value: value.clone(),
                            created_at: crate::monitor::current_timestamp_millis(),
                        });
                    }
                    // A Learner also "learns" locally when it receives a Success message,
                    // so emit LearnedValue here as well.
                    self.observer.on_event(Event::LearnedValue {
                        decree_num,
                        id: self.id,
                        value: value.clone(),
                        created_at: crate::monitor::current_timestamp_millis(),
                    });
                    tracing::info!(
                        "[Node {}] Learned decree {} with value from proposer {}",
                        self.id,
                        decree_num,
                        ballot_proposer
                    );
                }
            }
            _ => {}
        }
    }
}
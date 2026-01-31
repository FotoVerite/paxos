mod decrees;
mod learner_quorum;

use std::{sync::Arc};

use tokio::sync::Mutex;

use crate::{
    common::types::{NodeId},
    message::Message,
    monitor::{Event, PaxosObserver},
    node::paxos_state::{
        decree_notes::DecreeNotes,
        learner::decrees::Decrees,
        ledger::Ledger,
    },
};

pub struct Learner {
    id: NodeId,
    quorum_number: usize,
    decree_notes: Option<Arc<Mutex<DecreeNotes>>>,
    state: Decrees,

    observer: Arc<dyn PaxosObserver>,
}

impl Learner {
    pub fn new(
        id: NodeId,
        quorum_number: usize,
        decree_notes: Option<Arc<Mutex<DecreeNotes>>>,
        observer: Arc<dyn PaxosObserver>,
    ) -> Self {
        Self {
            id: id,
            decree_notes,
            state: Decrees::init(),
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
                // Only check lastTried if we have decree_notes (proposer+learner node)
                if let Some(decree_notes_arc) = &self.decree_notes {
                    let decree_notes = decree_notes_arc.lock().await;
                    if let Some(notes) = decree_notes.state.get(&decree_num) {
                        if ballot != notes.last_tried {
                            // Proposer+Learner: only count votes for our own ballots
                            return Message::NACK;
                        }
                    }
                }
                // Pure learners (no decree_notes) count all ballots

                let reply = self
                    .state
                    .add_vote(
                        self.id,
                        from,
                        decree_num,
                        ballot,
                        self.quorum_number,
                        Arc::clone(&self.observer),
                        value.clone(),
                    )
                    .await;
                
                // When quorum is reached locally, emit Success event and insert into ledger
                // Note: the Success message will also be broadcast, but learn_decree will
                // deduplicate by checking if already in ledger
                if let Message::Success {
                    decree_num: success_decree_num,
                    value: success_value,
                    ..
                } = &reply {
                    if ledger.insert(*success_decree_num, success_value.clone()).await {
                        // Only emit if this is the first insertion
                        self.observer.on_event(Event::Success {
                            decree_num: *success_decree_num,
                            from: self.id,
                            id: self.id,
                            value: success_value.clone(),
                            created_at: crate::monitor::current_timestamp_millis(),
                        });
                        self.observer.on_event(Event::LearnedValue {
                            decree_num: *success_decree_num,
                            id: self.id,
                            value: success_value.clone(),
                            created_at: crate::monitor::current_timestamp_millis(),
                        });
                        tracing::info!(
                            "[Node {}] Reached quorum for decree {}",
                            self.id,
                            success_decree_num
                        );
                    }
                }
                
                return reply;
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
                    if self.id != from {
                        // Emit Event::Success only if it's from another node
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

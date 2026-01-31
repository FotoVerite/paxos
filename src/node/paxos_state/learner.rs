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
                // For proposers with learner role: count votes to determine quorum
                // For pure learners: ignore Accepted messages (only receive Success via learn_decree)
                if self.decree_notes.is_none() {
                    // Pure learner - no voting logic needed
                    return Message::NACK;
                }

                // Proposer+Learner: only count votes for our own ballots
                let decree_notes_arc = self.decree_notes.as_ref().unwrap();
                let decree_notes = decree_notes_arc.lock().await;
                if let Some(notes) = decree_notes.state.get(&decree_num) {
                    if ballot != notes.last_tried {
                        return Message::NACK;
                    }
                }

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
                
                // When quorum is reached (proposer learns it reached quorum), 
                // emit Success and insert into ledger
                if let Message::Success {
                    decree_num: success_decree_num,
                    value: success_value,
                    ..
                } = &reply {
                    if ledger.insert(*success_decree_num, success_value.clone()).await {
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
                            "[Node {}] Proposer reached quorum for decree {}",
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
                decree_num,
                value,
                ballot_proposer,
                ..
            } => {
                // This handles a *received* Message::Success broadcast from another learner
                // The learner that reached quorum already emitted Success + LearnedValue in handle_message
                // This node just needs to learn the value if it hasn't already
                if ledger.insert(decree_num, value.clone()).await {
                    // Only emit LearnedValue - the first learner already emitted Success
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

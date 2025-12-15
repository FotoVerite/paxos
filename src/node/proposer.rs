use crate::message::Message;
use crate::node::ballot::Ballot;
use crate::monitor::{Event, PaxosObserver};
use std::sync::Arc;

pub struct Proposer {
    id: usize,
    ballot: Ballot,
    proposed_value: String,
    highest_seen_ballot: Ballot,
    observer: Arc<dyn PaxosObserver>,
}

impl Proposer {
    pub fn new(id: usize, observer: Arc<dyn PaxosObserver>) -> Self {
        Self { 
            id,
            ballot: Ballot::new(0, id), // Start at round 0
            proposed_value: "".to_string(),
            highest_seen_ballot: Ballot::new(0, 0),
            observer,
        }
    }

    pub fn propose(&mut self, value: String) -> Message {
        // Increment round for new proposal
        self.ballot.number += 1;
        self.proposed_value = value.clone();
        
        self.observer.on_event(Event::Proposal { id: self.id, value: value.clone() });
        
        Message::Prepare { ballot: self.ballot }
    }

    pub async fn handle_message(&mut self, msg: Message) -> Message{
        match msg {
            Message::Promise { ballot, accepted_ballot, accepted_value } => {
                if ballot != self.ballot {
                     return None;
                }

                if accepted_ballot > self.highest_seen_ballot {
                    self.highest_seen_ballot = accepted_ballot;
                    if let Some(val) = accepted_value {
                        self.proposed_value = val;
                    }
                }
                
                return Some(Message::Accept {
                    ballot: self.ballot, 
                    value: self.proposed_value.clone(),
                });
            }
            Message::NACK => {
                // If rejected, jump ahead
                self.ballot.number += 1;
                Some(Message::Prepare { ballot: self.ballot })
            }
            _ => None
        }
    }
}

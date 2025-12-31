use std::sync::Arc;

use crate::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::ledger::Ledger,
};

pub struct Learner {
    id: usize,
    observer: Arc<dyn PaxosObserver>,
}

impl Learner {
    pub fn new(id: usize, observer: Arc<dyn PaxosObserver>) -> Self {
        Self { id: id, observer }
    }

    pub async fn handle_message(&mut self, msg: Message, ledger: &mut Ledger) {
        match msg {
            Message::Accepted {
                from,
                decree_num,
                ballot,
                value,
            } => {
                if let Some(chosen_value) = ledger.vote(decree_num, ballot, value, from).await {
                    self.observer.on_event(Event::Learn {
                        decree_num,
                        id: self.id,
                        value: chosen_value,
                        created_at: crate::monitor::current_timestamp_millis(),
                    });
                }
            }
            _ => {}
        }
    }
    pub async fn learn_decree(&mut self, msg: Message, ledger: &mut Ledger) {
        match msg {
            Message::Success {
                from,
                decree_num,
                value,
            } => {
                
                if ledger.insert(decree_num, value.clone()).await {
                    self.observer.on_event(Event::Success {
                        decree_num,
                        from,
                        id: self.id,
                        value: value,
                        created_at: crate::monitor::current_timestamp_millis(),
                    });
                }
            }
            _ => {}
        }
    }
}

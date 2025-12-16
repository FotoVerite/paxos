use std::sync::Arc;

use crate::{
    message::Message,
    monitor::{Event, PaxosObserver}, node::ledger::Ledger,
};

pub struct Learner {
    id: usize,
    observer: Arc<dyn PaxosObserver>,
}

impl Learner {
    pub fn new(id: usize, observer: Arc<dyn PaxosObserver>) -> Self {
        Self {
            id: id,
            observer,
        }
    }

    pub async fn handle_message(&mut self, msg: Message, ledger: &mut Ledger) {
        match msg {
            Message::Accepted {
                decree_num,
                ballot,
                value,
                ..
            } => {
                ledger.vote(decree_num, ballot, value.clone()).await;
                self.observer.on_event(Event::Learn {
                    decree_num,
                    id: self.id,
                    value: value.clone(),
                });
            }
            _ => {}
        }
    }
}

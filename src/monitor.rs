use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::message::{Message, MessageTrace};
mod event;
pub use event::{Event, EventProtocol};

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

pub trait PaxosObserver: Send + Sync {
    fn on_event(&self, event: Event);

    fn on_message_trace(&self, trace: MessageTrace);

    fn on_message(&self, indexes: &[Uuid], message: Message) {
        self.on_message_trace(MessageTrace::from_legacy(indexes, &message));
    }
}

pub struct NoOpObserver;

impl PaxosObserver for NoOpObserver {
    fn on_event(&self, _event: Event) {}
    fn on_message_trace(&self, _trace: MessageTrace) {}
}

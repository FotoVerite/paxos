use std::time::{SystemTime, UNIX_EPOCH};

mod event;
mod message_trace;
pub use event::{Event, EventProtocol, ReconfigurationPhase, ReconfigurationProposalOutcome};
pub use message_trace::{MessageProtocol, MessageTrace, ObservedMessage, TraceableMessage};

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

pub trait PaxosObserver: Send + Sync {
    fn on_event(&self, event: Event);

    fn on_message_trace(&self, trace: MessageTrace);
}

pub struct NoOpObserver;

impl PaxosObserver for NoOpObserver {
    fn on_event(&self, _event: Event) {}
    fn on_message_trace(&self, _trace: MessageTrace) {}
}

use paxos::monitor::{Event, PaxosObserver};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RecordedEvent {
    pub event_type: String,
    pub node_id: usize,
    pub decree_num: usize,
}

/// Observer that records all events for later verification
#[derive(Clone)]
pub struct RecordingObserver {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
    learned_values: Arc<Mutex<HashMap<usize, String>>>, // decree_num -> value
    pending_tasks: Arc<AtomicUsize>, // Track spawned tasks waiting to complete
}

impl RecordingObserver {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            learned_values: Arc::new(Mutex::new(HashMap::new())),
            pending_tasks: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Wait for all pending event recording tasks to complete.
    /// This is necessary because on_event() spawns async tasks that may not
    /// complete immediately. Call this before asserting on event counts.
    pub async fn wait_for_events(&self) {
        loop {
            if self.pending_tasks.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }

    #[allow(dead_code)]
    pub async fn get_events(&self) -> Vec<RecordedEvent> {
        self.events.lock().await.clone()
    }

    #[allow(dead_code)]
    pub async fn get_learned_values(&self) -> HashMap<usize, String> {
        self.learned_values.lock().await.clone()
    }

    pub async fn count_events(&self, event_type: &str) -> usize {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| e.event_type == event_type)
            .count()
    }

    pub async fn count_decrees_learned(&self) -> usize {
        self.learned_values.lock().await.len()
    }

    #[allow(dead_code)]
    pub async fn was_decree_learned(&self, decree_num: usize) -> bool {
        self.learned_values.lock().await.contains_key(&decree_num)
    }

    #[allow(dead_code)]
    pub async fn clear(&self) {
        self.events.lock().await.clear();
        self.learned_values.lock().await.clear();
    }
}

impl PaxosObserver for RecordingObserver {
    fn on_event(&self, event: Event) {
        let events = Arc::clone(&self.events);
        let learned = Arc::clone(&self.learned_values);
        let pending = Arc::clone(&self.pending_tasks);

        // Increment pending task counter before spawning
        pending.fetch_add(1, Ordering::Release);

        tokio::spawn(async move {
            match event {
                Event::Proposal {
                    id,
                    decree_num,
                    ..
                } => {
                    events.lock().await.push(RecordedEvent {
                        event_type: "Proposal".to_string(),
                        node_id: id,
                        decree_num,
                    });
                }
                Event::Promise {
                    id,
                    decree_num,
                    ..
                } => {
                    events.lock().await.push(RecordedEvent {
                        event_type: "Promise".to_string(),
                        node_id: id,
                        decree_num,
                    });
                }
                Event::Accept {
                    id,
                    decree_num,
                    ..
                } => {
                    events.lock().await.push(RecordedEvent {
                        event_type: "Accept".to_string(),
                        node_id: id,
                        decree_num,
                    });
                }
                Event::Learn {
                    id,
                    decree_num,
                    value,
                    ..
                } => {
                    events.lock().await.push(RecordedEvent {
                        event_type: "Learn".to_string(),
                        node_id: id,
                        decree_num,
                    });
                    learned.lock().await.insert(decree_num, value.to_string());
                }
                _ => {
                    // Other events are not recorded
                }
            }
            // Decrement pending task counter when done
            pending.fetch_sub(1, Ordering::Release);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paxos::paxos_command::PaxosCommand;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_recording_observer() {
        let observer = RecordingObserver::new();

        // Simulate events
        observer.on_event(Event::Proposal {
            id: 0,
            decree_num: 1,
            value: PaxosCommand::NOOP,
            created_at: 0,
        });

        observer.on_event(Event::Promise {
            id: 1,
            from: 0,
            decree_num: 1,
            ballot: 1,
            created_at: 0,
        });

        observer.on_event(Event::Accept {
            id: 0,
            decree_num: 1,
            ballot: 1,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
            created_at: 0,
        });

        // Wait for events to be recorded
        observer.wait_for_events().await;

        assert_eq!(observer.count_events("Proposal").await, 1);
        assert_eq!(observer.count_events("Promise").await, 1);
        assert_eq!(observer.count_events("Accept").await, 1);
    }
}

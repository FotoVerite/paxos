use std::sync::{Arc, Mutex};

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{ballot::Ballot, learner::Learner},
    paxos_command::PaxosCommand,
};

// ============================================================================
// TEST OBSERVER
// ============================================================================

#[derive(Clone)]
struct TestObserver {
    events: Arc<Mutex<Vec<Event>>>,
}

impl TestObserver {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn as_arc(&self) -> Arc<dyn PaxosObserver> {
        Arc::new(self.clone())
    }
}

impl PaxosObserver for TestObserver {
    fn on_event(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

// ============================================================================
// HELPERS
// ============================================================================

fn new_observer() -> TestObserver {
    TestObserver::new()
}

fn new_learner(id: usize, observer: &TestObserver) -> Learner {
    Learner::new(id, observer.as_arc())
}

// ============================================================================
// LEARNER TESTS
// ============================================================================

#[tokio::test]
async fn learner_receives_accepted_values() {
    let observer = new_observer();
    let mut learner = new_learner(1, &observer);

    let cmd1 = PaxosCommand::GET {
        key: "key1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "key2".to_string(),
    };

    learner
        .handle_message(
            Message::Accepted {
                from: 2,
                decree_num: 0,
                ballot: Ballot::new(1, 1),
                value: cmd1.clone(),
            },
            &mut paxos::node::ledger::Ledger::init(2),
        )
        .await;
    learner
        .handle_message(
            Message::Accepted {
                from: 3,
                decree_num: 0,
                ballot: Ballot::new(1, 1),
                value: cmd2.clone(),
            },
            &mut paxos::node::ledger::Ledger::init(2),
        )
        .await;

    let events = observer.get_events();
    let learns = events
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 2);
}

#[tokio::test]
async fn learner_ignores_non_accepted_messages() {
    let observer = new_observer();
    let mut learner = new_learner(1, &observer);

    // Try to send a Prepare (learner should ignore it)
    learner
        .handle_message(
            Message::Prepare {
                from: 2,
                decree_num: 0,
                ballot: Ballot::new(1, 1),
            },
            &mut paxos::node::ledger::Ledger::init(2),
        )
        .await;

    let events = observer.get_events();
    assert_eq!(events.len(), 0); // No events should be recorded
}

#[tokio::test]
async fn learner_learns_multiple_decrees() {
    let observer = new_observer();
    let mut learner = new_learner(1, &observer);
    let mut ledger = paxos::node::ledger::Ledger::init(1);

    let b = Ballot::new(1, 1);

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };

    // Learner receives Accepted for decree 0
    learner
        .handle_message(
            Message::Accepted {
                from: 2,
                decree_num: 0,
                ballot: b,
                value: cmd0.clone(),
            },
            &mut ledger,
        )
        .await;

    // Learner receives Accepted for decree 1
    learner
        .handle_message(
            Message::Accepted {
                from: 2,
                decree_num: 1,
                ballot: b,
                value: cmd1.clone(),
            },
            &mut ledger,
        )
        .await;

    // Both should generate Learn events
    let learns = observer
        .get_events()
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 2);
}

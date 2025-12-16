use std::sync::{Arc, Mutex};

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{ballot::Ballot, proposer::Proposer},
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

fn new_proposer(id: usize, quorum: usize, observer: &TestObserver) -> Proposer {
    Proposer::new(id, quorum, observer.as_arc())
}

fn send_prepare(proposer: &mut Proposer, decree_num: usize, cmd: PaxosCommand) -> Message {
    proposer.propose(decree_num, cmd)
}

async fn handle_at_proposer(proposer: &mut Proposer, msg: Message) -> Message {
    proposer.handle_message(msg).await
}

// ============================================================================
// PROPOSER TESTS
// ============================================================================

#[tokio::test]
async fn proposer_issues_prepare_with_correct_ballot() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let msg = send_prepare(
        &mut proposer,
        0,
        PaxosCommand::GET {
            key: "key".to_string(),
        },
    );

    if let Message::Prepare { ballot, .. } = msg {
        assert_eq!(ballot.number, 1);
        assert_eq!(ballot.node_id, 1);
    } else {
        panic!("Expected Prepare message");
    }
}

#[tokio::test]
async fn proposer_sends_accept_on_promise() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let cmd = PaxosCommand::GET {
        key: "mykey".to_string(),
    };

    send_prepare(&mut proposer, 0, cmd.clone());

    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };
    let resp = handle_at_proposer(&mut proposer, promise).await;

    if let Message::Accept { ballot, value, .. } = resp {
        assert_eq!(ballot, Ballot::new(1, 1));
        assert_eq!(value, cmd);
    } else {
        panic!("Expected Accept message, got {:?}", resp);
    }
}

#[tokio::test]
async fn proposer_adopts_previously_accepted_value() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let proposed_cmd = PaxosCommand::GET {
        key: "newkey".to_string(),
    };
    let previous_cmd = PaxosCommand::PUT {
        key: "oldkey".to_string(),
        version: 1,
    };

    send_prepare(&mut proposer, 0, proposed_cmd);

    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(5, 1),
        accepted_value: previous_cmd.clone(),
    };
    let resp = handle_at_proposer(&mut proposer, promise).await;

    if let Message::Accept { ballot, value, .. } = resp {
        assert_eq!(ballot, Ballot::new(1, 1));
        assert_eq!(value, previous_cmd);
    } else {
        panic!("Expected Accept message");
    }
}

#[tokio::test]
async fn proposer_ignores_promise_for_wrong_ballot() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    send_prepare(
        &mut proposer,
        0,
        PaxosCommand::GET {
            key: "key".to_string(),
        },
    );

    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(2, 1),
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };
    let resp = handle_at_proposer(&mut proposer, promise).await;

    // Wrong ballot should return NACK
    assert!(matches!(resp, Message::NACK));
}

#[tokio::test]
async fn proposer_picks_highest_accepted_ballot() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let proposed_cmd = PaxosCommand::GET {
        key: "key".to_string(),
    };

    send_prepare(&mut proposer, 0, proposed_cmd);

    let value_from_3 = PaxosCommand::PUT {
        key: "ballot3".to_string(),
        version: 1,
    };
    let value_from_5 = PaxosCommand::PUT {
        key: "ballot5".to_string(),
        version: 2,
    };

    let promise1 = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(5, 1),
        accepted_value: value_from_5.clone(),
    };
    let resp = handle_at_proposer(&mut proposer, promise1).await;

    if let Message::Accept { value, .. } = resp {
        assert_eq!(value, value_from_5);
    } else {
        panic!("Expected Accept message");
    }
}

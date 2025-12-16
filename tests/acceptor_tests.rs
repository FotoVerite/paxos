use std::sync::{Arc, Mutex};

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{acceptor::Acceptor, ballot::Ballot},
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

fn new_acceptor(id: usize, observer: &TestObserver) -> Acceptor {
    Acceptor::new(id, observer.as_arc())
}

async fn handle_at_acceptor(acceptor: &mut Acceptor, msg: Message) -> Message {
    acceptor.handle_message(msg).await
}

// ============================================================================
// ACCEPTOR TESTS
// ============================================================================

#[tokio::test]
async fn acceptor_rejects_lower_ballot_prepare() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b_high = Ballot::new(5, 1);
    let b_low = Ballot::new(3, 1);

    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_high,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Promise { ballot, .. } if ballot == b_high));

    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_low,
        },
    )
    .await;
    assert!(matches!(resp2, Message::NACK));
}

#[tokio::test]
async fn acceptor_accepts_higher_ballot_prepare() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);
    let b7 = Ballot::new(7, 1);

    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Promise { ballot, .. } if ballot == b5));

    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b7,
        },
    )
    .await;
    assert!(matches!(resp2, Message::Promise { ballot, .. } if ballot == b7));
}

#[tokio::test]
async fn acceptor_rejects_accept_below_min_ballot() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);
    let b3 = Ballot::new(3, 1);

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b3,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp, Message::NACK));
}

#[tokio::test]
async fn acceptor_accepts_accept_at_min_ballot() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b5,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(
        resp,
        Message::Accepted { ballot, value, .. } if value == PaxosCommand::NOOP && ballot == b5
    ));
}

#[tokio::test]
async fn acceptor_accepts_accept_above_min_ballot() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);
    let b7 = Ballot::new(7, 1);

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b7,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp, Message::Accepted { ballot, .. } if ballot == b7));
}

#[tokio::test]
async fn acceptor_returns_previous_accepted_value() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b1 = Ballot::new(1, 1);
    let b3 = Ballot::new(3, 1);

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b1,
        },
    )
    .await;
    handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b1,
            value: PaxosCommand::PUT {
                key: "original".to_string(),
                version: 1,
            },
        },
    )
    .await;

    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b3,
        },
    )
    .await;

    if let Message::Promise {
        ballot,
        accepted_ballot,
        accepted_value,
        ..
    } = resp
    {
        assert_eq!(ballot, b3);
        assert_eq!(accepted_ballot, b1);
        assert_eq!(
            accepted_value,
            PaxosCommand::PUT {
                key: "original".to_string(),
                version: 1
            }
        );
    } else {
        panic!("Expected Promise with accepted value");
    }
}

#[tokio::test]
async fn acceptor_handles_equal_ballot_prepare() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;
    assert!(matches!(resp, Message::NACK));
}

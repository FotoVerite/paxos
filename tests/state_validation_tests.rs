use std::sync::{Arc, Mutex};

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{acceptor::Acceptor, ballot::Ballot, proposer::Proposer},
    paxos_command::PaxosCommand,
};

// ============================================================================
// TEST OBSERVER WITH STATE TRACKING
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

fn new_acceptor(id: usize, observer: &TestObserver) -> Acceptor {
    Acceptor::new(id, observer.as_arc())
}

fn new_proposer(id: usize, quorum: usize, observer: &TestObserver) -> Proposer {
    Proposer::new(id, quorum, observer.as_arc())
}

async fn handle_at_acceptor(acceptor: &mut Acceptor, msg: Message) -> Message {
    acceptor.handle_message(msg).await
}

async fn handle_at_proposer(proposer: &mut Proposer, msg: Message) -> Message {
    proposer.handle_message(msg).await
}

// ============================================================================
// STATE VALIDATION TESTS
// ============================================================================

/// Invariant: Acceptor never promises a ballot and then accepts at a lower ballot
#[tokio::test]
async fn acceptor_ballot_monotonicity_within_decree() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);
    let b3 = Ballot::new(3, 1);

    // Promise to ballot (5, 1)
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    // Try to accept at lower ballot (3, 1) - should be rejected
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

    // INVARIANT: Must reject (ballot went backwards)
    assert!(
        matches!(resp, Message::NACK),
        "Acceptor violated monotonicity: accepted lower ballot after higher promise"
    );
}

/// Invariant: Acceptor never promises same decree at same or lower ballot twice
#[tokio::test]
async fn acceptor_no_promise_downgrade_same_decree() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);

    // First promise to (5, 1)
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Second promise attempt with same ballot - should be rejected
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    // INVARIANT: Must reject (same ballot requested twice)
    assert!(
        matches!(resp2, Message::NACK),
        "Acceptor violated state invariant: accepted same ballot twice for same decree"
    );
}

/// Invariant: Acceptor maintains monotonic ballot progression per decree
#[tokio::test]
async fn acceptor_monotonic_promise_progression() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b3 = Ballot::new(3, 1);
    let b5 = Ballot::new(5, 1);
    let b7 = Ballot::new(7, 1);
    let b6 = Ballot::new(6, 1); // Would be out of order after 7

    // Promise sequence: 3 -> 5 -> 7 (valid progression)
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b3,
        },
    )
    .await;

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b7,
        },
    )
    .await;

    // Try to promise at 6 after promising at 7 - should fail
    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b6,
        },
    )
    .await;

    // INVARIANT: Must reject (ballot went backwards)
    assert!(
        matches!(resp, Message::NACK),
        "Acceptor violated monotonicity: accepted lower ballot after higher"
    );
}

/// Invariant: Acceptor state is independent per decree
#[tokio::test]
async fn acceptor_decree_independence_for_ballots() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5_1 = Ballot::new(5, 1);
    let b3_1 = Ballot::new(3, 1);

    // Decree 0: Promise to (5, 1)
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5_1,
        },
    )
    .await;

    // Decree 1: Should be able to promise to (3, 1) independently
    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 1,
            ballot: b3_1,
        },
    )
    .await;

    // INVARIANT: Must succeed - decrees are independent
    assert!(
        matches!(resp, Message::Promise { .. }),
        "Acceptor violated decree independence: ballot in decree 0 affected decree 1"
    );
}

/// Invariant: Proposer doesn't send Accept with lower ballot after higher
#[tokio::test]
async fn proposer_ballot_monotonicity_per_decree() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // First proposal creates ballot (1, 1) for decree 0
    let msg1 = proposer.propose(0, cmd.clone());
    let b1 = match msg1 {
        Message::Prepare { ballot, .. } => ballot,
        other => panic!("Expected Prepare, got {:?}", other),
    };

    // Second proposal for same decree
    let msg2 = proposer.propose(0, cmd.clone());

    // If it returns a message, it should have ballot >= first one
    match msg2 {
        Message::Prepare { ballot: b2, .. } => {
            // INVARIANT: Proposer ballot must be monotonic per decree
            assert!(
                b2 >= b1,
                "Proposer violated monotonicity: ballot went backwards from {:?} to {:?}",
                b1,
                b2
            );
        }
        other => {
            // Proposer may return something else (cached, etc) - that's ok
            // The key invariant is: if it returns a Prepare for same decree, ballot must be >= b1
        }
    }
}

/// Invariant: Proposer tracks multiple decrees with same ballot number
#[tokio::test]
async fn proposer_same_ballot_different_decrees() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };

    // Both decrees in same proposal phase should get same ballot
    let msg0 = proposer.propose(0, cmd0);
    let msg1 = proposer.propose(1, cmd1);

    if let (Message::Prepare { ballot: b0, .. }, Message::Prepare { ballot: b1, .. }) = (msg0, msg1) {
        // INVARIANT: All decrees in same proposal wave should have same ballot
        assert_eq!(
            b0, b1,
            "Proposer ballot inconsistency: decree 0 got {:?}, decree 1 got {:?}",
            b0, b1
        );
    }
}

/// Invariant: Acceptor never returns accepted_value for rejected Prepare
#[tokio::test]
async fn acceptor_never_leaks_value_on_nack() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b5 = Ballot::new(5, 1);
    let b3 = Ballot::new(3, 1);

    // Accept a value at ballot (5, 1)
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b5,
        },
    )
    .await;

    handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b5,
            value: PaxosCommand::PUT {
                key: "secret".to_string(),
                version: 1,
            },
        },
    )
    .await;

    // Try to prepare with lower ballot - gets NACK
    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b3,
        },
    )
    .await;

    // INVARIANT: NACK should never expose accepted values
    assert!(
        matches!(resp, Message::NACK),
        "Acceptor violated invariant: should reject lower prepare, not leak state"
    );
}

/// Invariant: Proposer adopts previously accepted value with highest ballot
#[tokio::test]
async fn proposer_value_adoption_invariant() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let new_value = PaxosCommand::GET {
        key: "new".to_string(),
    };
    let old_value = PaxosCommand::PUT {
        key: "old".to_string(),
        version: 1,
    };
    let older_value = PaxosCommand::PUT {
        key: "older".to_string(),
        version: 0,
    };

    // Propose initial value
    proposer.propose(0, new_value);

    // Receive promise with old accepted value from ballot (5, 2)
    let promise1 = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(5, 2),
        accepted_value: old_value.clone(),
    };

    let msg1 = handle_at_proposer(&mut proposer, promise1).await;

    // Verify proposer adopted the old value
    if let Message::Accept { value, .. } = msg1 {
        assert_eq!(
            value, old_value,
            "Proposer failed to adopt accepted value from higher ballot"
        );
    }

    // Now try to override with even older value - should keep old_value
    let promise2 = Message::Promise {
        from: 3,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(3, 1), // Lower than (5, 2)
        accepted_value: older_value,
    };

    let msg2 = handle_at_proposer(&mut proposer, promise2).await;

    // INVARIANT: Must keep the value from highest accepted_ballot
    if let Message::Accept { value, .. } = msg2 {
        assert_eq!(
            value, old_value,
            "Proposer violated invariant: should keep value from highest accepted_ballot"
        );
    }
}

/// Invariant: Accept message must match the ballot in Prepare -> Promise flow
#[tokio::test]
async fn proposer_accept_ballot_matches_promise() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // Send prepare
    let prepare = proposer.propose(0, cmd.clone());
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Receive promise with same ballot
    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };

    let accept = handle_at_proposer(&mut proposer, promise).await;

    // INVARIANT: Accept ballot must match Promise ballot (which matched Prepare)
    if let Message::Accept { ballot: accept_ballot, .. } = accept {
        assert_eq!(
            accept_ballot, prepare_ballot,
            "Proposer ballot mismatch: Prepare {:?} but Accept {:?}",
            prepare_ballot,
            accept_ballot
        );
    }
}

/// Invariant: Acceptor rejects Accept messages from lower ballot than promised
#[tokio::test]
async fn acceptor_accept_ballot_validation() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b7 = Ballot::new(7, 1);
    let b5 = Ballot::new(5, 1);
    let b9 = Ballot::new(9, 1);

    // Promise to ballot (7, 1)
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b7,
        },
    )
    .await;

    // Try to accept at (5, 1) - too low
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b5,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp1, Message::NACK));

    // Accept at (7, 1) - exact ballot
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b7,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp2, Message::Accepted { .. }));

    // Accept at (9, 1) - higher than promised, but we need a new promise first
    // This tests that acceptor enforces min_ballot >= promised ballot
    let new_promise = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b9,
        },
    )
    .await;
    assert!(matches!(new_promise, Message::Promise { .. }));

    let resp3 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b9,
            value: PaxosCommand::NOOP,
        },
    )
    .await;

    // INVARIANT: Accept must be at or above min_ballot
    assert!(
        matches!(resp3, Message::Accepted { .. }),
        "Acceptor violated invariant: rejected valid Accept at promised ballot"
    );
}

/// Invariant: Multiple proposals at different ballots maintain isolation
#[tokio::test]
async fn concurrent_proposals_ballot_isolation() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b1_1 = Ballot::new(1, 1); // Proposer 1
    let b1_2 = Ballot::new(1, 2); // Proposer 2
    let b1_3 = Ballot::new(1, 3); // Proposer 3

    // All three proposers promise with same round
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b1_1,
        },
    )
    .await;

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 2,
            decree_num: 0,
            ballot: b1_2,
        },
    )
    .await;

    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 3,
            decree_num: 0,
            ballot: b1_3,
        },
    )
    .await;

    // Proposer 1 and 2 try to accept - but 3 has highest ballot
    let accept1 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b1_1,
            value: PaxosCommand::GET {
                key: "p1".to_string(),
            },
        },
    )
    .await;

    let accept2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 2,
            decree_num: 0,
            ballot: b1_2,
            value: PaxosCommand::GET {
                key: "p2".to_string(),
            },
        },
    )
    .await;

    // INVARIANT: Both should be rejected (lower than min_ballot b1_3)
    assert!(
        matches!(accept1, Message::NACK),
        "Acceptor accepted value from lower ballot when higher exists"
    );
    assert!(
        matches!(accept2, Message::NACK),
        "Acceptor accepted value from lower ballot when highest exists"
    );

    // Only proposer 3 succeeds
    let accept3 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 3,
            decree_num: 0,
            ballot: b1_3,
            value: PaxosCommand::GET {
                key: "p3".to_string(),
            },
        },
    )
    .await;
    assert!(matches!(accept3, Message::Accepted { value, .. } if value == PaxosCommand::GET { key: "p3".to_string(), }));
}

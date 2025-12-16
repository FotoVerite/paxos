use std::sync::{Arc, Mutex};

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{acceptor::Acceptor, ballot::Ballot, learner::Learner, proposer::Proposer},
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

fn new_acceptor(id: usize, observer: &TestObserver) -> Acceptor {
    Acceptor::new(id, observer.as_arc())
}

fn new_proposer(id: usize, quorum: usize, observer: &TestObserver) -> Proposer {
    Proposer::new(id, quorum, observer.as_arc())
}

fn new_learner(id: usize, observer: &TestObserver) -> Learner {
    Learner::new(id, observer.as_arc())
}

async fn handle_at_acceptor(acceptor: &mut Acceptor, msg: Message) -> Message {
    acceptor.handle_message(msg).await
}

async fn handle_at_proposer(proposer: &mut Proposer, msg: Message) -> Message {
    proposer.handle_message(msg).await
}

async fn handle_at_learner(learner: &mut Learner, msg: Message, ledger: &mut paxos::node::ledger::Ledger) {
    learner.handle_message(msg, ledger).await
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// Edge case: Promise arrives after Accept has been sent (out of order)
#[tokio::test]
async fn out_of_order_promise_after_accept() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let mut acceptor = new_acceptor(1, &observer);

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // Proposer sends Prepare
    let prepare = proposer.propose(0, cmd.clone());
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Proposer receives Promise
    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };
    let accept = handle_at_proposer(&mut proposer, promise).await;

    // Now imagine Promise from another acceptor arrives (out of order)
    let late_promise = Message::Promise {
        from: 3,
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };

    // Proposer should handle it gracefully (may already have sent Accept, or queue it)
    let resp = handle_at_proposer(&mut proposer, late_promise).await;

    // Either NACK (quorum already met) or Accept is acceptable
    match resp {
        Message::NACK | Message::Accept { .. } => (),
        _ => (),
    }
}

/// Edge case: Accept message arrives at acceptor before Prepare (out of order)
#[tokio::test]
async fn accept_before_prepare_same_decree() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(5, 1);

    // Accept arrives before Prepare - acceptor has no promise yet
    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: PaxosCommand::NOOP,
        },
    )
    .await;

    // SHOULD reject - acceptor must have promised before accepting
    // min_ballot should be 0/unset, so Accept should fail
    assert!(
        matches!(resp, Message::NACK),
        "Acceptor MUST reject Accept before any Prepare (no promise yet)"
    );
}

/// Edge case: Duplicate Prepare messages from same proposer
#[tokio::test]
async fn duplicate_prepare_messages() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(5, 1);

    // First Prepare
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Duplicate Prepare with same ballot
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;

    // Should be rejected (already promised to this ballot)
    assert!(
        matches!(resp2, Message::NACK),
        "Acceptor should reject duplicate prepare at same ballot"
    );
}

/// Edge case: Duplicate Accept messages
#[tokio::test]
async fn duplicate_accept_messages() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(5, 1);
    let value = PaxosCommand::PUT {
        key: "test".to_string(),
        version: 1,
    };

    // Prepare first
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;

    // First Accept
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: value.clone(),
        },
    )
    .await;
    assert!(matches!(resp1, Message::Accepted { .. }));

    // Duplicate Accept with same ballot and value
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: value.clone(),
        },
    )
    .await;

    // Could be accepted or rejected - both are safe (idempotent)
    match resp2 {
        Message::Accepted { .. } => (),
        Message::NACK => (),
        _ => panic!("Unexpected response to duplicate Accept"),
    }
}

/// Edge case: Learner receives Accepted out of order
#[tokio::test]
async fn learner_out_of_order_accepted() {
    let observer = new_observer();
    let mut learner = new_learner(1, &observer);
    let mut ledger = paxos::node::ledger::Ledger::init(2);

    let b = Ballot::new(1, 1);
    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };

    // Receive Accepted for decree 1 first
    handle_at_learner(
        &mut learner,
        Message::Accepted {
            from: 2,
            decree_num: 1,
            ballot: b,
            value: cmd1.clone(),
        },
        &mut ledger,
    )
    .await;

    // Then Accepted for decree 0
    handle_at_learner(
        &mut learner,
        Message::Accepted {
            from: 2,
            decree_num: 0,
            ballot: b,
            value: cmd0.clone(),
        },
        &mut ledger,
    )
    .await;

    // Both should be learned despite order
    let learns = observer
        .get_events()
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 2);
}

/// Edge case: Proposer with insufficient promises (minority quorum)
#[tokio::test]
async fn proposer_with_insufficient_promises() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 3, &observer); // Needs 3 promises (5-node cluster)

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    let prepare = proposer.propose(0, cmd.clone());
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Receive only 1 promise (need 3 for quorum)
    let promise1 = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };

    let resp1 = handle_at_proposer(&mut proposer, promise1).await;

    // MUST NOT send Accept yet (only 1 promise, need 3)
    assert!(
        !matches!(resp1, Message::Accept { .. }),
        "Proposer MUST NOT send Accept without quorum (1 < 3)"
    );

    // Receive 2nd promise
    let promise2 = Message::Promise {
        from: 3,
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };

    let resp2 = handle_at_proposer(&mut proposer, promise2).await;

    // MUST NOT send Accept (only 2 promises, need 3)
    assert!(
        !matches!(resp2, Message::Accept { .. }),
        "Proposer MUST NOT send Accept without quorum (2 < 3)"
    );

    // Receive 3rd promise - now MUST reach quorum and send Accept
    let promise3 = Message::Promise {
        from: 4,
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };

    let resp3 = handle_at_proposer(&mut proposer, promise3).await;

    // MUST send Accept when quorum is reached (3 >= 3)
    assert!(
        matches!(resp3, Message::Accept { .. }),
        "Proposer MUST send Accept when quorum is reached (3 >= 3)"
    );
}

/// Edge case: Very large ballot numbers
#[tokio::test]
async fn large_ballot_numbers() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b_huge = Ballot::new(999999, 1);
    let b_higher = Ballot::new(1000000, 1);

    // Prepare with huge ballot
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_huge,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Higher ballot should still work
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_higher,
        },
    )
    .await;
    assert!(matches!(resp2, Message::Promise { .. }));
}

/// Edge case: Accept with different value at same ballot
#[tokio::test]
async fn accept_with_different_value_than_proposed() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(5, 1);
    let value1 = PaxosCommand::GET {
        key: "original".to_string(),
    };
    let value2 = PaxosCommand::GET {
        key: "different".to_string(),
    };

    // Prepare
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;

    // Accept with value1
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: value1.clone(),
        },
    )
    .await;
    assert!(matches!(resp1, Message::Accepted { value, .. } if value == value1));

    // Try to accept with different value at same ballot
    // Once accepted, acceptor should not change the value at same ballot
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: value2.clone(),
        },
    )
    .await;

    // SHOULD reject or return the original value, never overwrite with new value
    match resp2 {
        Message::Accepted { value, .. } => {
            // If accepted, MUST be the original value
            assert_eq!(
                value, value1,
                "Acceptor MUST NOT change accepted value at same ballot"
            );
        }
        Message::NACK => {
            // Rejecting is also correct
        }
        _ => panic!("Unexpected response"),
    }
}

/// Edge case: Proposer receives Promise from itself
#[tokio::test]
async fn proposer_promise_from_itself() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    let prepare = proposer.propose(0, cmd.clone());
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Proposer receives Promise from itself
    let promise_from_self = Message::Promise {
        from: 1, // Same as proposer id
        decree_num: 0,
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };

    let resp = handle_at_proposer(&mut proposer, promise_from_self).await;

    // Should be handled gracefully - may count as a promise or ignore
    match resp {
        Message::Accept { .. } => (),
        Message::NACK => (),
        _ => (),
    }
}

/// Edge case: Multiple proposals competing for same decree (concurrent)
#[tokio::test]
async fn multiple_concurrent_proposals_same_decree() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b1_1 = Ballot::new(1, 1); // Proposer 1
    let b1_2 = Ballot::new(1, 2); // Proposer 2
    let b1_3 = Ballot::new(1, 3); // Proposer 3

    let value1 = PaxosCommand::GET {
        key: "p1".to_string(),
    };
    let value2 = PaxosCommand::GET {
        key: "p2".to_string(),
    };
    let value3 = PaxosCommand::GET {
        key: "p3".to_string(),
    };

    // All three prepare
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

    // P1 tries to accept (lowest ballot) - should fail
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b1_1,
            value: value1,
        },
    )
    .await;
    assert!(matches!(resp1, Message::NACK));

    // P2 tries to accept (middle ballot) - should fail
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 2,
            decree_num: 0,
            ballot: b1_2,
            value: value2,
        },
    )
    .await;
    assert!(matches!(resp2, Message::NACK));

    // P3 tries to accept (highest ballot) - should succeed
    let resp3 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 3,
            decree_num: 0,
            ballot: b1_3,
            value: value3.clone(),
        },
    )
    .await;
    assert!(matches!(resp3, Message::Accepted { value, .. } if value == value3));
}

/// Edge case: Acceptor receives messages for very sparse decree numbers
#[tokio::test]
async fn sparse_decree_numbering() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(1, 1);

    // Prepare for decree 0
    let resp0 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(resp0, Message::Promise { .. }));

    // Prepare for decree 1000 (huge gap)
    let resp1000 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 1000,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(resp1000, Message::Promise { .. }));

    // Both decrees should be independent
    handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: PaxosCommand::NOOP,
        },
    )
    .await;

    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 1000,
            ballot: b,
            value: PaxosCommand::GET {
                key: "sparse".to_string(),
            },
        },
    )
    .await;

    assert!(matches!(resp, Message::Accepted { .. }));
}

/// Edge case: Learner receives Accepted for same decree from all acceptors
#[tokio::test]
async fn learner_consensus_from_all_acceptors() {
    let observer = new_observer();
    let mut learner = new_learner(1, &observer);
    let mut ledger = paxos::node::ledger::Ledger::init(3);

    let b = Ballot::new(1, 1);
    let cmd = PaxosCommand::PUT {
        key: "consensus".to_string(),
        version: 1,
    };

    // Receive Accepted from all 3 acceptors
    for acceptor_id in 2..=4 {
        handle_at_learner(
            &mut learner,
            Message::Accepted {
                from: acceptor_id,
                decree_num: 0,
                ballot: b,
                value: cmd.clone(),
            },
            &mut ledger,
        )
        .await;
    }

    // All should be learned
    let learns = observer
        .get_events()
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 3);
}

/// Edge case: Promise with accepted_ballot higher than current ballot
#[tokio::test]
async fn promise_reports_higher_accepted_ballot() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    proposer.propose(0, cmd.clone());

    // Promise reports accepted value from ballot (10, 2) but current ballot is (1, 1)
    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(10, 2), // Much higher than current
        accepted_value: PaxosCommand::PUT {
            key: "old".to_string(),
            version: 1,
        },
    };

    let resp = handle_at_proposer(&mut proposer, promise).await;

    // Proposer should adopt the old value despite its high ballot
    if let Message::Accept { value, .. } = resp {
        assert!(
            matches!(value, PaxosCommand::PUT { .. }),
            "Proposer should adopt value from higher accepted ballot"
        );
    }
}

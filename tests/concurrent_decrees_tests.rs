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

// ============================================================================
// CONCURRENT DECREES TESTS
// ============================================================================

#[tokio::test]
async fn proposer_can_track_multiple_decrees() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "decree2".to_string(),
    };

    // Proposer proposes three different decrees
    let msg0 = proposer.propose(0, cmd0.clone());
    let msg1 = proposer.propose(1, cmd1.clone());
    let msg2 = proposer.propose(2, cmd2.clone());

    // All should be Prepare with ballot (1,1)
    assert!(matches!(msg0, Message::Prepare { decree_num: 0, ballot, .. } if ballot.number == 1));
    assert!(matches!(msg1, Message::Prepare { decree_num: 1, ballot, .. } if ballot.number == 1));
    assert!(matches!(msg2, Message::Prepare { decree_num: 2, ballot, .. } if ballot.number == 1));
}

#[tokio::test]
async fn acceptor_can_accept_multiple_decrees() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(5, 1);

    // Acceptor receives Prepare for decree 0
    let resp0 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(resp0, Message::Promise { decree_num: 0, .. }));

    // Acceptor receives Prepare for decree 1
    let resp1 = handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 1,
            ballot: b,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Promise { decree_num: 1, .. }));

    // Acceptor receives Accept for decree 0
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp2, Message::Accepted { decree_num: 0, .. }));

    // Acceptor receives Accept for decree 1 with different value
    let resp3 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 1,
            ballot: b,
            value: PaxosCommand::GET {
                key: "key1".to_string(),
            },
        },
    )
    .await;
    assert!(matches!(resp3, Message::Accepted { decree_num: 1, .. }));

    // Both decrees should be independent
    let events = observer.get_events();
    let accepts = events
        .iter()
        .filter(|e| matches!(e, Event::Accept { .. }))
        .count();
    assert_eq!(accepts, 2);
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

#[tokio::test]
async fn multiple_decrees_with_different_ballots() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    // Decree 0: ballot (1,1)
    let b1_1 = Ballot::new(1, 1);
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b1_1,
        },
    )
    .await;

    // Decree 1: ballot (1,2) - higher ballot number
    let b1_2 = Ballot::new(1, 2);
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 2,
            decree_num: 1,
            ballot: b1_2,
        },
    )
    .await;

    // Decree 0 cannot go back to lower ballot
    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: Ballot::new(1, 1),
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp, Message::Accepted { .. })); // Should still succeed for decree 0

    // But decree 1 is independent - it can accept at its ballot
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 2,
            decree_num: 1,
            ballot: b1_2,
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp2, Message::Accepted { .. }));
}

#[tokio::test]
async fn proposer_handles_promises_for_different_decrees() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };

    // Propose both decrees
    proposer.propose(0, cmd0.clone());
    proposer.propose(1, cmd1.clone());

    // Both have ballot (1,1)
    let b = Ballot::new(1, 1);

    // Receive promise for decree 0
    let resp0 = handle_at_proposer(
        &mut proposer,
        Message::Promise {
            from: 2,
            decree_num: 0,
            ballot: b,
            accepted_ballot: Ballot::new(0, 0),
            accepted_value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp0, Message::Accept { decree_num: 0, value, .. } if value == cmd0));

    // Receive promise for decree 1
    let resp1 = handle_at_proposer(
        &mut proposer,
        Message::Promise {
            from: 2,
            decree_num: 1,
            ballot: b,
            accepted_ballot: Ballot::new(0, 0),
            accepted_value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp1, Message::Accept { decree_num: 1, value, .. } if value == cmd1));
}

#[tokio::test]
async fn concurrent_decrees_dont_interfere() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b = Ballot::new(5, 1);

    // Promise for decree 0
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        },
    )
    .await;

    // Promise for decree 1 with DIFFERENT ballot
    let b_higher = Ballot::new(5, 2);
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 2,
            decree_num: 1,
            ballot: b_higher,
        },
    )
    .await;

    // Decree 0 can accept higher ballot than (5,1) - ballots progress independently
    let resp = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: Ballot::new(6, 1), // Higher than decree 0's min_ballot (5,1)
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp, Message::Accepted { ballot, .. } if ballot.number == 6)); // Should succeed

    // Decree 1 requires ballot >= its min_ballot (5,2), so (5,1) is rejected
    let resp2 = handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 1,
            ballot: Ballot::new(5, 1), // Lower than decree 1's min_ballot (5,2)
            value: PaxosCommand::NOOP,
        },
    )
    .await;
    assert!(matches!(resp2, Message::NACK)); // Correctly rejected - ballot too low
}

#[tokio::test]
async fn sequential_decrees_same_proposer() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 2, &observer);

    let mut acceptor1 = new_acceptor(1, &observer);
    let mut acceptor2 = new_acceptor(2, &observer);

    let cmd0 = PaxosCommand::PUT {
        key: "key0".to_string(),
        version: 1,
    };
    let cmd1 = PaxosCommand::PUT {
        key: "key1".to_string(),
        version: 2,
    };

    // Propose decree 0
    let prepare0 = proposer.propose(0, cmd0.clone());
    assert!(matches!(prepare0, Message::Prepare { decree_num: 0, .. }));

    // Propose decree 1
    let prepare1 = proposer.propose(1, cmd1.clone());
    assert!(matches!(prepare1, Message::Prepare { decree_num: 1, .. }));

    // Both should have ballot (1,1)
    if let Message::Prepare { ballot: b0, .. } = prepare0 {
        if let Message::Prepare { ballot: b1, .. } = prepare1 {
            assert_eq!(b0, b1);
        }
    }
}

#[tokio::test]
async fn learner_ledger_tracks_concurrent_decrees() {
    let observer = new_observer();
    let ledger = paxos::node::ledger::Ledger::init(1);

    let b = Ballot::new(1, 1);

    // Vote on decree 0
    ledger
        .vote(
            0,
            b,
            PaxosCommand::GET {
                key: "d0".to_string(),
            },
        )
        .await;

    // Vote on decree 1
    ledger
        .vote(
            1,
            b,
            PaxosCommand::GET {
                key: "d1".to_string(),
            },
        )
        .await;

    // Vote on decree 2
    ledger
        .vote(
            2,
            b,
            PaxosCommand::GET {
                key: "d2".to_string(),
            },
        )
        .await;

    // After voting on decrees 0, 1, 2, log length is 3, so next() returns 3
    let next = ledger.next().await;
    assert_eq!(next, 3); // Ledger.next() returns log.len() for sequential decrees
}

#[tokio::test]
async fn mixed_single_and_multi_decree_flow() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let mut acceptor = new_acceptor(1, &observer);
    let mut learner = new_learner(1, &observer);
    let mut ledger = paxos::node::ledger::Ledger::init(1);

    let cmd0 = PaxosCommand::GET {
        key: "cmd0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "cmd1".to_string(),
    };

    // Phase 1: Decree 0
    let prepare0 = proposer.propose(0, cmd0.clone());
    let promise0 = handle_at_acceptor(&mut acceptor, prepare0).await;
    let accept0 = handle_at_proposer(&mut proposer, promise0).await;
    let accepted0 = handle_at_acceptor(&mut acceptor, accept0).await;

    // Learner learns decree 0
    learner
        .handle_message(accepted0.clone(), &mut ledger)
        .await;

    // Phase 2: Decree 1 while decree 0 is being processed
    let prepare1 = proposer.propose(1, cmd1.clone());
    let promise1 = handle_at_acceptor(&mut acceptor, prepare1).await;
    let accept1 = handle_at_proposer(&mut proposer, promise1).await;
    let accepted1 = handle_at_acceptor(&mut acceptor, accept1).await;

    // Learner learns decree 1
    learner
        .handle_message(accepted1.clone(), &mut ledger)
        .await;

    // Both should be learned
    let learns = observer
        .get_events()
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 2);
}

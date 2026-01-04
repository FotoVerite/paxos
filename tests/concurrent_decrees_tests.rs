mod test_helpers;

use paxos::{message::Message, monitor::Event, node::ballot::Ballot, paxos_command::PaxosCommand};
use std::collections::HashSet;
use test_helpers::{cleanup_persisted_state, NodeBuilder, RecordingObserver};
use std::sync::Arc; // Added Arc import

// ============================================================================
// CONCURRENT DECREES TESTS
// ============================================================================

#[tokio::test]
async fn proposer_can_track_multiple_decrees() {
    let builder: NodeBuilder = NodeBuilder::new();

    let proposer = builder.proposer(1, 1).await.unwrap();

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decre1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "decre2".to_string(),
    };

    // Proposer proposes three different decrees
    let msg0 = proposer.propose(0, cmd0.clone()).await;
    let msg1 = proposer.propose(1, cmd1.clone()).await;
    let msg2 = proposer.propose(2, cmd2.clone()).await;

    // Each proposal increments ballot: (1,1), (2,1), (3,1)
    assert!(
        matches!(msg0, Message::Prepare { decree_num: 0, ballot, .. } if ballot.number == 1)
    );
    assert!(
        matches!(msg1, Message::Prepare { decree_num: 1, ballot, .. } if ballot.number == 1)
    );
    assert!(
        matches!(msg2, Message::Prepare { decree_num: 2, ballot, .. } if ballot.number == 1)
    );
}

#[tokio::test]
async fn acceptor_can_accept_multiple_decrees() {
    let observer = RecordingObserver::new().arc();
    let builder = NodeBuilder::with_observer(Arc::clone(&observer));
    let acceptor = builder.acceptor(1).await.unwrap();

    let b = Ballot::new(5, 1);

    // Acceptor receives Prepare for decree 0
    let resp0 = acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        })
        .await;
    assert!(matches!(resp0, Message::Promise { decree_num: 0, .. }));

    // Acceptor receives Prepare for decree 1
    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 1,
            ballot: b,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { decree_num: 1, .. }));

    // Acceptor receives Accept for decree 0
    let resp2 = acceptor
        .handle_message(Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp2, Message::Accepted { decree_num: 0, .. }));

    // Acceptor receives Accept for decree 1 with different value
    let resp3 = acceptor
        .handle_message(Message::Accept {
            from: 1,
            decree_num: 1,
            ballot: b,
            value: PaxosCommand::GET {
                key: "key1".to_string(),
            },
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp3, Message::Accepted { decree_num: 1, .. }));

    // Both decrees should be independent
    observer.wait_for_events().await;
    let accepted_events = observer.accepted().await.len();
    assert_eq!(accepted_events, 2);
}

#[tokio::test]
async fn learner_learns_multiple_decrees() {
    use test_helpers::cleanup_persisted_state;
    cleanup_persisted_state();

    let observer = RecordingObserver::new().arc();
    let builder = NodeBuilder::with_observer(Arc::clone(&observer));
    let learner = builder.learner(1, 1);
    let ledger = paxos::node::ledger::Ledger::init(1, 1).await.unwrap();

    let b = Ballot::new(1, 1);

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decre1".to_string(),
    };

    // Manually insert the DecreeNote
    {
        let decree_notes_arc = builder.decree_notes();
        let mut decree_notes = decree_notes_arc.lock().await;
        decree_notes.state.insert(0, paxos::node::decree_notes::DecreeNote { last_tried: b });
        decree_notes.state.insert(1, paxos::node::decree_notes::DecreeNote { last_tried: b });
    }

    // Learner receives Accepted for decree 0
    learner
        .handle_message(
            Message::Accepted {
                from: 2,
                decree_num: 0,
                ballot: b,
                value: cmd0.clone(),
            },
            &ledger,
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
            &ledger,
        )
        .await;

    // Both should generate Learn events
    observer.wait_for_events().await;
    let learns = observer.get_learned_values().await.len();
    assert_eq!(learns, 2);
}

#[tokio::test]
async fn multiple_decrees_with_different_ballots() {
    let builder: NodeBuilder = NodeBuilder::new();

    let acceptor = builder.acceptor(1).await.unwrap();

    // Decree 0: ballot (1,1)
    let b1_1 = Ballot::new(1, 1);
    acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b1_1,
        })
        .await;

    // Decree 1: ballot (1,2) - higher ballot number
    let b1_2 = Ballot::new(1, 2);
    acceptor
        .handle_message(Message::Prepare {
            from: 2,
            decree_num: 1,
            ballot: b1_2,
        })
        .await;

    // Decree 0 should be accepted at its ballot
    let resp = acceptor
        .handle_message(Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: Ballot::new(1, 1),
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp, Message::Accepted { .. }));

    // But decree 1 is independent - it can accept at its ballot
    let resp2: Message = acceptor
        .handle_message(Message::Accept {
            from: 2,
            decree_num: 1,
            ballot: b1_2,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp2, Message::Accepted { .. }));
}

#[tokio::test]
async fn proposer_handles_promises_for_different_decrees() {
    let builder: NodeBuilder = NodeBuilder::new();

    let proposer = builder.proposer(1, 1).await.unwrap(); // Quorum size 1

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decre1".to_string(),
    };

    // Propose both decrees
    let prepare0 = proposer.propose(0, cmd0.clone()).await;
    let prepare1 = proposer.propose(1, cmd1.clone()).await;

    // Each proposal increments ballot separately
    let b0 = if let Message::Prepare { ballot, .. } = prepare0 {
        ballot
    } else {
        panic!()
    };
    let b1 = if let Message::Prepare { ballot, .. } = prepare1 {
        ballot
    } else {
        panic!()
    };

    // Corrected assertion: Separate decrees get independent ballot numbers, so both should start at 1
    assert_eq!(b0.number, 1);
    assert_eq!(b1.number, 1);
    
    // Receive promise for decree 0
    let resp0 = proposer
        .handle_message(Message::Promise {
            from: 2,
            decree_num: 0,
            ballot: b0,
            accepted_ballot: Ballot::new(0, 0),
            accepted_value: PaxosCommand::NOOP,
        })
        .await;
    assert!(
        matches!(resp0, Message::Accept { decree_num: 0, value, .. } if value == cmd0)
    );

    // Receive promise for decree 1
    let resp1 = proposer
        .handle_message(Message::Promise {
            from: 2,
            decree_num: 1,
            ballot: b1,
            accepted_ballot: Ballot::new(0, 0),
            accepted_value: PaxosCommand::NOOP,
        })
        .await;
    assert!(
        matches!(resp1, Message::Accept { decree_num: 1, value, .. } if value == cmd1)
    );
}

#[tokio::test]
async fn concurrent_decrees_dont_interfere() {
    let builder: NodeBuilder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b = Ballot::new(5, 1);

    // Promise for decree 0
    acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b,
        })
        .await;

    // Promise for decree 1 with SAME ballot
    acceptor
        .handle_message(Message::Prepare {
            from: 2,
            decree_num: 1,
            ballot: b,
        })
        .await;

    // Accept for decree 0 should succeed
    let resp = acceptor
        .handle_message(Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp, Message::Accepted { ballot, .. } if ballot.number == 5));

    // Accept for decree 1 should also succeed, as decrees are independent
    let resp2 = acceptor
        .handle_message(Message::Accept {
            from: 1,
            decree_num: 1,
            ballot: b,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp2, Message::Accepted { ballot, .. } if ballot.number == 5));
}

#[tokio::test]
async fn sequential_decrees_same_proposer() {
    let builder: NodeBuilder = NodeBuilder::new();

    let proposer = builder.proposer(1, 2).await.unwrap();

    let _acceptor1 = builder.acceptor(1).await.unwrap();
    let _acceptor2 = builder.acceptor(2).await.unwrap();

    let cmd0 = PaxosCommand::PUT {
        key: "key0".to_string(),
        version: 1,
    };
    let cmd1 = PaxosCommand::PUT {
        key: "key1".to_string(),
        version: 2,
    };

    // Propose decree 0
    let prepare0 = proposer.propose(0, cmd0.clone()).await;
    assert!(matches!(prepare0, Message::Prepare { decree_num: 0, .. }));

    // Propose decree 1
    let prepare1 = proposer.propose(1, cmd1.clone()).await;
    assert!(matches!(prepare1, Message::Prepare { decree_num: 1, .. }));

    // Each proposal increments ballot separately
    if let Message::Prepare { ballot: b0, .. } = prepare0 {
        if let Message::Prepare { ballot: b1, .. } = prepare1 {
            assert_eq!(b0.number, 1);
            assert_eq!(b1.number, 1); // Decrees are independent, so ballot numbers don't interact
            assert_eq!(b0.node_id, b1.node_id);
        }
    }
}

#[tokio::test]
async fn learner_ledger_tracks_concurrent_decrees() {
    cleanup_persisted_state();
    let ledger = paxos::node::ledger::Ledger::init(1, 1).await.unwrap();

    // Insert decrees for different slots
    ledger
        .insert(
            0,
            PaxosCommand::GET {
                key: "d0".to_string(),
            },
        )
        .await;

    ledger
        .insert(
            1,
            PaxosCommand::GET {
                key: "d1".to_string(),
            },
        )
        .await;

    ledger
        .insert(
            2,
            PaxosCommand::GET {
                key: "d2".to_string(),
            },
        )
        .await;

    // After inserting decrees for slots 0, 1, 2, the length is 3, so next() returns 3
    let next = ledger.next().await;
    assert_eq!(next, 3);
}

#[tokio::test]
async fn mixed_single_and_multi_decree_flow() {
    use test_helpers::cleanup_persisted_state;
    cleanup_persisted_state();
    
    let observer = RecordingObserver::new().arc();
    let builder = NodeBuilder::with_observer(Arc::clone(&observer));

    let proposer = builder.proposer(1, 1).await.unwrap();
    let acceptor = builder.acceptor(1).await.unwrap();
    let learner = builder.learner(1, 1);
    let ledger = paxos::node::ledger::Ledger::init(1, 1).await.unwrap();

    let cmd0 = PaxosCommand::GET {
        key: "cmd0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "cmd1".to_string(),
    };

    // Phase 1: Decree 0
    let prepare0 = proposer.propose(0, cmd0.clone()).await;
    // Extract the ballot from the Prepare message
    let ballot0 = if let paxos::message::Message::Prepare { ballot, .. } = &prepare0 {
        *ballot
    } else {
        panic!("Expected Prepare message");
    };
    
    let promise0 = acceptor.handle_message(prepare0).await;
    let accept0 = proposer.handle_message(promise0).await;
    let accepted0 = acceptor.handle_message(accept0).await;

    // Set up promised ballot for decree 0 in learner
    {
        let decree_notes_arc = builder.decree_notes();
        let mut decree_notes = decree_notes_arc.lock().await;
        decree_notes.state.insert(0, paxos::node::decree_notes::DecreeNote {
            last_tried: ballot0,
        });
    }

    // Learner handles accepted message
    learner.handle_message(accepted0.clone(), &ledger).await;

    // Phase 2: Decree 1 while decree 0 is being processed
    let prepare1 = proposer.propose(1, cmd1.clone()).await;
    // Extract the ballot from the Prepare message
    let ballot1 = if let paxos::message::Message::Prepare { ballot, .. } = &prepare1 {
        *ballot
    } else {
        panic!("Expected Prepare message");
    };
    
    let promise1 = acceptor.handle_message(prepare1).await;
    let accept1 = proposer.handle_message(promise1).await;
    let accepted1 = acceptor.handle_message(accept1).await;

    // Set up promised ballot for decree 1 in learner
    {
        let decree_notes_arc = builder.decree_notes();
        let mut decree_notes = decree_notes_arc.lock().await;
        decree_notes.state.insert(1, paxos::node::decree_notes::DecreeNote {
            last_tried: ballot1,
        });
    }

    // Learner handles accepted message
    learner.handle_message(accepted1.clone(), &ledger).await;

    // Both should be learned - wait for async event tasks
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    let learns = observer.get_learned_values().await.len();
    assert_eq!(learns, 2, "Expected 2 learned values, got {}", learns);
}
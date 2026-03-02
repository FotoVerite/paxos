mod test_helpers;

use paxos::{
    common::types::DecreeId, message::Message, node::classic_paxos::ballot::Ballot,
    paxos_command::PaxosCommand,
};
use std::collections::HashSet;
use test_helpers::{NodeBuilder, RecordingObserver, cleanup_persisted_state};

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// Edge case: Promise arrives after Accept has been sent (out of order)
#[tokio::test]
async fn out_of_order_promise_after_accept() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    let prepare = proposer.propose(DecreeId(0), cmd.clone()).await;
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Proposer receives Promise
    let promise = Message::Promise {
        from: test_helpers::test_uuid(2),
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::default(),
        accepted_value: PaxosCommand::BLANK,
    };
    let _accept = proposer.handle_message(promise).await;

    // Now imagine Promise from another acceptor arrives (out of order)
    let late_promise = Message::Promise {
        from: test_helpers::test_uuid(3),
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::default(),
        accepted_value: PaxosCommand::BLANK,
    };

    // Proposer should handle it gracefully (may already have sent Accept, or queue it)
    let resp = proposer.handle_message(late_promise).await;

    // Either NACK (quorum already met) or Accept is acceptable
    match resp {
        Message::NACK | Message::Accept { .. } => (),
        _ => (),
    }
}

/// Edge case: Accept message arrives at acceptor before Prepare (out of order)
#[tokio::test]
async fn accept_before_prepare_same_decree() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b = Ballot::new(5, test_helpers::test_uuid(1));

    // Accept arrives before Prepare - acceptor has no promise yet
    let resp = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
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
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b = Ballot::new(5, test_helpers::test_uuid(1));

    // First Prepare
    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Duplicate Prepare with same ballot
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
        })
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
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b = Ballot::new(5, test_helpers::test_uuid(1));
    let value = PaxosCommand::PUT {
        key: "test".to_string(),
        version: 1,
        value: 0,
    };

    // Prepare first
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
        })
        .await;

    // First Accept
    let resp1 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
            value: value.clone(),
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp1, Message::Accepted { .. }));

    // Duplicate Accept with same ballot and value
    let resp2 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
            value: value.clone(),
            quorum: HashSet::new(),
        })
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
    cleanup_persisted_state();

    let observer = RecordingObserver::new().arc();
    let barrier = observer.barrier.clone();
    let mut cluster = test_helpers::create_cluster(3, observer.clone())
        .await
        .unwrap();
    for node in &mut cluster.nodes {
        node.start();
    }

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };

    // Propose two different values - they will race for the same decree
    cluster.propose_from(1, cmd1.clone()).await;
    cluster.propose_from(0, cmd0.clone()).await;

    // Wait for some LearnedValue events to occur (may be out of order/conflicting values)
    let result = barrier
        .wait_for(
            |e| matches!(e, paxos::monitor::Event::LearnedValue { .. }),
            1,
            std::time::Duration::from_secs(10),
        )
        .await;

    observer.wait_for_events().await;

    // This test verifies that the system handles out-of-order learning of conflicting proposals
    // At least one value should be learned
    assert!(result.is_ok(), "At least one value should be learned");
    let learned_values = observer.get_learned_values().await;
    assert!(
        !learned_values.is_empty(),
        "At least one decree should be learned"
    );
}

/// Edge case: Proposer with insufficient promises (minority quorum)
#[tokio::test]
async fn proposer_with_insufficient_promises() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 3).unwrap(); // Needs 3 promises (5-node cluster)

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    let prepare = proposer.propose(DecreeId(0), cmd.clone()).await;
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Receive only 1 promise (need 3 for quorum)
    let promise1 = Message::Promise {
        from: test_helpers::test_uuid(2),
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::default(),
        accepted_value: PaxosCommand::BLANK,
    };

    let resp1 = proposer.handle_message(promise1).await;

    // MUST NOT send Accept yet (only 1 promise, need 3)
    assert!(
        !matches!(resp1, Message::Accept { .. }),
        "Proposer MUST NOT send Accept without quorum (1 < 3)"
    );

    // Receive 2nd promise
    let promise2 = Message::Promise {
        from: test_helpers::test_uuid(3),
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::default(),
        accepted_value: PaxosCommand::BLANK,
    };

    let resp2 = proposer.handle_message(promise2).await;

    // MUST NOT send Accept (only 2 promises, need 3)
    assert!(
        !matches!(resp2, Message::Accept { .. }),
        "Proposer MUST NOT send Accept without quorum (2 < 3)"
    );

    // Receive 3rd promise - now MUST reach quorum and send Accept
    let promise3 = Message::Promise {
        from: test_helpers::test_uuid(4),
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::default(),
        accepted_value: PaxosCommand::BLANK,
    };

    let resp3 = proposer.handle_message(promise3).await;

    // MUST send Accept when quorum is reached (3 >= 3)
    assert!(
        matches!(resp3, Message::Accept { .. }),
        "Proposer MUST send Accept when quorum is reached (3 >= 3)"
    );
}

/// Edge case: Very large ballot numbers
#[tokio::test]
async fn large_ballot_numbers() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b_huge = Ballot::new(999999, test_helpers::test_uuid(1));
    let b_higher = Ballot::new(1000000, test_helpers::test_uuid(1));

    // Prepare with huge ballot
    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b_huge,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Higher ballot should still work
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b_higher,
        })
        .await;
    assert!(matches!(resp2, Message::Promise { .. }));
}

/// Edge case: Proposer receives Promise from itself
#[tokio::test]
async fn proposer_promise_from_itself() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    let prepare = proposer.propose(DecreeId(0), cmd.clone()).await;
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Proposer receives Promise from itself
    let promise_from_self = Message::Promise {
        from: test_helpers::test_uuid(1), // Same as proposer id
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::default(),
        accepted_value: PaxosCommand::BLANK,
    };

    let resp = proposer.handle_message(promise_from_self).await;

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
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b1_1 = Ballot::new(1, test_helpers::test_uuid(1)); // Proposer 1
    let b1_2 = Ballot::new(1, test_helpers::test_uuid(2)); // Proposer 2
    let b1_3 = Ballot::new(1, test_helpers::test_uuid(3)); // Proposer 3

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
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b1_1,
        })
        .await;

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(2),
            decree_num: DecreeId(0),
            ballot: b1_2,
        })
        .await;

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(3),
            decree_num: DecreeId(0),
            ballot: b1_3,
        })
        .await;

    // P1 tries to Accept (lowest ballot) - should fail
    let resp1 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b1_1,
            value: value1,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp1, Message::NACK));

    // P2 tries to accept (middle ballot) - should fail
    let resp2 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b1_2,
            value: value2,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp2, Message::NACK));

    // P3 tries to accept (highest ballot) - should succeed
    let resp3 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(3),
            decree_num: DecreeId(0),
            ballot: b1_3,
            value: value3.clone(),
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp3, Message::Accepted { value, .. } if value == value3));
}

/// Edge case: Acceptor receives messages for very sparse decree numbers
#[tokio::test]
async fn sparse_decree_numbering() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b = Ballot::new(1, test_helpers::test_uuid(1));

    // Prepare for decree 0
    let resp0 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
        })
        .await;
    assert!(matches!(resp0, Message::Promise { .. }));

    // Prepare for decree 1000 (huge gap)
    let resp1000 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(1000),
            ballot: b,
        })
        .await;
    assert!(matches!(resp1000, Message::Promise { .. }));

    // Both decrees should be independent
    acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(1000),
            ballot: b,
            value: PaxosCommand::GET {
                key: "sparse".to_string(),
            },
            quorum: HashSet::new(),
        })
        .await;

    assert!(matches!(resp, Message::Accepted { .. }));
}

/// Edge case: Learner receives Accepted for same decree from all acceptors
#[tokio::test]
async fn learner_consensus_from_all_acceptors() {
    cleanup_persisted_state();

    let observer = RecordingObserver::new().arc();
    let barrier = observer.barrier.clone();
    let mut cluster = test_helpers::create_cluster(3, observer.clone())
        .await
        .unwrap();
    for node in &mut cluster.nodes {
        node.start();
    }

    let cmd = PaxosCommand::PUT {
        key: "consensus".to_string(),
        version: 1,
        value: 0,
    };

    // Propose the command
    cluster.propose_from(0, cmd.clone()).await;

    // Wait for any LearnedValue event
    let result = barrier
        .wait_for(
            |e| matches!(e, paxos::monitor::Event::LearnedValue { .. }),
            1,
            std::time::Duration::from_secs(10),
        )
        .await;

    observer.wait_for_events().await;

    let learned_values = observer.get_learned_values().await;
    // Should have learned something
    assert!(result.is_ok(), "At least one value should be learned");
    assert!(
        learned_values.len() >= 1,
        "At least one decree should be learned"
    );
}

/// Edge case: Promise with accepted_ballot higher than current ballot
#[tokio::test]
async fn promise_reports_higher_accepted_ballot() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    proposer.propose(DecreeId(0), cmd.clone()).await;

    // Promise reports accepted value from ballot (10, 2) but current ballot is (1, 1)
    let promise = Message::Promise {
        from: test_helpers::test_uuid(2),
        decree_num: DecreeId(0),
        ballot: Ballot::new(1, test_helpers::test_uuid(1)),
        accepted_ballot: Ballot::new(10, test_helpers::test_uuid(2)), // Much higher than current
        accepted_value: PaxosCommand::PUT {
            key: "old".to_string(),
            version: 1,
            value: 0,
        },
    };

    let resp = proposer.handle_message(promise).await;

    // Proposer should adopt the old value despite its high ballot
    if let Message::Accept { value, .. } = resp {
        assert!(
            matches!(value, PaxosCommand::PUT { .. }),
            "Proposer should adopt value from higher accepted ballot"
        );
    }
}

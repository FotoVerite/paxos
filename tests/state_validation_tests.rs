mod support;
mod test_helpers {
    pub use super::support::*;
}

use paxos::{
    common::ballot::Ballot, common::types::DecreeId,
    node::classic_paxos::message::ClassicMessage as Message, paxos_command::PaxosCommand,
};
use std::collections::HashSet;
use support::NodeBuilder;

// ============================================================================
// STATE VALIDATION TESTS
// ============================================================================

/// Invariant: Acceptor never promises a ballot and then accepts at a lower ballot
#[tokio::test]
async fn acceptor_ballot_monotonicity_within_decree() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b3 = Ballot::new(3, test_helpers::test_uuid(1));

    // Promise to ballot (5, 1)
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    // Try to accept at lower ballot (3, 1) - should be rejected
    let resp = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b3,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;

    // INVARIANT: Must reject (ballot went backwards)
    assert!(
        resp.is_none(),
        "Acceptor violated monotonicity: accepted lower ballot after higher promise"
    );
}

/// Invariant: Acceptor never promises same decree at same or lower ballot twice
#[tokio::test]
async fn acceptor_no_promise_downgrade_same_decree() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));

    // First promise to (5, 1)
    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;
    assert!(matches!(resp1, Some(Message::Promise { .. })));
    // Second promise attempt with same ballot - should be rejected
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    // INVARIANT: Must reject (same ballot requested twice)
    assert!(
        resp2.is_none(),
        "Acceptor violated state invariant: accepted same ballot twice for same decree"
    );
}

/// Invariant: Acceptor maintains monotonic ballot progression per decree
#[tokio::test]
async fn acceptor_monotonic_promise_progression() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b3 = Ballot::new(3, test_helpers::test_uuid(1));
    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b7 = Ballot::new(7, test_helpers::test_uuid(1));
    let b6 = Ballot::new(6, test_helpers::test_uuid(1)); // Would be out of order after 7

    // Promise sequence: 3 -> 5 -> 7 (valid progression)
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b3,
        })
        .await;

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b7,
        })
        .await;

    // Try to promise at 6 after promising at 7 - should fail
    let resp = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b6,
        })
        .await;

    // INVARIANT: Must reject (ballot went backwards)
    assert!(
        resp.is_none(),
        "Acceptor violated monotonicity: accepted lower ballot after higher"
    );
}

/// Invariant: Acceptor state is independent per decree
#[tokio::test]
async fn acceptor_decree_independence_for_ballots() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5_1 = Ballot::new(5, test_helpers::test_uuid(1));
    let b3_1 = Ballot::new(3, test_helpers::test_uuid(1));

    // Decree 0: Promise to (5, 1)
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5_1,
        })
        .await;

    // Decree 1: Should be able to promise to (3, 1) independently
    let resp = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(1),
            ballot: b3_1,
        })
        .await;

    // INVARIANT: Must succeed - decrees are independent
    assert!(
        matches!(resp, Some(Message::Promise { .. })),
        "Acceptor violated decree independence: ballot in decree 0 affected decree 1"
    );
}

/// Invariant: Proposer doesn't send Accept with lower ballot after higher
#[tokio::test]
async fn proposer_ballot_monotonicity_per_decree() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // First proposal creates ballot (1, 1) for decree 0
    let msg1 = proposer.propose(DecreeId(0), cmd.clone()).await;
    let b1 = match msg1 {
        Message::Prepare { ballot, .. } => ballot,
        other => panic!("Expected Prepare, got {:?}", other),
    };

    // Second proposal for same decree
    let msg2 = proposer.propose(DecreeId(0), cmd.clone()).await;

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
        _other => {
            // Proposer may return something else (cached, etc) - that's ok
            // The key invariant is: if it returns a Prepare for same decree, ballot must be >= b1
        }
    }
}

/// Note: Current implementation increments ballot for each propose() call.
/// Each proposal gets its own ballot number, not shared across decrees.
#[tokio::test]
async fn proposer_same_ballot_different_decrees() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd0 = PaxosCommand::GET {
        key: "decree0".to_string(),
    };
    let cmd1 = PaxosCommand::GET {
        key: "decree1".to_string(),
    };

    // Each proposal increments ballot: decree 0 gets (1,1), decree 1 gets (1,1) if from same proposer
    let msg0 = proposer.propose(DecreeId(0), cmd0).await;
    let msg1 = proposer.propose(DecreeId(1), cmd1).await;

    if let (Message::Prepare { ballot: b0, .. }, Message::Prepare { ballot: b1, .. }) = (msg0, msg1)
    {
        // Current implementation: each proposal gets its own initial ballot number
        assert_eq!(b0.number, 1);
        assert_eq!(b1.number, 1);
        assert_eq!(b0.node_id, b1.node_id);
    } else {
        panic!("Expected Prepare messages");
    }
}

/// Invariant: Acceptor never returns accepted_value for rejected Prepare
#[tokio::test]
async fn acceptor_never_leaks_value_on_nack() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b3 = Ballot::new(3, test_helpers::test_uuid(1));

    // Accept a value at ballot (5, 1)
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
            value: PaxosCommand::PUT {
                key: "secret".to_string(),
                version: 1,
                value: 0,
            },
            quorum: HashSet::new(),
        })
        .await;

    // Try to prepare with lower ballot - gets NACK
    let resp = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b3,
        })
        .await;

    // INVARIANT: NACK should never expose accepted values
    assert!(
        resp.is_none(),
        "Acceptor violated invariant: should reject lower prepare, not leak state"
    );
}

/// Invariant: Proposer adopts previously accepted value with highest ballot
#[tokio::test]
async fn proposer_value_adoption_invariant() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let new_value = PaxosCommand::GET {
        key: "new".to_string(),
    };
    let old_value = PaxosCommand::PUT {
        key: "old".to_string(),
        version: 1,
        value: 0,
    };
    let older_value = PaxosCommand::PUT {
        key: "older".to_string(),
        version: 0,
        value: 0,
    };

    // Propose initial value
    proposer.propose(DecreeId(0), new_value).await;

    // Receive promise with old accepted value from ballot (5, 2)
    let promise1 = Message::Promise {
        from: test_helpers::test_uuid(2),
        decree_num: DecreeId(0),
        ballot: Ballot::new(1, test_helpers::test_uuid(1)),
        accepted_ballot: Ballot::new(5, test_helpers::test_uuid(2)),
        accepted_value: old_value.clone(),
    };

    let msg1 = proposer.handle_message(promise1).await;

    // Verify proposer adopted the old value
    if let Some(Message::Accept { value, .. }) = msg1 {
        assert_eq!(
            value, old_value,
            "Proposer failed to adopt accepted value from higher ballot"
        );
    } else {
        panic!("Expected Accept message");
    }

    // Now try to override with even older value - should keep old_value
    let promise2 = Message::Promise {
        from: test_helpers::test_uuid(3),
        decree_num: DecreeId(0),
        ballot: Ballot::new(1, test_helpers::test_uuid(1)),
        accepted_ballot: Ballot::new(3, test_helpers::test_uuid(1)), // Lower than (5, 2)
        accepted_value: older_value,
    };

    let msg2 = proposer.handle_message(promise2).await;

    // INVARIANT: Must keep the value from highest accepted_ballot
    if let Some(Message::Accept { value, .. }) = msg2 {
        assert_eq!(
            value, old_value,
            "Proposer violated invariant: should keep value from highest accepted_ballot"
        );
    } else {
        panic!("Expected Accept message");
    }
}

/// Invariant: Accept message must match the ballot in Prepare -> Promise flow
#[tokio::test]
async fn proposer_accept_ballot_matches_promise() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // Send prepare
    let prepare = proposer.propose(DecreeId(0), cmd.clone()).await;
    let prepare_ballot = if let Message::Prepare { ballot, .. } = prepare {
        ballot
    } else {
        panic!("Expected Prepare");
    };

    // Receive promise with same ballot
    let promise = Message::Promise {
        from: test_helpers::test_uuid(2),
        decree_num: DecreeId(0),
        ballot: prepare_ballot,
        accepted_ballot: Ballot::new(0, test_helpers::test_uuid(0)),
        accepted_value: PaxosCommand::NOOP,
    };

    let accept = proposer.handle_message(promise).await;

    // INVARIANT: Accept ballot must match Promise ballot (which matched Prepare)
    if let Some(Message::Accept {
        ballot: accept_ballot,
        ..
    }) = accept
    {
        assert_eq!(
            accept_ballot, prepare_ballot,
            "Proposer ballot mismatch: Prepare {:?} but Accept {:?}",
            prepare_ballot, accept_ballot
        );
    } else {
        panic!("Expected Accept message");
    }
}

/// Invariant: Acceptor rejects Accept messages from lower ballot than promised
#[tokio::test]
async fn acceptor_accept_ballot_validation() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b7 = Ballot::new(7, test_helpers::test_uuid(1));
    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b9 = Ballot::new(9, test_helpers::test_uuid(1));

    // Promise to ballot (7, 1)
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b7,
        })
        .await;

    // Try to accept at (5, 1) - too low
    let resp1 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(resp1.is_none());

    // Accept at (7, 1) - exact ballot
    let resp2 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b7,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp2, Some(Message::Accepted { .. })));
    // Accept at (9, 1) - higher than promised, but we need a new promise first
    // This tests that acceptor enforces min_ballot >= promised ballot
    let new_promise = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b9,
        })
        .await;
    assert!(matches!(new_promise, Some(Message::Promise { .. })));
    let resp3 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b9,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;

    // INVARIANT: Accept must be at or above min_ballot
    assert!(
        matches!(resp3, Some(Message::Accepted { .. })),
        "Acceptor violated invariant: rejected valid Accept at promised ballot"
    );
}

/// Invariant: Multiple proposals at different ballots maintain isolation
#[tokio::test]
async fn concurrent_proposals_ballot_isolation() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b1_1 = Ballot::new(1, test_helpers::test_uuid(1)); // Proposer 1
    let b1_2 = Ballot::new(1, test_helpers::test_uuid(2)); // Proposer 2
    let b1_3 = Ballot::new(1, test_helpers::test_uuid(3)); // Proposer 3

    // All three proposers promise with same round
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

    // Proposer 1 and 2 try to accept - but 3 has highest ballot
    let accept1 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b1_1,
            value: PaxosCommand::GET {
                key: "p1".to_string(),
            },
            quorum: HashSet::new(),
        })
        .await;

    let accept2 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(2),
            decree_num: DecreeId(0),
            ballot: b1_2,
            value: PaxosCommand::GET {
                key: "p2".to_string(),
            },
            quorum: HashSet::new(),
        })
        .await;

    // INVARIANT: Both should be rejected (lower than min_ballot b1_3)
    assert!(
        accept1.is_none(),
        "Acceptor accepted value from lower ballot when higher exists"
    );
    assert!(
        accept2.is_none(),
        "Acceptor accepted value from lower ballot when highest exists"
    );

    // Only proposer 3 succeeds
    let accept3 = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(3),
            decree_num: DecreeId(0),
            ballot: b1_3,
            value: PaxosCommand::GET {
                key: "p3".to_string(),
            },
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(
        accept3,
        Some(Message::Accepted { value, .. })
            if value == PaxosCommand::GET {
                key: "p3".to_string(),
            }
    ));
}

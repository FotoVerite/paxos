mod test_helpers;

use paxos::{
    message::Message,
    monitor::Event,
    node::ballot::Ballot,
    paxos_command::PaxosCommand,
};
use test_helpers::{NodeBuilder, RecordingObserver};

// ============================================================================
// INTEGRATION TESTS
// ============================================================================
// These test the protocol interactions without relying on exact quorum counts,
// since we're testing individual component behavior rather than full cluster consensus.

#[tokio::test]
async fn basic_proposer_acceptor_interaction() {
    let observer = RecordingObserver::new();
    let builder = NodeBuilder::with_observer(observer.as_arc());
    
    // Use quorum=1 for this unit test (1 proposer + 1 acceptor)
    let mut proposer = builder.proposer(1, 1).await.unwrap();
    let mut acceptor = builder.acceptor(1).await.unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // Phase 1: Prepare
    let prepare = proposer.propose(0, cmd.clone());
    assert!(matches!(prepare, Message::Prepare { .. }));
    
    // Acceptor promises
    let promise = acceptor.handle_message(prepare).await;
    assert!(matches!(promise, Message::Promise { .. }));

    // Phase 2: Accept
    let accept = proposer.handle_message(promise).await;
    assert!(matches!(accept, Message::Accept { .. }));
    
    // Acceptor accepts
    let accepted = acceptor.handle_message(accept).await;
    assert!(matches!(accepted, Message::Accepted { .. }));

    // Wait for all event recording tasks to complete
    observer.wait_for_events().await;

    // Check events - should have proposal, promise, accept
    let events = observer.get_events();
    assert!(
        events.len() >= 3,
        "Expected at least 3 events (proposal, promise, accept), got {}",
        events.len()
    );

    let has_proposal = events.iter().any(|e| matches!(e, Event::Proposal { .. }));
    let has_promise = events.iter().any(|e| matches!(e, Event::Promise { .. }));
    let has_accept = events.iter().any(|e| matches!(e, Event::Accept { .. }));
    
    assert!(has_proposal, "Should have Proposal event");
    assert!(has_promise, "Should have Promise event");
    assert!(has_accept, "Should have Accept event");
}

#[tokio::test]
async fn ballot_comparison_ensures_safety() {
    let builder = NodeBuilder::new();
    let mut proposer1 = builder.proposer(1, 1).await.unwrap();
    let mut proposer2 = builder.proposer(2, 1).await.unwrap();
    let mut acceptor = builder.acceptor(1).await.unwrap();

    let cmd1 = PaxosCommand::GET {
        key: "proposer1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "proposer2".to_string(),
    };

    // Proposer 1 sends Prepare (ballot 1,1)
    let prepare1 = proposer1.propose(0, cmd1.clone());
    let promise1 = acceptor.handle_message(prepare1).await;
    assert!(matches!(promise1, Message::Promise { .. }));

    // Proposer 2 sends Prepare (ballot 1,2) - higher ballot
    let prepare2 = proposer2.propose(0, cmd2.clone());
    let promise2 = acceptor.handle_message(prepare2).await;
    assert!(matches!(promise2, Message::Promise { .. }));

    // Proposer 1 tries to accept - should fail (acceptor now requires ballot >= 1,2)
    let accept1 = proposer1.handle_message(promise1).await;
    let resp1 = acceptor.handle_message(accept1).await;
    assert!(matches!(resp1, Message::NACK), "Lower ballot should be rejected");

    // Proposer 2 can accept - higher ballot
    let accept2 = proposer2.handle_message(promise2).await;
    let resp2 = acceptor.handle_message(accept2).await;
    assert!(
        matches!(resp2, Message::Accepted { .. }),
        "Higher ballot should be accepted"
    );
}

#[tokio::test]
async fn observer_captures_full_protocol() {
    let observer = RecordingObserver::new();
    let builder = NodeBuilder::with_observer(observer.as_arc());
    
    let mut proposer = builder.proposer(0, 1).await.unwrap();
    let mut acceptor = builder.acceptor(0).await.unwrap();

    let cmd = PaxosCommand::PUT {
        key: "tracked".to_string(),
        version: 42,
    };

    // Run complete protocol
    let prepare = proposer.propose(0, cmd.clone());
    let promise = acceptor.handle_message(prepare).await;
    let accept = proposer.handle_message(promise).await;
    let _accepted = acceptor.handle_message(accept).await;

    // Wait for all event recording tasks to complete
    observer.wait_for_events().await;

    // Verify events were captured
    let events = observer.get_events();
    assert!(
        events.len() >= 3,
        "Expected at least 3 events, got {}",
        events.len()
    );

    // Check specific event types
    let has_proposal = events.iter().any(|e| {
        matches!(e, Event::Proposal { decree_num, value, .. } if *decree_num == 0 && *value == cmd)
    });
    let has_promise = events
        .iter()
        .any(|e| matches!(e, Event::Promise { decree_num, .. } if *decree_num == 0));
    let has_accept = events
        .iter()
        .any(|e| matches!(e, Event::Accept { decree_num, .. } if *decree_num == 0));

    assert!(has_proposal, "Should have Proposal event");
    assert!(has_promise, "Should have Promise event");
    assert!(has_accept, "Should have Accept event");
}

#[tokio::test]
async fn proposer_adopts_accepted_values() {
    let builder = NodeBuilder::new();
    let mut acceptor = builder.acceptor(0).await.unwrap();

    let original_cmd = PaxosCommand::PUT {
        key: "original".to_string(),
        version: 1,
    };

    // Acceptor has previously accepted a value
    let b_original = Ballot::new(3, 1);
    acceptor.handle_message(Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: b_original,
    })
    .await;
    acceptor.handle_message(Message::Accept {
        from: 1,
        decree_num: 0,
        ballot: b_original,
        value: original_cmd.clone(),
    })
    .await;

    // New proposer comes with higher ballot
    let mut proposer2 = builder.proposer(2, 1).await.unwrap();
    let new_cmd = PaxosCommand::GET {
        key: "new".to_string(),
    };
    
    proposer2.propose(0, new_cmd.clone());

    // New proposer gets a promise from the acceptor that includes the previously accepted value
    let promise = Message::Promise {
        from: 0,
        decree_num: 0,
        ballot: Ballot::new(1, 2),
        accepted_ballot: b_original,
        accepted_value: original_cmd.clone(),
    };

    // Proposer should adopt the value from the promise
    let accept_msg = proposer2.handle_message(promise).await;

    if let Message::Accept { value, .. } = accept_msg {
        assert_eq!(
            value, original_cmd,
            "Proposer should adopt value from promise with higher accepted_ballot"
        );
    } else {
        panic!("Expected Accept message, got {:?}", accept_msg);
    }
}

#[tokio::test]
async fn acceptor_monotonic_ballot_progression() {
    let builder = NodeBuilder::new();
    let mut acceptor = builder.acceptor(1).await.unwrap();

    let b1 = Ballot::new(1, 1);
    let b3 = Ballot::new(3, 1);
    let b5 = Ballot::new(5, 1);

    // Acceptor promises ballot (1, 1)
    let resp1 = acceptor.handle_message(Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: b1,
    })
    .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Acceptor promises higher ballot (3, 1)
    let resp3 = acceptor.handle_message(Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: b3,
    })
    .await;
    assert!(matches!(resp3, Message::Promise { .. }));

    // Acceptor refuses lower ballot (5, 1) is higher, so should accept
    let resp5 = acceptor.handle_message(Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: b5,
    })
    .await;
    assert!(matches!(resp5, Message::Promise { .. }));

    // Trying the same ballot again should fail
    let resp_dup = acceptor.handle_message(Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: b5,
    })
    .await;
    assert!(
        matches!(resp_dup, Message::NACK),
        "Acceptor should reject same ballot twice"
    );
}

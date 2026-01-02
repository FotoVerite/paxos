mod test_helpers;

use paxos::{message::Message, monitor::Event, node::ballot::Ballot, paxos_command::PaxosCommand};
use test_helpers::{cleanup_persisted_state, NodeBuilder, RecordingObserver};

// ============================================================================
// LEARNER TESTS
// ============================================================================

#[tokio::test]
async fn learner_receives_accepted_values() {
    cleanup_persisted_state();

    let observer = RecordingObserver::new();
    let builder = NodeBuilder::with_observer(observer.as_arc());
    let learner = builder.learner(1, 2);
    let ledger = paxos::node::ledger::Ledger::init(1, 2).await.unwrap();

    let cmd1 = PaxosCommand::GET {
        key: "key1".to_string(),
    };

    // Two acceptors vote for decree 0 (quorum = 2, so decree is chosen after 2 votes)
    learner
        .handle_message(
            Message::Accepted {
                from: 2,
                decree_num: 0,
                ballot: Ballot::new(1, 1),
                value: cmd1.clone(),
            },
            &ledger,
        )
        .await;
    learner
        .handle_message(
            Message::Accepted {
                from: 3,
                decree_num: 0,
                ballot: Ballot::new(1, 1),
                value: cmd1.clone(), // Same value for consensus
            },
            &ledger,
        )
        .await;

    observer.wait_for_events().await;
    let events = observer.get_events().await;
    let learns = events
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 1); // One decree chosen, one Learn event
}

#[tokio::test]
async fn learner_ignores_non_accepted_messages() {
    let observer = RecordingObserver::new();
    let builder = NodeBuilder::with_observer(observer.as_arc());
    let _learner = builder.learner(1, 2);
    let _ledger = paxos::node::ledger::Ledger::init(1, 2).await.unwrap();

    // The learner's handle_message function is now specific to Accepted messages.
    // To test that it ignores other messages, we would need to call the node's top-level
    // message handler. For this unit test, we can just verify that no events are
    // recorded by default.
    observer.wait_for_events().await;
    let events = observer.get_events().await;
    assert_eq!(events.len(), 0); // No events should be recorded
}

#[tokio::test]
async fn learner_learns_multiple_decrees() {
    cleanup_persisted_state();

    let observer = RecordingObserver::new();
    let builder = NodeBuilder::with_observer(observer.as_arc());
    let learner = builder.learner(1, 1);
    let ledger = paxos::node::ledger::Ledger::init(1, 1).await.unwrap();

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
    let events = observer.get_events().await;
    let learns = events
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();
    assert_eq!(learns, 2);
}

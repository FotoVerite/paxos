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
    let mut learner = builder.learner(1);
    let mut ledger = paxos::node::ledger::Ledger::init(1, 2).await.unwrap();

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
            &mut ledger,
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
            &mut ledger,
        )
        .await;

    let events = observer.get_events();
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
    let mut learner = builder.learner(1);
    let mut ledger = paxos::node::ledger::Ledger::init(1, 2).await.unwrap();

    // Try to send a Prepare (learner should ignore it)
    learner
        .handle_message(
            Message::Prepare {
                from: 2,
                decree_num: 0,
                ballot: Ballot::new(1, 1),
            },
            &mut ledger,
        )
        .await;

    let events = observer.get_events();
    assert_eq!(events.len(), 0); // No events should be recorded
}

#[tokio::test]
async fn learner_learns_multiple_decrees() {
    cleanup_persisted_state();
    
    let observer = RecordingObserver::new();
    let builder = NodeBuilder::with_observer(observer.as_arc());
    let mut learner = builder.learner(1);
    let mut ledger = paxos::node::ledger::Ledger::init(1, 1).await.unwrap();

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

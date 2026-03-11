mod test_helpers;

use paxos::{
    common::ballot::Ballot, common::types::DecreeId,
    node::classic_paxos::message::ClassicMessage as Message, paxos_command::PaxosCommand,
};
use std::collections::HashSet;
use test_helpers::NodeBuilder;

// ============================================================================
// ACCEPTOR TESTS
// ============================================================================

#[tokio::test]
async fn acceptor_rejects_lower_ballot_prepare() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b_high = Ballot::new(5, test_helpers::test_uuid(1));
    let b_low = Ballot::new(3, test_helpers::test_uuid(1));

    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b_high,
        })
        .await;
    assert!(matches!(
        resp1,
        Some(Message::Promise { ballot, .. }) if ballot == b_high
    ));
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b_low,
        })
        .await;
    assert!(resp2.is_none());
}

#[tokio::test]
async fn acceptor_accepts_higher_ballot_prepare() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b7 = Ballot::new(7, test_helpers::test_uuid(1));

    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;
    assert!(matches!(
        resp1,
        Some(Message::Promise { ballot, .. }) if ballot == b5
    ));
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b7,
        })
        .await;
    assert!(matches!(
        resp2,
        Some(Message::Promise { ballot, .. }) if ballot == b7
    ));
}

#[tokio::test]
async fn acceptor_rejects_accept_below_min_ballot() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b3 = Ballot::new(3, test_helpers::test_uuid(1));

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b3,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(resp.is_none());
}

#[tokio::test]
async fn acceptor_accepts_accept_at_min_ballot() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(
        resp,
        Some(Message::Accepted { ballot, value, .. })
            if value == PaxosCommand::NOOP && ballot == b5
    ));
}

#[tokio::test]
async fn acceptor_accepts_accept_above_min_ballot() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));
    let b7 = Ballot::new(7, test_helpers::test_uuid(1));

    // Acceptor promises ballot (5, 1)
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    // Now, a prepare for the higher ballot (7,1) must come
    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b7,
        })
        .await;

    // Then accept for ballot (7,1)
    let resp = acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b7,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(
        resp,
        Some(Message::Accepted { ballot, .. }) if ballot == b7
    ));
}

#[tokio::test]
async fn acceptor_returns_previous_accepted_value() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b1 = Ballot::new(1, test_helpers::test_uuid(1));
    let b3 = Ballot::new(3, test_helpers::test_uuid(1));

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b1,
        })
        .await;
    acceptor
        .handle_message(Message::Accept {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b1,
            value: PaxosCommand::PUT {
                key: "original".to_string(),
                version: 1,
                value: 0,
            },
            quorum: HashSet::new(),
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b3,
        })
        .await;

    if let Some(Message::Promise {
        ballot,
        accepted_ballot,
        accepted_value,
        ..
    }) = resp
    {
        assert_eq!(ballot, b3);
        assert_eq!(accepted_ballot, b1);
        assert_eq!(
            accepted_value,
            PaxosCommand::PUT {
                key: "original".to_string(),
                version: 1,
                value: 0,
            }
        );
    } else {
        panic!("Expected Promise with accepted value");
    }
}

#[tokio::test]
async fn acceptor_handles_equal_ballot_prepare() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, test_helpers::test_uuid(1));

    acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Prepare {
            from: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;
    assert!(resp.is_none());
}

mod test_helpers;

use paxos::{
    message::Message,
    node::paxos_state::ballot::Ballot,
    paxos_command::PaxosCommand,
    common::types::{NodeId, DecreeId},
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

    let b_high = Ballot::new(5, NodeId(1));
    let b_low = Ballot::new(3, NodeId(1));

    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b_high,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { ballot, .. } if ballot == b_high));

    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b_low,
        })
        .await;
    assert!(matches!(resp2, Message::NACK));
}

#[tokio::test]
async fn acceptor_accepts_higher_ballot_prepare() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, NodeId(1));
    let b7 = Ballot::new(7, NodeId(1));

    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { ballot, .. } if ballot == b5));

    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b7,
        })
        .await;
    assert!(matches!(resp2, Message::Promise { ballot, .. } if ballot == b7));
}

#[tokio::test]
async fn acceptor_rejects_accept_below_min_ballot() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, NodeId(1));
    let b3 = Ballot::new(3, NodeId(1));

    acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Accept {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b3,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp, Message::NACK));
}

#[tokio::test]
async fn acceptor_accepts_accept_at_min_ballot() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, NodeId(1));

    acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Accept {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(
        matches!(resp, Message::Accepted { ballot, value, .. } if value == PaxosCommand::NOOP && ballot == b5)
    );
}

#[tokio::test]
async fn acceptor_accepts_accept_above_min_ballot() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b5 = Ballot::new(5, NodeId(1));
    let b7 = Ballot::new(7, NodeId(1));

    // Acceptor promises ballot (5, 1)
    acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    // Now, a prepare for the higher ballot (7,1) must come
    acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b7,
        })
        .await;
    
    // Then accept for ballot (7,1)
    let resp = acceptor
        .handle_message(Message::Accept {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b7,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp, Message::Accepted { ballot, .. } if ballot == b7));
}

#[tokio::test]
async fn acceptor_returns_previous_accepted_value() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b1 = Ballot::new(1, NodeId(1));
    let b3 = Ballot::new(3, NodeId(1));

    acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b1,
        })
        .await;
    acceptor
        .handle_message(Message::Accept {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b1,
            value: PaxosCommand::PUT {
                key: "original".to_string(),
                version: 1,
            },
            quorum: HashSet::new(),
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b3,
        })
        .await;

    if let Message::Promise {
        ballot,
        accepted_ballot,
        accepted_value,
        ..
    } = resp
    {
        assert_eq!(ballot, b3);
        assert_eq!(accepted_ballot, b1);
        assert_eq!(
            accepted_value,
            PaxosCommand::PUT {
                key: "original".to_string(),
                version: 1
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

    let b5 = Ballot::new(5, NodeId(1));

    acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;

    let resp = acceptor
        .handle_message(Message::Prepare {
            from: NodeId(1),
            decree_num: DecreeId(0),
            ballot: b5,
        })
        .await;
    assert!(matches!(resp, Message::NACK));
}

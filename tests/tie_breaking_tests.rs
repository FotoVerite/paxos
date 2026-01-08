mod test_helpers;

use paxos::{
    message::Message,
    node::paxos_state::ballot::Ballot,
    paxos_command::PaxosCommand,
};
use test_helpers::NodeBuilder;
use std::collections::HashSet;

// ============================================================================
// TIE-BREAKING TESTS
// ============================================================================

#[tokio::test]
async fn tie_breaking_same_round_higher_node_id_wins() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b_node1 = Ballot::new(5, 1); // ballot (5, 1)
    let b_node2 = Ballot::new(5, 2); // ballot (5, 2) - higher node_id

    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_node1,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Node 2 has same round but higher node_id - should be accepted
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: 2,
            decree_num: 0,
            ballot: b_node2,
        })
        .await;
    assert!(matches!(resp2, Message::Promise { .. }));
}

#[tokio::test]
async fn tie_breaking_lower_node_id_rejected_same_round() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b_node2 = Ballot::new(5, 2);
    let b_node1 = Ballot::new(5, 1); // Lower node_id

    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: 2,
            decree_num: 0,
            ballot: b_node2,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Node 1 has same round but lower node_id - should be rejected
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_node1,
        })
        .await;
    assert!(matches!(resp2, Message::NACK));
}

#[tokio::test]
async fn ballot_ordering_complete_comparisons() {
    // Test all comparison combinations
    let b1_1 = Ballot::new(1, 1);
    let b1_2 = Ballot::new(1, 2);
    let b2_1 = Ballot::new(2, 1);
    let b2_2 = Ballot::new(2, 2);

    // Lower round always loses
    assert!(b1_1 < b2_1);
    assert!(b1_2 < b2_1);
    assert!(b1_2 < b2_2);

    // Same round: lower node_id loses
    assert!(b1_1 < b1_2);
    assert!(b2_1 < b2_2);

    // Equality
    assert!(b1_1 == b1_1);
    assert!(b2_2 == b2_2);
}

#[tokio::test]
async fn proposer_ballot_ordering_from_proposer_perspective() {
    use test_helpers::cleanup_persisted_state;
    cleanup_persisted_state();
    
    let builder1 = NodeBuilder::new();
    let proposer = builder1.proposer(1, 1).unwrap();

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    let msg = proposer.propose(0, cmd.clone()).await;

    if let Message::Prepare { ballot, .. } = msg {
        // Proposer 1 should create ballot (1, 1)
        assert_eq!(ballot.number, 1);
        assert_eq!(ballot.node_id, 1);
    } else {
        panic!("Expected Prepare message");
    }

    // Now proposer 2 with separate builder (separate state)
    let builder2 = NodeBuilder::new();
    let proposer2 = builder2.proposer(2, 1).unwrap();
    let msg2 = proposer2.propose(0, cmd).await;

    if let Message::Prepare { ballot: b2, .. } = msg2 {
        // Proposer 2 should create ballot (1, 2)
        assert_eq!(b2.number, 1);
        assert_eq!(b2.node_id, 2);
        // Ballot from proposer 2 should be higher
        if let Message::Prepare { ballot: b1, .. } = msg {
            assert!(b1 < b2);
        }
    } else {
        panic!("Expected Prepare message");
    }
}

#[tokio::test]
async fn acceptor_rejects_lower_node_id_when_already_promised_to_higher() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b_higher_node = Ballot::new(5, 2);
    let b_lower_node = Ballot::new(5, 1);

    // Promise to higher node_id first
    let resp1 = acceptor
        .handle_message(Message::Prepare {
            from: 2,
            decree_num: 0,
            ballot: b_higher_node,
        })
        .await;
    assert!(matches!(resp1, Message::Promise { .. }));

    // Try lower node_id - should be rejected
    let resp2 = acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_lower_node,
        })
        .await;
    assert!(matches!(resp2, Message::NACK));
}

#[tokio::test]
async fn tie_breaking_affects_accept_phase() {
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    let b_node1 = Ballot::new(5, 1);
    let b_node2 = Ballot::new(5, 2); // Higher due to node_id

    // Promise to node 1
    acceptor
        .handle_message(Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_node1,
        })
        .await;

    // Promise to node 2 with higher ballot
    acceptor
        .handle_message(Message::Prepare {
            from: 2,
            decree_num: 0,
            ballot: b_node2,
        })
        .await;

    // Node 1 tries to Accept at its ballot (5,1) - should fail (too low)
    let resp = acceptor
        .handle_message(Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b_node1,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp, Message::NACK));

    // Node 2 tries to Accept at its ballot (5,2) - should succeed
    let resp2 = acceptor
        .handle_message(Message::Accept {
            from: 2,
            decree_num: 0,
            ballot: b_node2,
            value: PaxosCommand::NOOP,
            quorum: HashSet::new(),
        })
        .await;
    assert!(matches!(resp2, Message::Accepted { .. }));
}

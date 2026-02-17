mod test_helpers;
use paxos::{
    common::types::DecreeId, message::Message, node::classic_paxos::ballot::Ballot,
    paxos_command::PaxosCommand,
};
use std::collections::HashSet;
use test_helpers::{NodeBuilder, cleanup_persisted_state};

#[tokio::test]
async fn test_basic_paxos_flow() {
    cleanup_persisted_state();
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();
    let proposer = builder.proposer(1, 1).unwrap();

    let value = PaxosCommand::GET {
        key: "test_key".to_string(),
    };
    let msg = proposer.propose(DecreeId(0), value.clone()).await;

    let response = acceptor.handle_message(msg).await;

    assert!(
        matches!(response, Message::Promise { ballot, .. } if ballot.number == 1),
        "Should have received a promise for ballot 1"
    );

    let accept_request = proposer.handle_message(response).await;

    assert!(
        matches!(accept_request, Message::Accept { ballot, value: v, quorum, .. } if ballot.number == 1 && v == value && quorum == HashSet::from([test_helpers::test_uuid(1)])),
        "Proposer should have sent Accept with correct ballot and value"
    );
}

#[tokio::test]
async fn test_acceptor_rejects_lower_ballot() {
    cleanup_persisted_state();
    let builder = NodeBuilder::new();
    let acceptor = builder.acceptor(1).await.unwrap();

    // First prepare with ballot 5
    let msg1 = Message::Prepare {
        from: test_helpers::test_uuid(1),
        decree_num: DecreeId(0),
        ballot: Ballot::new(5, test_helpers::test_uuid(1)),
    };
    let resp1 = acceptor.handle_message(msg1).await;
    assert!(matches!(resp1, Message::Promise { ballot, .. } if ballot.number == 5));

    // Second prepare with lower ballot 3 should be rejected
    let msg2 = Message::Prepare {
        from: test_helpers::test_uuid(1),
        decree_num: DecreeId(0),
        ballot: Ballot::new(3, test_helpers::test_uuid(1)),
    };
    let resp2 = acceptor.handle_message(msg2).await;
    assert!(matches!(resp2, Message::NACK));
}

#[tokio::test]
async fn test_proposer_adopts_previous_value() {
    cleanup_persisted_state();
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let cmd1 = PaxosCommand::GET {
        key: "key1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "key2".to_string(),
    };

    // Propose first value
    proposer.propose(DecreeId(0), cmd1.clone()).await;

    // Receive promise that reports a previously accepted value
    let promise = Message::Promise {
        from: test_helpers::test_uuid(2),
        decree_num: DecreeId(0),
        ballot: Ballot::new(1, test_helpers::test_uuid(1)),
        accepted_ballot: Ballot::new(5, test_helpers::test_uuid(1)),
        accepted_value: cmd2.clone(),
    };

    let accept_msg = proposer.handle_message(promise).await;

    // Should adopt the previous value
    assert!(
        matches!(accept_msg, Message::Accept { value, quorum, .. } if value == cmd2 && quorum == HashSet::from([test_helpers::test_uuid(2)]))
    );
}

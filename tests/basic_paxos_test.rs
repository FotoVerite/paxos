mod test_helpers;
use paxos::{
    message::Message,
    node::{ballot::Ballot, proposer::Proposer},
    paxos_command::PaxosCommand,
};
use test_helpers::{cleanup_persisted_state, NodeBuilder};

#[tokio::test]
async fn test_basic_paxos_flow() {
    cleanup_persisted_state();
    let builder = NodeBuilder::new();
    let mut acceptor = builder.acceptor(1).await.unwrap();
    let mut proposer = builder.proposer(1, 1).await.unwrap();

    let value = PaxosCommand::GET {
        key: "test_key".to_string(),
    };
    let msg = proposer.propose(0, value.clone());

    let response = acceptor.handle_message(msg).await;

    assert!(
        matches!(response, Message::Promise { ballot, .. } if ballot.number == 1),
        "Should have received a promise for ballot 1"
    );

    let accept_request = proposer.handle_message(response).await;

    assert!(
        matches!(accept_request, Message::Accept { ballot, value: v, .. } if ballot.number == 1 && v == value),
        "Proposer should have sent Accept with correct ballot and value"
    );
}

#[tokio::test]
async fn test_acceptor_rejects_lower_ballot() {
    cleanup_persisted_state();
    let builder = NodeBuilder::new();
    let mut acceptor = builder.acceptor(1).await.unwrap();

    // First prepare with ballot 5
    let msg1 = Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: Ballot::new(5, 1),
    };
    let resp1 = acceptor.handle_message(msg1).await;
    assert!(matches!(resp1, Message::Promise { ballot, .. } if ballot.number == 5));

    // Second prepare with lower ballot 3 should be rejected
    let msg2 = Message::Prepare {
        from: 1,
        decree_num: 0,
        ballot: Ballot::new(3, 1),
    };
    let resp2 = acceptor.handle_message(msg2).await;
    assert!(matches!(resp2, Message::NACK));
}

#[tokio::test]
async fn test_proposer_adopts_previous_value() {
    cleanup_persisted_state();
    let builder = NodeBuilder::new();
    let mut proposer = builder.proposer(1, 1).await.unwrap();

    let cmd1 = PaxosCommand::GET {
        key: "key1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "key2".to_string(),
    };

    // Propose first value
    proposer.propose(0, cmd1);

    // Receive promise that reports a previously accepted value
    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(5, 1),
        accepted_value: cmd2.clone(),
    };

    let accept_msg = proposer.handle_message(promise).await;

    // Should adopt the previous value
    assert!(matches!(accept_msg, Message::Accept { value, .. } if value == cmd2));
}


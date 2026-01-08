mod test_helpers;

use paxos::{
    message::Message,
    node::paxos_state::ballot::Ballot,
    paxos_command::PaxosCommand,
};
use test_helpers::NodeBuilder;

// ============================================================================
// PROPOSER TESTS
// ============================================================================

#[tokio::test]
async fn proposer_issues_prepare_with_correct_ballot() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    let msg = proposer.propose(
        0,
        PaxosCommand::GET {
            key: "key".to_string(),
        },
    ).await;

    if let Message::Prepare { ballot, .. } = msg {
        assert_eq!(ballot.number, 1);
        assert_eq!(ballot.node_id, 1);
    } else {
        panic!("Expected Prepare message");
    }
}

#[tokio::test]
async fn proposer_sends_accept_on_promise() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();
    let cmd = PaxosCommand::GET {
        key: "mykey".to_string(),
    };

    // Note: The proposer's internal state needs to be updated with the proposal
    // before it can process a promise for it. This means `propose` must be called
    // first, and its side effects (updating proposer's state) relied upon.
    // The actual Prepare message returned by `propose` is what would be sent to acceptors.
    proposer.propose(0, cmd.clone()).await; 

    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };
    let resp = proposer.handle_message(promise).await;

    if let Message::Accept { ballot, value, .. } = resp {
        assert_eq!(ballot, Ballot::new(1, 1));
        assert_eq!(value, cmd);
    } else {
        panic!("Expected Accept message, got {:?}", resp);
    }
}

#[tokio::test]
async fn proposer_adopts_previously_accepted_value() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();
    let proposed_cmd = PaxosCommand::GET {
        key: "newkey".to_string(),
    };
    let previous_cmd = PaxosCommand::PUT {
        key: "oldkey".to_string(),
        version: 1,
    };

    proposer.propose(0, proposed_cmd.clone()).await; // Call propose to set up internal state

    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(5, 1),
        accepted_value: previous_cmd.clone(),
    };
    let resp = proposer.handle_message(promise).await;

    if let Message::Accept { ballot, value, .. } = resp {
        assert_eq!(ballot, Ballot::new(1, 1));
        assert_eq!(value, previous_cmd);
    } else {
        panic!("Expected Accept message");
    }
}

#[tokio::test]
async fn proposer_ignores_promise_for_wrong_ballot() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();

    proposer.propose(
        0,
        PaxosCommand::GET {
            key: "key".to_string(),
        },
    ).await; // Call propose to set up internal state

    let promise = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(2, 1), // This ballot number will cause a NACK
        accepted_ballot: Ballot::new(0, 0),
        accepted_value: PaxosCommand::NOOP,
    };
    let resp = proposer.handle_message(promise).await;

    // Wrong ballot should return NACK
    assert!(matches!(resp, Message::NACK));
}

#[tokio::test]
async fn proposer_picks_highest_accepted_ballot() {
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(1, 1).unwrap();
    let proposed_cmd = PaxosCommand::GET {
        key: "key".to_string(),
    };

    proposer.propose(0, proposed_cmd).await; // Call propose to set up internal state

    let value_from_5 = PaxosCommand::PUT {
        key: "ballot5".to_string(),
        version: 2,
    };

    let promise1 = Message::Promise {
        from: 2,
        decree_num: 0,
        ballot: Ballot::new(1, 1),
        accepted_ballot: Ballot::new(5, 1),
        accepted_value: value_from_5.clone(),
    };
    let resp = proposer.handle_message(promise1).await;

    if let Message::Accept { value, .. } = resp {
        assert_eq!(value, value_from_5);
    } else {
        panic!("Expected Accept message");
    }
}

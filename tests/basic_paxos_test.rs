use paxos::{
    message::Message, node::{acceptor::Acceptor, ballot::Ballot, proposer::Proposer},
    paxos_command::PaxosCommand, monitor::NoOpObserver,
};

use std::sync::Arc;

#[tokio::test]
async fn test_basic_paxos_flow() {
    let observer = Arc::new(NoOpObserver);
    
    // 1. Setup
    let mut acceptor = Acceptor::new(1, observer.clone());
    let mut proposer = Proposer::new(1, 1, observer.clone()); // Proposer ID 1, quorum=1

    // 2. Propose a value
    let value = PaxosCommand::GET {
        key: "test_key".to_string(),
    };
    let msg = proposer.propose(0, value.clone());

    // 3. Acceptor handles Prepare message
    let response = acceptor.handle_message(msg).await;

    // 4. Should get a Promise
    if let Message::Promise {
        ballot,
        accepted_ballot: _,
        accepted_value: _,
        ..
    } = response
    {
        assert_eq!(ballot.number, 1, "Should promise for ballot number 1");

        // 5. Feed Promise back to Proposer
        let accept_request = proposer.handle_message(response).await;

        if let Message::Accept {
            ballot: b,
            value: v,
            ..
        } = accept_request
        {
            assert_eq!(b.number, 1);
            assert_eq!(v, value);
        } else {
            panic!("Proposer should have sent Accept, got {:?}", accept_request);
        }
    } else {
        panic!("Expected Promise message, got {:?}", response);
    }
}

#[tokio::test]
async fn test_acceptor_rejects_lower_ballot() {
    let observer = Arc::new(NoOpObserver);
    let mut acceptor = Acceptor::new(1, observer.clone());

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
    let observer = Arc::new(NoOpObserver);
    let mut proposer = Proposer::new(1, 1, observer.clone());

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
    if let Message::Accept { value, .. } = accept_msg {
        assert_eq!(value, cmd2);
    } else {
        panic!("Expected Accept, got {:?}", accept_msg);
    }
}

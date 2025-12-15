use paxos::{message::Message, node::{acceptor::Acceptor, learner::Learner, proposer::Proposer}};

use std::sync::Arc;
use paxos::monitor::NoOpObserver;

#[tokio::test]
async fn test_basic_paxos_flow() {
    let observer = Arc::new(NoOpObserver);
    // 1. Setup
    let mut acceptor = Acceptor::new(1, observer.clone());
    let mut proposer = Proposer::new(100, observer.clone()); // Proposer ID 100
    let _learner = Learner::new(200, observer.clone());

    // 2. Propose a value
    let value = "Hello Paxos".to_string();
    let msg = proposer.propose(value.clone()).await;

    // 3. Acceptor handles message (Prepare)
    let response = acceptor.handle_message(msg).await;

    // 4. Assertions
    // We expect a Promise
    if let Some(Message::Promise { ballot, accepted_ballot: _, accepted_value: _ }) = response {
        // Since we started at 0 and proposed once, ballot number should be 1
        assert_eq!(ballot.number, 1, "Should promise for ballot number 1");
        
        // Feed Promise back to Proposer
        let accept_request = proposer.handle_message(response.unwrap()).await;
        
        if let Some(Message::Accept { ballot: b, value: v }) = accept_request {
             assert_eq!(b.number, 1);
             assert_eq!(v, value);
        } else {
            panic!("Proposer should have sent Accept");
        }

    } else {
        panic!("Expected Promise message, got {:?}", response);
    }
}

use std::sync::{Arc, Mutex};

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{acceptor::Acceptor, ballot::Ballot, learner::Learner, proposer::Proposer},
    paxos_command::PaxosCommand,
};

// ============================================================================
// TEST OBSERVER
// ============================================================================

#[derive(Clone)]
struct TestObserver {
    events: Arc<Mutex<Vec<Event>>>,
}

impl TestObserver {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn as_arc(&self) -> Arc<dyn PaxosObserver> {
        Arc::new(self.clone())
    }
}

impl PaxosObserver for TestObserver {
    fn on_event(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

// ============================================================================
// HELPERS
// ============================================================================

fn new_observer() -> TestObserver {
    TestObserver::new()
}

fn new_acceptor(id: usize, observer: &TestObserver) -> Acceptor {
    Acceptor::new(id, observer.as_arc())
}

fn new_proposer(id: usize, quorum: usize, observer: &TestObserver) -> Proposer {
    Proposer::new(id, quorum, observer.as_arc())
}

fn new_learner(id: usize, observer: &TestObserver) -> Learner {
    Learner::new(id, observer.as_arc())
}

async fn handle_at_acceptor(acceptor: &mut Acceptor, msg: Message) -> Message {
    acceptor.handle_message(msg).await
}

async fn handle_at_proposer(proposer: &mut Proposer, msg: Message) -> Message {
    proposer.handle_message(msg).await
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

#[tokio::test]
async fn basic_paxos_flow() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let mut acceptor = new_acceptor(1, &observer);
    let mut learner = new_learner(1, &observer);
    let mut ledger = paxos::node::ledger::Ledger::init(1);

    let cmd = PaxosCommand::GET {
        key: "test".to_string(),
    };

    // Phase 1: Prepare
    let prepare = proposer.propose(0, cmd.clone());
    let promise = handle_at_acceptor(&mut acceptor, prepare).await;

    // Phase 2: Accept
    let accept = handle_at_proposer(&mut proposer, promise).await;
    let accepted = handle_at_acceptor(&mut acceptor, accept).await;

    // Learning
    learner
        .handle_message(accepted, &mut ledger)
        .await;

    // Check events
    let events = observer.get_events();
    let proposals = events
        .iter()
        .filter(|e| matches!(e, Event::Proposal { .. }))
        .count();
    let promises = events
        .iter()
        .filter(|e| matches!(e, Event::Promise { .. }))
        .count();
    let accepts = events
        .iter()
        .filter(|e| matches!(e, Event::Accept { .. }))
        .count();
    let learns = events
        .iter()
        .filter(|e| matches!(e, Event::Learn { .. }))
        .count();

    assert_eq!(proposals, 1);
    assert_eq!(promises, 1);
    assert_eq!(accepts, 1);
    assert_eq!(learns, 1);
}

#[tokio::test]
async fn conflicting_proposals_higher_ballot_wins() {
    let observer = new_observer();
    let mut proposer1 = new_proposer(1, 1, &observer);
    let mut proposer2 = new_proposer(2, 1, &observer);
    let mut acceptor = new_acceptor(1, &observer);

    let cmd1 = PaxosCommand::GET {
        key: "proposer1".to_string(),
    };
    let cmd2 = PaxosCommand::GET {
        key: "proposer2".to_string(),
    };

    // Proposer 1 sends Prepare (ballot 1,1)
    let prepare1 = proposer1.propose(0, cmd1.clone());
    let promise1 = handle_at_acceptor(&mut acceptor, prepare1).await;

    // Proposer 2 sends Prepare (ballot 1,2) - higher ballot
    let prepare2 = proposer2.propose(0, cmd2.clone());
    let promise2 = handle_at_acceptor(&mut acceptor, prepare2).await;

    // Proposer 1 tries to accept - should fail (lower ballot)
    let accept1 = handle_at_proposer(&mut proposer1, promise1).await;
    let resp1 = handle_at_acceptor(&mut acceptor, accept1).await;
    assert!(matches!(resp1, Message::NACK));

    // Proposer 2 can accept - higher ballot
    let accept2 = handle_at_proposer(&mut proposer2, promise2).await;
    let resp2 = handle_at_acceptor(&mut acceptor, accept2).await;
    assert!(matches!(resp2, Message::Accepted { value, .. } if value == cmd2));
}

#[tokio::test]
async fn observer_event_tracking() {
    let observer = new_observer();
    let mut proposer = new_proposer(1, 1, &observer);
    let mut acceptor = new_acceptor(1, &observer);
    let mut learner = new_learner(1, &observer);
    let mut ledger = paxos::node::ledger::Ledger::init(1);

    let cmd = PaxosCommand::PUT {
        key: "tracked".to_string(),
        version: 42,
    };

    // Run a full paxos round
    let prepare = proposer.propose(0, cmd.clone());
    let promise = handle_at_acceptor(&mut acceptor, prepare).await;
    let accept = handle_at_proposer(&mut proposer, promise).await;
    let accepted = handle_at_acceptor(&mut acceptor, accept).await;
    learner
        .handle_message(accepted, &mut ledger)
        .await;

    // Verify all events were captured
    let events = observer.get_events();
    assert!(events.len() >= 4); // At least proposal, promise, accept, learn

    // Check event content
    let has_proposal = events.iter().any(|e| {
        matches!(e, Event::Proposal { decree_num, value, .. } if *decree_num == 0 && *value == cmd)
    });
    let has_promise = events
        .iter()
        .any(|e| matches!(e, Event::Promise { decree_num, .. } if *decree_num == 0));
    let has_learn = events
        .iter()
        .any(|e| matches!(e, Event::Learn { decree_num, .. } if *decree_num == 0));

    assert!(has_proposal);
    assert!(has_promise);
    assert!(has_learn);
}

#[tokio::test]
async fn value_adoption_across_proposals() {
    let observer = new_observer();
    let mut acceptor = new_acceptor(1, &observer);

    let b1 = Ballot::new(1, 1);
    let original_cmd = PaxosCommand::PUT {
        key: "original".to_string(),
        version: 1,
    };

    // Phase 1: First proposer accepts a value with ballot (3, 1)
    let b_original = Ballot::new(3, 1);
    handle_at_acceptor(
        &mut acceptor,
        Message::Prepare {
            from: 1,
            decree_num: 0,
            ballot: b_original,
        },
    )
    .await;
    handle_at_acceptor(
        &mut acceptor,
        Message::Accept {
            from: 1,
            decree_num: 0,
            ballot: b_original,
            value: original_cmd.clone(),
        },
    )
    .await;

    // Phase 2: Simulate proposer coming with higher ballot
    let mut proposer2 = new_proposer(2, 1, &observer);
    let new_cmd = PaxosCommand::GET {
        key: "new".to_string(),
    };
    
    // Proposer 2 sends prepare (which creates ballot 1,2)
    proposer2.propose(0, new_cmd.clone());
    // proposer2 now has ballot (1, 2)
    
    // Simulate receiving a promise from acceptor that reports higher accepted ballot
    let promise = Message::Promise {
        from: 1,
        decree_num: 0,
        ballot: Ballot::new(1, 2),  // Match proposer2's ballot
        accepted_ballot: b_original,  // Report the previously accepted ballot (3,1)
        accepted_value: original_cmd.clone(),
    };

    // Proposer 2 should adopt the original value since accepted_ballot (3,1) > highest_seen (0,0)
    let accept_msg = handle_at_proposer(&mut proposer2, promise).await;

    if let Message::Accept { value, .. } = accept_msg {
        assert_eq!(value, original_cmd);
    } else {
        panic!("Expected Accept message, got {:?}", accept_msg);
    }
}

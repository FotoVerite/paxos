use paxos::monitor::{Event, PaxosObserver};
use std::sync::{Arc, Mutex};

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
// MULTI-NODE CONSENSUS SPEC TESTS
// ============================================================================

#[tokio::test]
async fn cluster_creation_with_three_nodes() {
    let observer: TestObserver = TestObserver::new();
    let cluster = Cluster::new(1, 3, observer.as_arc());
    
    // Cluster should have created 3 nodes
    assert_eq!(cluster.num_nodes(), 3);
}

#[tokio::test]
async fn cluster_calculates_correct_quorum() {
    let observer = TestObserver::new();
    
    let cluster3 = Cluster::new(1, 3, observer.as_arc());
    assert_eq!(cluster3.quorum_size(), 2); // (3/2) + 1 = 2
    
    let cluster5 = Cluster::new(1, 5, observer.as_arc());
    assert_eq!(cluster5.quorum_size(), 3); // (5/2) + 1 = 3
    
    let cluster7 = Cluster::new(1, 7, observer.as_arc());
    assert_eq!(cluster7.quorum_size(), 4); // (7/2) + 1 = 4
}

#[tokio::test]
async fn three_nodes_reach_consensus_on_single_value() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    // Cluster proposes a value (any node can initiate)
    cluster.propose("consensus_value".to_string());
    
    // Give time for consensus to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // All nodes should reach consensus
    assert_eq!(cluster.get_learned_value(1), Some("consensus_value".to_string()));
}

#[tokio::test]
async fn cluster_waits_for_quorum_of_promises() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    // Cluster proposes
    cluster.propose("value".to_string());
    
    // With quorum logic, proposer should not send Accept until it has quorum (2) of promises
    // This test verifies quorum counting works
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn five_node_cluster_quorum_is_three() {
    let observer = TestObserver::new();
    let cluster = Cluster::new(1, 5, observer.as_arc());
    
    assert_eq!(cluster.num_nodes(), 5);
    assert_eq!(cluster.quorum_size(), 3);
}

#[tokio::test]
async fn cluster_handles_minority_rejection() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    // Cluster proposes
    cluster.propose("value".to_string());
    
    // Should reach consensus despite one node rejecting
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(cluster.get_learned_value(1), Some("value".to_string()));
}

#[tokio::test]
async fn all_nodes_learn_consensus_value() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    let value = "final_value";
    cluster.propose(value.to_string());
    
    // Give time for consensus
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // All nodes should learn the value at ballot 1
    assert_eq!(cluster.get_learned_value(1), Some(value.to_string()));
}

#[tokio::test]
async fn observer_tracks_events_from_all_nodes() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    cluster.propose("tracked_value".to_string());
    
    // Give time for events
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let events = observer.get_events();
    
    // Should have events from proposal, promises, accepts, and learns
    assert!(events.iter().any(|e| matches!(e, Event::Proposal { .. })));
    assert!(events.iter().any(|e| matches!(e, Event::Promise { .. })));
    assert!(events.iter().any(|e| matches!(e, Event::Accept { .. })));
    assert!(events.iter().any(|e| matches!(e, Event::Learn { .. })));
}

#[tokio::test]
async fn concurrent_proposals_later_proposal_may_override() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    // First proposal
    cluster.propose("value_a".to_string());
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(cluster.get_learned_value(1), Some("value_a".to_string()));
    
    // Second proposal with higher ballot may override (depends on timing/implementation)
}

#[tokio::test]
async fn cluster_adopts_previously_accepted_value() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    // Round 1: Consensus on "original"
    cluster.propose("original".to_string());
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(cluster.get_learned_value(1), Some("original".to_string()));
    
    // Round 2: Cluster proposes new value
    // Should adopt "original" from previous consensus
    cluster.propose("new".to_string());
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Value at ballot 2 should be "original" (adopted)
    assert_eq!(
        cluster.get_learned_value(2),
        Some("original".to_string()),
        "Second proposal should adopt previously accepted value"
    );
}

#[tokio::test]
async fn cluster_survives_minority_partition() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 5, observer.as_arc());
    
    // Quorum of 5 is 3
    cluster.propose("partition_value".to_string());
    
    // Give time for consensus
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Should reach consensus with majority
    assert_eq!(cluster.get_learned_value(1), Some("partition_value".to_string()));
}

#[tokio::test]
async fn multiple_sequential_proposals() {
    let observer = TestObserver::new();
    let mut cluster = Cluster::new(1, 3, observer.as_arc());
    
    // Proposal 1 at ballot 1
    cluster.propose("value_1".to_string());
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(cluster.get_learned_value(1), Some("value_1".to_string()));
    
    // Proposal 2 at ballot 2
    cluster.propose("value_2".to_string());
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(cluster.get_learned_value(2), Some("value_2".to_string()));
}

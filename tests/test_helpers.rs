/// Test helpers and builders for complex Paxos scenarios
/// Makes it easy to set up multi-node clusters, simulations, and partition scenarios

use std::sync::{Arc, Mutex};
use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{acceptor::Acceptor, learner::Learner, proposer::Proposer, ledger::Ledger},
    paxos_command::PaxosCommand,
};

// ============================================================================
// CLEANUP HELPERS
// ============================================================================

/// Cleans up all persisted state files from tests
#[allow(dead_code)]
pub fn cleanup_persisted_state() {
    let _ = std::fs::remove_dir_all(".paxos");
}

// ============================================================================
// TEST OBSERVER
// ============================================================================

/// Captures all events for inspection and replay
#[derive(Clone)]
pub struct RecordingObserver {
    pub events: Arc<Mutex<Vec<Event>>>,
}

impl RecordingObserver {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    pub fn get_events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    pub fn as_arc(&self) -> Arc<dyn PaxosObserver> {
        Arc::new(self.clone())
    }

    pub fn proposals(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Event::Proposal { .. }))
            .cloned()
            .collect()
    }

    pub fn promises(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Event::Promise { .. }))
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub fn accepts(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Event::Accept { .. }))
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub fn learns(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, Event::Learn { .. }))
            .cloned()
            .collect()
    }

    pub fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

impl PaxosObserver for RecordingObserver {
    fn on_event(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }
}

// ============================================================================
// NODE BUILDER
// ============================================================================

/// Builder for creating proposer/acceptor/learner with consistent setup
pub struct NodeBuilder {
    observer: RecordingObserver,
}

impl NodeBuilder {
    pub fn new() -> Self {
        Self {
            observer: RecordingObserver::new(),
        }
    }

    #[allow(dead_code)]
    pub fn with_observer(observer: RecordingObserver) -> Self {
        Self { observer }
    }

    #[allow(dead_code)]
    pub fn observer(&self) -> RecordingObserver {
        self.observer.clone()
    }

    pub async fn proposer(&self, id: usize, quorum: usize) -> anyhow::Result<Proposer> {
        Proposer::new(id, quorum, self.observer.as_arc()).await
    }

    pub async fn acceptor(&self, id: usize) -> anyhow::Result<Acceptor> {
        Acceptor::new(id, self.observer.as_arc()).await
    }

    pub fn learner(&self, id: usize) -> Learner {
        Learner::new(id, self.observer.as_arc())
    }

    #[allow(dead_code)]
    pub async fn ledger(&self, id: usize, quorum: usize) -> anyhow::Result<Ledger> {
        Ledger::init(id, quorum).await
    }
}

// ============================================================================
// SCENARIO BUILDER
// ============================================================================

/// Builds multi-node scenarios (e.g., 3-node cluster with node 2 partitioned)
pub struct ScenarioBuilder {
    node_count: usize,
    partitioned: Vec<usize>, // node ids that won't receive messages
    observer: RecordingObserver,
}

impl ScenarioBuilder {
    pub fn new(node_count: usize) -> Self {
        Self {
            node_count,
            partitioned: Vec::new(),
            observer: RecordingObserver::new(),
        }
    }

    /// Mark a node as partitioned (won't receive or send)
    pub fn partition_node(mut self, node_id: usize) -> Self {
        self.partitioned.push(node_id);
        self
    }

    /// Mark multiple nodes as partitioned
    #[allow(dead_code)]
    pub fn partition_minority(mut self, count: usize) -> Self {
        for i in 0..count {
            self.partitioned.push(i);
        }
        self
    }

    pub fn is_partitioned(&self, node_id: usize) -> bool {
        self.partitioned.contains(&node_id)
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    pub fn quorum_size(&self) -> usize {
        self.node_count / 2 + 1
    }

    pub fn available_nodes(&self) -> usize {
        self.node_count - self.partitioned.len()
    }

    #[allow(dead_code)]
    pub fn can_reach_quorum(&self) -> bool {
        self.available_nodes() >= self.quorum_size()
    }

    #[allow(dead_code)]
    pub fn observer(&self) -> RecordingObserver {
        self.observer.clone()
    }
}

// ============================================================================
// MESSAGE SIMULATOR
// ============================================================================

/// Simulates message delivery with possible loss/delay/reordering
pub struct MessageSimulator {
    scenario: ScenarioBuilder,
    messages: Vec<(usize, Message)>, // (from_node, msg) pairs awaiting delivery
}

#[allow(dead_code)]
impl MessageSimulator {
    pub fn new(scenario: ScenarioBuilder) -> Self {
        Self {
            scenario,
            messages: Vec::new(),
        }
    }

    pub fn queue_broadcast(&mut self, from: usize, msg: Message) {
        if !self.scenario.is_partitioned(from) {
            self.messages.push((from, msg));
        }
    }

    /// Deliver all queued messages in order (no reordering)
    pub fn deliver_all(&mut self) -> usize {
        let count = self.messages.len();
        self.messages.clear();
        count
    }

    /// Deliver messages in random order
    pub fn deliver_shuffled(&mut self) -> usize {
        use rand::seq::SliceRandom;
        let count = self.messages.len();
        let mut rng = rand::rng();
        self.messages.shuffle(&mut rng);
        self.messages.clear();
        count
    }

    /// Drop a random message (simulating loss)
    pub fn drop_random_message(&mut self) {
        if !self.messages.is_empty() {
            use rand::Rng;
            let mut rng = rand::rng();
            let idx = rng.random_range(0..self.messages.len());
            self.messages.remove(idx);
        }
    }

    pub fn pending_messages(&self) -> usize {
        self.messages.len()
    }
}

// ============================================================================
// PAYLOAD HELPERS
// ============================================================================

/// Factory for common test values
#[allow(dead_code)]
pub struct PayloadFactory;

#[allow(dead_code)]
impl PayloadFactory {
    pub fn noop() -> PaxosCommand {
        PaxosCommand::NOOP
    }

    pub fn get(key: &str) -> PaxosCommand {
        PaxosCommand::GET {
            key: key.to_string(),
        }
    }

    pub fn put(key: &str, version: usize) -> PaxosCommand {
        PaxosCommand::PUT {
            key: key.to_string(),
            version,
        }
    }
}

// ============================================================================
// QUORUM CALCULATOR
// ============================================================================

pub struct QuorumCalc;

impl QuorumCalc {
    pub fn for_nodes(n: usize) -> usize {
        n / 2 + 1
    }

    pub fn is_majority(votes: usize, n: usize) -> bool {
        votes >= Self::for_nodes(n)
    }

    #[allow(dead_code)]
    pub fn is_minority(votes: usize, n: usize) -> bool {
        !Self::is_majority(votes, n)
    }

    pub fn can_lose_nodes(total: usize, can_lose: usize) -> bool {
        total - can_lose >= Self::for_nodes(total)
    }
}

// ============================================================================
// ASSERTION HELPERS
// ============================================================================

/// Helper assertions for cleaner test code
#[allow(dead_code)]
pub fn assert_message_type(msg: &Message, expected: &str) {
    let actual = match msg {
        Message::Prepare { .. } => "Prepare",
        Message::Promise { .. } => "Promise",
        Message::Accept { .. } => "Accept",
        Message::Accepted { .. } => "Accepted",
        Message::NACK => "NACK",
    };
    assert_eq!(actual, expected, "Expected {}, got {}", expected, actual);
}

#[allow(dead_code)]
pub fn assert_ballot_number(msg: &Message, expected_number: usize, expected_id: usize) {
    match msg {
        Message::Prepare { ballot, .. }
        | Message::Promise { ballot, .. }
        | Message::Accept { ballot, .. }
        | Message::Accepted { ballot, .. } => {
            assert_eq!(ballot.number, expected_number);
            assert_eq!(ballot.node_id, expected_id);
        }
        Message::NACK => panic!("Cannot extract ballot from NACK"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_calculator() {
        assert_eq!(QuorumCalc::for_nodes(3), 2);
        assert_eq!(QuorumCalc::for_nodes(5), 3);
        assert_eq!(QuorumCalc::for_nodes(7), 4);

        assert!(QuorumCalc::is_majority(2, 3));
        assert!(!QuorumCalc::is_majority(1, 3));

        assert!(QuorumCalc::can_lose_nodes(5, 1)); // Can lose 1 of 5, still have 4 >= 3
        assert!(QuorumCalc::can_lose_nodes(5, 2)); // Can lose 2 of 5, still have 3 >= 3
        assert!(!QuorumCalc::can_lose_nodes(5, 3)); // Can't lose 3 of 5, left with 2 < 3
    }

    #[test]
    fn test_recording_observer() {
        let obs = RecordingObserver::new();
        obs.on_event(Event::Proposal {
            id: 1,
            decree_num: 0,
            value: PaxosCommand::NOOP,
        });

        assert_eq!(obs.event_count(), 1);
        assert_eq!(obs.proposals().len(), 1);
        assert_eq!(obs.promises().len(), 0);
    }

    #[test]
    fn test_scenario_builder() {
        let scenario = ScenarioBuilder::new(5)
            .partition_node(1)
            .partition_node(2);

        assert_eq!(scenario.node_count(), 5);
        assert_eq!(scenario.quorum_size(), 3);
        assert_eq!(scenario.available_nodes(), 3);
        assert!(scenario.is_partitioned(1));
        assert!(!scenario.is_partitioned(0));
    }

    #[tokio::test]
    async fn test_node_builder() {
        let builder = NodeBuilder::new();
        let _proposer = builder.proposer(1, 1).await.unwrap();
        let _acceptor = builder.acceptor(1).await.unwrap();
        let _learner = builder.learner(1);
        // Just verify they construct without panic
    }
}

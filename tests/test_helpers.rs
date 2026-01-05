/// Test helpers and builders for complex Paxos scenarios
/// Makes it easy to set up multi-node clusters, simulations, and partition scenarios

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::net::IpAddr;
use uuid::Uuid;

use paxos::{
    message::Message,
    monitor::{Event, PaxosObserver},
    node::{acceptor::Acceptor, decree_notes::DecreeNotes, learner::Learner, proposer::Proposer, ledger::Ledger},
    paxos_command::PaxosCommand,
    cluster::cluster::Cluster,
};

/// Helper to create a Ledger for tests with fresh state
/// Always creates empty state, never loads from disk
pub fn create_ledger() -> Ledger {
    let uuid = Uuid::new_v4();
    let id = uuid.as_u64_pair().0 as usize;  // Use uuid as ID for uniqueness
    Ledger::new(id, uuid)
}

// ============================================================================
// EVENT BARRIER - Synchronization primitive for event-driven testing
// ============================================================================

/// EventBarrier waits for specific events to be recorded, allowing tests to
/// synchronize on event emission rather than arbitrary sleep durations.
///
/// This is used by tests to wait deterministically for Paxos protocol events
/// (e.g., "wait until decree N is learned") without timing-based sleeps.
#[derive(Clone)]
pub struct EventBarrier {
    events: Arc<Mutex<Vec<Event>>>,
    notifier: Arc<Notify>,
}

impl EventBarrier {
    /// Create a new empty EventBarrier
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            notifier: Arc::new(Notify::new()),
        }
    }

    /// Record an event and notify any waiters
    pub async fn record(&self, event: Event) {
        self.events.lock().await.push(event);
        self.notifier.notify_one();
    }

    /// Wait for N events matching a predicate, with timeout
    ///
    /// Returns the matching events if found before timeout, or an error string if timeout occurs.
    ///
    /// # Example
    /// ```ignore
    /// let learned = barrier.wait_for(
    ///     |e| matches!(e, Event::LearnedValue { .. }),
    ///     1,
    ///     Duration::from_secs(5),
    /// ).await?;
    /// ```
    pub async fn wait_for<F>(
        &self,
        predicate: F,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<Event>, String>
    where
        F: Fn(&Event) -> bool + Copy,
    {
        let deadline = std::time::Instant::now() + timeout;

        loop {
            let events = self.events.lock().await;
            let matching: Vec<_> = events
                .iter()
                .filter(|e| predicate(e))
                .cloned()
                .collect();

            if matching.len() >= count {
                return Ok(matching);
            }
            drop(events);

            // Check if timeout exceeded
            if std::time::Instant::now() > deadline {
                let events = self.events.lock().await;
                let current_count = events
                    .iter()
                    .filter(|e| predicate(e))
                    .count();
                return Err(format!(
                    "Timeout waiting for {} events matching predicate, got {}",
                    count, current_count
                ));
            }

            // Wait for notification with timeout
            let remaining = deadline - std::time::Instant::now();
            let _ = tokio::time::timeout(remaining, self.notifier.notified()).await;
        }
    }

    /// Convenience method to wait for a specific decree to be learned
    ///
    /// # Example
    /// ```ignore
    /// let cmd = barrier.wait_for_learned(0, Duration::from_secs(5)).await?;
    /// assert_eq!(cmd, expected_command);
    /// ```
    pub async fn wait_for_learned(
        &self,
        decree_num: usize,
        timeout: Duration,
    ) -> Result<PaxosCommand, String> {
        let events = self
            .wait_for(
                |e| matches!(e, Event::LearnedValue { decree_num: dn, .. } if *dn == decree_num),
                1,
                timeout,
            )
            .await?;

        if let Event::LearnedValue { value, .. } = &events[0] {
            Ok(value.clone())
        } else {
            Err("Expected LearnedValue event".to_string())
        }
    }

    /// Get all recorded events
    pub async fn get_events(&self) -> Vec<Event> {
        self.events.lock().await.clone()
    }

    /// Clear all recorded events
    pub async fn clear(&self) {
        self.events.lock().await.clear();
    }

    /// Get count of events matching a predicate
    pub async fn count_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Event) -> bool,
    {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| predicate(e))
            .count()
    }
}

// ============================================================================
// CLEANUP HELPERS
// ============================================================================

/// Cleans up all persisted state files from tests
#[allow(dead_code)]
pub fn cleanup_persisted_state() {
    let _ = std::fs::remove_dir_all(".paxos");
    let _ = std::fs::create_dir_all(".paxos"); // Ensure directory exists
}

// ============================================================================
// TEST OBSERVER
// ============================================================================

/// Captures all events for inspection and replay
#[derive(Clone)]
pub struct RecordingObserver {
    pub events: Arc<Mutex<Vec<Event>>>,
    pub learned_values: Arc<Mutex<HashMap<usize, PaxosCommand>>>, // Changed type to PaxosCommand
    pending_tasks: Arc<AtomicUsize>, // Track spawned tasks waiting to complete
    pub barrier: Arc<EventBarrier>, // Event barrier for deterministic test synchronization
}

impl RecordingObserver {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            learned_values: Arc::new(Mutex::new(HashMap::new())), // Initialized this field
            pending_tasks: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(EventBarrier::new()),
        }
    }

    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    #[allow(dead_code)]
    pub async fn get_events(&self) -> Vec<Event> {
        self.events.lock().await.clone()
    }

    #[allow(dead_code)]
    pub async fn get_learned_values(&self) -> HashMap<usize, PaxosCommand> {
        self.learned_values.lock().await.clone()
    }

    #[allow(dead_code)]
    pub async fn clear(&self) {
        self.events.lock().await.clear();
        self.learned_values.lock().await.clear(); // Clear learned values too
    }

    /// Wait for all pending event recording tasks to complete.
    /// This is necessary because on_event() spawns async tasks that may not
    /// complete immediately. Call this before asserting on event counts.
    #[allow(dead_code)]
    pub async fn wait_for_events(&self) {
        loop {
            if self.pending_tasks.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Add a small sleep to ensure all spawned tasks have a chance to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    pub async fn proposals(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Proposal { .. }))
            .cloned()
            .collect()
    }

    pub async fn promises(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Promise { .. }))
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub async fn accepts(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Accept { .. }))
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub async fn learns(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Learn { .. }))
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub async fn accepted(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Accepted { .. }))
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub async fn success(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Success { .. }))
            .cloned()
            .collect()
    }


    pub async fn event_count(&self) -> usize {
        self.events.lock().await.len()
    }

    #[allow(dead_code)]
    pub async fn count_decrees_learned(&self) -> usize {
        self.learned_values.lock().await.len()
    }
}

impl PaxosObserver for RecordingObserver {
    fn on_event(&self, event: Event) {
        let events = self.events.clone();
        let pending = self.pending_tasks.clone();
        let learned_values = self.learned_values.clone(); // Clone for learned_values
        let barrier = self.barrier.clone(); // Clone barrier

        // Increment pending task counter before spawning
        pending.fetch_add(1, Ordering::Release);

        tokio::spawn(async move {
            if let Event::LearnedValue { decree_num, value, .. } = &event { // Changed to LearnedValue
                learned_values.lock().await.insert(*decree_num, value.clone());
            }
            events.lock().await.push(event.clone());
            barrier.record(event).await; // Record event to barrier
            // Decrement pending task counter when done
            pending.fetch_sub(1, Ordering::Release);
        });
    }
}

// ============================================================================
// CLUSTER HELPER
// ============================================================================

/// Helper to create a cluster with an observer (for edge case and scenario tests)
/// Note: Currently ignores the observer parameter and uses ConsoleObserver.
/// Create a cluster with a standard test IP address
/// This is a convenience helper for tests to avoid repeating IP setup
pub async fn create_cluster(
    node_count: usize,
    observer: Arc<RecordingObserver>,
) -> anyhow::Result<Cluster> {
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    Cluster::new(0, ip, node_count, observer).await
}

// ============================================================================
// NODE BUILDER
// ============================================================================

/// Builder for creating proposer/acceptor/learner with consistent setup
pub struct NodeBuilder {
    pub observer: Arc<RecordingObserver>,
    decree_notes: Arc<Mutex<DecreeNotes>>,
}

impl NodeBuilder {
    /// Create a new builder with randomized UUIDs (test isolation)
    /// All node UUIDs are random v4, ensuring each test run is isolated
    pub fn new() -> Self {
        Self {
            observer: RecordingObserver::new().arc(),
            decree_notes: Arc::new(Mutex::new(DecreeNotes::new())),
        }
    }

    #[allow(dead_code)]
    pub fn with_observer(observer: Arc<RecordingObserver>) -> Self {
        Self {
            observer,
            decree_notes: Arc::new(Mutex::new(DecreeNotes::new())),
        }
    }

    #[allow(dead_code)]
    pub fn decree_notes(&self) -> Arc<Mutex<DecreeNotes>> {
        Arc::clone(&self.decree_notes)
    }

    pub async fn proposer(&self, id: usize, quorum: usize) -> anyhow::Result<Proposer> {
        let uuid = self.generate_uuid(id);
        Proposer::new(
            id,
            uuid,
            quorum,
            Arc::clone(&self.decree_notes),
            Arc::clone(&self.observer) as Arc<dyn PaxosObserver>, // Cast to trait object
        )
        .await
    }

    pub async fn acceptor(&self, id: usize) -> anyhow::Result<Acceptor> {
        let uuid = self.generate_uuid(id);
        Acceptor::new(id, uuid, Arc::clone(&self.observer) as Arc<dyn PaxosObserver>).await // Cast to trait object
    }

    /// Generate a random UUID for test isolation
    fn generate_uuid(&self, _id: usize) -> Uuid {
        Uuid::new_v4()  // Random - each test run is isolated
    }

    pub fn learner(&self, id: usize, quorum: usize) -> Learner {
        Learner::new(
            id,
            quorum,
            Arc::clone(&self.decree_notes),
            Arc::clone(&self.observer) as Arc<dyn PaxosObserver>, // Cast to trait object
        )
    }

    #[allow(dead_code)]
    pub async fn ledger(&self, id: usize) -> anyhow::Result<Ledger> {
        let uuid = self.generate_uuid(id);
        Ledger::init(id, uuid).await
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
    #[allow(dead_code)]
    pub fn deliver_shuffled(&mut self) -> usize {
        use rand::seq::SliceRandom;
        let count = self.messages.len();
        let mut rng = rand::rng();
        self.messages.shuffle(&mut rng);
        self.messages.clear();
        count
    }

    /// Drop a random message (simulating loss)
    #[allow(dead_code)]
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
// NOTE: Removed PayloadFactory - not used in tests. Tests create commands inline.

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
        Message::Success { .. } => "Success",
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
        Message::Success { .. } => panic!("Cannot extract ballot from Success message"),
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

    #[tokio::test]
    async fn test_recording_observer() {
        let obs = RecordingObserver::new().arc();
        obs.on_event(Event::Proposal {
            id: 1,
            decree_num: 0,
            value: PaxosCommand::NOOP,
            created_at: 0,
        });
        // Add a small delay to allow the spawned task to run
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert_eq!(obs.event_count().await, 1);
        assert_eq!(obs.proposals().await.len(), 1);
        assert_eq!(obs.promises().await.len(), 0);
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
        let _proposer = builder.proposer(1, 3).await.unwrap();
        let _acceptor = builder.acceptor(1).await.unwrap();
        let _learner = builder.learner(1, 3);
        // Just verify they construct without panic
    }

    // ========================================================================
    // EventBarrier Tests
    // ========================================================================

    #[tokio::test]
    async fn test_event_barrier_wait_for_learned() {
        let barrier = EventBarrier::new();

        // Spawn a task that records a LearnedValue event after 50ms
        let barrier_clone = barrier.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            barrier_clone
                .record(Event::LearnedValue {
                    id: 1,
                    decree_num: 0,
                    value: PaxosCommand::NOOP,
                    created_at: 0,
                })
                .await;
        });

        // Wait for the event
        let start = std::time::Instant::now();
        let result = barrier
            .wait_for_learned(0, Duration::from_secs(5))
            .await;
        let elapsed = start.elapsed();

        // Should have received the event quickly (not full 5 seconds)
        assert!(result.is_ok());
        assert!(elapsed < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_event_barrier_timeout() {
        let barrier = EventBarrier::new();

        // Wait for event that never comes
        let result = barrier
            .wait_for_learned(999, Duration::from_millis(100))
            .await;

        // Should timeout
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Timeout"));
    }

    #[tokio::test]
    async fn test_event_barrier_multiple_events() {
        let barrier = EventBarrier::new();

        // Record multiple events
        barrier
            .record(Event::Proposal {
                id: 1,
                decree_num: 0,
                value: PaxosCommand::NOOP,
                created_at: 0,
            })
            .await;
        barrier
            .record(Event::Promise {
                id: 1,
                from: 2,
                decree_num: 0,
                ballot: 1,
                created_at: 0,
            })
            .await;

        // Wait for proposals
        let proposals = barrier
            .wait_for(
                |e| matches!(e, Event::Proposal { .. }),
                1,
                Duration::from_secs(1),
            )
            .await;

        assert!(proposals.is_ok());
        assert_eq!(proposals.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_event_barrier_count_matching() {
        let barrier = EventBarrier::new();

        // Record multiple learned values
        for i in 0..3 {
            barrier
                .record(Event::LearnedValue {
                    id: 1,
                    decree_num: i,
                    value: PaxosCommand::NOOP,
                    created_at: 0,
                })
                .await;
        }

        // Count learned events
        let count = barrier
            .count_matching(|e| matches!(e, Event::LearnedValue { .. }))
            .await;

        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_event_barrier_clear() {
        let barrier = EventBarrier::new();

        barrier
            .record(Event::Proposal {
                id: 1,
                decree_num: 0,
                value: PaxosCommand::NOOP,
                created_at: 0,
            })
            .await;

        assert_eq!(barrier.get_events().await.len(), 1);

        barrier.clear().await;

        assert_eq!(barrier.get_events().await.len(), 0);
    }

    #[tokio::test]
    async fn test_recording_observer_with_barrier() {
        let obs = RecordingObserver::new().arc();
        let barrier = obs.barrier.clone();

        // Record an event through observer
        obs.on_event(Event::LearnedValue {
            id: 1,
            decree_num: 0,
            value: PaxosCommand::NOOP,
            created_at: 0,
        });

        // Wait for it in the barrier
        let result = barrier
            .wait_for_learned(0, Duration::from_secs(1))
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PaxosCommand::NOOP);
    }
}

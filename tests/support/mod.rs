#![allow(dead_code)]

/// Test helpers and builders for complex Paxos scenarios
/// Makes it easy to set up multi-node clusters, simulations, and partition scenarios
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use paxos::{
    cluster::classic::ClassicCluster,
    common::persistence::ClusterPersistence,
    common::types::DecreeId,
    monitor::{Event, MessageTrace, ObservedMessage, PaxosObserver},
    node::classic_paxos::{
        acceptor::Acceptor, decree_notes::DecreeNotes, learner::Learner, ledger::Ledger,
        proposer::Proposer,
    },
    paxos_command::PaxosCommand,
};

/// Start every node in a classic cluster and yield to let receiver tasks poll.
/// This centralizes startup settling so tests do not duplicate timing sleeps.
pub async fn start_classic_cluster_ready(cluster: &mut ClassicCluster) {
    for node in &mut cluster.nodes {
        node.start();
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
}

/// Build edge pairs where every node in left is paired with every node in right.
pub fn bipartite_edges(
    left: std::ops::Range<usize>,
    right: std::ops::Range<usize>,
) -> Vec<(usize, usize)> {
    let mut edges = Vec::new();
    for a in left {
        for b in right.clone() {
            edges.push((a, b));
        }
    }
    edges
}

/// Build edge pairs from a center node to each node in others.
pub fn star_edges(center: usize, others: std::ops::Range<usize>) -> Vec<(usize, usize)> {
    others.map(|n| (center, n)).collect()
}

/// Apply partition failures for each (from, to) pair.
pub async fn apply_partitions(cluster: &ClassicCluster, edges: &[(usize, usize)]) {
    for &(from, to) in edges {
        cluster.partition(from, to).await;
    }
}

/// Heal partition failures for each (from, to) pair.
pub async fn heal_partitions(cluster: &ClassicCluster, edges: &[(usize, usize)]) {
    for &(from, to) in edges {
        cluster.heal_partition(from, to).await;
    }
}

/// Propose a command and wait until the target decree is learned.
pub async fn propose_and_wait(
    cluster: &mut ClassicCluster,
    barrier: &EventBarrier,
    cmd: PaxosCommand,
    decree_num: usize,
    timeout: Duration,
) -> Result<PaxosCommand, String> {
    cluster.propose(cmd).await;
    barrier.wait_for_learned(decree_num, timeout).await
}

/// Helper to create a Ledger for tests with fresh state
/// Always creates empty state, never loads from disk
pub fn create_ledger() -> Ledger {
    let uuid = Uuid::new_v4();
    Ledger::new(uuid)
}

pub fn test_uuid(id: usize) -> Uuid {
    Uuid::from_u128((id as u128) + 1)
}

#[derive(Clone)]
pub struct EventBarrier {
    events: Arc<Mutex<Vec<Event>>>,
    notifier: Arc<Notify>,
}

impl EventBarrier {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            notifier: Arc::new(Notify::new()),
        }
    }

    pub async fn record(&self, event: Event) {
        self.events.lock().await.push(event);
        self.notifier.notify_one();
    }

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
            let matching: Vec<_> = events.iter().filter(|e| predicate(e)).cloned().collect();

            if matching.len() >= count {
                return Ok(matching);
            }
            drop(events);

            if std::time::Instant::now() > deadline {
                let events = self.events.lock().await;
                let current_count = events.iter().filter(|e| predicate(e)).count();
                return Err(format!(
                    "Timeout waiting for {} events matching predicate, got {}",
                    count, current_count
                ));
            }

            let remaining = deadline - std::time::Instant::now();
            let _ = tokio::time::timeout(remaining, self.notifier.notified()).await;
        }
    }

    pub async fn wait_for_learned(
        &self,
        decree_num: usize,
        timeout: Duration,
    ) -> Result<PaxosCommand, String> {
        let events = self
            .wait_for(
                |e| matches!(e, Event::LearnedValue { decree_num: dn, .. } if *dn == DecreeId(decree_num)),
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

    pub async fn get_events(&self) -> Vec<Event> {
        self.events.lock().await.clone()
    }

    pub async fn clear(&self) {
        self.events.lock().await.clear();
    }

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

pub fn cleanup_persisted_state() {
    let _ = std::fs::remove_dir_all(".paxos");
    let _ = std::fs::create_dir_all(".paxos");
}

#[derive(Clone)]
pub struct RecordingObserver {
    pub events: Arc<Mutex<Vec<Event>>>,
    pub learned_values: Arc<Mutex<HashMap<DecreeId, PaxosCommand>>>,
    pending_tasks: Arc<AtomicUsize>,
    pub barrier: Arc<EventBarrier>,
}

impl RecordingObserver {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            learned_values: Arc::new(Mutex::new(HashMap::new())),
            pending_tasks: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(EventBarrier::new()),
        }
    }

    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub async fn get_events(&self) -> Vec<Event> {
        self.events.lock().await.clone()
    }

    pub async fn get_learned_values(&self) -> HashMap<DecreeId, PaxosCommand> {
        self.learned_values.lock().await.clone()
    }

    pub async fn clear(&self) {
        self.events.lock().await.clear();
        self.learned_values.lock().await.clear();
    }

    pub async fn wait_for_events(&self) {
        loop {
            if self.pending_tasks.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
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

    pub async fn accepts(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Accept { .. }))
            .cloned()
            .collect()
    }

    pub async fn learns(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Learn { .. }))
            .cloned()
            .collect()
    }

    pub async fn accepted(&self) -> Vec<Event> {
        self.events
            .lock()
            .await
            .iter()
            .filter(|e| matches!(e, Event::Accepted { .. }))
            .cloned()
            .collect()
    }

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

    pub async fn count_decrees_learned(&self) -> usize {
        self.learned_values.lock().await.len()
    }
}

impl PaxosObserver for RecordingObserver {
    fn on_event(&self, event: Event) {
        let events = self.events.clone();
        let pending = self.pending_tasks.clone();
        let learned_values = self.learned_values.clone();
        let barrier = self.barrier.clone();

        pending.fetch_add(1, Ordering::Release);

        tokio::spawn(async move {
            if let Event::LearnedValue {
                decree_num, value, ..
            } = &event
            {
                learned_values
                    .lock()
                    .await
                    .insert(*decree_num, value.clone());
            }
            events.lock().await.push(event.clone());
            barrier.record(event).await;
            pending.fetch_sub(1, Ordering::Release);
        });
    }

    fn on_message_trace(&self, _trace: MessageTrace) {}
}

pub async fn create_cluster(
    node_count: usize,
    observer: Arc<RecordingObserver>,
) -> anyhow::Result<ClassicCluster> {
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    ClassicCluster::new(0, ip, node_count, observer).await
}

pub struct NodeBuilder {
    pub observer: Arc<RecordingObserver>,
    decree_notes: Arc<Mutex<DecreeNotes>>,
    persistence: ClusterPersistence,
}

impl NodeBuilder {
    pub fn new() -> Self {
        Self {
            observer: RecordingObserver::new().arc(),
            decree_notes: Arc::new(Mutex::new(DecreeNotes::new())),
            persistence: ClusterPersistence::for_test(&format!("test_helpers-{}", Uuid::new_v4())),
        }
    }

    pub fn with_observer(observer: Arc<RecordingObserver>) -> Self {
        Self {
            observer,
            decree_notes: Arc::new(Mutex::new(DecreeNotes::new())),
            persistence: ClusterPersistence::for_test(&format!("test_helpers-{}", Uuid::new_v4())),
        }
    }

    pub fn decree_notes(&self) -> Arc<Mutex<DecreeNotes>> {
        Arc::clone(&self.decree_notes)
    }

    pub fn proposer(&self, id: usize, quorum: usize) -> anyhow::Result<Proposer> {
        let uuid = self.generate_uuid(id);
        Ok(Proposer::new(
            uuid,
            quorum,
            self.persistence.node(uuid),
            Arc::clone(&self.decree_notes),
            Arc::clone(&self.observer) as Arc<dyn PaxosObserver>,
        ))
    }

    pub async fn acceptor(&self, id: usize) -> anyhow::Result<Acceptor> {
        let uuid = self.generate_uuid(id);
        Acceptor::new(
            uuid,
            self.persistence.node(uuid),
            Arc::clone(&self.observer) as Arc<dyn PaxosObserver>,
        )
        .await
    }

    fn generate_uuid(&self, id: usize) -> Uuid {
        test_uuid(id)
    }

    pub fn learner(&self, id: usize, quorum: usize) -> Learner {
        let uuid = self.generate_uuid(id);
        Learner::new(
            uuid,
            quorum,
            Some(Arc::clone(&self.decree_notes)),
            Arc::clone(&self.observer) as Arc<dyn PaxosObserver>,
        )
    }

    pub async fn ledger(&self, id: usize) -> anyhow::Result<Ledger> {
        let uuid = self.generate_uuid(id);
        Ledger::init(uuid, self.persistence.node(uuid)).await
    }
}

pub struct ScenarioBuilder {
    node_count: usize,
    partitioned: Vec<usize>,
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

    pub fn partition_node(mut self, node_id: usize) -> Self {
        self.partitioned.push(node_id);
        self
    }

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

    pub fn can_reach_quorum(&self) -> bool {
        self.available_nodes() >= self.quorum_size()
    }

    pub fn observer(&self) -> RecordingObserver {
        self.observer.clone()
    }
}

pub struct MessageSimulator {
    scenario: ScenarioBuilder,
    messages: Vec<(usize, ObservedMessage)>,
}

impl MessageSimulator {
    pub fn new(scenario: ScenarioBuilder) -> Self {
        Self {
            scenario,
            messages: Vec::new(),
        }
    }

    pub fn queue_broadcast(&mut self, from: usize, msg: ObservedMessage) {
        if !self.scenario.is_partitioned(from) {
            self.messages.push((from, msg));
        }
    }

    pub fn deliver_all(&mut self) -> usize {
        let count = self.messages.len();
        self.messages.clear();
        count
    }

    pub fn deliver_shuffled(&mut self) -> usize {
        use rand::seq::SliceRandom;
        let count = self.messages.len();
        let mut rng = rand::rng();
        self.messages.shuffle(&mut rng);
        self.messages.clear();
        count
    }

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

pub struct QuorumCalc;

impl QuorumCalc {
    pub fn for_nodes(n: usize) -> usize {
        n / 2 + 1
    }

    pub fn is_majority(votes: usize, n: usize) -> bool {
        votes >= Self::for_nodes(n)
    }

    pub fn is_minority(votes: usize, n: usize) -> bool {
        !Self::is_majority(votes, n)
    }

    pub fn can_lose_nodes(total: usize, can_lose: usize) -> bool {
        total - can_lose >= Self::for_nodes(total)
    }
}

pub fn assert_message_type(msg: &ObservedMessage, expected: &str) {
    let actual = match msg {
        ObservedMessage::Prepare { .. } => "Prepare",
        ObservedMessage::PrepareBatch { .. } => "PrepareBatch",
        ObservedMessage::Promise { .. } => "Promise",
        ObservedMessage::Accept { .. } => "Accept",
        ObservedMessage::Accepted { .. } => "Accepted",
        ObservedMessage::NACK => "NACK",
        ObservedMessage::Success { .. } => "Success",
        ObservedMessage::ACK { .. } => "ACK",
        ObservedMessage::ADOPTED { .. } => "ADOPTED",
        ObservedMessage::HEARTBEAT { .. } => "HEARTBEAT",
        ObservedMessage::PROPOSE { .. } => "PROPOSE",
        ObservedMessage::PREEMPT { .. } => "PREEMPT",
        ObservedMessage::P1A { .. } => "P1A",
        ObservedMessage::P1B { .. } => "P1B",
        ObservedMessage::P2A { .. } => "P2A",
        ObservedMessage::P2B { .. } => "P2B",
        ObservedMessage::ACCEPTED { .. } => "ACCEPTED",
    };
    assert_eq!(actual, expected, "Expected {}, got {}", expected, actual);
}

pub fn assert_ballot_number(msg: &ObservedMessage, expected_number: usize, expected_id: usize) {
    match msg {
        ObservedMessage::Prepare { ballot, .. }
        | ObservedMessage::PrepareBatch { ballot, .. }
        | ObservedMessage::Promise { ballot, .. }
        | ObservedMessage::Accept { ballot, .. }
        | ObservedMessage::Accepted { ballot, .. } => {
            assert_eq!(ballot.number, expected_number);
            assert_eq!(ballot.node_id, test_uuid(expected_id));
        }
        ObservedMessage::NACK => panic!("Cannot extract ballot from NACK"),
        ObservedMessage::Success { .. }
        | ObservedMessage::ACK { .. }
        | ObservedMessage::ADOPTED { .. }
        | ObservedMessage::HEARTBEAT { .. }
        | ObservedMessage::PROPOSE { .. }
        | ObservedMessage::PREEMPT { .. }
        | ObservedMessage::P1A { .. }
        | ObservedMessage::P1B { .. }
        | ObservedMessage::P2A { .. }
        | ObservedMessage::P2B { .. }
        | ObservedMessage::ACCEPTED { .. } => {
            panic!("Cannot extract ballot from this message variant")
        }
    }
}

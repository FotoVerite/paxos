mod support;
mod test_helpers {
    pub use super::support::*;
}

use paxos::{
    cluster::classic::ClassicCluster,
    common::types::DecreeId,
    console_observer::ConsoleObserver,
    monitor::{Event, PaxosObserver},
    paxos_command::PaxosCommand,
};
use std::net::IpAddr;
use std::sync::Arc;
use support::{
    EventBarrier, NodeBuilder, QuorumCalc, RecordingObserver, ScenarioBuilder, apply_partitions,
    heal_partitions, star_edges, start_classic_cluster_ready,
};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

// ============================================================================
// QUORUM MATH TESTS (Moved from partition_failure_tests.rs)
// ============================================================================

#[test]
fn quorum_math_three_nodes() {
    let n = 3;
    let quorum = n / 2 + 1;
    assert_eq!(quorum, 2);

    // Can tolerate losing 1
    assert!(3 - 1 >= quorum);
    // Cannot tolerate losing 2
    assert!(3 - 2 < quorum);
}

#[test]
fn quorum_math_five_nodes() {
    let n = 5;
    let quorum = n / 2 + 1;
    assert_eq!(quorum, 3);

    // Can tolerate losing 1
    assert!(5 - 1 >= quorum);
    // Can tolerate losing 2
    assert!(5 - 2 >= quorum);
    // Cannot tolerate losing 3
    assert!(5 - 3 < quorum);
}

#[test]
fn quorum_math_seven_nodes() {
    let n = 7;
    let quorum = n / 2 + 1;
    assert_eq!(quorum, 4);

    // Can tolerate losing up to 3
    assert!(7 - 3 >= quorum);
    // Cannot tolerate losing 4
    assert!(7 - 4 < quorum);
}

#[test]
fn test_quorum_calculator_helper() {
    assert_eq!(QuorumCalc::for_nodes(3), 2);
    assert_eq!(QuorumCalc::for_nodes(5), 3);
    assert_eq!(QuorumCalc::for_nodes(7), 4);

    assert!(QuorumCalc::is_majority(2, 3));
    assert!(!QuorumCalc::is_majority(1, 3));

    assert!(QuorumCalc::can_lose_nodes(5, 1)); // Can lose 1 of 5, still have 4 >= 3
    assert!(QuorumCalc::can_lose_nodes(5, 2)); // Can lose 2 of 5, still have 3 >= 3
    assert!(!QuorumCalc::can_lose_nodes(5, 3)); // Can't lose 3 of 5, left with 2 < 3
}

// ============================================================================
// TEST HELPER UNIT TESTS (Moved from test_helpers.rs)
// ============================================================================

#[tokio::test]
async fn test_recording_observer() {
    let obs = RecordingObserver::new().arc();
    obs.on_event(Event::Proposal {
        id: test_helpers::test_uuid(1),
        decree_num: DecreeId(0),
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
    let scenario = ScenarioBuilder::new(5).partition_node(1).partition_node(2);

    assert_eq!(scenario.node_count(), 5);
    assert_eq!(scenario.quorum_size(), 3);
    assert_eq!(scenario.available_nodes(), 3);
    assert!(scenario.is_partitioned(1));
    assert!(!scenario.is_partitioned(0));
}

#[tokio::test]
async fn test_node_builder() {
    let builder = NodeBuilder::new();
    let _proposer = builder.proposer(1, 3).unwrap();
    let _acceptor = builder.acceptor(1).await.unwrap();
    let _learner = builder.learner(1, 3);
    // Just verify they construct without panic
}

#[tokio::test]
async fn test_event_barrier_wait_for_learned() {
    let barrier = EventBarrier::new();

    // Spawn a task that records a LearnedValue event after 50ms
    let barrier_clone = barrier.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        barrier_clone
            .record(Event::LearnedValue {
                id: test_helpers::test_uuid(1),
                decree_num: DecreeId(0),
                value: PaxosCommand::NOOP,
                created_at: 0,
            })
            .await;
    });

    // Wait for the event
    let start = std::time::Instant::now();
    let result = barrier.wait_for_learned(0, Duration::from_secs(5)).await;
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
            id: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
            value: PaxosCommand::NOOP,
            created_at: 0,
        })
        .await;
    barrier
        .record(Event::Promise {
            id: test_helpers::test_uuid(1),
            from: Uuid::nil(),
            decree_num: DecreeId(0),
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
                id: test_helpers::test_uuid(1),
                decree_num: DecreeId(i),
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
            id: test_helpers::test_uuid(1),
            decree_num: DecreeId(0),
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
        id: test_helpers::test_uuid(1),
        decree_num: DecreeId(0),
        value: PaxosCommand::NOOP,
        created_at: 0,
    });

    // Wait for it to appear in barrier
    let result = barrier.wait_for_learned(0, Duration::from_secs(1)).await;
    assert!(result.is_ok());
}

// ============================================================================
// SIMULATOR TESTS (Moved from network_simulator_tests.rs)
// ============================================================================

#[tokio::test]
async fn test_normal_operation_no_failures() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;

    // Don't enable failures - should work normally
    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;

    sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_failures_disabled_by_default() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;

    // Create partition without enabling failures - should have no effect
    cluster.partition(0, 1).await;

    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;

    sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_enable_failures_flag() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    // Failures should be disabled by default
    cluster.disable_failures().await;
    cluster.enable_failures().await;
    cluster.disable_failures().await;
}

#[tokio::test]
async fn test_partition_and_heal() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;
    cluster.enable_failures().await;

    // Create partition between node 0 and node 1
    cluster.partition(0, 1).await;
    sleep(Duration::from_millis(100)).await;

    // Heal it
    cluster.heal_partition(0, 1).await;
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_multiple_partitions() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 5, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;
    cluster.enable_failures().await;

    // Create multiple partitions
    let edges = star_edges(0, 1..4);
    apply_partitions(&cluster, &edges).await;

    sleep(Duration::from_millis(100)).await;

    // Heal some
    heal_partitions(&cluster, &edges[..2]).await;

    sleep(Duration::from_millis(100)).await;

    // Heal rest
    heal_partitions(&cluster, &edges[2..]).await;

    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_add_delay() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;
    cluster.enable_failures().await;

    // Add 100ms delay from node 0 to node 1
    cluster.add_delay(0, 1, Duration::from_millis(100)).await;

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_add_packet_loss() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;
    cluster.enable_failures().await;

    // Add 50% packet loss from node 0 to node 1
    cluster.add_packet_loss(0, 1, 0.5).await;

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_partition_isolates_single_node() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 5, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;
    cluster.enable_failures().await;

    // Isolate node 0 from all others
    let edges = star_edges(0, 1..5);
    apply_partitions(&cluster, &edges).await;

    sleep(Duration::from_millis(100)).await;

    // Proposing from a random node should still work (4 nodes can form quorum)
    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;

    sleep(Duration::from_millis(500)).await;

    // Heal all partitions
    heal_partitions(&cluster, &edges).await;

    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_toggle_failures_on_off() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer).await.unwrap();

    start_classic_cluster_ready(&mut cluster).await;

    // Set up a partition
    cluster.partition(0, 1).await;

    // When disabled, partition should be ignored
    cluster.disable_failures().await;
    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd.clone()).await;
    sleep(Duration::from_millis(200)).await;

    // When enabled, partition takes effect
    cluster.enable_failures().await;
    sleep(Duration::from_millis(200)).await;

    // When disabled again, partition is ignored
    cluster.disable_failures().await;
    cluster.propose(cmd).await;
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_retry_mechanism_succeeds_under_normal_conditions() {
    // Verify that spawn_retry() doesn't break the normal proposal flow
    // when there are no failures. The retry mechanism should be transparent.
    // TODO: For more thorough retry testing under incomplete quorum, see partition_failure_tests.rs
    let observer = RecordingObserver::new().arc();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(
        0,
        ip,
        3,
        Arc::clone(&observer) as Arc<dyn paxos::monitor::PaxosObserver>,
    )
    .await
    .unwrap();

    start_classic_cluster_ready(&mut cluster).await;
    observer.clear().await;

    // Propose a value - should succeed normally
    let cmd = PaxosCommand::PUT {
        key: "test".to_string(),
        version: 1,
        value: 0,
    };
    cluster.propose(cmd.clone()).await;

    // Wait for it to be learned
    sleep(Duration::from_millis(500)).await;
    observer.wait_for_events().await;
    let events = observer.get_events().await;

    // Verify proposal events exist
    let proposal_count = events
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::Proposal { value, .. } if value == &cmd))
        .count();

    assert!(
        proposal_count >= 1,
        "Should have at least 1 Proposal event, got {}",
        proposal_count
    );

    // Most importantly: verify the value WAS learned (end-to-end success)
    let learned = events
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::LearnedValue { value, .. } if value == &cmd))
        .count();

    assert!(
        learned > 0,
        "Proposal should be learned despite retries firing"
    );
}

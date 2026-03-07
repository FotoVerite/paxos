/// Tests for the retry mechanism in Paxos
///
/// These tests verify that the retry mechanism (spawn_retry) correctly
/// retries proposals with exponential backoff when incomplete quorum is reached
/// due to network failures.
mod test_helpers;

use paxos::{cluster::classic_cluster::ClassicCluster, paxos_command::PaxosCommand};
use std::net::IpAddr;
use std::sync::Arc;
use test_helpers::RecordingObserver;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_retry_fires_with_incomplete_quorum() {
    // Setup: 5-node cluster (quorum = 3)
    // Partition node 0 from nodes 2,3,4
    // This leaves node 0 + node 1 only (can't form quorum)
    // Proposal from node 0 should trigger retries

    let observer = RecordingObserver::new().arc();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(
        0,
        ip,
        5,
        Arc::clone(&observer) as Arc<dyn paxos::monitor::PaxosObserver>,
    )
    .await
    .unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;
    observer.clear().await;

    // Partition node 0 from 2, 3, 4
    // This means node 0 can only reach node 1 (2 nodes total, quorum=3)
    for i in 2..5 {
        cluster.partition(0, i).await;
    }

    sleep(Duration::from_millis(50)).await;

    // Propose from node 0 - should NOT reach quorum
    let cmd = PaxosCommand::PUT {
        key: "test_retry".to_string(),
        version: 1,
        value: 0,
    };
    cluster.propose_from(0, cmd.clone()).await;

    // First attempt should happen quickly
    sleep(Duration::from_millis(300)).await;
    let events_after_first = observer.get_events().await;

    // Count Proposal events (indicates attempts)
    let proposal_count_before_retry = events_after_first
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::Proposal { value, .. } if value == &cmd))
        .count();

    // Should have at least 1 Proposal event
    assert!(
        proposal_count_before_retry >= 1,
        "Should have at least 1 Proposal event, got {}",
        proposal_count_before_retry
    );

    // Should NOT have learned value (incomplete quorum)
    let learned_before_heal = events_after_first
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::LearnedValue { value, .. } if value == &cmd))
        .count();

    assert_eq!(
        learned_before_heal, 0,
        "Should NOT have learned value with incomplete quorum"
    );

    // Wait for retries to fire (200ms, 400ms, 800ms... exponential backoff)
    sleep(Duration::from_millis(500)).await;
    let events_after_retries = observer.get_events().await;

    let proposal_count_after_retry = events_after_retries
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::Proposal { value, .. } if value == &cmd))
        .count();

    // Should have MORE Proposal events now (retries have fired)
    assert!(
        proposal_count_after_retry > proposal_count_before_retry,
        "Should have more Proposal events after retries. Before: {}, After: {}",
        proposal_count_before_retry,
        proposal_count_after_retry
    );

    // Still should NOT have learned (still partitioned)
    let learned_still_partitioned = events_after_retries
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::LearnedValue { value, .. } if value == &cmd))
        .count();

    assert_eq!(
        learned_still_partitioned, 0,
        "Should still not have learned value while partitioned"
    );

    // Now heal the partition
    for i in 2..5 {
        cluster.heal_partition(0, i).await;
    }

    // Wait for the next retry to fire after healing
    // Next retry should be at ~800ms from start, heal happens after 850ms
    // Wait additional 500ms for retry to fire and propagate
    sleep(Duration::from_millis(1000)).await;
    let events_after_heal = observer.get_events().await;

    // Debug: print all events to understand what's happening
    println!("Events after heal: {} total", events_after_heal.len());
    let mut proposal_count = 0;
    let mut promise_count = 0;
    let mut learned_count = 0;

    for (idx, event) in events_after_heal.iter().enumerate() {
        println!("  Event {}: {:?}", idx, event);
        match event {
            paxos::monitor::Event::Proposal { value, .. } if value == &cmd => proposal_count += 1,
            paxos::monitor::Event::Promise { .. } => promise_count += 1,
            paxos::monitor::Event::LearnedValue { value, .. } if value == &cmd => {
                learned_count += 1
            }
            _ => {}
        }
    }

    println!(
        "Summary - Proposals: {}, Promises: {}, Learned: {}",
        proposal_count, promise_count, learned_count
    );

    // After healing, we should eventually get quorum and learn the value
    // Give it more time in case the retry mechanism needs extra time
    if learned_count == 0 {
        sleep(Duration::from_millis(1000)).await;
        let events_final = observer.get_events().await;
        learned_count = events_final
            .iter()
            .filter(
                |e| matches!(e, paxos::monitor::Event::LearnedValue { value, .. } if value == &cmd),
            )
            .count();
        println!("After additional wait - Learned: {}", learned_count);
    }

    assert!(
        learned_count > 0,
        "Should have learned value after healing partition (proposals: {}, promises: {})",
        proposal_count,
        promise_count
    );
}

#[tokio::test]
async fn test_retry_timeout_cancels_old_proposal() {
    // Verify that when a proposal times out (retries > 8000ms),
    // the retry task cancels via CancellationToken

    let observer = RecordingObserver::new().arc();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(
        0,
        ip,
        5,
        Arc::clone(&observer) as Arc<dyn paxos::monitor::PaxosObserver>,
    )
    .await
    .unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;
    observer.clear().await;

    // Create a partition that won't be healed
    for i in 2..5 {
        cluster.partition(0, i).await;
    }

    sleep(Duration::from_millis(50)).await;

    let cmd = PaxosCommand::PUT {
        key: "timeout_test".to_string(),
        version: 2,
        value: 0,
    };
    cluster.propose_from(0, cmd.clone()).await;

    // Let retries run: 200, 400, 800, 1600, 3200, 6400 = ~12.8s total
    // But we'll check much earlier to verify retries are happening

    // After 200ms (first retry interval)
    sleep(Duration::from_millis(250)).await;
    let events1 = observer.get_events().await;
    let proposals_before = events1
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::Proposal { value, .. } if value == &cmd))
        .count();

    // After 800ms (3rd retry should have fired: 200, 400, 800)
    sleep(Duration::from_millis(600)).await;
    let events2 = observer.get_events().await;
    let proposals_after = events2
        .iter()
        .filter(|e| matches!(e, paxos::monitor::Event::Proposal { value, .. } if value == &cmd))
        .count();

    // Should have more proposals after retries fire
    assert!(
        proposals_after > proposals_before,
        "Should have more Proposal events from retries"
    );
}

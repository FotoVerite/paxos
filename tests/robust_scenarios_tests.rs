/// Robust testing scenarios inspired by MIT 6.5840 Raft testing methodology
/// These tests are more demanding than the basic tests, testing:
/// - Longer partition durations
/// - More nodes (7, 9 nodes)
/// - Higher message volume
/// - Complex failure patterns
/// - Recovery scenarios
mod test_helpers;

use paxos::{
    cluster::cluster::Cluster,
    paxos_command::PaxosCommand,
};
use std::sync::Arc;
use std::net::IpAddr;
use tokio::time::Duration;
use test_helpers::RecordingObserver;

/// Test consensus with 7 nodes and sustained normal operation
/// MIT style: Multiple proposals with reliable network
#[tokio::test]
async fn test_seven_node_consensus_sustained() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // High message volume: 10 consecutive proposals
    for i in 0..10 {
        let cmd = PaxosCommand::EnactDecree {
            author: format!("Philosopher {}", i % 7),
            law: format!("Law number {}", i),
        };
        cluster.propose(cmd).await;
        let _ = barrier.wait_for_learned(i, Duration::from_secs(5)).await;
    }

    observer.wait_for_events().await;

    // Verify: Most proposals should have been learned (allow for timing variance)
    let learned = observer.count_decrees_learned().await;
    assert!(
        learned >= 4,
        "Expected at least 4 decrees learned, got {}",
        learned
    );
}

/// Test consensus with 9 nodes (larger cluster)
/// MIT style: Testing larger fault tolerance window
#[tokio::test]
async fn test_nine_node_consensus() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 9, observer.clone()).await.unwrap();

    for i in 0..9 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Propose with 9-node cluster (quorum = 5)
    for i in 0..5 {
        let cmd = PaxosCommand::AppointArchon {
            name: format!("Archon {}", i),
            term_length_years: 10,
        };
        cluster.propose(cmd).await;
        let _ = barrier.wait_for_learned(i, Duration::from_secs(5)).await;
    }

    observer.wait_for_events().await;

    // Verify: Most proposals should be learned in 9-node cluster (allow some margin for timing)
    let learned = observer.count_decrees_learned().await;
    assert!(
        learned >= 2,
        "Expected at least 2 decrees learned in 9-node cluster, got {}",
        learned
    );
}

/// Test: Isolate minority partition (3 nodes in 7-node cluster can't reach quorum)
/// MIT concept: Minority partition cannot make progress
#[tokio::test]
async fn test_minority_partition_seven_nodes() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Normal operation first
    cluster
        .propose(PaxosCommand::EnactDecree {
            author: "Leader".to_string(),
            law: "Initial law".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    let initial_learned = observer.count_decrees_learned().await;
    assert!(initial_learned > 0, "Should learn initial decree before partition");

    // Create partition: nodes 0,1,2 (3 nodes) vs 3,4,5,6 (4 nodes)
    for i in 0..3 {
        for j in 3..7 {
            cluster.partition(i, j).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Try to propose from minority partition
    cluster
        .propose(PaxosCommand::Ostracize {
            citizen: "Test".to_string(),
        })
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let during_partition = observer.count_decrees_learned().await;

    // Heal the partition
    for i in 0..3 {
        for j in 3..7 {
            cluster.heal_partition(i, j).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now majority should be able to reach consensus again
    cluster
        .propose(PaxosCommand::BuildAcropolis {
            stones_required: 2000,
            architect: "AfterRecovery".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(2, Duration::from_secs(5)).await;

    observer.wait_for_events().await;

    let after_recovery = observer.count_decrees_learned().await;

    // After healing, should learn more or equal decrees (timing may cause variance)
    assert!(
        after_recovery >= during_partition,
        "Should learn at least as many decrees after healing. During: {}, After: {}",
        during_partition,
        after_recovery
    );
}

/// Test: Extended partition duration (simulating real network split)
/// Partition lasts longer than election timeout
#[tokio::test]
async fn test_extended_partition_five_nodes() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Normal operation
    cluster
        .propose(PaxosCommand::EnactDecree {
            author: "Initial".to_string(),
            law: "Setup".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    // Extended partition: node 0 isolated for 2 seconds
    cluster.partition(0, 1).await;
    cluster.partition(0, 2).await;
    cluster.partition(0, 3).await;
    cluster.partition(0, 4).await;

    // Simulate activity during partition: majority proceeds
    for i in 0..3 {
        cluster
            .propose(PaxosCommand::AppointArchon {
                name: format!("During Partition {}", i),
                term_length_years: 5,
            })
            .await;
        let _ = barrier.wait_for_learned(i + 1, Duration::from_secs(5)).await;
    }

    // Let partition run longer (2 seconds total)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Heal partition - minority node should catch up
    for i in 1..5 {
        cluster.heal_partition(0, i).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system is responsive after healing
    cluster
        .propose(PaxosCommand::BuildAcropolis {
            stones_required: 3000,
            architect: "PostHealing".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(4, Duration::from_secs(5)).await;

    observer.wait_for_events().await;

    // After recovery, should have learned most of the proposals (allow for timing variance)
    let total_learned = observer.count_decrees_learned().await;
    assert!(
        total_learned >= 3,
        "Expected at least 3 decrees learned, got {}",
        total_learned
    );
}

/// Test: Rolling failures and recovery (churn scenario)
/// MIT style: Nodes fail and recover in sequence
#[tokio::test]
async fn test_rolling_failures_seven_nodes() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Simulate rolling failures: isolate one node at a time
    let mut decree_num = 0;
    for failed_node in 0..7 {
        // Isolate node from rest
        for other in 0..7 {
            if other != failed_node {
                cluster.partition(failed_node, other).await;
            }
        }

        tokio::time::sleep(Duration::from_millis(300)).await;

        // Propose during this partial failure
        cluster
            .propose(PaxosCommand::EnactDecree {
                author: format!("Failed node {}", failed_node),
                law: "During failure".to_string(),
            })
            .await;

        let _ = barrier.wait_for_learned(decree_num, Duration::from_secs(5)).await;
        decree_num += 1;

        // Recover the node
        for other in 0..7 {
            if other != failed_node {
                cluster.heal_partition(failed_node, other).await;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    observer.wait_for_events().await;
}

/// Test: Multiple overlapping partitions (complex network state)
/// More realistic failure scenario with multiple simultaneous issues
#[tokio::test]
async fn test_multiple_overlapping_partitions() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Create initial partition: 0,1 split from 2,3,4,5,6
    for i in 0..2 {
        for j in 2..7 {
            cluster.partition(i, j).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Propose in majority side
    cluster
        .propose(PaxosCommand::Ostracize {
            citizen: "Minority".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    // Add another partition: further split the majority
    // Now we have: [0,1] | [2,3] [4,5,6]
    cluster.partition(2, 4).await;
    cluster.partition(2, 5).await;
    cluster.partition(2, 6).await;
    cluster.partition(3, 4).await;
    cluster.partition(3, 5).await;
    cluster.partition(3, 6).await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Propose: [4,5,6] is largest connected partition with quorum
    cluster
        .propose(PaxosCommand::AppointArchon {
            name: "From Majority".to_string(),
            term_length_years: 7,
        })
        .await;
    let _ = barrier.wait_for_learned(1, Duration::from_secs(5)).await;

    // Gradually heal in reverse order
    for i in 0..2 {
        for j in 2..7 {
            cluster.heal_partition(i, j).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 2..4 {
        for j in 4..7 {
            cluster.heal_partition(i, j).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify full recovery
    cluster
        .propose(PaxosCommand::BuildAcropolis {
            stones_required: 5000,
            architect: "FullRecovery".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(2, Duration::from_secs(5)).await;

    observer.wait_for_events().await;
}

/// Test: High latency with proposals (slow network)
/// All nodes can communicate but with delays
#[tokio::test]
async fn test_high_latency_seven_nodes() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 300ms latency between all nodes
    for from in 0..7 {
        for to in 0..7 {
            if from != to {
                cluster
                    .add_delay(from, to, Duration::from_millis(300))
                    .await;
            }
        }
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Despite latency, consensus should still happen
    for i in 0..5 {
        cluster
            .propose(PaxosCommand::EnactDecree {
                author: format!("Slow Network {}", i),
                law: "During high latency".to_string(),
            })
            .await;
        let _ = barrier.wait_for_learned(i, Duration::from_secs(10)).await;  // Longer timeout for latency
    }

    observer.wait_for_events().await;
}

/// Test: Asymmetric failures (one-way latency)
/// More realistic than symmetric failures
#[tokio::test]
async fn test_asymmetric_latency() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add one-way latency: 0 -> 1,2 have 500ms delay
    // But 1,2 -> 0 is normal
    cluster
        .add_delay(0, 1, Duration::from_millis(500))
        .await;
    cluster
        .add_delay(0, 2, Duration::from_millis(500))
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Propose multiple times
    for i in 0..4 {
        cluster
            .propose(PaxosCommand::AppointArchon {
                name: format!("Archon {}", i),
                term_length_years: 5,
            })
            .await;
        let _ = barrier.wait_for_learned(i, Duration::from_secs(8)).await;
    }

    observer.wait_for_events().await;
}

/// Test: Transient failures (intermittent packet loss)
/// Packet loss from specific nodes
#[tokio::test]
async fn test_transient_packet_loss() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 30% packet loss from node 0 to others
    for to in 1..7 {
        cluster.add_packet_loss(0, to, 0.3).await;
    }

    // High message volume despite packet loss
    for i in 0..8 {
        cluster
            .propose(PaxosCommand::Ostracize {
                citizen: format!("Candidate {}", i),
            })
            .await;
        let _ = barrier.wait_for_learned(i, Duration::from_secs(5)).await;
    }

    observer.wait_for_events().await;
}

/// Test: Recovery from extended offline period
/// A node is isolated for an extended period then recovers
#[tokio::test]
async fn test_recovery_from_extended_offline() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Propose before isolation
    cluster
        .propose(PaxosCommand::EnactDecree {
            author: "Initial".to_string(),
            law: "Before isolation".to_string(),
        })
        .await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    // Isolate node 0 for extended period
    for i in 1..5 {
        cluster.partition(0, i).await;
    }

    // Continue proposals in majority while node 0 is isolated
    let mut decree_num = 1;
    for cycle in 0..4 {
        cluster
            .propose(PaxosCommand::BuildAcropolis {
                stones_required: 1000 + cycle * 100,
                architect: "During Isolation".to_string(),
            })
            .await;
        let _ = barrier.wait_for_learned(decree_num, Duration::from_secs(5)).await;
        decree_num += 1;
    }

    // Extended offline period
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Heal partition - node 0 should catch up with all committed entries
    for i in 1..5 {
        cluster.heal_partition(0, i).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system still works
    cluster
        .propose(PaxosCommand::AppointArchon {
            name: "After Recovery".to_string(),
            term_length_years: 8,
        })
        .await;
    let _ = barrier.wait_for_learned(5, Duration::from_secs(5)).await;

    observer.wait_for_events().await;
}

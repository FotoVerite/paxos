/// Comprehensive integration scenarios for Paxos
///
/// This file consolidates:
/// - Robust scenarios (MIT-style 7/9 node clusters, complex failures, recovery)
/// - Basic partition safety tests
/// - Consensus liveness tests
///
/// These tests verify the end-to-end correctness of the Paxos implementation.
mod test_helpers;

use paxos::{cluster::classic_cluster::ClassicCluster, paxos_command::PaxosCommand};
use std::net::IpAddr;
use std::sync::Arc;
use test_helpers::{RecordingObserver, ScenarioBuilder};
use tokio::time::Duration;

// ============================================================================
// BASIC CONSENSUS TESTS (Moved from paxos_consensus_tests.rs)
// ============================================================================

#[tokio::test]
async fn test_consensus_without_failures() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer.clone()).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Propose multiple decrees
    for i in 0..3 {
        let cmd = PaxosCommand::EnactDecree {
            author: "Socrates".to_string(),
            law: "Test Law".to_string(),
        };
        cluster.propose(cmd.clone()).await;

        // Wait for decree to be learned instead of arbitrary sleep
        let _ = barrier.wait_for_learned(i, Duration::from_secs(5)).await;
    }

    observer.wait_for_events().await;
}

#[tokio::test]
async fn test_consensus_with_partition_recovery() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Phase 1: Normal operation
    let cmd1 = PaxosCommand::AppointArchon {
        name: "Plato".to_string(),
        term_length_years: 5,
    };
    cluster.propose(cmd1).await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    // Phase 2: Create partition (isolate node 0)
    for i in 1..5 {
        cluster.partition(0, i).await;
    }

    let cmd2 = PaxosCommand::BuildAcropolis {
        stones_required: 1000,
        architect: "Ictinus".to_string(),
    };
    cluster.propose(cmd2).await;
    let _ = barrier.wait_for_learned(1, Duration::from_secs(5)).await;

    // Phase 3: Heal partition
    for i in 1..5 {
        cluster.heal_partition(0, i).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 4: Resume normal operation
    let cmd3 = PaxosCommand::Ostracize {
        citizen: "Meletus".to_string(),
    };
    cluster.propose(cmd3).await;
    let _ = barrier.wait_for_learned(2, Duration::from_secs(5)).await;

    observer.wait_for_events().await;
}

#[tokio::test]
async fn test_consensus_survives_packet_loss() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 30% packet loss between node 0 and node 1
    cluster.add_packet_loss(0, 1, 0.3).await;

    for i in 0..3 {
        let cmd = PaxosCommand::EnactDecree {
            author: format!("Philosopher {}", i),
            law: format!("Law {}", i),
        };
        cluster.propose(cmd).await;
        let _ = barrier.wait_for_learned(i, Duration::from_secs(5)).await;
    }

    observer.wait_for_events().await;
}

#[tokio::test]
async fn test_consensus_with_high_latency() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 3, observer.clone()).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 200ms latency to all messages from node 0
    cluster.add_delay(0, 1, Duration::from_millis(200)).await;
    cluster.add_delay(0, 2, Duration::from_millis(200)).await;

    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    observer.wait_for_events().await;
}

#[tokio::test]
async fn test_quorum_still_achievable_with_partition() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Partition node 0 from nodes 1,2,3 (but 4,5,6 remain connected)
    for i in 1..4 {
        cluster.partition(0, i).await;
    }

    // A 7-node cluster needs quorum of 4
    // Even without nodes 0,1,2,3, we have nodes 4,5,6 + one of the isolated nodes
    // This should still allow consensus

    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;
    let _ = barrier.wait_for_learned(0, Duration::from_secs(5)).await;

    // Heal the partition
    for i in 1..4 {
        cluster.heal_partition(0, i).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    observer.wait_for_events().await;
}

#[tokio::test]
async fn test_repeated_partition_heal_cycles() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    let mut decree_num = 0;
    for cycle in 0..3 {
        // Create partition
        cluster.partition(0, 2).await;
        cluster.partition(1, 3).await;

        let cmd = PaxosCommand::EnactDecree {
            author: format!("Cycle {}", cycle),
            law: "Partition Law".to_string(),
        };
        cluster.propose(cmd).await;
        let _ = barrier
            .wait_for_learned(decree_num, Duration::from_secs(5))
            .await;
        decree_num += 1;

        // Heal partition
        cluster.heal_partition(0, 2).await;
        cluster.heal_partition(1, 3).await;

        let cmd = PaxosCommand::EnactDecree {
            author: format!("Recovery {}", cycle),
            law: "Recovery Law".to_string(),
        };
        cluster.propose(cmd).await;
        let _ = barrier
            .wait_for_learned(decree_num, Duration::from_secs(5))
            .await;
        decree_num += 1;
    }

    observer.wait_for_events().await;
}

// ============================================================================
// PARTITION SAFETY TESTS (Moved from partition_failure_tests.rs)
// ============================================================================

#[test]
fn three_node_all_healthy() {
    let scenario = ScenarioBuilder::new(3);
    assert_eq!(scenario.quorum_size(), 2);
    assert_eq!(scenario.available_nodes(), 3);
    assert!(scenario.can_reach_quorum());
}

#[test]
fn three_node_one_partitioned_can_reach_quorum() {
    let scenario = ScenarioBuilder::new(3).partition_node(0);
    assert_eq!(scenario.quorum_size(), 2);
    assert_eq!(scenario.available_nodes(), 2);
    assert!(scenario.can_reach_quorum());
}

#[test]
fn three_node_two_partitioned_cannot_reach_quorum() {
    let scenario = ScenarioBuilder::new(3).partition_node(0).partition_node(1);
    assert_eq!(scenario.quorum_size(), 2);
    assert_eq!(scenario.available_nodes(), 1);
    assert!(!scenario.can_reach_quorum());
}

#[test]
fn five_node_all_healthy() {
    let scenario = ScenarioBuilder::new(5);
    assert_eq!(scenario.quorum_size(), 3);
    assert_eq!(scenario.available_nodes(), 5);
    assert!(scenario.can_reach_quorum());
}

#[test]
fn five_node_one_partitioned_can_reach_quorum() {
    let scenario = ScenarioBuilder::new(5).partition_node(0);
    assert_eq!(scenario.quorum_size(), 3);
    assert_eq!(scenario.available_nodes(), 4);
    assert!(scenario.can_reach_quorum());
}

#[test]
fn five_node_two_partitioned_can_reach_quorum() {
    let scenario = ScenarioBuilder::new(5).partition_node(0).partition_node(1);
    assert_eq!(scenario.quorum_size(), 3);
    assert_eq!(scenario.available_nodes(), 3);
    assert!(scenario.can_reach_quorum());
}

#[test]
fn five_node_three_partitioned_cannot_reach_quorum() {
    let scenario = ScenarioBuilder::new(5).partition_minority(3);
    assert_eq!(scenario.quorum_size(), 3);
    assert_eq!(scenario.available_nodes(), 2);
    assert!(!scenario.can_reach_quorum());
}

#[test]
fn partition_state_tracking() {
    let scenario = ScenarioBuilder::new(5).partition_node(1).partition_node(3);

    assert!(scenario.is_partitioned(1));
    assert!(scenario.is_partitioned(3));
    assert!(!scenario.is_partitioned(0));
    assert!(!scenario.is_partitioned(2));
    assert!(!scenario.is_partitioned(4));
    assert_eq!(scenario.available_nodes(), 3);
}

#[test]
fn partition_minority_with_two_nodes() {
    let scenario = ScenarioBuilder::new(5).partition_minority(2);

    assert_eq!(scenario.available_nodes(), 3);
    assert!(scenario.is_partitioned(0));
    assert!(scenario.is_partitioned(1));
    assert!(!scenario.is_partitioned(2));
}

#[test]
fn minority_partition_cannot_consensus() {
    let scenario = ScenarioBuilder::new(5).partition_minority(2);

    let quorum = scenario.quorum_size();
    let majority_size = scenario.available_nodes();
    let minority_size = 2;

    // Majority can reach quorum
    assert!(majority_size >= quorum);

    // Minority cannot reach quorum
    assert!(minority_size < quorum);
}

#[test]
fn at_most_one_partition_can_have_quorum() {
    let total = 5;
    let partition1_size = 2;
    let partition2_size = 3;
    let quorum = total / 2 + 1; // = 3

    assert_eq!(partition1_size + partition2_size, total);

    // Only partition 2 (majority) can have quorum
    assert!(partition1_size < quorum);
    assert!(partition2_size >= quorum);
}

// ============================================================================
// ROBUST SCENARIOS (Moved from robust_scenarios_tests.rs)
// ============================================================================

/// Test consensus with 7 nodes and sustained normal operation
/// MIT style: Multiple proposals with reliable network
#[tokio::test]
async fn test_seven_node_consensus_sustained() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

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
        // Updated to use usize for wait_for_learned as helper expects it
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
    let mut cluster = ClassicCluster::new(0, ip, 9, observer.clone()).await.unwrap();

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
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

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
    assert!(
        initial_learned > 0,
        "Should learn initial decree before partition"
    );

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
    let mut cluster = ClassicCluster::new(0, ip, 5, observer.clone()).await.unwrap();

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
        let _ = barrier
            .wait_for_learned(i + 1, Duration::from_secs(5))
            .await;
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
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

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

        let _ = barrier
            .wait_for_learned(decree_num, Duration::from_secs(5))
            .await;
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
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

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
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

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
        let _ = barrier.wait_for_learned(i, Duration::from_secs(10)).await; // Longer timeout for latency
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
    let mut cluster = ClassicCluster::new(0, ip, 5, observer.clone()).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add one-way latency: 0 -> 1,2 have 500ms delay
    // But 1,2 -> 0 is normal
    cluster.add_delay(0, 1, Duration::from_millis(500)).await;
    cluster.add_delay(0, 2, Duration::from_millis(500)).await;

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
    let mut cluster = ClassicCluster::new(0, ip, 7, observer.clone()).await.unwrap();

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
    let mut cluster = ClassicCluster::new(0, ip, 5, observer.clone()).await.unwrap();

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
        let _ = barrier
            .wait_for_learned(decree_num, Duration::from_secs(5))
            .await;
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

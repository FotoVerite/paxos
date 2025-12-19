use paxos::{
    cluster::cluster::Cluster,
    console_observer::ConsoleObserver,
    paxos_command::PaxosCommand,
};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_consensus_without_failures() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;

    // Propose multiple decrees
    for _ in 0..3 {
        let cmd = PaxosCommand::EnactDecree {
            author: "Socrates".to_string(),
            law: "Test Law".to_string(),
        };
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(300)).await;
    }

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_consensus_with_partition_recovery() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 5, observer).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Phase 1: Normal operation
    let cmd1 = PaxosCommand::AppointArchon {
        name: "Plato".to_string(),
        term_length_years: 5,
    };
    cluster.propose(cmd1).await;
    sleep(Duration::from_millis(300)).await;

    // Phase 2: Create partition (isolate node 0)
    for i in 1..5 {
        cluster.partition(0, i).await;
    }

    let cmd2 = PaxosCommand::BuildAcropolis {
        stones_required: 1000,
        architect: "Ictinus".to_string(),
    };
    cluster.propose(cmd2).await;
    sleep(Duration::from_millis(300)).await;

    // Phase 3: Heal partition
    for i in 1..5 {
        cluster.heal_partition(0, i).await;
    }
    sleep(Duration::from_millis(100)).await;

    // Phase 4: Resume normal operation
    let cmd3 = PaxosCommand::Ostracize {
        citizen: "Meletus".to_string(),
    };
    cluster.propose(cmd3).await;
    sleep(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn test_consensus_survives_packet_loss() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 5, observer).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 30% packet loss between node 0 and node 1
    cluster.add_packet_loss(0, 1, 0.3).await;

    for i in 0..3 {
        let cmd = PaxosCommand::EnactDecree {
            author: format!("Philosopher {}", i),
            law: format!("Law {}", i),
        };
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::test]
async fn test_consensus_with_high_latency() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 200ms latency to all messages from node 0
    cluster
        .add_delay(0, 1, Duration::from_millis(200))
        .await;
    cluster
        .add_delay(0, 2, Duration::from_millis(200))
        .await;

    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;
    sleep(Duration::from_millis(800)).await;
}

#[tokio::test]
async fn test_quorum_still_achievable_with_partition() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 7, observer).await.unwrap();

    for i in 0..7 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
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
    sleep(Duration::from_millis(500)).await;

    // Heal the partition
    for i in 1..4 {
        cluster.heal_partition(0, i).await;
    }
    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_repeated_partition_heal_cycles() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 5, observer).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    for cycle in 0..3 {
        // Create partition
        cluster.partition(0, 2).await;
        cluster.partition(1, 3).await;

        let cmd = PaxosCommand::EnactDecree {
            author: format!("Cycle {}", cycle),
            law: "Partition Law".to_string(),
        };
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(300)).await;

        // Heal partition
        cluster.heal_partition(0, 2).await;
        cluster.heal_partition(1, 3).await;

        let cmd = PaxosCommand::EnactDecree {
            author: format!("Recovery {}", cycle),
            law: "Recovery Law".to_string(),
        };
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(300)).await;
    }
}

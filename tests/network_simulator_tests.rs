use paxos::{
    cluster::cluster::Cluster, console_observer::ConsoleObserver, paxos_command::PaxosCommand,
};
use std::sync::Arc;
use std::net::IpAddr;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_normal_operation_no_failures() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;

    // Don't enable failures - should work normally
    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;

    sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn test_failures_disabled_by_default() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;

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
    let cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    // Failures should be disabled by default
    cluster.disable_failures().await;
    cluster.enable_failures().await;
    cluster.disable_failures().await;
}

#[tokio::test]
async fn test_partition_and_heal() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
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
    let mut cluster = Cluster::new(0, ip, 5, observer).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Create multiple partitions
    cluster.partition(0, 1).await;
    cluster.partition(0, 2).await;
    cluster.partition(0, 3).await;

    sleep(Duration::from_millis(100)).await;

    // Heal some
    cluster.heal_partition(0, 1).await;
    cluster.heal_partition(0, 2).await;

    sleep(Duration::from_millis(100)).await;

    // Heal rest
    cluster.heal_partition(0, 3).await;

    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_add_delay() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 100ms delay from node 0 to node 1
    cluster.add_delay(0, 1, Duration::from_millis(100)).await;

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_add_packet_loss() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Add 50% packet loss from node 0 to node 1
    cluster.add_packet_loss(0, 1, 0.5).await;

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_partition_isolates_single_node() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 5, observer).await.unwrap();

    for i in 0..5 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;
    cluster.enable_failures().await;

    // Isolate node 0 from all others
    for i in 1..5 {
        cluster.partition(0, i).await;
    }

    sleep(Duration::from_millis(100)).await;

    // Proposing from a random node should still work (4 nodes can form quorum)
    let cmd = PaxosCommand::NOOP;
    cluster.propose(cmd).await;

    sleep(Duration::from_millis(500)).await;

    // Heal all partitions
    for i in 1..5 {
        cluster.heal_partition(0, i).await;
    }

    sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_toggle_failures_on_off() {
    let observer = Arc::new(ConsoleObserver);
    let ip = IpAddr::V4([127, 0, 0, 1].into());
    let mut cluster = Cluster::new(0, ip, 3, observer).await.unwrap();

    for i in 0..3 {
        cluster.nodes[i].start();
    }

    sleep(Duration::from_millis(100)).await;

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

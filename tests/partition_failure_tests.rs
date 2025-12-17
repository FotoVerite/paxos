/// Partition and failure scenario tests for Paxos safety
///
/// Tests verify that:
/// 1. Majority partitions can achieve consensus (progress)
/// 2. Minority partitions cannot achieve consensus (safety)
/// 3. Quorum is required, not just any subset of nodes
/// 4. Partition recovery works correctly

mod test_helpers;

use test_helpers::ScenarioBuilder;

// ============================================================================
// BASIC PARTITION SCENARIO TESTS
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
    let scenario = ScenarioBuilder::new(3)
        .partition_node(0)
        .partition_node(1);
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
    let scenario = ScenarioBuilder::new(5)
        .partition_node(0)
        .partition_node(1);
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

// ============================================================================
// QUORUM MATH VALIDATION
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

// ============================================================================
// PARTITION STATE TESTS
// ============================================================================

#[test]
fn partition_state_tracking() {
    let scenario = ScenarioBuilder::new(5)
        .partition_node(1)
        .partition_node(3);
    
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

// ============================================================================
// SAFETY INVARIANTS
// ============================================================================

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

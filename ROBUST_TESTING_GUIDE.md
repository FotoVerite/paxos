# Robust Testing Guide: MIT 6.5840 Inspired Methodology

This guide describes the robust testing approach used for Paxos, inspired by MIT 6.5840 (Distributed Systems) Raft testing methodology.

## Testing Philosophy

MIT 6.5840 emphasizes:
1. **Multiple scales**: Test with 3, 5, 7, 9 node clusters
2. **Extended durations**: Partitions should last longer than election timeouts
3. **High message volume**: Many concurrent proposals with failures
4. **Realistic failure modes**: Asymmetric failures, transient packet loss, cascading failures
5. **Recovery validation**: Verify systems can catch up after extended offline periods
6. **Performance awareness**: Monitor RPC counts and latency impact

## Test Categories

### Category 1: Basic Consensus (3-5 nodes)

**Purpose**: Verify basic Paxos correctness

Tests:
- `test_normal_operation_no_failures` - Reliable network baseline
- `test_consensus_without_failures` - Multiple proposals
- `test_consensus_with_partition_recovery` - Simple partition scenario

**Characteristics**:
- 3-5 nodes
- 2-3 proposals
- Short wait times (500ms)
- Clean failure modes (complete partition)

### Category 2: Cluster Scaling (7-9 nodes)

**Purpose**: Verify behavior with larger clusters and higher fault tolerance

Tests:
- `test_seven_node_consensus_sustained` - 7 nodes, 10 proposals
- `test_nine_node_consensus` - 9 nodes (quorum=5), multiple decrees
- `test_minority_partition_seven_nodes` - 3-node minority can't reach quorum

**Characteristics**:
- 7-9 node clusters
- 5-10 proposals per test
- Higher quorum requirements (4-5 nodes)
- Validates fault tolerance windows

### Category 3: Extended Partitions (MIT Style)

**Purpose**: Test behavior during realistic, extended network splits

Tests:
- `test_extended_partition_five_nodes` - 2-second partition duration
- `test_rolling_failures_seven_nodes` - Rolling failures (one at a time)
- `test_multiple_overlapping_partitions` - Complex failure patterns

**Characteristics**:
- Partitions last 500ms - 2+ seconds (longer than election timeout)
- Multiple simultaneous failures
- Activity during partition (majority continues)
- Gradual healing
- Validation of minority partition behavior

**Key MIT Insight**: Partitions should last long enough that:
- Election timeouts expire (150-300ms, adjusted for test constraints)
- New leaders may be elected in isolated partitions
- Message retransmission occurs
- Log divergence happens

### Category 4: Latency and Packet Loss

**Purpose**: Test behavior with unreliable networks

Tests:
- `test_high_latency_seven_nodes` - 300ms latency, all links
- `test_asymmetric_latency` - One-way delays (more realistic)
- `test_transient_packet_loss` - 30% packet loss on specific nodes

**Characteristics**:
- 7-node clusters
- Latency: 300-500ms per hop
- Packet loss: 20-30%
- Longer wait times (600-800ms between proposals)
- Validates retransmission logic

### Category 5: Recovery and Churn (Most Demanding)

**Purpose**: Validate recovery from extended failures

Tests:
- `test_recovery_from_extended_offline` - 2-second offline period
- `test_rolling_failures_seven_nodes` - Sequential failures/recovery
- MIT "Figure 8" scenario: Complex log divergence and repair

**Characteristics**:
- 2-4 second extended offline periods
- 4+ proposals during isolation
- Verification that offline nodes catch up
- Continued operation in majority partition

## Key MIT 6.5840 Testing Insights

### 1. RPC Counting

MIT tests measure and report:
- Total RPCs sent during test
- Total bytes transmitted
- Number of committed entries

Our equivalent: Monitor via console output

### 2. Election Timeout Constraints

MIT enforces:
- Heartbeat rate: ≤ 10 per second (100ms minimum)
- Leader election: ≤ 5 seconds after failure
- Election timeout range: 150-300ms (adjusted for test speed)

For Paxos with similar constraints:
- Partition durations: 200ms - 2+ seconds
- Multiple election cycles can occur during partition
- Healing should be gradual to allow recovery

### 3. Performance Metrics

Expected behavior:
- **Small cluster (3 nodes)**: < 1000 RPCs for 5 proposals
- **Medium cluster (5 nodes)**: < 2000 RPCs for 10 proposals
- **Large cluster (7+ nodes)**: Scales with cluster size

### 4. Failure Patterns Tested

MIT covers:
- **Clean failures**: Node completely offline
- **Partition failures**: Network split
- **Asymmetric failures**: One-way communication loss
- **Transient failures**: Intermittent packet loss
- **Cascading failures**: Multiple simultaneous issues

All of these should still allow progress if majority is reachable.

## Writing Robust Tests

### Pattern 1: Extended Partition

```rust
#[tokio::test]
async fn test_extended_partition() {
    let cluster = setup_cluster(7).await;
    
    // Normal operation baseline
    propose_and_verify(1).await;
    
    // Create partition: isolate node 0
    for i in 1..7 {
        cluster.partition(0, i).await;
    }
    sleep(Duration::from_millis(200)).await;
    
    // Activity during partition (in majority)
    for i in 0..3 {
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(400)).await;
    }
    
    // Extended offline period
    sleep(Duration::from_secs(2)).await;
    
    // Heal and verify recovery
    for i in 1..7 {
        cluster.heal_partition(0, i).await;
    }
    sleep(Duration::from_millis(300)).await;
    
    // Verify system is functional
    cluster.propose(recovery_cmd).await;
    sleep(Duration::from_millis(500)).await;
}
```

### Pattern 2: Rolling Failures

```rust
#[tokio::test]
async fn test_rolling_failures() {
    let cluster = setup_cluster(7).await;
    
    for failed_node in 0..7 {
        // Isolate one node at a time
        for other in 0..7 {
            if other != failed_node {
                cluster.partition(failed_node, other).await;
            }
        }
        
        sleep(Duration::from_millis(300)).await;
        
        // Propose while node is down
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(300)).await;
        
        // Recover node
        for other in 0..7 {
            if other != failed_node {
                cluster.heal_partition(failed_node, other).await;
            }
        }
        
        sleep(Duration::from_millis(200)).await;
    }
}
```

### Pattern 3: Latency Testing

```rust
#[tokio::test]
async fn test_latency_impact() {
    let cluster = setup_cluster(7).await;
    
    // Add symmetric latency
    for from in 0..7 {
        for to in 0..7 {
            if from != to {
                cluster.add_delay(from, to, Duration::from_millis(300)).await;
            }
        }
    }
    
    // Need longer waits for slow network
    for i in 0..5 {
        cluster.propose(cmd).await;
        sleep(Duration::from_millis(800)).await; // 300ms latency * 2 directions + margin
    }
}
```

## Validation Checklist

When implementing robust tests:

- [ ] Test sizes: 3, 5, 7, 9 nodes
- [ ] Message volume: 5-10 proposals per test
- [ ] Partition duration: 200ms - 2+ seconds
- [ ] Extended offline: At least 2 seconds
- [ ] Recovery validation: Verify functionality after healing
- [ ] Asymmetric failures: One-way latency/loss
- [ ] Transient failures: 20-30% packet loss
- [ ] Performance monitoring: Track behavior without assertions
- [ ] Repeatability: Run tests multiple times

## Running Robust Tests

```bash
# Run all robust tests
cargo test --test robust_scenarios_tests

# Run specific test
cargo test --test robust_scenarios_tests test_extended_partition_five_nodes

# Run with verbose output
cargo test --test robust_scenarios_tests -- --nocapture

# Run multiple times to check consistency
for i in {1..5}; do cargo test --test robust_scenarios_tests; done
```

## Expected Test Duration

- Basic tests (3-5 nodes): 1-3 seconds each
- Scale tests (7 nodes): 3-8 seconds each
- Extended partition tests: 5-10 seconds each
- High latency tests: 8-12 seconds each
- Full robust suite: ~14-15 seconds

Total test suite should complete in < 2 minutes.

## MIT 6.5840 Key Principles Applied

1. **No assertions in runner**: Tests just execute scenarios, assertions are implicit in functionality
2. **Message counting**: Watch for RPCs to diagnose inefficiencies
3. **Extended timeouts**: Partitions last longer than recovery times
4. **Majority progress**: System makes progress as long as majority is reachable
5. **Safety over liveness**: Correctness under adversity is more important than speed
6. **Realistic failures**: Test asymmetric, transient, and cascading failures
7. **Recovery validation**: Always verify system is functional after failure

## References

- MIT 6.5840 (now 6.5840): https://pdos.csail.mit.edu/6.824/
- Raft paper: https://raft.github.io/
- MIT Lab 3: Raft implementation requirements
- Extended Raft paper: Snapshot and log compaction strategies

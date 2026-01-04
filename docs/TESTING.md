# Testing Guide

## Quick Start

```bash
# Run all 255 tests
cargo test --tests

# Run cluster tests only
cargo test --test paxos_consensus_tests
cargo test --test robust_scenarios_tests

# Run with output
cargo test --test FILE -- --nocapture
```

## EventBarrier Testing Framework

Instead of arbitrary sleeps, tests wait for **actual events**.

### Before (Bad)
```rust
cluster.propose(cmd).await;
tokio::time::sleep(Duration::from_secs(5)).await;  // Could timeout, flaky
assert!(learned >= 1);
```

### After (Good)
```rust
cluster.propose(cmd).await;
barrier.wait_for_learned(0, Duration::from_secs(5)).await?;  // Fast, deterministic
observer.wait_for_events().await;
assert!(learned >= 1);
```

## Test Pattern

Every cluster test follows this structure:

```rust
#[tokio::test]
async fn test_scenario() {
    // 1. Create observer & barrier
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    
    // 2. Create cluster with observer (IMPORTANT!)
    let mut cluster = Cluster::new(0, N, observer.clone()).await?;
    
    // 3. Start nodes
    for i in 0..N { cluster.nodes[i].start(); }
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // 4. Operate
    cluster.propose(cmd).await;
    
    // 5. Wait for result
    let _ = barrier.wait_for_learned(decree_num, Duration::from_secs(5)).await;
    
    // 6. Verify async completion
    observer.wait_for_events().await;
    
    // 7. Assert
    let learned = observer.count_decrees_learned().await;
    assert!(learned >= 1);
}
```

## EventBarrier API

```rust
// Wait for specific decree to be learned
barrier.wait_for_learned(decree_num, timeout).await?

// Wait for N events matching predicate
barrier.wait_for(|e| matches!(e, Event::Proposal { .. }), count, timeout).await?

// Count events matching predicate (non-blocking)
barrier.count_matching(|e| matches!(e, Event::LearnedValue { .. })).await

// Get all recorded events
barrier.get_events().await

// Clear event log
barrier.clear().await
```

## Timeout Selection

| Scenario | Timeout |
|----------|---------|
| Normal consensus | 5s |
| High latency (300ms+) | 10s |
| Packet loss (30%) | 5s |
| Recovery/offline | 5s |

## Writing a New Test

1. **Choose the file**:
   - Cluster tests → `tests/paxos_consensus_tests.rs`
   - Complex scenarios → `tests/robust_scenarios_tests.rs`
   - Edge cases → `tests/edge_case_tests.rs`

2. **Use the pattern** from above

3. **Pick appropriate timeout** (see table above)

4. **Run and verify**:
   ```bash
   cargo test --test FILE test_name -- --nocapture
   ```

## Pre-Commit Validation

Before committing:

```bash
# Run all tests
cargo test --tests

# Verify count
cargo test --tests 2>&1 | grep "passed; 0 failed"

# Check for sleep calls (should be empty)
grep -r "sleep(" tests/*.rs
```

## Common Issues & Solutions

**Test times out**:
- Increase timeout: `Duration::from_secs(10)`
- Verify observer passed: `Cluster::new(0, N, observer.clone())`
- Check events: `barrier.get_events().await` in test output

**No events recorded**:
- Did you extract barrier? `let barrier = observer.barrier.clone();`
- Did you pass observer to cluster? `Cluster::new(0, N, observer.clone())`

**Test fails but logs show consensus**:
- Async tasks might not have completed
- Call: `observer.wait_for_events().await` before assertions

## What's Tested

✅ **Basic Consensus**
- Single & multi-node (3-9 nodes)
- Sequential proposals
- Multiple decrees

✅ **Network Failures**
- Symmetric partitions
- Asymmetric latency
- Packet loss (up to 30%)
- Extended offline periods
- Rolling failures

✅ **Edge Cases**
- Out-of-order messages
- Duplicate messages
- Conflicting proposals
- Sparse decree numbering

✅ **Protocol Correctness**
- Ballot ordering
- Quorum detection
- Promise/Accept protocol
- Tie-breaking

✅ **Persistence**
- Ledger storage
- Recovery on restart
- Gap handling

## Test Statistics

- **Total**: 255 tests
- **Pass Rate**: 100%
- **Execution Time**: ~130 seconds
- **Coverage**: All major protocol paths

## Debugging

Print events during test:

```rust
let events = barrier.get_events().await;
eprintln!("Events: {}", events.len());
for event in &events {
    eprintln!("  {:?}", event);
}
```

Run with logs:

```bash
RUST_LOG=debug cargo test --test FILE test_name -- --nocapture
```

## Performance Notes

- EventBarrier tests: 5-50x faster than sleep-based
- Average cluster test: 0.5-3 seconds
- Tests scale well to 7-9 node clusters

## Coverage Gaps (Low Priority)

Not tested:
- Byzantine faults (assumed honest nodes)
- Clock skew
- Resource exhaustion
- Performance benchmarks

These are nice-to-haves but not critical for correctness.

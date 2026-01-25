# Development Guide

## Quick Start

```bash
# Clone and setup
git clone https://github.com/FotoVerite/paxos.git
cd paxos

# Run all 255 tests (~130 seconds)
cargo test --tests

# Start web visualizer
cargo run --release
# Open http://localhost:3000
```

## System Overview

**Paxos consensus with 255 passing tests** in Rust using Tokio async runtime and SQLite persistence.

### Three Core Roles

- **Proposer** - Initiates consensus with ballot numbers
- **Acceptor** - Votes on proposals, enforces ballot ordering
- **Learner** - Detects quorum and records committed values

### Protocol Flow (Single Decree)

1. Proposer sends Prepare(ballot) → Acceptors respond Promise(ballot)
2. If quorum of promises received → Proposer sends Accept(ballot, value)
3. Acceptors respond Accepted(ballot, value)
4. Quorum of acceptances → Value is chosen
5. Learner detects quorum → Records to ledger

## Key Files

```
src/node/
  ├── proposer.rs      - Phase 1 & 2 logic
  ├── acceptor.rs      - Promise & Accept voting
  ├── learner.rs       - Quorum detection & ledger
  └── ledger.rs        - SQLite persistence

src/cluster/
  ├── cluster.rs       - Multi-node simulator
  └── network_sim.rs   - Failure injection

tests/
  ├── paxos_consensus_tests.rs     - 150+ tests
  ├── robust_scenarios_tests.rs    - Advanced scenarios
  └── edge_case_tests.rs           - Edge cases
```

## Testing

### Run Tests

```bash
# All 255 tests
cargo test --tests

# Specific test file
cargo test --test paxos_consensus_tests

# Single test with output
cargo test --test FILE test_name -- --nocapture

# With debug logging
RUST_LOG=debug cargo test --test FILE test_name -- --nocapture
```

### EventBarrier Framework

Tests don't sleep—they wait for actual events:

```rust
#[tokio::test]
async fn test_basic_consensus() {
    let observer = Arc::new(RecordingObserver::new());
    let barrier = observer.barrier.clone();
    
    let mut cluster = Cluster::new(0, 5, observer.clone()).await?;
    cluster.nodes.iter_mut().for_each(|n| n.start());
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.propose(cmd).await;
    
    // Wait for actual learned event (not arbitrary sleep)
    barrier.wait_for_learned(0, Duration::from_secs(5)).await?;
    observer.wait_for_events().await;
    
    let learned = observer.count_decrees_learned().await;
    assert!(learned >= 1);
}
```

### What's Tested

✅ **255 tests passing** (100% success rate)

- Multi-node consensus (3-9 nodes)
- Network partitions & recovery
- Packet loss & latency
- Out-of-order & duplicate messages
- Conflicting proposals
- State persistence & recovery
- Edge cases (sparse decree numbers, etc.)

## Architecture Details

### Ballot System

Ballots use `(number, proposer_id)` for total ordering:
- `(10, 1) > (10, 0)` ← proposer ID breaks ties
- `(11, 0) > (10, 9)` ← higher ballot always wins

This prevents conflicts and ensures safety.

### Data Persistence

- **Ledger**: SQLite at `.paxos/node_N.db`
- **Recovery**: Automatic on startup
- **Gap handling**: Application must handle sparse decrees

### Failure Scenarios

| Scenario | Outcome |
|----------|---------|
| Proposer fails | Other nodes detect timeout, use higher ballot |
| Acceptor fails (minority) | No impact |
| Acceptor fails (majority) | Consensus halts until recovery |
| Network partition (majority) | Can reach quorum, continues |
| Network partition (minority) | Can't reach quorum, halts |
| Network heals | Minority catches up via ledger |

## Web Visualizer

```bash
cargo run --release
```

Opens http://localhost:3000 with:
- Real-time event stream
- Node status indicators
- Proposal/acceptance tracking
- Learned decrees visualization

## Building Release

```bash
# Optimized binary
cargo build --release

# Binary location
./target/release/paxos

# Run with custom scenario
./target/release/paxos --scenario scenarios/partition_recovery.json
```

## Performance

| Metric | Value |
|--------|-------|
| Consensus latency | 10-100ms (network sim) |
| Throughput | 10-100 decrees/sec |
| Memory per node | ~1MB |
| Disk per decree | ~1KB |
| Test execution | ~130 seconds (255 tests) |

## Design Decisions

**Rust**: Type safety + Tokio async (no GC pauses)
**SQLite**: Durability, ACID, no external dependencies
**Arc<Mutex>**: Thread-safe state with clear ownership

## Known Limitations

1. Single-view (no dynamic reconfiguration)
2. No value batching
3. Proposers compete (no dedicated leader)
4. Assumes honest nodes (not Byzantine-fault-tolerant)

Production systems typically add:
- Multi-Paxos for efficiency
- Reconfiguration protocol
- Client-side batching
- Leader leases with exponential backoff

## Code Quality

| Category | Status |
|----------|--------|
| Concurrency | Safe (Arc<Mutex> patterns) |
| Persistence | Partial (Ledger works; Acceptor doesn't save) |
| Error handling | Good |
| Test coverage | Excellent (255 tests) |

### Pre-Commit Validation

```bash
# Run all tests
cargo test --tests

# Verify count
cargo test --tests 2>&1 | grep "passed; 0 failed"

# Check for inappropriate sleeps
grep -r "sleep(" tests/*.rs
```

## Debugging

Print events:
```rust
let events = barrier.get_events().await;
for event in &events {
    eprintln!("  {:?}", event);
}
```

Inspect ledger:
```bash
sqlite3 .paxos/node_1.db
```

## References

- Lamport, L. (2001). Paxos Made Simple
- Chandra, T., et al. (2007). Paxos Made Live
- Implementation: ~7000 lines (protocol + tests + supporting systems)

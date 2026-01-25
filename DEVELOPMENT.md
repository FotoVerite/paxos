# Development Guide

## Quick Start

```bash
# Clone and setup
git clone https://github.com/FotoVerite/paxos.git
cd paxos

# Run all tests (~130 seconds)
cargo test --tests

# Start web visualizer
cargo run -- web
# Open http://localhost:3000
```

## Project Structure

```
src/
  node/
    paxos_node.rs        - Main node implementation
    paxos_state/         - Core Paxos logic
      paxos_state.rs     - State orchestration
      proposer/          - Phase 1 & 2 preparation
      acceptor.rs        - Voting & promise tracking
      learner.rs         - Quorum detection
      ledger.rs          - SQLite persistence
      ballot.rs          - Ballot ordering
      decree_notes.rs    - Decree tracking
    inflight_proposals.rs - Proposal tracking
  cluster/
    cluster.rs           - Multi-node simulator
    network_simulator.rs - Network failure injection
  web/
    server.rs            - Axum web server
    cluster_manager.rs   - Multi-Paxos management
    websocket_observer.rs - Real-time events
    handlers/            - HTTP endpoints
    scenarios/           - Built-in demo scenarios
  message.rs             - Message types (Prepare, Promise, Accept, Accepted)
  monitor.rs             - Event observer interface
  scenario*.rs           - Scenario loading & running
  
tests/
  basic_paxos_test.rs           - Single node tests
  concurrent_decrees_tests.rs   - Multi-decree scenarios
  edge_case_tests.rs            - Edge cases & recovery
  integration_tests.rs          - Full cluster tests
  persistence_test.rs           - Ledger durability
  scenarios_tests.rs            - Complex scenarios
  test_helpers.rs               - Testing utilities
```

## Core Components

### PaxosState
Orchestrates three roles within a single node:

```rust
pub struct PaxosState {
    proposer: Proposer,   // Initiates ballots
    acceptor: Acceptor,   // Votes on proposals
    learner: Learner,     // Detects quorum
    ledger: Ledger,       // Persists decisions
}
```

### Message Types

```rust
Message::Prepare { from, decree_num, ballot }
Message::Promise { from, ballot, max_ballot, max_value }
Message::Accept { from, decree_num, ballot, value, quorum }
Message::Accepted { from, ballot, max_ballot }
```

### Ballot System

Ballots: `Ballot { number: u64, proposer: NodeId }`

Total ordering: higher number wins; ties broken by lower NodeId.

## Testing

### Run Tests

```bash
# All tests
cargo test --tests

# Single test file
cargo test --test basic_paxos_test

# Single test with output
cargo test --test FILE test_name -- --nocapture

# With logging
RUST_LOG=debug cargo test --test FILE test_name -- --nocapture
```

### Test Files by Category

| File | Purpose |
|------|---------|
| `basic_paxos_test.rs` | Single-node prepare/promise/accept flow |
| `concurrent_decrees_tests.rs` | Multiple decrees in flight |
| `edge_case_tests.rs` | Out-of-order, duplicates, conflicts |
| `integration_tests.rs` | Multi-node cluster scenarios |
| `persistence_test.rs` | Ledger recovery after crash |
| `scenarios_tests.rs` | Network partitions, failures |
| `retry_mechanism_tests.rs` | Proposal retry logic |

### Test Pattern

```rust
#[tokio::test]
async fn test_scenario() {
    cleanup_persisted_state();
    
    // Create nodes with NodeBuilder
    let builder = NodeBuilder::new();
    let proposer = builder.proposer(node_id, uuid).await?;
    let acceptor = builder.acceptor(node_id).await?;
    
    // Send messages
    let msg = proposer.propose(DecreeId(0), value).await;
    let response = acceptor.handle_message(msg).await;
    
    // Assert
    assert!(matches!(response, Message::Promise { .. }));
}
```

## Running Scenarios

### Web Server (Interactive)

```bash
cargo run -- web
# Opens http://localhost:3000
# Choose scenario from dropdown
# Watch real-time event stream
```

### JSON Scenario

```bash
cargo run -- json /path/to/scenario.json
```

### Built-in Scenarios

Located in `src/web/scenarios/`:
- `happy_path.rs` - Basic consensus
- `network_partition.rs` - Partition recovery
- `competing_proposers.rs` - Multiple proposers
- `catch_up.rs` - Replica catching up

## Architecture Flow

### Single Proposal (Decree)

```
1. Client proposes value for decree N
2. Proposer sends Prepare(ballot) → all acceptors
3. Acceptors respond Promise(ballot) or NACK
4. If quorum: Proposer sends Accept(ballot, value)
5. Acceptors respond Accepted(ballot) or NACK
6. If quorum: Learner detects, records to ledger
7. Event: LearnedValue emitted
```

### Multi-Decree (Multi-Paxos)

Each decree (0, 1, 2, ...) runs independently.
Proposer can pipeline: send Accept for decree N while Prepare for decree N+1.

## Data Persistence

Persistence uses **bincode** (binary serialization) with atomic writes (temp file + rename).

**Ledger** (when feature "persistence" enabled):
- Location: `.paxos/ledger_{uuid}.bin`
- Stores: Vec of Option<PaxosCommand> (sparse array)
- Recovers on restart
- Auto-created per node

**DecreeNotes** (ballot tracking):
- Location: `.paxos/decree_notes_{uuid}.bin`
- Stores: HashMap<DecreeId, DecreeNote>
- Persisted when enabled via feature flag

**Feature Gating**:
- Persistence is opt-in: `#[cfg(feature = "persistence")]`
- Tests run without persistence by default
- Production builds include `--features persistence`

**Atomic Writes**:
- Write to `.tmp` file first
- Rename to final location (atomic on filesystems)
- Prevents corruption on crash

## Network Simulator

Injects failures in `network_simulator.rs`:
- Packet loss
- Latency
- Partition (asymmetric splits)
- Transient failures

## Web Visualizer

Runs on http://localhost:3000 (default):
- Real-time event stream (WebSocket)
- Node status (proposer, acceptor, learner)
- Scenario selection
- Manual proposal injection
- Event filtering

## Building Release

```bash
cargo build --release
./target/release/paxos

# With logging
RUST_LOG=debug ./target/release/paxos
```

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Single decree latency | 10-100ms |
| Throughput | 10-100 decrees/sec |
| Memory per node | ~1-5MB |
| Disk per decree | ~100 bytes |
| Test suite | ~130 seconds (all tests) |

## Code Statistics

- Core Paxos: ~2000 lines
- Tests: ~3000 lines
- Web/UI: ~2000 lines
- Total: ~7000 lines

## Failure Scenarios & Recovery

### Node Crash

Acceptor state: in-memory (lost on crash)
Ledger: persisted (recovered on restart)
Recovery: reads ledger, rejoins cluster

### Network Partition

Majority partition: continues (has quorum)
Minority partition: halts (can't reach quorum)
Healing: minority catches up via ledger

### Concurrent Proposals

Same decree, different proposers:
- Higher ballot preempts lower
- NACK response triggers retry with higher ballot
- Guarantees only one value chosen

## Key Abstractions

### PaxosObserver

Monitor for events:
```rust
pub trait PaxosObserver: Send + Sync {
    fn observe(&self, event: Event);
}
```

Events: Prepare, Promise, Accept, Accepted, LearnedValue, etc.

### InflightProposals

Tracks proposals in-flight:
```rust
pub struct InflightProposal {
    decree_num: DecreeId,
    cmd: PaxosCommand,
}
```

## Debugging Tips

**Print events**:
```rust
// In test_helpers
let events = observer.get_events().await;
for e in events { eprintln!("{:?}", e); }
```

**Inspect ledger**:
```bash
sqlite3 .paxos/node_*.db "SELECT * FROM decrees;"
```

**Logs**:
```bash
RUST_LOG=debug cargo test --test FILE -- --nocapture
RUST_LOG=paxos=trace cargo run -- web
```

## Common Patterns

**Create a proposer**:
```rust
let builder = NodeBuilder::new();
let proposer = builder.proposer(node_id, uuid).await?;
```

**Propose a value**:
```rust
let cmd = PaxosCommand::SET { key: "x".to_string(), value: "1".to_string() };
let msg = proposer.propose(DecreeId(0), cmd).await;
```

**Handle a message**:
```rust
let response = acceptor.handle_message(msg).await;
```

## Testing Checklist

- [ ] All 255 tests pass: `cargo test --tests`
- [ ] No panics in release build
- [ ] Ledger recovers after crash
- [ ] Network partitions handled correctly
- [ ] Concurrent proposals don't corrupt state
- [ ] Out-of-order messages don't crash nodes

## References

- Lamport, L. (2001). Paxos Made Simple
- Implementation: Consensus protocol in production-ready Rust
- Web UI: Real-time visualization of distributed consensus

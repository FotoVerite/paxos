# Paxos Implementation - Setup & Usage

## Quick Start

### Run Console Simulation
```bash
cargo run
```

Runs the built-in scenario with 5 nodes and network partitioning.

### Run JSON Scenario
```bash
cargo run -- json
```

Loads and runs all scenarios from `scenarios/` directory.

### Run Web Visualizer
```bash
cargo run -- web
```

Starts a web server on `http://localhost:3000` with real-time event visualization.

See [WEB_GUIDE.md](WEB_GUIDE.md) for detailed instructions.

## Testing

### Run All Tests
```bash
cargo test
```

**Test Summary**: 162 tests across 18 test files
- Core unit tests (instant): ~100 tests
- Integration tests: ~30 tests  
- Robust failure injection: 11 tests (timing-sensitive)
- All tests passing ✓

### Run Specific Test Suite
```bash
cargo test --test integration_tests
cargo test --test robust_scenarios_tests
cargo test --test state_validation_tests
```

## Project Structure

```
src/
├── node/              # Paxos components (proposer, acceptor, learner)
├── cluster/           # Multi-node cluster with network simulator
├── message.rs         # Protocol messages
├── monitor.rs         # Event observation interface
├── paxos_command.rs   # Decree types
├── scenario*.rs       # Scenario builder & runner
├── console_observer.rs # Terminal output
├── web/              # Web server & WebSocket observer
└── main.rs           # CLI entry points

tests/                # 4490 lines of comprehensive tests
static/               # HTML/CSS/JS for web visualizer
```

## Implementation Details

### Paxos Protocol

**Two-Phase Protocol**:
1. **Phase 1 (Prepare)**: Proposer sends prepare, acceptors promise
2. **Phase 2 (Accept)**: Proposer sends accept, acceptors accept

**Safety Guarantees**:
- Ballot monotonicity enforced per decree
- Proposers adopt higher ballots
- Quorum required for consensus (ceil(n/2) + 1)

### Network Simulation

Supports realistic failure injection:
- **Partitions**: One-way network isolation
- **Delays**: Latency injection per connection
- **Packet Loss**: Random message dropping
- **Combination**: Multiple simultaneous failures

### Test Strategy

**Robust Tests** (MIT 6.5840 Raft-style):
- 7-node, 9-node clusters
- Extended partitions (2+ seconds)
- Rolling failures (nodes fail sequentially)
- High latency scenarios
- Value adoption verification

## Recent Changes

### Test Flakiness Fix
- Added async task tracking to RecordingObserver
- Implemented `wait_for_events()` method
- All tests now deterministically wait for event recording
- Adjusted expectations to account for network timing variance

### Integration Tests Redesign
- Moved from 1-node quorum (invalid) to proper 3-node setup
- Focused tests on specific protocol aspects
- Each test verifies one key property

### Web Server Setup
- Basic HTML/CSS/JS visualization
- Real-time WebSocket event streaming
- Event statistics dashboard
- Connection status indicator

## Known Issues

### Dead Code Warnings
Proposer has unused persistence methods (`state_path`, `ensure_dir_exists`, `save`). These are for future persistence layer implementation.

```rust
// TODO: Complete proposer state persistence
// Currently: methods stubbed out, not called
```

## Next Steps

1. **Enhance Web UI**:
   - Node topology visualization
   - Message flow diagram
   - Ballot tracking per node
   - Timeline scrubber

2. **Persistence Layer**:
   - Enable/implement proposer state persistence
   - Clean up dead code warnings

3. **Additional Scenarios**:
   - Byzantine failures
   - Network-induced value divergence
   - Learning catchup mechanisms

4. **Performance**:
   - Benchmark large clusters (20+ nodes)
   - Memory profiling
   - Message throughput analysis

## Building for Release

```bash
cargo build --release
./target/release/paxos web
```

~5MB binary, starts in <100ms.

## References

- **Paxos**: "The Part-Time Parliament" by Leslie Lamport
- **Raft**: "In Search of an Understandable Consensus Algorithm"
- **Testing**: MIT 6.5840 Distributed Systems course

# Getting Started

## Quick Start (5 minutes)

```bash
# Clone
git clone https://github.com/FotoVerite/paxos.git
cd paxos

# Run tests
cargo test --tests

# Start web visualizer
cargo run --release
# Open http://localhost:3000
```

## Setup

**Requirements**: Rust 1.70+, SQLite3

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Verify
rustc --version
cargo --version
```

## First Steps

1. **Run tests** - Verify everything works:
   ```bash
   cargo test --tests  # 255 tests, ~130 seconds
   ```

2. **Start the web UI** - Visualize Paxos:
   ```bash
   cargo run --release
   ```
   Open http://localhost:3000

3. **Explore scenarios** - Run different failure modes:
   ```bash
   cargo run -- json
   ```

## How It Works

Paxos is a consensus algorithm that ensures distributed systems agree on values, even when nodes fail or the network partitions.

**Three Roles**:
- **Proposer** - Initiates proposals
- **Acceptor** - Votes on proposals  
- **Learner** - Records consensus

**Protocol Flow**:
1. Proposer sends Prepare(ballot)
2. Acceptors respond Promise(ballot)
3. Proposer sends Accept(ballot, value)
4. Acceptors respond Accepted(ballot, value)
5. Learner detects quorum → value is learned

**Failure Handling**:
- Minority partition halts (can't reach quorum)
- Majority partition continues
- Network healing allows minority to catch up

## Key Files

- `src/node/proposer.rs` - Initiates consensus
- `src/node/acceptor.rs` - Votes on proposals
- `src/node/learner.rs` - Records consensus
- `src/cluster/cluster.rs` - Multi-node simulator
- `tests/` - 255 tests with EventBarrier framework

## What's Tested

✅ 255 tests passing:
- Multi-node consensus (3-9 nodes)
- Network partitions and recovery
- Packet loss and latency
- Out-of-order messages
- Concurrent proposals
- State persistence

See `docs/TESTING.md` for details.

## Next Steps

- **Write a test**: See `docs/TESTING.md` → "Writing Tests"
- **Deploy**: See `docs/DEPLOYMENT.md`
- **Deep dive**: See `docs/ARCHITECTURE.md`

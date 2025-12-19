# Quick Start - Paxos Visualizer

## 1. Start Web Server (Recommended)
```bash
cargo run -- web
```
Opens a 5-node Paxos simulation with network partitioning scenario.

## 2. Open Browser
```
http://localhost:3000
```

## 3. Watch in Real-Time
- Green dot = Connected ✓
- See live Paxos events streaming in
- Counters show proposals/promises/accepts/learned

## What You'll See

### Events (Color-Coded)
```
🟪 Proposal  - Proposer initiates decree
🟨 Promise   - Acceptor commits to ballot  
🟥 Accept    - Acceptor accepts value
🟩 Learn     - Consensus achieved!
```

### Scenario Phases
1. **initialization** - Enable failures
2. **normal_operation** - First proposal
3. **create_partition** - Isolate node 0
4. **during_partition** - Second proposal (fails)
5. **heal_partition** - Restore network
6. **recovery** - Third proposal (succeeds)

## Other Commands

### Console Only
```bash
cargo run              # Default scenario, no web
cargo run -- json     # Load scenarios from scenarios/ folder
```

### Testing
```bash
cargo test            # All 162 tests
cargo test --test integration_tests
cargo test --test robust_scenarios_tests
```

### Build for Release
```bash
cargo build --release
./target/release/paxos web
```

## Customizing Scenarios

Edit `src/main.rs` → `run_with_web_server()` function:

```rust
// Add more proposals
.propose(PaxosCommand::EnactDecree {
    author: "Your Name".to_string(),
    law: "Your Decree".to_string(),
})
.wait(Duration::from_millis(500))

// Create partitions
.partition(0, 1)  // Isolate node 0 from node 1
.partition(0, 2)
.wait(Duration::from_millis(500))

// Heal partitions
.heal_partition(0, 1)
```

## Troubleshooting

**WebSocket connection error?**
- Wait 2-3 seconds for server to start
- Check terminal shows "Open http://localhost:3000"
- Refresh browser

**No events appearing?**
- Refresh the page
- Check browser console (F12)
- Verify server is still running in terminal

**Port 3000 already in use?**
- Kill existing process: `lsof -i :3000`
- Or modify server.rs to use different port

## Architecture

```
Browser (localhost:3000)
    ↑↓ WebSocket
Web Server (Axum)
    ↑↓ Events
Paxos Cluster (5 nodes)
    ↓ Network Simulator
Acceptors/Proposers/Learners
```

## Files Modified
- `src/main.rs` - Added `run_with_web_server()` function
- `src/web/server.rs` - Already existed
- `src/web/websocket_observer.rs` - Already existed
- `static/index.html` - Created new dashboard
- `src/lib.rs` - Exported web module

## Next Steps

1. Try different failure scenarios (edit src/main.rs)
2. Add more nodes: change `node_count = 5` to larger number
3. Create custom scenarios using ScenarioBuilder
4. Implement new decree types in PaxosCommand

See `IMPLEMENTATION_SUMMARY.md` for full details.

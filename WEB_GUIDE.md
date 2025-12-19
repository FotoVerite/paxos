# Paxos Web Visualizer

Real-time visualization of the Paxos consensus protocol in action.

## Running the Web Visualizer

### Start the Server

```bash
cargo run -- web
```

This will:
1. Start the web server on `http://localhost:3000`
2. Begin a Paxos simulation with 5 nodes
3. Run a "Partition Recovery" scenario demonstrating failure handling

### Open in Browser

Navigate to:
```
http://localhost:3000
```

## What You'll See

The web interface displays:

### Real-Time Statistics
- **Proposals**: Number of decree proposals made
- **Promises**: Number of acceptor promises received
- **Accepts**: Number of acceptances from acceptors
- **Learned**: Number of decrees consensus was reached on

### Connection Status
- Green dot: WebSocket connected ✓
- Red dot: WebSocket disconnected ✗

### Event Stream
Live feed of all Paxos protocol events:
- **Proposal** (Purple): Proposer initiates proposal
- **Promise** (Yellow): Acceptor promises to accept ballot
- **Accept** (Red): Acceptor accepts value at ballot
- **Learn** (Green): Learner detects consensus achieved

Each event shows:
- Event type
- Node ID
- Decree number
- Ballot number (for promises/accepts)
- Value being proposed/accepted

## Architecture

### Backend
- **WebSocketObserver**: Catches all Paxos events and broadcasts via WebSocket
- **Web Server** (Axum): Serves HTML and manages WebSocket connections
- **Scenario Runner**: Executes Paxos simulation with failure injection

### Frontend
- **Pure JavaScript**: No frameworks, vanilla DOM manipulation
- **WebSocket Client**: Real-time event streaming
- **Responsive Design**: Works on desktop and mobile
- **Color-coded Events**: Visual categorization of protocol events

## Customizing Scenarios

Edit `src/main.rs` in the `run_with_web_server()` function to change:
- Number of nodes
- Failure patterns (partitions, delays, packet loss)
- Proposals to make
- Wait times between operations

### Example: More Proposals

```rust
.propose(PaxosCommand::EnactDecree {
    author: "Your Name".to_string(),
    law: "Your Decree".to_string(),
})
.wait(Duration::from_millis(500))
```

### Example: Partition Specific Nodes

```rust
.partition(0, 1)  // Isolate node 0 from node 1
.partition(0, 2)  // Isolate node 0 from node 2
.wait(Duration::from_millis(500))
```

## Monitoring Terminal Output

While the web visualizer runs, the terminal shows:
- Scenario phases
- Proposed decrees
- Partition/healing operations
- Total execution time

## Performance Notes

- Events are kept in memory (last 50 shown in UI)
- WebSocket buffer: 100 events
- All events are JSON-serialized and broadcast to all connected clients
- Server handles multiple simultaneous browser connections

## Troubleshooting

### WebSocket Connection Error
- Ensure server is running: `cargo run -- web`
- Check browser console: F12 → Console tab
- Verify port 3000 is not in use

### No Events Appearing
- Check that server has started (wait ~2 seconds)
- Refresh browser page
- Check terminal output for errors

### Server Exits Immediately
- Check if port 3000 is already in use
- Run on a different machine/terminal tab

## Next Steps

Future enhancements:
- [ ] Visual node topology/diagram
- [ ] Message flow visualization
- [ ] Timeline scrubber
- [ ] Pause/resume simulation
- [ ] Ballot tracking per node
- [ ] Ledger state inspector
- [ ] Export scenario results

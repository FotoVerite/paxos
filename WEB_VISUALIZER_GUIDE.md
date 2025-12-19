# Paxos Web Visualizer Guide

## Running the Visualizer

```bash
cargo run --release -- web
```

Then open http://localhost:3000 in your browser.

## Features

### Cluster Visualization
- Displays all nodes as interactive circles
- Node numbers shown in the center (0-4, etc.)
- Real-time status display showing node role and ballot number
- Nodes flash green when they participate in events

### Scenario Controls

**Start a Scenario:**
- Set desired node count (3-20)
- Set duration in seconds (10+)
- Click "Start Scenario"
- Cluster automatically initializes and starts consensus operations

**Propose a Decree:**
- Enter author name
- Enter decree content
- Click "Propose"
- The decree will be sent to the cluster for consensus

### Event Dashboard

**Statistics Panel** - Real-time counts:
- Proposals sent
- Promises received
- Accepts received
- Values learned

**Event Log** - Last 50 events with color-coded types:
- 🔵 Proposal (purple border)
- 🟡 Promise (yellow border)
- 🔴 Accept (red border)
- 🟢 Learn (green border)

### Status Indicator
- Green = Connected to WebSocket
- Red = Disconnected

## Architecture

### Backend

**cluster_manager.rs**
- Manages cluster lifecycle
- Stores active cluster in shared Arc<Mutex>
- Spawns background scenario tasks
- Auto-generates proposals every ~2 seconds

**websocket_observer.rs**
- Broadcasts all events to connected clients
- Stores cluster info and sends to new clients
- RwLock for efficient concurrent reads

**server.rs**
- Axum-based HTTP/WebSocket server
- REST API endpoints:
  - `POST /api/start-scenario` - Start new scenario
  - `POST /api/propose` - Submit decree proposal
  - `GET /ws` - WebSocket connection for real-time updates

### Frontend

**Event Handling**
- ClusterInitialized - Initialize visualization
- Event.Proposal/Promise/Accept/Learn - Update stats and log
- Event.NodeState - Update node display state

**Node States**
- Updates every 500ms
- Shows current role and ballot number
- Uses RwLock for non-blocking reads

## Example Scenarios

### Quick Test (30 seconds, 5 nodes)
- Nodes: 5
- Duration: 30s
- Good for seeing basic consensus

### Long Running (10 minutes, 7 nodes)
- Nodes: 7
- Duration: 600s
- Good for observing multiple rounds

### Stress Test (Small cluster, many proposals)
- Nodes: 3
- Duration: 120s
- Manually submit many proposals
- Watch consensus handle high throughput

## API Examples

### Start Scenario
```bash
curl -X POST http://localhost:3000/api/start-scenario \
  -H "Content-Type: application/json" \
  -d '{"node_count": 5, "duration_secs": 120}'
```

### Propose Decree
```bash
curl -X POST http://localhost:3000/api/propose \
  -H "Content-Type: application/json" \
  -d '{
    "author": "Aristotle",
    "decree": "All knowledge is derived from observation"
  }'
```

## Performance Notes

- Broadcast buffer: 500 messages
- Node state updates: Every 500ms
- Event log: Last 50 events displayed
- Auto-cleanup: Keeps frontend responsive
- WebSocket reconnection: Automatic cluster info resync

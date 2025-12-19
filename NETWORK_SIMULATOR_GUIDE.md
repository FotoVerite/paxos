# Network Simulator Guide

The network simulator allows testing of Paxos consensus behavior under various network failure conditions like partitions, latency, and packet loss.

## Architecture

The network simulator is implemented as a wrapper around message passing:

```
PaxosNode -> NetworkSimulator -> [tokio mpsc channels] -> PaxosNode
```

### Key Components

- **NetworkSimulator** (`src/cluster/network_simulator.rs`): Wraps all inter-node communication
- **NetworkFailure** enum: Defines failure modes (Partition, PacketLoss, Delay, None)
- **Cluster methods**: Control failure injection and recovery

## Features

### Failure Types

```rust
pub enum NetworkFailure {
    None,                                // Normal operation
    Delay(Duration),                     // Add latency to messages
    PacketLoss { drop_rate: f32 },      // Drop percentage of messages (0.0-1.0)
    Partition { nodes: HashSet<usize> }, // Prevent communication with specific nodes
}
```

### Enabling/Disabling Failures

```rust
cluster.enable_failures().await;   // Activate failure simulation
cluster.disable_failures().await;  // Deactivate failure simulation
```

When disabled, failures have **zero performance overhead** - just a flag check.

### Partition Control

```rust
// Create bidirectional partition between two nodes
cluster.partition(node1, node2).await;

// Heal the partition
cluster.heal_partition(node1, node2).await;

// Isolate a node from multiple others
for i in 1..5 {
    cluster.partition(0, i).await;
}
```

### Latency Simulation

```rust
// Add 100ms delay from node 0 to node 1
cluster.add_delay(0, 1, Duration::from_millis(100)).await;
```

### Packet Loss Simulation

```rust
// Add 30% packet loss from node 0 to node 1
cluster.add_packet_loss(0, 1, 0.3).await;
```

## Usage Examples

### Basic Partition Test

```rust
let observer = Arc::new(ConsoleObserver);
let mut cluster = Cluster::new(0, 5, observer).await?;

for i in 0..5 {
    cluster.nodes[i].start();
}

cluster.enable_failures().await;
cluster.partition(0, 1).await;  // Node 0 and 1 can't communicate

cluster.propose(PaxosCommand::NOOP).await;

sleep(Duration::from_secs(1)).await;

cluster.heal_partition(0, 1).await;
```

### Partition Recovery Scenario

```rust
// Normal operation
cluster.propose(cmd1).await;
sleep(Duration::from_millis(300)).await;

// Introduce partition
for i in 1..5 {
    cluster.partition(0, i).await;
}
cluster.propose(cmd2).await;
sleep(Duration::from_millis(300)).await;

// Heal partition
for i in 1..5 {
    cluster.heal_partition(0, i).await;
}
sleep(Duration::from_millis(100)).await;

// Resume normal operation
cluster.propose(cmd3).await;
sleep(Duration::from_millis(300)).await;
```

### High Latency Scenario

```rust
cluster.enable_failures().await;

// Add 500ms latency in both directions
cluster.add_delay(0, 1, Duration::from_millis(500)).await;
cluster.add_delay(1, 0, Duration::from_millis(500)).await;

cluster.propose(PaxosCommand::NOOP).await;
sleep(Duration::from_secs(3)).await;  // Give it time
```

## Testing

Run network simulator tests:

```bash
cargo test --test network_simulator_tests
cargo test --test paxos_consensus_tests
cargo test --all
```

### Test Coverage

**network_simulator_tests.rs** (9 tests):
- Normal operation without failures
- Failures disabled by default
- Enable/disable flag toggling
- Partition and healing
- Multiple partitions
- Single node isolation
- Delay injection
- Packet loss injection

**paxos_consensus_tests.rs** (6 tests):
- Consensus without failures
- Consensus with partition recovery
- Consensus survives packet loss
- Consensus with high latency
- Quorum still achievable with partition
- Repeated partition/heal cycles

## Key Design Decisions

1. **Always-on wrapper**: NetworkSimulator wraps all inter-node communication, activated by a flag
2. **Zero overhead when disabled**: Just a mutex lock and boolean check
3. **Per-target configuration**: Failures are configured per (source, target) pair
4. **Directional partitions**: Partition must be set in both directions to be bidirectional
5. **Runtime control**: All failure injection can be modified while cluster is running

## Integration with Paxos Paper Scenarios

The simulator enables testing of scenarios from the Paxos paper:

- **Leader election recovery**: Partition then heal
- **Minority partition**: Isolate nodes that can't reach quorum
- **Network jitter**: Add delays to simulate real networks
- **Message loss**: Test message retransmission behavior
- **Split brain prevention**: Verify quorum requirements prevent conflicting decisions

## Performance Notes

- Disabled failures: negligible overhead (~100ns per message)
- Enabled failures without failures set: minimal overhead (mutex lock + lookup)
- With failures: additional sleeps and message drops as configured
- All failure state is shared via Arc<Mutex<>> for runtime modification

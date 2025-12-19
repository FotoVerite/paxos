# Scenario Framework Guide

The scenario framework allows you to define complex Paxos testing scenarios in code or JSON, run them against a cluster, and observe behavior without assertions in the runner itself.

## Overview

Scenarios consist of:
- **Phases**: Named groups of steps (e.g., "Normal Operation", "Partition Node 0")
- **Steps**: Actions like proposing values, partitioning nodes, adding latency, etc.

Two ways to use scenarios:

1. **Programmatic** (code-based, fluent API)
2. **JSON** (serializable, external configuration)

## Programmatic Scenarios (ScenarioBuilder)

### Basic Example

```rust
use paxos::scenario::ScenarioBuilder;
use paxos::paxos_command::PaxosCommand;
use tokio::time::Duration;

let scenario = ScenarioBuilder::new("My Scenario", 5)
    .description("A test scenario")
    .phase("setup")
    .enable_failures()
    .wait(Duration::from_millis(100))
    .phase("normal")
    .propose(PaxosCommand::NOOP)
    .wait(Duration::from_millis(500))
    .build();
```

### Available Builder Methods

```rust
// Phases
.phase(name: &str)            // Start a new phase

// Actions
.propose(command: PaxosCommand)           // Propose a value
.wait(duration: Duration)                 // Wait
.enable_failures()                        // Enable network failures
.disable_failures()                       // Disable network failures

// Network Control
.partition(node1: usize, node2: usize)    // Create bidirectional partition
.heal_partition(node1: usize, node2: usize)
.add_delay(from: usize, to: usize, delay: Duration)
.add_packet_loss(from: usize, to: usize, drop_rate: f32)
.clear_failures(node: usize)
```

### Partition Recovery Example

```rust
let scenario = ScenarioBuilder::new("Partition Recovery", 5)
    .description("Node 0 isolated then recovered")
    .phase("initialization")
    .enable_failures()
    .wait(Duration::from_millis(100))
    
    .phase("normal_operation")
    .propose(PaxosCommand::EnactDecree {
        author: "Socrates".to_string(),
        law: "Knowledge is virtue".to_string(),
    })
    .wait(Duration::from_millis(500))
    
    .phase("partition")
    .partition(0, 1)
    .partition(0, 2)
    .partition(0, 3)
    .partition(0, 4)
    .wait(Duration::from_millis(100))
    
    .phase("during_partition")
    .propose(PaxosCommand::AppointArchon {
        name: "Plato".to_string(),
        term_length_years: 5,
    })
    .wait(Duration::from_millis(500))
    
    .phase("recovery")
    .heal_partition(0, 1)
    .heal_partition(0, 2)
    .heal_partition(0, 3)
    .heal_partition(0, 4)
    .wait(Duration::from_millis(200))
    
    .propose(PaxosCommand::BuildAcropolis {
        stones_required: 1000,
        architect: "Ictinus".to_string(),
    })
    .wait(Duration::from_millis(500))
    .build();
```

## JSON Scenarios

Scenarios can be serialized and deserialized from JSON for external configuration.

### JSON Structure

```json
{
  "name": "Scenario Name",
  "description": "What this scenario tests",
  "node_count": 5,
  "phases": [
    {
      "name": "Phase Name",
      "steps": [
        {
          "type": "propose",
          "data": {
            "command": {
              "EnactDecree": {
                "author": "Name",
                "law": "Law text"
              }
            }
          }
        }
      ]
    }
  ]
}
```

### Step Types

All steps use `"type"` and optional `"data"` fields:

```json
// Simple steps (no data)
{"type": "enable_failures"}
{"type": "disable_failures"}

// Steps with data
{"type": "wait", "data": {"duration": 500}}
{"type": "partition", "data": {"node1": 0, "node2": 1}}
{"type": "heal_partition", "data": {"node1": 0, "node2": 1}}
{"type": "add_delay", "data": {"from": 0, "to": 1, "delay": 500}}
{"type": "add_packet_loss", "data": {"from": 0, "to": 1, "drop_rate": 0.3}}
{"type": "propose", "data": {"command": {...}}}
```

### PaxosCommand in JSON

```json
{"NOOP": null}

{"EnactDecree": {"author": "Name", "law": "Text"}}

{"Ostracize": {"citizen": "Name"}}

{"AppointArchon": {"name": "Name", "term_length_years": 5}}

{"BuildAcropolis": {"stones_required": 1000, "architect": "Name"}}

{"GET": {"key": "key_name"}}

{"PUT": {"key": "key_name", "version": 1}}
```

### Example JSON Files

See `scenarios/` directory for examples:
- `partition_recovery.json` - Partition and recovery
- `high_latency.json` - Consensus with high latency
- `packet_loss.json` - Resilience to packet loss

## Running Scenarios

### ScenarioRunner

The runner executes scenarios without assertions. It just logs what happens.

```rust
use paxos::scenario_runner::ScenarioRunner;

let scenario = ScenarioBuilder::new("Test", 3).build();
ScenarioRunner::run(&mut cluster, &scenario).await?;
```

### Loading JSON Scenarios

```rust
use paxos::scenario_loader::ScenarioLoader;

// Load single scenario
let scenario = ScenarioLoader::load("scenarios/partition_recovery.json").await?;

// Load all scenarios from directory
let scenarios = ScenarioLoader::load_all("scenarios").await?;
for (filename, scenario) in scenarios {
    ScenarioRunner::run(&mut cluster, &scenario).await?;
}
```

### Command Line

Run default programmatic scenario:
```bash
cargo run
```

Run all JSON scenarios:
```bash
cargo run -- json
```

## Testing with Scenarios

Scenarios don't have assertions. Test assertions in your test code:

```rust
#[tokio::test]
async fn test_consensus_with_partition() {
    let observer = Arc::new(ConsoleObserver);
    let mut cluster = Cluster::new(0, 5, observer).await.unwrap();
    
    for i in 0..5 {
        cluster.nodes[i].start();
    }

    let scenario = ScenarioBuilder::new("Partition Test", 5)
        .phase("partition")
        .enable_failures()
        .partition(0, 1)
        .propose(PaxosCommand::NOOP)
        .wait(Duration::from_millis(500))
        .build();

    ScenarioRunner::run(&mut cluster, &scenario).await.unwrap();
    
    // Add your assertions here based on observer state, etc.
    // The runner just executes and logs
}
```

## Creating Custom Scenarios

### For Partition Testing

```rust
ScenarioBuilder::new("Partition", node_count)
    .enable_failures()
    .partition(a, b)
    .propose(cmd)
    .wait(Duration::from_secs(1))
    .heal_partition(a, b)
    .propose(cmd)
    .build()
```

### For Latency Testing

```rust
ScenarioBuilder::new("Latency", node_count)
    .enable_failures()
    .add_delay(0, 1, Duration::from_millis(500))
    .add_delay(1, 0, Duration::from_millis(500))
    .propose(cmd)
    .wait(Duration::from_secs(3))
    .build()
```

### For Packet Loss Testing

```rust
ScenarioBuilder::new("Packet Loss", node_count)
    .enable_failures()
    .add_packet_loss(0, 1, 0.3)  // 30% loss
    .propose(cmd)
    .wait(Duration::from_millis(1000))
    .build()
```

## Design Principles

1. **Scenarios are declarative**: Define what happens, runner executes it
2. **No assertions in runner**: Tests assert on outcomes, runner just logs
3. **Easy to debug**: Each step printed with its parameters
4. **Serializable**: JSON support for external scenario definitions
5. **Fluent API**: Chain methods for readable programmatic scenarios
6. **Runtime control**: All failures can be set/modified during scenario execution

## Output

ScenarioRunner prints each step as it executes:

```
=== Running Scenario: Partition Recovery ===
Description: Tests Paxos behavior when...
Nodes: 5

--- Phase: Normal Operation ---
  [PROPOSE] Enact Decree by Socrates: 'Knowledge is virtue'
  [WAIT] 500ms

--- Phase: Create Partition ---
  [PARTITION] 0 <-> 1
  [WAIT] 100ms

=== Scenario Complete: Partition Recovery ===
```

This makes debugging easy - you see exactly what happened in order.

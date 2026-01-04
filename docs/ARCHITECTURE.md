# Architecture & Implementation Details

## Core Components

### Proposer (`src/node/proposer.rs`)
Initiates consensus. Manages ballot numbers and collects promises/acceptances.

**Phase 1**: Send Prepare → collect Promises
**Phase 2**: Send Accept → collect Acceptances

### Acceptor (`src/node/acceptor.rs`)
Votes on proposals. Enforces ballot ordering to prevent conflicts.

- Responds to Prepare with Promise if ballot is higher than previous
- Responds to Accept if ballot matches promised ballot
- Returns NACK if ballot is too low

### Learner (`src/node/learner.rs`)
Detects when quorum is reached and records the value.

- Tracks accepted votes per decree
- Emits LearnedValue event when quorum achieved
- Records to ledger (SQLite)

### Cluster (`src/cluster/cluster.rs`)
Manages multi-node simulation with network failure injection.

- Creates N nodes
- Routes messages between them
- Simulates partitions, latency, packet loss
- Provides high-level API: `propose(cmd)`

## Message Types

| Message | Purpose |
|---------|---------|
| Prepare | Phase 1: Request promises |
| Promise | Phase 1: Commit to ballot |
| Accept | Phase 2: Request acceptance |
| Accepted | Phase 2: Confirm acceptance |
| Success | Learned value notification |
| NACK | Rejection (ballot too low) |

## Ballot System

Ballots use `(number, proposer_id)` for total ordering.

Ordering: `(n1, p1) > (n2, p2)` if `n1 > n2` OR `(n1 == n2 AND p1 > p2)`

This prevents conflicts: higher ballot always wins.

## Data Persistence

**Ledger** (`src/node/ledger.rs`):
- SQLite database
- Stores: decree_number → PaxosCommand
- Recovers on restart
- Handles gaps in decree numbers

**Ballot Tracking**:
- In-memory during consensus
- Persisted to avoid promise violations

## Protocol Flow - Single Decree

```
Client proposes value X for decree N

1. Proposer sends Prepare(ballot_1) to all acceptors
2. Acceptors respond with Promise(ballot_1) or NACK
3. If quorum of Promises received, proceed to Phase 2
4. Proposer sends Accept(ballot_1, value=X) to all acceptors  
5. Acceptors respond with Accepted(ballot_1, X) or NACK
6. If quorum of Accepted received, value is chosen
7. Learners detect quorum, record value, emit LearnedValue event
8. Value persisted to ledger
```

## Multi-Decree Consensus

Each decree (0, 1, 2, ...) runs the above protocol **independently**.

Proposer can pipeline: send Phase 2 for decree N while Phase 1 for decree N+1.

## Failure Scenarios

### Proposer Fails
- Other nodes detect timeout, start new proposer with higher ballot
- Existing promises remain valid, new proposer still succeeds

### Acceptor Fails
- If minority: no impact
- If majority: consensus halts
- On recovery: reads ledger, rejoins with previous promises

### Network Partition (Majority Side)
- Can reach quorum, consensus continues
- Minority can't reach quorum, halts

### Network Partition (Healing)
- Minority node reads ledger
- Catches up with majority's learned values
- Full consistency restored

## Testing Framework: EventBarrier

Instead of sleeping, tests wait for **events**.

```rust
// Before: arbitrary sleep
tokio::time::sleep(Duration::from_secs(5)).await;

// After: wait for actual event
let learned = barrier.wait_for_learned(decree_num, Duration::from_secs(5)).await?;
```

**Benefits**:
- No flaky timing-dependent tests
- Tests run as fast as protocol allows
- Can inspect event sequences

See `docs/TESTING.md` for details.

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Consensus latency | 10-100ms (network sim) |
| Throughput | 10-100 decrees/sec |
| Memory per node | ~1MB |
| Disk per decree | ~1KB |

## Code Statistics

- Core protocol: ~2000 lines
- Tests: ~3000 lines
- Supporting systems: ~2000 lines
- **Total**: ~7000 lines

## Design Decisions

**Why Rust?**
- Type safety ensures correctness
- Tokio async for efficiency
- No GC pauses (important for consensus)

**Why SQLite?**
- Durability guarantees
- ACID transactions
- No external dependencies
- Simple schema

**Why Arc<Mutex>?**
- Thread-safe state sharing
- Async-compatible with Tokio
- Clear ownership semantics

## Known Limitations

1. **Single-view** - No reconfiguration
2. **No batching** - One decree at a time
3. **No leader** - Proposers compete
4. **Not Byzantine** - Assumes honest nodes

These are intentional simplifications. Production systems typically add:
- Multi-Paxos for efficiency
- Reconfiguration protocol
- Client batching
- Leader leases

## References

- Lamport, L. (2001). Paxos Made Simple
- Chandra, T., et al. (2007). Paxos Made Live
- MIT 6.5840 Distributed Systems course

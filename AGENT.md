# PMMC Agent Handoff

## Goal
Stabilize PMMC core behavior first (roles + routing + churn control), then iterate scenario/visualizer behavior.

## Current Status
- PMMC routing fixed for role-split: `ACCEPTED -> learners/replicas`.
- Hot-spin fixed in node election wait branch for non-leader nodes.
- Hot-spin fixed in replica applier via `Notify` (drain decisions, then park).
- Non-persistence constructor cfg paths fixed for PMMC leader/replica.
- Router seam tests added for PMMC routing policy.
- Commander compaction regression exists at role level (`commander_tests`), not flaky cluster integration.

## Key Files
- `src/node/pmmc/pmmc_node.rs`
- `src/node/pmmc/node_state.rs`
- `src/node/pmmc/replica.rs`
- `src/node/pmmc/replica/replica_state.rs`
- `src/node/message_router.rs`
- `src/node/pmmc/leader/commander_tests.rs`
- `TODO_PMMC.md`

## Known Outstanding Work (highest value first)
1. Ensure cached duplicate client requests do not rebroadcast `PROPOSE`.
2. Guard cache update path against missing client identity (`unwrap` removal).
3. Add tests for duplicate-request no-rebroadcast behavior.
4. Trim dead PMMC fields/methods or gate for tests only.

## Test Boundaries
- Role tests: role behavior only (`Leader`, `Commander`, `Scout`, `Acceptor`, `Replica`).
- Routing seam tests: `MessageRouter` only.
- Cluster tests: topology/wiring smoke only; avoid timing-heavy election assertions.

## Fast Commands For New Thread
- PMMC suite:  
  `cargo test pmmc:: -- --nocapture`
- Router seam tests:  
  `cargo test message_router::tests -- --nocapture`
- Replica tests:  
  `cargo test node::pmmc::replica::tests:: -- --nocapture`
- Commander tests:  
  `cargo test node::pmmc::leader::commander::tests:: -- --nocapture`

## Notes To Avoid Regressions
- In async loops, every idle path must block/yield (`Notify`, channel, timer, or `pending`).
- Keep `start_election()` non-blocking; never park inside it.
- Prefer deterministic role/router tests over churn-prone full cluster integration tests.

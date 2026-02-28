# PMMC TODO

This file tracks PMMC work across protocol, cluster/runtime structure, persistence,
scenarios, visualizer behavior, and upcoming reconfiguration support.

## Current Priorities

### P0: Keep PMMC stable while we reorganize
- [ ] Run and verify the current PMMC scenarios end-to-end after the persistence refactor:
  - [ ] `pmmc_single_client`
  - [ ] `pmmc_role_split`
  - [ ] `pmmc_leader_crash`
  - [ ] `pmmc_replica_crash_failover`
  - [ ] `pmmc_leader_partition_heal`
  - [ ] `pmmc_acceptor_majority_loss_then_recover`
  - [ ] `pmmc_staggered_leader_join`
- [ ] Add one regression test that asserts PMMC state files land under `.paxos/ip/<ip>/nodes/<uuid>/`.
- [ ] Remove any remaining PMMC code/tests that still defensively reference legacy flat filenames.

### P1: Finish the architecture cleanup before deeper feature work
- [ ] Rename the node-facing compatibility type `NetworkSimulator` to `NetworkHandle`.
- [ ] Stop treating `network_simulator.rs` as a real abstraction; leave it as a compatibility shim or remove it entirely.
- [ ] Replace parallel `node_uuids` / `peers` / `receivers` / `configs` construction in clusters with a single endpoint/build record.
- [ ] Centralize PMMC cluster construction paths so `new`, `new_with_configs`, and `new_with_configuration` share one builder pipeline.
- [ ] Do the same cleanup for Classic so cluster bootstrapping shape matches PMMC.

## Protocol / Correctness

### PMMC core behavior
- [x] Fix PMMC routing so `Message::ACCEPTED` reaches replicas in role-split topologies.
- [x] Remove hot-spin in `PmmcNode` election branch when node has no leader role.
- [x] Remove hot-spin in replica applier loop when no decision is available.
- [x] Ensure role-split cluster can reach replica `ACK` compaction for decided slots.
- [ ] Ensure duplicate client requests use cache-only response path and never rebroadcast `PROPOSE`.
- [ ] Guard cache updates and reply paths against commands with missing client metadata.
- [ ] Audit `Leader`, `Replica`, and `Commander` for any remaining repeated broadcast loops after `ACK` compaction.
- [ ] Revisit heartbeat/election timing constants now that startup churn is understood better.

### PMMC code structure
- [ ] Move PMMC routing policy out of generic `MessageRouter` path into PMMC-specific routing code.
- [ ] Make PMMC message flow boundaries explicit:
  - [ ] replica-originated proposes
  - [ ] leader-only phase 1 / phase 2 orchestration
  - [ ] acceptor-only ballot/value adoption
  - [ ] replica-only application / client replies
- [ ] Remove or gate dead PMMC methods and fields that only exist because of earlier refactors.
- [ ] Split large PMMC test modules when they become hard to reason about.

## Cluster / Runtime Reorganization

### Transport and node lifecycle
- [x] Introduce shared `NetworkFabric`.
- [ ] Make `NetworkFabric` the only real transport abstraction.
- [ ] Support node lifecycle operations cleanly at the cluster level:
  - [ ] add logical node
  - [ ] remove logical node
  - [ ] rebuild runtime for existing logical node
  - [ ] update role assignment without changing transport endpoint
- [ ] Introduce a single endpoint/build record for node startup:
  - [ ] `uuid`
  - [ ] `Sender<Message>`
  - [ ] `Receiver<Message>`
  - [ ] node config
  - [ ] persistence handle
- [ ] Make cluster membership/configuration the source of truth for UUID ownership.

### Configuration model
- [x] Introduce `ClusterConfiguration`.
- [x] Add `TryFrom`-based patch conversion.
- [x] Validate structural requirements (`NoLeaders`, `NoAcceptors`, `NoLearners`).
- [ ] Decide whether `Default` should remain available for `ClusterConfiguration`.
- [ ] Move configuration code to a clearer module layout if it keeps growing:
  - [ ] `configuration/`
  - [ ] `patch/`
  - [ ] `errors/`
  - [ ] `conversion/`
- [ ] Add activation/transition status semantics once reconfiguration begins:
  - [ ] `Pending`
  - [ ] `Active`
  - [ ] `Retired`

## Persistence

### New persistence layout
- [x] Move PMMC/Classic state under `.paxos/ip/<ip>/nodes/<uuid>/`.
- [x] Introduce `ClusterPersistence -> NodePersistence`.
- [ ] Consider one more cleanup pass to make role code use semantic filenames only and never mention UUID-derived filenames.
- [ ] Decide whether logs should move under `.paxos/ip/<ip>/logs/`.

### Persistence API cleanup
- [ ] Remove unused generic `Persistence` wrapper if `ClusterPersistence` is sufficient.
- [ ] Add helper methods for common role files if repeated string literals start spreading:
  - [ ] `leader_file()`
  - [ ] `acceptor_file()`
  - [ ] `replica_file()`
  - [ ] `store_file()`
- [ ] Add cluster-scoped purge helpers for:
  - [ ] all node state
  - [ ] one node
  - [ ] one role on one node

## Reconfiguration Foundation

This is the next major section, but we should prepare the code now so it lands cleanly.

### Immediate groundwork
- [ ] Make configuration the source of truth for node identity and role assignment.
- [ ] Thread `ClusterConfiguration` through PMMC cluster construction by default instead of only special paths.
- [ ] Add explicit config id / epoch to active runtime state.
- [ ] Decide where config awareness lives in message handling:
  - [ ] on every message
  - [ ] on transition-only control path
  - [ ] on leader/acceptor messages only

### Reconfiguration engine
- [ ] Add first-class `reconfigure(target_config, strategy)` orchestration.
- [ ] Introduce a `ReconfigurationStrategy` abstraction.
- [ ] Keep scenario scripts responsible for when transitions happen.
- [ ] Emit reconfiguration lifecycle events for the visualizer:
  - [ ] `ReconfigStarted`
  - [ ] `ReconfigActivated`
  - [ ] `ReconfigRetired`
  - [ ] `ReconfigFailed`

### Strategy backlog
- [ ] Stop-and-switch / brick wall style transition.
- [ ] Reconfiguration-made-easy style transition.
- [ ] Joint-consensus style transition.
- [ ] Vertical-like / master-driven approach only if needed for comparison.

## Scenario Backlog

### Existing scenarios
- [x] `pmmc_single_client`
- [x] `pmmc_role_split`
- [x] `pmmc_leader_crash`
- [x] `pmmc_replica_crash_failover`
- [x] `pmmc_leader_partition_heal`
- [x] `pmmc_acceptor_majority_loss_then_recover`
- [x] `pmmc_staggered_leader_join`

### Still needed
- [ ] `leader_crash_recovery_with_client_retry`
- [ ] `acceptor_minority_crash`
- [ ] `dual_leader_preemption_storm`
- [ ] `delayed_messages_reordering`
- [ ] `client_duplicate_request_dedup`
- [ ] `late_joining_replica_catchup`
- [ ] `rolling_restart`
- [ ] `leader_addition_under_static_config`
- [ ] `role_reassignment_without_node_add`

### Reconfiguration scenarios
- [ ] add leader through config transition
- [ ] remove leader through config transition
- [ ] add replica and demonstrate catch-up
- [ ] remove replica and keep service live
- [ ] acceptor set transition with overlapping quorums
- [ ] failed reconfiguration / abandoned transition

## Visualizer / Demo Work

### Current PMMC visualizer cleanup
- [ ] Verify the new persistence/cleanup flow does not leave stale state that contaminates demos.
- [ ] Audit event ordering for PMMC websocket output against the visualizer playback engine.
- [ ] Keep the selector/reset path topology-safe when scenarios change node count or role layout.
- [ ] Add a regression check for event batches that previously caused long pauses after `PREEMPT`.

### PMMC demo backlog
- [ ] Show config/epoch in PMMC topology once reconfiguration starts.
- [ ] Add visual distinction for cluster membership state:
  - [ ] active
  - [ ] joining
  - [ ] retired
  - [ ] crashed
- [ ] Add reconfiguration-specific event rendering.
- [ ] Add a simple topology panel for role-split / reconfiguration scenarios.

## Reconfiguring a State Machine Paper Track

### Content scaffolding
- [x] Create the paper section and routes.
- [x] Add the railway / control-room theme.
- [ ] Fill each section with actual content instead of placeholders/rough structure.
- [ ] Add diagrams tied to the actual implementation plan.

### Algorithm planning
- [ ] Turn the paper into an implementation note:
  - [ ] terminology mapping into this codebase
  - [ ] state variables
  - [ ] control messages
  - [ ] safety invariants
  - [ ] transition rules
- [ ] Decide which version we are implementing first:
  - [ ] rough teaching/demo version
  - [ ] stricter production-like version

## Documentation / Contributor Notes

- [ ] Add a concise PMMC architecture note:
  - [ ] role responsibilities
  - [ ] message flow
  - [ ] runtime ownership
  - [ ] persistence layout
- [ ] Add a reconfiguration architecture note once the strategy boundary is finalized.
- [ ] Keep `/Users/matthewbergman/learning/paxos/AGENTS.md` in sync when architecture assumptions change.

## Small Cleanup Backlog

- [ ] Fix obvious typo/duplicate cleanup where touched.
- [ ] Remove `timeoutes.html` or fold it into the correct `timeouts.html` path if it is accidental duplication.
- [ ] Audit stale warnings introduced by old PMMC experiments.
- [ ] Trim compatibility shims once the new names and boundaries settle.

## Suggested Next Order

1. Stabilize current PMMC scenarios after the persistence refactor.
2. Finish cluster/runtime cleanup around `NetworkFabric` and node construction.
3. Make `ClusterConfiguration` the default PMMC construction path.
4. Add reconfiguration events and strategy boundary.
5. Start with the simplest reconfiguration scenario and visualizer support.

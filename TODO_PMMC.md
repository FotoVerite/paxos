# PMMC TODO

This file tracks PMMC work across protocol, cluster/runtime structure, persistence,
scenarios, visualizer behavior, and upcoming reconfiguration support.

## Topline Architecture Plan

This is the intended runtime/control-plane shape for PMMC and, eventually, Classic.
Use this section as the reference when refactoring constructors, runtime ownership,
and reconfiguration support.

### Core boundaries

1. `ClusterConfiguration` is desired state.
- It says which logical members should exist, in what order, with which roles.
- It owns config identity/epoch and transition strategy metadata.
- It must preserve declared member order so scenario indices and display labels remain stable.

2. `RuntimeRegistry` is actual live state.
- It owns the currently running node runtimes keyed by UUID.
- It tracks runtime lifecycle, not just existence.
- Expected lifecycle states:
  - `Starting`
  - `Passive`
  - `CatchingUp`
  - `Active`
  - `Retiring`
  - `Stopped`
  - `Crashed`

3. `NetworkFabric` is transport only.
- It owns routable endpoints and injected failures.
- It should not know protocol semantics, config epochs, or role logic.
- It should support:
  - register endpoint
  - unregister endpoint
  - send / broadcast
  - partition / loss / delay rules

4. `Reconciler` moves actual runtime toward desired configuration.
- It diffs `ClusterConfiguration` against `RuntimeRegistry`.
- It decides:
  - add runtime
  - remove runtime
  - rebuild runtime
  - update roles under stable UUID
- It gates activation based on protocol/config transition safety.

### What this means concretely

- UUID is the internal durable identity.
- Node index/label is presentation and scenario convenience only.
- Role assignment must be changeable without changing node identity.
- Desired state and actual state must not be the same object.
- Scenarios should request control-plane actions, not hand-build clusters directly.

### Immediate implementation order

1. Make `ClusterConfiguration` the canonical PMMC input path.
- [ ] Add explicit bootstrap constructors for Classic configuration too.

2. Introduce runtime registry.
- [ ] Add `RuntimeRegistry` under `/Users/matthewbergman/learning/paxos/src/cluster/`.
- [ ] Store live runtime handles, simulator handles, and lifecycle state by UUID.
- [ ] Stop treating `Vec<Node>` as the only runtime source of truth.

3. Separate runtime lifecycle from transport registration.
- [ ] Give each logical node a stable ingress/endpoint entry in the registry/fabric.
- [ ] Make node rebuild/restart possible without changing logical identity.
- [ ] Make add/remove/update work through registry operations instead of cluster reconstruction.

4. Add configuration application/reconciliation.
- [ ] Add `apply_configuration(new_cfg)` on PMMC runtime.
- [ ] Compute `added`, `removed`, `changed`, and `unchanged` members.
- [ ] Handle:
  - [ ] add node
  - [ ] retire node
  - [ ] role change under same UUID
  - [ ] activation after catch-up

5. Move scenario layer onto the control plane.
- [ ] Scenarios should request:
  - [ ] `apply_configuration`
  - [ ] `crash_node`
  - [ ] `heal_node`
  - [ ] `propose`
- [ ] Scenarios should stop manufacturing raw cluster node lists directly.

### Best-practice rules to keep in mind

- Do not confuse desired configuration with running runtime state.
- Do not let ordering fall out of `HashMap` iteration or UUID sorting.
- Do not treat joins as instantly active; leave room for catch-up and passive startup.
- Do not delete retired nodes immediately; allow phased retirement and later cleanup.
- Keep safety in protocol/config state, not in transport code.
- Keep transport dumb and deterministic.

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

### P1: Finish the architecture cleanup before deeper feature work
- [ ] Rename the node-facing compatibility type `NetworkSimulator` to `NetworkHandle`.
- [ ] Stop treating `network_simulator.rs` as a real abstraction; leave it as a compatibility shim or remove it entirely.
- [ ] Do the same cleanup for Classic so cluster bootstrapping shape matches PMMC.

## Protocol / Correctness

### PMMC core behavior
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
- [ ] Introduce `RuntimeRegistry` as the actual source of truth for live node runtimes.
- [ ] Introduce a reconciler/apply path that moves runtime state toward `ClusterConfiguration`.

### Configuration model
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
- [ ] Thread `ClusterConfiguration` through Classic cluster construction by default instead of raw node-config vectors.
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
- Existing PMMC demo scenarios are implemented. Keep them verified as the runtime changes.

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
- [ ] Add a concise cluster/runtime note:
  - [ ] `ClusterConfiguration` vs `RuntimeRegistry`
  - [ ] `NetworkFabric` responsibilities
  - [ ] reconciler / `apply_configuration(...)`
- [ ] Add a reconfiguration architecture note once the strategy boundary is finalized.
- [ ] Keep `/Users/matthewbergman/learning/paxos/AGENTS.md` in sync when architecture assumptions change.

## Small Cleanup Backlog

- [ ] Fix obvious typo/duplicate cleanup where touched.
- [ ] Remove `timeoutes.html` or fold it into the correct `timeouts.html` path if it is accidental duplication.
- [ ] Audit stale warnings introduced by old PMMC experiments.
- [ ] Trim compatibility shims once the new names and boundaries settle.

## Suggested Next Order

1. Add `Cluster::new_with_configuration(...)` for Classic.
2. Introduce `RuntimeRegistry` for PMMC and move crash/heal/isolate through it.
3. Add a stable endpoint/runtime split so rebuilds do not change logical node identity.
4. Add `apply_configuration(...)` / reconciliation for PMMC.
5. Then start actual reconfiguration strategy work and visuals.

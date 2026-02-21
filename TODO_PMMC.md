# PMMC Refactor TODO

## Completed
- [x] Rename `ReplicaDate` to `ReplicaData` for clarity and consistency.
- [x] Rename `drop_scount` to `drop_scout`.

## P0 (Protocol/Liveness)
- [x] Fix PMMC routing so `Message::ACCEPTED` is sent to learners/replicas in role-split topologies (not acceptors).
- [x] Add regression test proving role-split cluster reaches replica `ACK` compaction for decided slots.
- [x] Remove hot-spin in `PmmcNode` election branch when node has no leader role.
- [x] Remove hot-spin in replica applier loop when no decision is available.

## P1 (Correctness/Churn)
- [x] Use proposal allocation slot (not execution slot) when emitting `Message::PROPOSE`.
- [ ] Ensure cached duplicate client requests do not rebroadcast `Message::PROPOSE`.
- [ ] Guard cache updates against commands without client identity (remove `unwrap` panic risk).
- [ ] Add tests for duplicate-request no-rebroadcast behavior.

## P2 (Structure/Cleanup)
- [x] Fix non-persistence cfg path using the correct durable defaults in PMMC leader/replica constructors.
- [x] Remove or populate `src/node/pmmc/types.rs` (currently empty).
- [ ] Trim dead PMMC fields/methods or gate them for tests only.
- [ ] Split larger PMMC test modules further when file size starts hurting readability.

## P3 (Nice-to-have)
- [ ] Move PMMC routing policy out of generic router path into a PMMC-specific routing module.
- [ ] Add a concise PMMC architecture note (role responsibilities + message flow) for future contributors.

## Scenario Backlog
- [x] Add `pmmc_replica_crash_failover` scenario.
- [x] Add `pmmc_leader_partition_heal` scenario.
- [x] Bugfix `pmmc_replica_crash_failover` scenario behavior/visualization.
- [x] Bugfix `pmmc_leader_partition_heal` scenario behavior/visualization.
- [ ] Add `leader_crash_recovery_with_client_retry` scenario.
- [ ] Add `acceptor_minority_crash` scenario.
- [ ] Add `acceptor_majority_loss_then_recover` scenario.
- [ ] Add `dual_leader_preemption_storm` scenario.
- [ ] Add `delayed_messages_reordering` scenario.
- [ ] Add `client_duplicate_request_dedup` scenario.
- [ ] Add `late_joining_replica_catchup` scenario.
- [ ] Add `rolling_restart` scenario.

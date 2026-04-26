# Paxos / Synod — Project Notes

## What This Is
A Rust Paxos implementation with a live demo app ("Synod") — audience members use their phones to submit emoji proposals to a real multi-Paxos cluster. Used as a talk demo.

## Test Structure

### Unit Tests (Rust) — `src/cluster/synod/tests.rs`
16 tests organized by module:
- **Membership**: client assignment, heartbeat, decommissioning lifecycle
- **Proposals**: submission, validation (unknown client / invalid emoji), idempotency, RSM command type
- **Reconfiguration**: config changes, concurrent proposals, sequential multi-client

### E2E Tests (Playwright) — `e2e/synod-*.spec.ts`
9 tests, shared utilities in `e2e/synod-shared.ts`:
- `synod-membership.spec.ts` — joining, unique IDs, concurrent join, rejoin with same ID
- `synod-proposals.spec.ts` — convergence, sequential, concurrent, ordered apply
- `synod-checkpoint.spec.ts` — late joiner gets applied room snapshot

**Key rule**: every test calls `uniqueRoom()` to get an isolated Paxos room. Tests sharing a room will accumulate slot state and interfere.

## E2E Test Helpers (`synod-shared.ts`)
- `uniqueRoom()` — generates a per-test room name
- `newMobileClient(browser, room)` — spins up a mobile context, waits for "Ready"
- `openMultipleClients(browser, n, room)` — N clients concurrently
- `submitAndWaitForApply(page)` — clicks submit, waits for `#clusterSlot` to advance, returns slot
- `waitForSlotConvergence(clients)` — polls until all clients agree on the same slot
- `waitForHeatConvergence(clients)` — polls until heat maps are identical
- `clusterSlot(page)`, `heatSnapshot(page)`, `clientId(page, room)` — state accessors

## Architecture Notes

### Room Isolation (server-side)
`AppState.rooms` is a `DashMap<String, Arc<Mutex<SynodCluster>>>` — rooms are created lazily on first join. Each room is an independent Paxos cluster. Fresh rooms start at `cluster_slot = 0`; the slot only advances when proposals are applied (joining alone does not advance it).

### Proposal Queue (client-side)
`submitPull()` is synchronous — pushes to `proposalQueue` and returns immediately (dice rolls, visual feedback). `drainProposalQueue()` processes one proposal at a time, serializing server submissions per client. This lets the UI feel fast without flooding the server.

### Key Bug Fixed
Nodes that survived a reconfiguration (present in both old and new replica sets) were being decommissioned by `decommission_to()`, clearing their `active_configuration`. Fixed in `src/cluster/synod_vertical/runtime.rs` — only decommission nodes NOT in the new replica set.

## Running Tests
```
cargo test                        # unit tests
npx playwright test               # all e2e
npx playwright test synod-proposals  # one suite
```

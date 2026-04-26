# Synod E2E Test Plan

Current test count: 1
- `isolated clients join, submit commands, and converge on room state`

## Test Organization

### Module 1: Membership & Joining
Tests client lifecycle: joining, reconnecting, being forgotten after idle timeout.

- [ ] `single_client_joins_and_ready` 
  - Single client loads page, joins room, is ready to submit
  - Verify: status shows "Ready", clientName populated, submitButton enabled
  
- [ ] `three_clients_join_sequentially`
  - 3 clients load and join in sequence
  - Verify: all have unique client IDs, all reach "Ready" status
  - Verify: clusterSlot increments with each join's reconfiguration

- [ ] `multiple_clients_join_concurrently`
  - 5+ clients load and join simultaneously
  - Verify: no race conditions, all get unique IDs
  - Verify: cluster stabilizes after all join (consistent slot across all)

- [ ] `rejoin_with_same_client_id`
  - Client A joins, close tab
  - New tab, same client ID (localStorage preserved)
  - Verify: rejoins with same ID, no duplicate registration
  - Verify: can submit proposals immediately

### Module 2: Proposal Submission
Tests core proposal lifecycle.

- [x] `isolated clients join, submit commands, and converge on room state` (EXISTING)
  - 3 clients, each submits 1 proposal concurrently
  - Verify: all reach Accepted, then Applied
  - Verify: heat map converges across all clients

- [ ] `single_client_multiple_proposals`
  - 1 client, submit 5 proposals sequentially
  - Verify: each goes Accepted → Applied
  - Verify: slot increments by 1 each time
  - Verify: heatpills accumulate in order

- [ ] `concurrent_proposals_all_clients`
  - 3 clients, each submit 3 proposals concurrently
  - Verify: 9 total proposals Accepted
  - Verify: all Applied (clusterSlot = baseline + 9)
  - Verify: heatpills identical across all clients

- [ ] `rapid_fire_proposals_same_client`
  - 1 client, click submit button 10 times rapidly (no waiting)
  - Verify: all queue and apply sequentially
  - Verify: no proposals lost or duplicated

### Module 3: Error Handling
Tests client-side error states and recovery.

- [ ] `invalid_emoji_rejects_immediately`
  - Client selects an emoji (mock non-Rust emoji)
  - Click submit
  - Verify: error message appears immediately (no waiting)
  - Verify: submitButton re-enabled to retry

- [ ] `cluster_not_ready_blocks_submission`
  - Rapidly open many clients and all submit immediately
  - Verify: early submissions show "Still pending" (not accepted)
  - Verify: after cluster stabilizes, submit succeeds

- [ ] `network_timeout_shows_timeout_message`
  - (Requires network simulation or backend slowdown)
  - Submit proposal, simulate 6+ second delay
  - Verify: timeout error shown
  - Verify: retry button available

### Module 4: UI State & Convergence
Tests that UI correctly reflects cluster state.

- [ ] `heat_map_reflects_applied_proposals`
  - 3 clients, submit emoji A, B, C concurrently
  - Verify: each emoji appears in heatpills
  - Verify: heat counts are identical across clients
  - Verify: order is deterministic (same across clients)

- [ ] `cluster_slot_synchronized_across_clients`
  - 3 clients submit 2 proposals each (staggered)
  - Poll clusterSlot on all clients
  - Verify: all clients converge to same slot within 2 seconds
  - Verify: no client ever shows higher slot than others (monotonic)

- [ ] `last_applied_timestamp_updates`
  - 3 clients, C1 submits, all check #lastApplied
  - Verify: C1's #lastApplied updates immediately
  - Verify: C2, C3's #lastApplied update within 500ms
  - Verify: timestamp is recent (within last 2 seconds)

- [ ] `status_line_message_sequence`
  - Single client submitting one proposal
  - Verify status goes: "Ready" → "Proposing..." → "Accepted" → "Applied"
  - Verify each message includes slot number
  - Verify timing: Accepted within 500ms, Applied within 2s

### Module 5: Stress & Edge Cases
Tests robustness under load and unusual scenarios.

- [ ] `ten_clients_sustained_load`
  - 10 clients, each submits 10 proposals over 10 seconds
  - Verify: 100 total proposals applied
  - Verify: no proposals lost
  - Verify: UI remains responsive
  - Verify: final heat maps identical across all clients

- [ ] `clients_rejoin_after_long_idle`
  - 2 clients join, C1 submits, C2 idle for 5s
  - C2 tab becomes active/focused
  - Verify: C2 still shows correct state (not reset)
  - C2 submits proposal
  - Verify: proposal applies correctly

- [ ] `rapid_client_churn`
  - Open client A, submit, close tab
  - Open client B, submit, close tab
  - Open client C, submit
  - Verify: all 3 proposals apply
  - Verify: heat map shows all 3 (no loss from churning clients)

- [ ] `all_clients_submit_same_emoji_repeatedly`
  - 3 clients, all submit emoji A 5 times each
  - Verify: 15 total increments applied
  - Verify: heat count for A shows 15 (or per-client count visible)

## Implementation Checklist

- [x] Create `e2e/synod-e2e-plan.md` (this file)
- [ ] Organize existing test into Module 2
- [ ] Add membership tests (Module 1)
- [ ] Add error handling tests (Module 3)
- [ ] Add UI convergence tests (Module 4)
- [ ] Add stress tests (Module 5)

## Test Infrastructure Notes

### Utilities to Create
```typescript
// Shared utilities needed:
async function waitForSlot(page: Page, expectedSlot: number, timeout?: number)
async function expectStatus(page: Page, status: string)
async function expectHeatCount(page: Page, emoji: string, count: number)
async function submitAndVerifyApplied(page: Page): Promise<number> // returns applied slot
async function openMultipleClients(browser: Browser, count: number): Promise<MobileClient[]>
async function waitForConvergence(clients: MobileClient[], property: string, timeout?: number)
```

### Timing Guidelines
- Proposal Accept: < 500ms (usually instant, waiting for cluster readiness)
- Proposal Apply: 100-2000ms (depends on learning propagation)
- Cluster convergence: < 2s for 3-10 clients
- UI update (after apply event): < 100ms

### Flakiness Mitigation
- Use generous timeouts (2-5s where possible)
- Poll rather than single check for convergence
- Wait for visible stability (2+ consecutive reads same value)
- Close all contexts in finally block

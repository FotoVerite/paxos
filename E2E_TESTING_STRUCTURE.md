# E2E Testing Structure for Synod

## Overview

The Synod e2e tests are now organized into logical modules with shared utilities, making them easier to navigate, extend, and maintain.

## File Structure

```
e2e/
├── synod-shared.ts           # Shared test utilities and helpers
├── synod-membership.spec.ts  # Client joining, rejoining, lifecycle
├── synod-proposals.spec.ts   # Proposal submission, acceptance, application
├── synod-ui.spec.ts          # UI state, error messages, convergence display
├── synod-stress.spec.ts      # Load testing, rapid operations, edge cases
└── synod-e2e-plan.md        # Detailed plan with all test scenarios
```

## Shared Utilities (`synod-shared.ts`)

### Client Management
```typescript
newMobileClient(browser)           // Create & ready a mobile client
openMultipleClients(browser, 5)    // Create N clients concurrently
closeAllClients(clients)            // Clean up all contexts
```

### State Access
```typescript
clientId(page)                      // Get client's stored ID
clusterSlot(page)                   // Get current cluster slot
heatSnapshot(page)                  // Get emoji heat map array
statusLine(page)                    // Get status message
lastApplied(page)                   // Get last applied timestamp
```

### Proposal Operations
```typescript
submitAndWaitForApply(page, timeout)  // Submit & wait for apply, return slot
```

### Synchronization
```typescript
waitForSlotConvergence(clients, timeout)   // Wait for all slots to match
waitForHeatConvergence(clients, timeout)   // Wait for all heat maps to match
```

## Test Modules

### 1. Membership (`synod-membership.spec.ts`)
Tests: 4 tests
- Single client joins and becomes ready
- Three clients join sequentially and converge
- Multiple clients (5) join concurrently
- Client rejoins with same ID from localStorage

Focus: Client registration, ID assignment, no duplicates, convergence after joins.

### 2. Proposals (`synod-proposals.spec.ts`)
Tests: 4 tests
- Isolated clients join, submit, and converge on room state
- Single client submits multiple proposals sequentially
- Concurrent proposals from 3 clients (each submits 3, all run parallel)
- Sequential proposals from 1 client (5 in order)

Focus: Proposal lifecycle (submit → accept → apply), convergence, heatmap accuracy.

### 3. UI & Error Handling (`synod-ui.spec.ts`) — TODO
Tests: ~6 planned
- Invalid emoji rejected immediately (no wait)
- Cluster not ready blocks early submissions
- Status message sequence (Ready → Proposing → Accepted → Applied)
- Heat map reflects applied proposals correctly
- Slot synchronized across clients
- Last applied timestamp updates correctly

Focus: UI correctness, error messages, convergence visibility.

### 4. Stress & Edge Cases (`synod-stress.spec.ts`) — TODO
Tests: ~4 planned
- 10 clients, sustained load (100 proposals total)
- Clients rejoin after idle
- Rapid client churn (join/leave/join)
- All clients submit same emoji repeatedly

Focus: Robustness, no data loss, UI responsiveness under load.

## Test Execution

Run all E2E tests:
```bash
npx playwright test e2e/synod-*.spec.ts
```

Run specific module:
```bash
npx playwright test e2e/synod-membership.spec.ts
npx playwright test e2e/synod-proposals.spec.ts
```

Run with headed browser:
```bash
npx playwright test e2e/synod-*.spec.ts --headed
```

Debug single test:
```bash
npx playwright test e2e/synod-proposals.spec.ts -g "concurrent proposals"
```

## Key Patterns

### Basic Test Structure
```typescript
test("test description", async ({ browser }) => {
  const client = await newMobileClient(browser);
  
  try {
    // Test logic here
    const slot = await submitAndWaitForApply(client.page);
    expect(slot).toBeGreaterThan(0);
  } finally {
    await client.context.close();
  }
});
```

### Multi-Client Sequential Proposals Per Client
```typescript
const clients = await openMultipleClients(browser, 3);

try {
  // Each client submits sequentially (but all 3 clients run in parallel)
  const allSubmissions = clients.map(({ page }) =>
    (async () => {
      const slots = [];
      for (let i = 0; i < 3; i++) {
        const slot = await submitAndWaitForApply(page, 20_000);
        slots.push(slot);
      }
      return slots;
    })()
  );

  const allAppliedSlots = await Promise.all(allSubmissions);
  
  // Wait for convergence
  const heat = await waitForHeatConvergence(clients);
  expect(heat.length).toBeGreaterThan(0);
} finally {
  await closeAllClients(clients);
}
```

### Important: Per-Page Serialization
- **Never submit multiple proposals concurrently on the same page**. Button clicks must be serialized per page.
- Multiple pages/clients can submit concurrently with each other.
- Use a for loop with await, not `flatMap` + `Promise.all` on the same page.

## Timing Guidelines

- **Proposal Accept**: < 500ms (usually instant after cluster ready)
- **Proposal Apply**: 100-2000ms (depends on learning propagation, first proposal may take longer)
- **Cluster Convergence**: < 2-3s for 3-10 clients
- **UI Update**: < 100ms after event
- **Default Timeout**: 30s for single proposal apply (accounts for cluster initialization delays)

## Common Issues & Solutions

### "Clients did not converge to same slot"
- Increase timeout in `waitForSlotConvergence(clients, 10_000)`
- Check if cluster is still initializing (early test)
- Verify no network/backend issues

### "Heat maps did not converge"
- May indicate proposals weren't applied consistently
- Check if all proposals actually received Accepted
- Increase timeout to wait longer for learning

### Multiple concurrent submissions on same page fail silently
- **Don't do this**. Use a for loop with await instead of `flatMap` + `Promise.all`.
- Each page's button can only be clicked once at a time.
- Race conditions cause silent failures or duplicate submissions.

### Heat total count doesn't match proposal count
- Remember: **emoji selection is random** (`pickEmoji()` in synod-mobile.js line 74-76).
- Submitting 10 times might create 10 emoji from 2-3 different types.
- Verify total heat count with `≥` operator, not `=`.
- If you need predictable emoji, the UI would need a picker control.

### Flaky timeouts
- Use `15_000` or `20_000` ms for apply waits
- Use polling (already built into convergence helpers)
- Avoid single `.toHaveText()` checks, use `waitFor` patterns

## Future Enhancements

- [ ] Add network simulation/latency for stress tests
- [ ] Add visual regression tests for UI
- [ ] Parameterize test counts (3, 5, 10, 20 clients)
- [ ] Add CI-specific timeout adjustments
- [ ] Mock slow backend responses for error handling tests
- [ ] Add video capture for failed tests

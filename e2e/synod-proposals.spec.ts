import { expect, test, type Browser } from "@playwright/test";
import {
  newMobileClient,
  openMultipleClients,
  closeAllClients,
  submitAndWaitForApply,
  clusterSlot,
  heatSnapshot,
  waitForSlotConvergence,
  waitForHeatConvergence,
  type MobileClient
} from "./synod-shared";

test.describe("Synod: Proposal Submission", () => {
  test("isolated clients join, submit commands, and converge on room state", async ({ browser }) => {
    const clients = await openMultipleClients(browser, 3);

    try {
      const initialSlot = await clusterSlot(clients[0].page);

      // All clients submit proposals concurrently
      await Promise.all(
        clients.map(({ page }) => submitAndWaitForApply(page))
      );

      // Verify all clients converged to same slot
      const expectedSlot = initialSlot + 3; // 3 new proposals
      await Promise.all(
        clients.map(({ page }) =>
          expect(page.locator("#clusterSlot")).toHaveText(String(expectedSlot))
        )
      );

      // Verify heat maps are identical
      const snapshots = await Promise.all(
        clients.map(({ page }) => heatSnapshot(page))
      );
      expect(new Set(snapshots.map(snapshot => snapshot.join("|"))).size).toBe(1);

      // Verify all have recent applied timestamps
      await Promise.all(
        clients.map(({ page }) =>
          expect(page.locator("#lastApplied")).toHaveText(/\d+/)
        )
      );
    } finally {
      await closeAllClients(clients);
    }
  });

  test("single client submits multiple proposals sequentially", async ({ browser }) => {
    const client = await newMobileClient(browser);

    try {
      const initialSlot = await clusterSlot(client.page);

      // Submit 5 proposals one after another
      const submittedSlots: number[] = [];
      for (let i = 0; i < 5; i++) {
        const slot = await submitAndWaitForApply(client.page);
        submittedSlots.push(slot);
      }

      // Verify slots increment by 1 each time
      for (let i = 1; i < submittedSlots.length; i++) {
        expect(submittedSlots[i]).toBe(submittedSlots[i - 1] + 1);
      }

      // Verify final slot is initial + 5
      const finalSlot = await clusterSlot(client.page);
      expect(finalSlot).toBe(initialSlot + 5);
    } finally {
      await client.context.close();
    }
  });

  test("concurrent proposals from multiple clients all apply", async ({ browser }) => {
    const clients = await openMultipleClients(browser, 3);

    try {
      const initialSlot = await clusterSlot(clients[0].page);

      // Each client submits 3 proposals sequentially (but all 3 clients run in parallel)
      const allSubmissions = clients.map(({ page }) =>
        (async () => {
          const slots = [];
          for (let i = 0; i < 3; i++) {
            const slot = await submitAndWaitForApply(page);
            slots.push(slot);
          }
          return slots;
        })()
      );

      const allAppliedSlots = await Promise.all(allSubmissions);
      const appliedSlots = allAppliedSlots.flat();

      // Verify all 9 proposals applied (and slots moved forward)
      const finalSlot = await clusterSlot(clients[0].page);
      expect(finalSlot).toBeGreaterThanOrEqual(initialSlot + 9);
      expect(appliedSlots.length).toBe(9);

      // Verify all clients converged to same state
      const heat = await waitForHeatConvergence(clients);
      expect(heat.length).toBeGreaterThan(0);
      
      // Verify total count matches proposals
      const totalCount = heat.reduce((sum, pill) => {
        const match = pill.match(/(\d+)$/);
        return sum + (match ? parseInt(match[1]) : 0);
      }, 0);
      expect(totalCount).toBeGreaterThanOrEqual(9);
    } finally {
      await closeAllClients(clients);
    }
  });

  test("sequential proposals from single client apply in order", async ({ browser }) => {
    const client = await newMobileClient(browser);

    try {
      const initialSlot = await clusterSlot(client.page);

      // Submit 5 proposals sequentially, verifying each applies
      const appliedSlots: number[] = [];
      for (let i = 0; i < 5; i++) {
        const slot = await submitAndWaitForApply(client.page);
        appliedSlots.push(slot);
      }

      // Verify all 5 applied
      const finalSlot = await clusterSlot(client.page);
      expect(finalSlot).toBeGreaterThanOrEqual(initialSlot + 5);

      // Verify slots are increasing (not necessarily consecutive due to reconfiguration)
      for (let i = 1; i < appliedSlots.length; i++) {
        expect(appliedSlots[i]).toBeGreaterThan(appliedSlots[i - 1]);
      }

      // Verify total heat count matches proposals
      const heat = await heatSnapshot(client.page);
      const totalCount = heat.reduce((sum, pill) => {
        const match = pill.match(/(\d+)$/);
        return sum + (match ? parseInt(match[1]) : 0);
      }, 0);
      expect(totalCount).toBeGreaterThanOrEqual(5);
    } finally {
      await client.context.close();
    }
  });
});

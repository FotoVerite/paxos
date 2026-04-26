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
  uniqueRoom,
  type MobileClient
} from "./synod-shared";

test.describe("Synod: Proposal Submission", () => {
  test("isolated clients join, submit commands, and converge on room state", async ({ browser }) => {
    const room = uniqueRoom();
    const clients = await openMultipleClients(browser, 3, room);

    try {
      const initialSlot = await clusterSlot(clients[0].page);

      await Promise.all(
        clients.map(({ page }) => submitAndWaitForApply(page))
      );

      // Wait for all clients to settle on the same slot
      const finalSlot = await waitForSlotConvergence(clients);
      expect(finalSlot).toBeGreaterThanOrEqual(initialSlot + 3);

      // Verify heat maps are identical
      const snapshots = await Promise.all(
        clients.map(({ page }) => heatSnapshot(page))
      );
      expect(new Set(snapshots.map(s => s.join("|"))).size).toBe(1);

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
    const room = uniqueRoom();
    const client = await newMobileClient(browser, room);

    try {
      const initialSlot = await clusterSlot(client.page);

      const submittedSlots: number[] = [];
      for (let i = 0; i < 5; i++) {
        const slot = await submitAndWaitForApply(client.page);
        submittedSlots.push(slot);
      }

      for (let i = 1; i < submittedSlots.length; i++) {
        expect(submittedSlots[i]).toBeGreaterThan(submittedSlots[i - 1]);
      }

      const finalSlot = await clusterSlot(client.page);
      expect(finalSlot).toBeGreaterThanOrEqual(initialSlot + 5);
    } finally {
      await client.context.close();
    }
  });

  test("concurrent proposals from multiple clients all apply", async ({ browser }) => {
    const room = uniqueRoom();
    const clients = await openMultipleClients(browser, 3, room);

    try {
      const initialSlot = await clusterSlot(clients[0].page);

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
      expect(appliedSlots.length).toBe(9);

      // Wait for all 9 proposals to be applied (concurrent submissions may lag)
      await clients[0].page.waitForFunction(
        (target: number) => {
          const el = document.querySelector("#clusterSlot");
          return el !== null && Number(el.textContent) >= target;
        },
        initialSlot + 9,
        { timeout: 30_000 }
      );

      const finalSlot = await clusterSlot(clients[0].page);
      expect(finalSlot).toBeGreaterThanOrEqual(initialSlot + 9);

      const heat = await waitForHeatConvergence(clients);
      expect(heat.length).toBeGreaterThan(0);

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
    const room = uniqueRoom();
    const client = await newMobileClient(browser, room);

    try {
      const initialSlot = await clusterSlot(client.page);

      const appliedSlots: number[] = [];
      for (let i = 0; i < 5; i++) {
        const slot = await submitAndWaitForApply(client.page);
        appliedSlots.push(slot);
      }

      const finalSlot = await clusterSlot(client.page);
      expect(finalSlot).toBeGreaterThanOrEqual(initialSlot + 5);

      for (let i = 1; i < appliedSlots.length; i++) {
        expect(appliedSlots[i]).toBeGreaterThan(appliedSlots[i - 1]);
      }

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

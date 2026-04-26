import { expect, test, type Page } from "@playwright/test";
import {
  closeAllClients,
  clusterSlot,
  heatSnapshot,
  newMobileClient,
  submitAndWaitForApply,
  waitForHeatConvergence
} from "./synod-shared";

type SynodStatus = {
  active_configuration?: {
    start_index: number;
  };
  state: {
    cluster_slot: number;
    heat: Array<{ emoji: string; count: number }>;
  };
};

async function synodStatus(page: Page): Promise<SynodStatus> {
  return page.evaluate(async () => {
    const response = await fetch("/synod/api/status");
    if (!response.ok) {
      throw new Error(`status failed: ${response.status}`);
    }
    return response.json();
  });
}

function totalHeat(status: SynodStatus): number {
  return status.state.heat.reduce((sum, item) => sum + item.count, 0);
}

test.describe("Synod: RSM checkpoint install", () => {
  test("late joining client starts from the applied room snapshot", async ({ browser }) => {
    const first = await newMobileClient(browser);
    const clients = [first];

    try {
      const baselineStatus = await synodStatus(first.page);
      const baselineHeat = totalHeat(baselineStatus);

      await submitAndWaitForApply(first.page);
      await submitAndWaitForApply(first.page);

      const checkpointSourceStatus = await synodStatus(first.page);
      const sourceSlot = await clusterSlot(first.page);
      const sourceHeat = await heatSnapshot(first.page);

      expect(totalHeat(checkpointSourceStatus)).toBeGreaterThanOrEqual(baselineHeat + 2);

      const late = await newMobileClient(browser);
      clients.push(late);

      await expect(late.page.locator("#clusterSlot")).toHaveText(String(sourceSlot));
      expect(await heatSnapshot(late.page)).toEqual(sourceHeat);

      const lateJoinStatus = await synodStatus(late.page);
      expect(lateJoinStatus.active_configuration?.start_index).toBeGreaterThanOrEqual(sourceSlot);
      expect(totalHeat(lateJoinStatus)).toBeGreaterThanOrEqual(baselineHeat + 2);

      await submitAndWaitForApply(late.page);

      const convergedHeat = await waitForHeatConvergence(clients);
      const finalStatus = await synodStatus(first.page);

      expect(totalHeat(finalStatus)).toBeGreaterThanOrEqual(baselineHeat + 3);
      expect(convergedHeat.join("|")).toBe((await heatSnapshot(first.page)).join("|"));
    } finally {
      await closeAllClients(clients);
    }
  });
});

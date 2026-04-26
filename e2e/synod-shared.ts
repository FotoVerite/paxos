import { expect, type Browser, type BrowserContext, type Page } from "@playwright/test";

export function uniqueRoom(): string {
  return `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function clientKey(room: string) {
  return `synod.${room}.client_id`;
}

export type MobileClient = {
  context: BrowserContext;
  page: Page;
};

/**
 * Create a new mobile browser context and load the Synod app.
 * Waits for the client to be ready before returning.
 */
export async function newMobileClient(browser: Browser, room = "main"): Promise<MobileClient> {
  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
    isMobile: true,
    hasTouch: true
  });
  const page = await context.newPage();
  await page.goto(`/synod?room=${encodeURIComponent(room)}`);

  await expect(page.locator("#statusLine")).toContainText("Ready");
  await expect(page.locator("#clientName")).not.toHaveText("Joining...");

  return { context, page };
}

/**
 * Get the client's stored ID from localStorage.
 */
export async function clientId(page: Page, room = "main"): Promise<string | null> {
  return page.evaluate((key) => window.localStorage.getItem(key), clientKey(room));
}

/**
 * Get the current heat map snapshot as an array of emoji counts.
 */
export async function heatSnapshot(page: Page): Promise<string[]> {
  return page.locator(".stream-lane").evaluateAll((lanes) =>
    lanes.map((lane) => {
      const emoji = (lane as HTMLElement).dataset.emoji ?? "";
      const countEl = lane.querySelector(".stream-count");
      const count = countEl?.textContent?.trim() ?? "0";
      return `${emoji}${count}`;
    })
  );
}

/**
 * Get the current cluster slot number.
 */
export async function clusterSlot(page: Page): Promise<number> {
  return Number(await page.locator("#clusterSlot").textContent());
}

/**
 * Get the last applied timestamp string.
 */
export async function lastApplied(page: Page): Promise<string> {
  return await page.locator("#lastApplied").textContent() ?? "";
}

/**
 * Get the current status message.
 */
export async function statusLine(page: Page): Promise<string> {
  return await page.locator("#statusLine").textContent() ?? "";
}

/**
 * Click submit button and wait for the proposal to be applied.
 * Returns the slot number where the proposal was applied.
 */
export async function submitAndWaitForApply(page: Page, timeout: number = 30_000): Promise<number> {
  await expect(page.locator("#submitButton")).toBeEnabled();

  const slotBefore = await clusterSlot(page);

  await page.locator("#submitButton").click();

  // Wait for clusterSlot to advance past slotBefore. This is more reliable than
  // watching status text: the "proposed" state can be too brief to observe on
  // fast loopback connections, and "applied at slot" text is stale across calls.
  await page.waitForFunction(
    (before: number) => {
      const el = document.querySelector("#clusterSlot");
      return el !== null && Number(el.textContent) > before;
    },
    slotBefore,
    { timeout }
  );

  return await clusterSlot(page);
}

/**
 * Wait for all clients to have the same cluster slot.
 * Polls until convergence or timeout.
 */
export async function waitForSlotConvergence(
  clients: MobileClient[],
  timeout: number = 5_000
): Promise<number> {
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeout) {
    const slots = await Promise.all(
      clients.map(({ page }) => clusterSlot(page))
    );
    
    const uniqueSlots = new Set(slots);
    if (uniqueSlots.size === 1) {
      return slots[0];
    }
    
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  throw new Error(
    `Clients did not converge to same slot within ${timeout}ms`
  );
}

/**
 * Wait for heat map to be identical across all clients.
 */
export async function waitForHeatConvergence(
  clients: MobileClient[],
  timeout: number = 5_000
): Promise<string[]> {
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeout) {
    const snapshots = await Promise.all(
      clients.map(({ page }) => heatSnapshot(page))
    );
    
    // Check if all snapshots are identical
    const first = snapshots[0].join("|");
    if (snapshots.every(snapshot => snapshot.join("|") === first)) {
      return snapshots[0];
    }
    
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  throw new Error(
    `Heat maps did not converge across all clients within ${timeout}ms`
  );
}

/**
 * Clean up all client contexts.
 */
export async function closeAllClients(clients: MobileClient[]): Promise<void> {
  await Promise.all(clients.map(({ context }) => context.close()));
}

/**
 * Create multiple mobile clients concurrently, all in the same room.
 */
export async function openMultipleClients(
  browser: Browser,
  count: number,
  room = "main"
): Promise<MobileClient[]> {
  return Promise.all(
    Array.from({ length: count }, () => newMobileClient(browser, room))
  );
}

import { expect, test, type Browser } from "@playwright/test";
import {
  newMobileClient,
  clientId,
  openMultipleClients,
  closeAllClients,
  waitForSlotConvergence,
  clusterSlot,
  type MobileClient
} from "./synod-shared";

test.describe("Synod: Membership & Client Joining", () => {
  test("single client joins and becomes ready", async ({ browser }) => {
    const client = await newMobileClient(browser);

    try {
      // Verify client has ID and is ready
      const id = await clientId(client.page);
      expect(id).toBeTruthy();
      expect(id).toMatch(/^c_/);

      // Verify UI shows ready state
      await expect(client.page.locator("#clientName")).not.toHaveText("Joining...");
      await expect(client.page.locator("#statusLine")).toContainText("Ready");
    } finally {
      await client.context.close();
    }
  });

  test("three clients join sequentially and converge", async ({ browser }) => {
    const clients: MobileClient[] = [];

    try {
      // Open clients one at a time
      clients.push(await newMobileClient(browser));
      clients.push(await newMobileClient(browser));
      clients.push(await newMobileClient(browser));

      // Verify all have unique client IDs
      const ids = await Promise.all(
        clients.map(({ page }) => clientId(page))
      );
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(3);
      ids.forEach(id => {
        expect(id).toMatch(/^c_/);
      });

      // Verify cluster slots converge
      const finalSlot = await waitForSlotConvergence(clients);
      expect(finalSlot).toBeGreaterThan(0); // Reconfigurations happened
    } finally {
      await closeAllClients(clients);
    }
  });

  test("multiple clients join concurrently", async ({ browser }) => {
    const clients = await openMultipleClients(browser, 5);

    try {
      // Verify all got unique IDs
      const ids = await Promise.all(
        clients.map(({ page }) => clientId(page))
      );
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(5);

      // Verify all are ready
      await Promise.all(
        clients.map(({ page }) =>
          expect(page.locator("#statusLine")).toContainText("Ready")
        )
      );

      // Verify cluster slots converge
      const finalSlot = await waitForSlotConvergence(clients);
      expect(finalSlot).toBeGreaterThan(0);
    } finally {
      await closeAllClients(clients);
    }
  });

  test("client can rejoin with same ID", async ({ browser, context }) => {
    // First client joins
    const client1 = await newMobileClient(browser);
    const id1 = await clientId(client1.page);
    expect(id1).toBeTruthy();

    // Store the client ID
    const storedId = id1;

    await client1.context.close();

    // New client, inject the same ID via localStorage
    const context2 = await browser.newContext({
      viewport: { width: 390, height: 844 },
      isMobile: true,
      hasTouch: true
    });
    
    // Inject localStorage before navigating
    await context2.addInitScript(({ clientId }) => {
      window.localStorage.setItem("synod.main.client_id", clientId);
    }, { clientId: storedId });
    
    const page2 = await context2.newPage();
    await page2.goto("/synod");
    await expect(page2.locator("#statusLine")).toContainText("Ready");

    // Verify same ID
    const id2 = await clientId(page2);
    expect(id2).toBe(id1);

    // Verify can submit immediately
    await expect(page2.locator("#submitButton")).toBeEnabled();

    await context2.close();
  });
});

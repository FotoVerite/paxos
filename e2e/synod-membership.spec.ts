import { expect, test, type Browser } from "@playwright/test";
import {
  newMobileClient,
  clientId,
  openMultipleClients,
  closeAllClients,
  waitForSlotConvergence,
  uniqueRoom,
  type MobileClient
} from "./synod-shared";

test.describe("Synod: Membership & Client Joining", () => {
  test("single client joins and becomes ready", async ({ browser }) => {
    const room = uniqueRoom();
    const client = await newMobileClient(browser, room);

    try {
      const id = await clientId(client.page, room);
      expect(id).toBeTruthy();
      expect(id).toMatch(/^c_/);

      await expect(client.page.locator("#clientName")).not.toHaveText("Joining...");
      await expect(client.page.locator("#statusLine")).toContainText("Ready");
    } finally {
      await client.context.close();
    }
  });

  test("three clients join sequentially and converge", async ({ browser }) => {
    const room = uniqueRoom();
    const clients: MobileClient[] = [];

    try {
      clients.push(await newMobileClient(browser, room));
      clients.push(await newMobileClient(browser, room));
      clients.push(await newMobileClient(browser, room));

      const ids = await Promise.all(
        clients.map(({ page }) => clientId(page, room))
      );
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(3);
      ids.forEach(id => expect(id).toMatch(/^c_/));

      await waitForSlotConvergence(clients);
    } finally {
      await closeAllClients(clients);
    }
  });

  test("multiple clients join concurrently", async ({ browser }) => {
    const room = uniqueRoom();
    const clients = await openMultipleClients(browser, 5, room);

    try {
      const ids = await Promise.all(
        clients.map(({ page }) => clientId(page, room))
      );
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(5);

      await Promise.all(
        clients.map(({ page }) =>
          expect(page.locator("#statusLine")).toContainText("Ready")
        )
      );

      await waitForSlotConvergence(clients);
    } finally {
      await closeAllClients(clients);
    }
  });

  test("client can rejoin with same ID", async ({ browser }) => {
    const room = uniqueRoom();
    const client1 = await newMobileClient(browser, room);
    const id1 = await clientId(client1.page, room);
    expect(id1).toBeTruthy();
    await client1.context.close();

    const context2 = await browser.newContext({
      viewport: { width: 390, height: 844 },
      isMobile: true,
      hasTouch: true
    });

    await context2.addInitScript(
      ({ key, id }) => { window.localStorage.setItem(key, id); },
      { key: `synod.${room}.client_id`, id: id1! }
    );

    const page2 = await context2.newPage();
    await page2.goto(`/synod?room=${encodeURIComponent(room)}`);
    await expect(page2.locator("#statusLine")).toContainText("Ready");

    const id2 = await clientId(page2, room);
    expect(id2).toBe(id1);
    await expect(page2.locator("#submitButton")).toBeEnabled();

    await context2.close();
  });
});

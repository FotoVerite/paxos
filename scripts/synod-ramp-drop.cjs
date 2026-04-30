/**
 * Synod ramp/drop simulation.
 *
 * Slowly ramps mobile clients up to a target, then closes a subset to exercise
 * idle decommissioning and reconfiguration while proposals remain in flight.
 *
 * Usage:
 *   node scripts/synod-ramp-drop.cjs
 *   ROOM=drop-test TARGET=55 DROP=15 node scripts/synod-ramp-drop.cjs
 *
 * Env vars:
 *   BASE_URL      server base (default http://127.0.0.1:3001)
 *   ROOM          room name (default ramp-drop-<timestamp>)
 *   TARGET        clients to ramp to (default 55)
 *   DROP          clients to close after ramp (default 15)
 *   RAMP_MS       ms between client joins (default 1500)
 *   DROP_MS       ms between client closes (default 3000)
 *   HOLD_MS       ms to hold at target before dropping (default 30000)
 *   INTERVAL      base ms between proposals per client (default 2500)
 *   HEADLESS      set to "0" to show browser windows (default headless)
 */

const { chromium } = require("@playwright/test");

const BASE_URL = process.env.BASE_URL ?? "http://127.0.0.1:3001";
const ROOM = process.env.ROOM ?? `ramp-drop-${Date.now()}`;
const TARGET = parseInt(process.env.TARGET ?? "55", 10);
const DROP = parseInt(process.env.DROP ?? "15", 10);
const RAMP_MS = parseInt(process.env.RAMP_MS ?? "1500", 10);
const DROP_MS = parseInt(process.env.DROP_MS ?? "3000", 10);
const HOLD_MS = parseInt(process.env.HOLD_MS ?? "30000", 10);
const INTERVAL = parseInt(process.env.INTERVAL ?? "2500", 10);
const HEADLESS = process.env.HEADLESS !== "0";

const clients = [];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function synodStatus() {
  const url = `${BASE_URL}/synod/api/status?room=${encodeURIComponent(ROOM)}`;
  const response = await fetch(url);
  if (!response.ok) return null;
  return response.json();
}

async function logStatus(label) {
  const status = await synodStatus().catch(() => null);
  if (!status) {
    console.log(`[${label}] status unavailable`);
    return;
  }
  const state = status.state || {};
  console.log(
    `[${label}] nodes=${status.active_nodes} slot=${state.cluster_slot} applied=${state.last_applied}`
  );
}

async function runClient(browser, index) {
  const ctx = await browser.newContext({
    viewport: { width: 390, height: 844 },
    isMobile: true,
    hasTouch: true,
  });
  const page = await ctx.newPage();
  const client = { index, ctx, page, closed: false, submissions: 0 };
  clients.push(client);

  await page.goto(`${BASE_URL}/synod?room=${encodeURIComponent(ROOM)}`);
  await page.waitForFunction(
    () => document.querySelector("#statusLine")?.textContent?.includes("Ready"),
    { timeout: 30_000 }
  );
  console.log(`[client ${String(index).padStart(3)}] joined`);

  const propose = async () => {
    if (client.closed) return;
    try {
      const btn = page.locator("#submitButton");
      if (await btn.isEnabled({ timeout: 500 })) {
        await btn.click();
        client.submissions += 1;
      }
    } catch {
      // The context may be closing or the page may be mid-reconfiguration.
    }
    const jitter = Math.floor(Math.random() * (INTERVAL * 0.6));
    setTimeout(propose, INTERVAL + jitter);
  };

  setTimeout(propose, Math.floor(Math.random() * INTERVAL));
  return client;
}

async function closeClient(client) {
  if (!client || client.closed) return;
  client.closed = true;
  await client.ctx.close().catch(() => {});
  console.log(
    `[client ${String(client.index).padStart(3)}] closed after ${client.submissions} clicks`
  );
}

async function main() {
  console.log(
    `Ramp/drop: room="${ROOM}" target=${TARGET} drop=${DROP} ramp=${RAMP_MS}ms drop=${DROP_MS}ms interval~${INTERVAL}ms`
  );

  const browser = await chromium.launch({ headless: HEADLESS });
  const dashCtx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const dashPage = await dashCtx.newPage();
  await dashPage.goto(`${BASE_URL}/synod/dashboard?room=${encodeURIComponent(ROOM)}`);
  console.log(`Dashboard open: ${BASE_URL}/synod/dashboard?room=${ROOM}`);

  for (let i = 0; i < TARGET; i += 1) {
    runClient(browser, i).catch((err) => {
      console.error(`[client ${i}] failed:`, err.message || err);
    });
    await sleep(RAMP_MS);
    if ((i + 1) % 5 === 0) await logStatus(`ramp ${i + 1}/${TARGET}`);
  }

  await logStatus("target reached");
  console.log(`Holding for ${(HOLD_MS / 1000).toFixed(1)}s before drop`);
  await sleep(HOLD_MS);

  const toDrop = clients.slice(0, Math.min(DROP, clients.length));
  for (let i = 0; i < toDrop.length; i += 1) {
    await closeClient(toDrop[i]);
    await sleep(DROP_MS);
    await logStatus(`drop ${i + 1}/${toDrop.length}`);
  }

  console.log("Drop complete. Waiting for idle TTL/decommissioning; Ctrl+C to stop.");
  while (true) {
    await sleep(15000);
    await logStatus("steady");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

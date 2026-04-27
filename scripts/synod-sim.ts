/**
 * Synod load simulation — opens the dashboard then staggers N mobile clients
 * joining and submitting proposals so you can watch the visualizer live.
 *
 * Usage:
 *   npx ts-node scripts/synod-sim.ts
 *   CLIENTS=50 STAGGER=300 npx ts-node scripts/synod-sim.ts
 *
 * Env vars:
 *   BASE_URL   — server base (default http://127.0.0.1:3001)
 *   ROOM       — room name (default "sim")
 *   CLIENTS    — number of mobile clients (default 20)
 *   STAGGER    — ms between each client joining (default 400)
 *   INTERVAL   — base ms between proposals per client (default 2500)
 *   HEADLESS   — set to "1" to hide browser windows (default shown)
 */

import { chromium, type Browser, type BrowserContext } from "playwright";

const BASE_URL  = process.env.BASE_URL  ?? "http://127.0.0.1:3001";
const ROOM      = process.env.ROOM      ?? "sim";
const CLIENTS   = parseInt(process.env.CLIENTS  ?? "20", 10);
const STAGGER   = parseInt(process.env.STAGGER  ?? "400", 10);
const INTERVAL  = parseInt(process.env.INTERVAL ?? "2500", 10);
const HEADLESS  = process.env.HEADLESS === "1";

async function runClient(browser: Browser, index: number): Promise<void> {
  let ctx: BrowserContext | null = null;
  try {
    ctx = await browser.newContext({
      viewport:  { width: 390, height: 844 },
      isMobile:  true,
      hasTouch:  true,
    });
    const page = await ctx.newPage();
    await page.goto(`${BASE_URL}/synod?room=${encodeURIComponent(ROOM)}`);

    await page.waitForSelector("#statusLine", { timeout: 15_000 });
    await page.waitForFunction(
      () => document.querySelector("#statusLine")?.textContent?.includes("Ready"),
      { timeout: 15_000 }
    );

    console.log(`[client ${String(index).padStart(3)}] joined`);

    const propose = async () => {
      try {
        const btn = page.locator("#submitButton");
        if (await btn.isEnabled({ timeout: 500 })) {
          await btn.click();
        }
      } catch {
        // page may have closed or button unavailable — silently skip
      }
      const jitter = Math.floor(Math.random() * (INTERVAL * 0.6));
      setTimeout(propose, INTERVAL + jitter);
    };

    // Stagger first proposal within the first interval
    setTimeout(propose, Math.floor(Math.random() * INTERVAL));

    // Keep context alive forever (until process exits)
    await new Promise<void>(() => {});
  } catch (err) {
    console.error(`[client ${index}] error:`, err);
    ctx?.close().catch(() => {});
  }
}

async function main() {
  console.log(`Starting simulation: ${CLIENTS} clients, room="${ROOM}", stagger=${STAGGER}ms`);

  const browser = await chromium.launch({
    headless: HEADLESS,
    args: HEADLESS ? [] : ["--start-maximized"],
  });

  // Open dashboard in first window
  const dashCtx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const dashPage = await dashCtx.newPage();
  await dashPage.goto(`${BASE_URL}/synod/dashboard?room=${encodeURIComponent(ROOM)}`);
  console.log(`Dashboard: ${BASE_URL}/synod/dashboard?room=${ROOM}`);

  // Launch clients with staggered joins
  for (let i = 0; i < CLIENTS; i++) {
    setTimeout(() => {
      runClient(browser, i).catch(console.error);
    }, i * STAGGER);
  }

  const totalMs = CLIENTS * STAGGER;
  console.log(`All ${CLIENTS} clients will be online in ~${(totalMs / 1000).toFixed(1)}s`);
  console.log("Press Ctrl+C to stop.\n");

  // Keep process alive
  await new Promise<void>(() => {});
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

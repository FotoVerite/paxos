/**
 * Synod load simulation — opens the dashboard then staggers N mobile clients
 * joining and submitting proposals so you can watch the visualizer live.
 *
 * Usage:
 *   node scripts/synod-sim.js
 *   CLIENTS=50 STAGGER=300 node scripts/synod-sim.js
 *
 * Env vars:
 *   BASE_URL   — server base (default http://127.0.0.1:3001)
 *   ROOM       — room name (default "main")
 *   CLIENTS    — number of mobile clients (default 20)
 *   STAGGER    — ms between each client joining (default 400)
 *   INTERVAL   — base ms between proposals per client (default 2500)
 *   HEADLESS   — set to "0" to show browser windows (default headless)
 */

const { chromium } = require("@playwright/test");

const BASE_URL = process.env.BASE_URL  ?? "http://127.0.0.1:3001";
const ROOM     = process.env.ROOM      ?? "main";
const CLIENTS  = parseInt(process.env.CLIENTS  ?? "20", 10);
const STAGGER  = parseInt(process.env.STAGGER  ?? "400", 10);
const INTERVAL = parseInt(process.env.INTERVAL ?? "2500", 10);
const HEADLESS = process.env.HEADLESS !== "0";

async function runClient(browser, index) {
  let ctx = null;
  try {
    ctx = await browser.newContext({
      viewport: { width: 390, height: 844 },
      isMobile: true,
      hasTouch: true,
    });
    const page = await ctx.newPage();
    await page.goto(`${BASE_URL}/synod?room=${encodeURIComponent(ROOM)}`);

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
        // silently skip — page may be busy or closing
      }
      const jitter = Math.floor(Math.random() * (INTERVAL * 0.6));
      setTimeout(propose, INTERVAL + jitter);
    };

    setTimeout(propose, Math.floor(Math.random() * INTERVAL));

    // Keep context alive until process exits
    await new Promise(() => {});
  } catch (err) {
    console.error(`[client ${index}] error:`, err);
    ctx?.close().catch(() => {});
  }
}

async function main() {
  console.log(`Simulation: ${CLIENTS} clients · room="${ROOM}" · stagger=${STAGGER}ms · interval~${INTERVAL}ms`);

  const browser = await chromium.launch({ headless: HEADLESS });

  // Dashboard window
  const dashCtx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const dashPage = await dashCtx.newPage();
  await dashPage.goto(`${BASE_URL}/synod/dashboard?room=${encodeURIComponent(ROOM)}`);
  console.log(`Dashboard open: ${BASE_URL}/synod/dashboard?room=${ROOM}`);

  // Staggered client joins
  for (let i = 0; i < CLIENTS; i++) {
    setTimeout(() => runClient(browser, i).catch(console.error), i * STAGGER);
  }

  const totalSec = (CLIENTS * STAGGER / 1000).toFixed(1);
  console.log(`All ${CLIENTS} clients online in ~${totalSec}s · Ctrl+C to stop\n`);

  await new Promise(() => {});
}

main().catch(err => { console.error(err); process.exit(1); });

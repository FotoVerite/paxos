import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.PAXOS_BASE_URL ?? "http://127.0.0.1:3001";
const reuseExistingServer = process.env.PAXOS_REUSE_SERVER !== "0";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: {
    timeout: 10_000
  },
  fullyParallel: false,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure"
  },
  webServer: {
    command: "cargo run -- web",
    url: `${baseURL}/synod/api/status`,
    reuseExistingServer,
    timeout: 60_000
  },
  projects: [
    {
      name: "mobile-chromium",
      use: {
        ...devices["iPhone 13"],
        viewport: { width: 390, height: 844 }
      }
    }
  ]
});

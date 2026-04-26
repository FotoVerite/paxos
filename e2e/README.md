# Browser Tests

These tests use Playwright to run the Synod demo as multiple isolated mobile clients.

Install once:

```bash
npm install
npx playwright install chromium
```

Run against a server started by Playwright:

```bash
npm run test:e2e
```

Run against an already-running server:

```bash
cargo run -- web
npm run test:e2e
```

Use another base URL:

```bash
PAXOS_BASE_URL=http://localhost:3001 npm run test:e2e
```

The first spec opens three isolated browser contexts. Each context has separate
`localStorage`, so each one joins the Synod room as a distinct mobile client.

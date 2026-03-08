# TieredEdge Live Bet Log

This project publishes your `betting-state.md` as a public transparency page.

## What it does
- Reads: `~/.openclaw/workspace/memory/betting-state.md`
- Builds: `public/data.json`
- Renders: `public/index.html`

## Quick start

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run build:data
npm run serve
```

Open:
- `http://127.0.0.1:8095/index.html`

No-server preview:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run build:all
open /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/public/standalone.html
```

## Update flow

When your state file changes, regenerate data:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run build:data
```

Optional: hide pending/open plays from public output:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
REDACT_PENDING=true npm run build:data
```

Or use the helper script:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
./scripts/update-live-log.sh
```

## Auto-update every 5 minutes (local cron)

```bash
crontab -e
```

Add:

```cron
*/5 * * * * cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /usr/local/bin/node scripts/build-live-log.mjs >/tmp/tierededge-live-log.log 2>&1
```

## Publish options

### Option A: GitHub Pages (recommended)
1. Create repo `tierededge-live-log`.
2. Push this project.
3. Enable GitHub Pages from `/public` (or root with `public` copied to docs).
4. Set cron/local automation to rebuild + commit `public/data.json` when updated.

Detailed guide:
- `DEPLOY-GITHUB-PAGES.md`

### Option B: Static host (Cloudflare Pages / Netlify)
- Deploy this folder as static site.
- Rebuild `public/data.json` before each deploy.

## Truth-first guidance
- This page is only as accurate as `betting-state.md`.
- Keep update timestamps visible.
- Do not publish claims that are not in the source data.

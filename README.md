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

Append-only recommendation utility:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run append:rec -- '{"rec_id":"20260308-001","timestamp_ct":"2026-03-08 10:00 AM","sport":"NBA","market":"ML","selection":"BOS","source_book":"FanDuel","recommended_odds_us":"+110","recommended_odds_dec":"2.10","true_prob":"48.5%","implied_prob_fair":"47.6%","edge_pct":"1.9%","kelly_stake":"$0.00","decision":"SIT","rejection_reason":"no_edge","odds_quality":"live","injury_quality":"n/a","market_quality":"tight","confidence_total":"medium"}'
```

Nightly integrity + backfill pass:

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run audit:rec-nightly
```

Machine-readable output:

- `public/integrity/recommendation-log-integrity.json`

One-time historical import (safe dry-run default):

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run import:rec-history -- /absolute/path/to/history.csv --dry-run
```

Apply mode (append-only validation enforced):

```bash
cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log
npm run import:rec-history -- /absolute/path/to/history.csv --apply
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
- Recommendation log is treated as append-only historical ledger.

## Optional market-context scaffold

This repo now supports optional context signals from recommendation rows without changing canonical decision authority.

- Hook config: `config/market-context-hooks.json`
- Payload outputs:
  - `market_context`
  - `market_context_hooks_config`
  - `daily_decision_summary.market_context_notes` (when context signals exist)

Supported optional recommendation-log columns:

- `market_leader_price`
- `market_leader_movement`
- `injury_confirmation_flag`
- `lineup_confirmation_flag`
- `rest_disadvantage_flag`
- `travel_disadvantage_flag`
- `market_context_source`
- `context_timestamp`
- `context_verification_status`
- `context_stale_flag`

Guardrail:
- Hook mode defaults to `advisory_only`; no mandatory dependency on context signals.
- If context is absent, output behavior remains unchanged.

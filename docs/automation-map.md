# TieredEdge Automation Map

## Active scheduled jobs

| Job | Trigger | Schedule (CT) | Canonical? | Reads | Writes |
|---|---|---:|---|---|---|
| `morning-edge-hunt` | OpenClaw cron | 06:00 daily | canonical runtime input | OpenClaw skill + memory | WhatsApp delivery, OpenClaw run history, may update OpenClaw memory/state |
| `friday-sgp` | OpenClaw cron | 10:00 Friday | canonical runtime input | OpenClaw skill + memory | WhatsApp delivery, OpenClaw run history, may update OpenClaw memory/state |
| `evening-grading` | OpenClaw cron | 23:00 daily | canonical runtime input | OpenClaw memory | WhatsApp delivery, OpenClaw run history, may update OpenClaw memory/state |
| `weekly-review` | OpenClaw cron | 09:00 Monday | derived review | OpenClaw memory | WhatsApp delivery, OpenClaw run history |
| `update-live-log.sh` | system crontab | every 10 minutes | canonical public rebuild | OpenClaw memory + repo data | `data/*`, `public/*`, repo-root deploy mirrors, optional git push |
| `run-monthly-bankroll-contribution.sh` | system crontab | 00:07 on day 1 | canonical contribution writer | OpenClaw bet log + repo ledger | `data/bankroll-contributions.csv`, `data/bankroll-contribution-status.json`, then canonical public rebuild |

## Manual or ad hoc jobs

| Job | Status | Notes |
|---|---|---|
| `run-nightly-data-hygiene.sh` | manual only | Backfills recommendation-log gaps, then calls the canonical rebuild path. Do not schedule separately from `update-live-log.sh`. |
| `build-live-log.mjs` | library-style build step | Safe for manual development, not a scheduled entrypoint. |
| `build-runtime-status.mjs` | library-style build step | Used by `update-live-log.sh`. |
| `build-monthly-suppression-audit.mjs` | derived build step | Used by `update-live-log.sh`. |
| `enrich-suppressed-candidates.mjs` | derived build step | Used by `update-live-log.sh`. |

## Collision rules

1. `update-live-log.sh` is the only scheduled public rebuild entrypoint.
2. `run-monthly-bankroll-contribution.sh` must call `update-live-log.sh` instead of rebuilding/pushing on its own.
3. Any script that mutates public outputs must hold the shared `/tmp` live-log lock.
4. If source files change during a rebuild, the rebuild must abort before deploy/push.
5. Do not schedule `node scripts/build-live-log.mjs` directly.

## Shared write paths at highest risk

- `data/openclaw-runtime-status.json`
- `data/candidate-markets.csv`
- `data/suppressed-candidates.csv`
- `data/suppression-audit-enrichment.csv`
- `public/data.json`
- `public/decision-terminal.txt`
- `public/decision-whatsapp.txt`
- `public/standalone.html`
- repo-root deploy mirrors when `LIVE_LOG_DEPLOY_REPO` points to this repo

## Allowed mutation order

1. OpenClaw scheduled job updates OpenClaw runtime/state.
2. `update-live-log.sh` snapshots source mtimes.
3. `update-live-log.sh` rebuilds runtime status, grading cache, payload, suppression artifacts, standalone page.
4. `update-live-log.sh` verifies source files did not change mid-build.
5. `update-live-log.sh` syncs/pushes public outputs.

If step 4 fails, the run is invalid and should be retried rather than deployed.

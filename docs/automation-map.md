# TieredEdge Automation Map

## Active scheduled jobs

| Job | Trigger | Schedule (CT) | Canonical? | Reads | Writes |
|---|---|---:|---|---|---|
| `telegram-operator-poll` | system crontab | every minute | operator interface | repo env + Telegram API | `data/telegram-operator-events.jsonl`, `data/telegram-operator-state.json`, `data/direct-automation-runs.jsonl` |
| `morning-edge-hunt` | system crontab | 06:00 daily | canonical runtime input | repo env + odds feed | `data/canonical-hunt-runs.jsonl`, canonical hunt artifact, `data/direct-automation-runs.jsonl`, then canonical public rebuild |
| `midday-edge-hunt` | system crontab | 12:00 daily | canonical runtime input | repo env + odds feed | `data/canonical-hunt-runs.jsonl`, canonical hunt artifact, `data/direct-automation-runs.jsonl`, then canonical public rebuild |
| `afternoon-edge-hunt` | system crontab | 15:00 daily | canonical runtime input | repo env + odds feed | `data/canonical-hunt-runs.jsonl`, canonical hunt artifact, `data/direct-automation-runs.jsonl`, then canonical public rebuild |
| `rejected-close-capture-evening` | system crontab | 23:35 daily | rejected-opportunity maintenance | repo env + odds feed | `data/rejected-close-capture-runs.jsonl`, `data/rejected-close-capture-log.jsonl`, `data/direct-automation-runs.jsonl`, then canonical public rebuild |
| `rejected-close-capture-late-night` | system crontab | 02:35 daily | rejected-opportunity maintenance | repo env + odds feed | `data/rejected-close-capture-runs.jsonl`, `data/rejected-close-capture-log.jsonl`, `data/direct-automation-runs.jsonl`, then canonical public rebuild |
| `rejected-close-capture-morning-cleanup` | system crontab | 09:15 daily | rejected-opportunity maintenance | repo env + odds feed | `data/rejected-close-capture-runs.jsonl`, `data/rejected-close-capture-log.jsonl`, `data/direct-automation-runs.jsonl`, then canonical public rebuild |
| `friday-sgp` | OpenClaw cron | 10:00 Friday | optional/manual only | OpenClaw skill + memory | currently disabled to reduce model/API burn |
| `evening-grading` | OpenClaw cron | 23:00 daily | optional/manual only | OpenClaw memory | currently disabled to reduce model/API burn |
| `weekly-review` | OpenClaw cron | 09:00 Monday | optional/manual only | OpenClaw memory | currently disabled to reduce model/API burn |
| `update-live-log.sh` | system crontab | every 10 minutes | canonical public rebuild | OpenClaw memory + repo data | `data/*`, `public/*`, repo-root deploy artifacts, optional git push |
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

1. Mission-critical hunt, Telegram, and rejected-close-capture jobs must run as direct local repo commands, not model-backed `agentTurn`.
2. WhatsApp is disabled for TieredEdge operations. Preserve history only; do not use it as a live operator surface.
3. `update-live-log.sh` is the only scheduled public rebuild entrypoint, except when it is called by a direct local wrapper after a canonical hunt or close-capture run.
4. `run-monthly-bankroll-contribution.sh` must call `update-live-log.sh` instead of rebuilding/pushing on its own.
5. Any script that mutates public outputs must hold the shared `/tmp` live-log lock.
6. If source files change during a rebuild, the rebuild must abort before deploy/push.
7. Do not schedule `node scripts/build-live-log.mjs` directly.

## Shared write paths at highest risk

- `data/openclaw-runtime-status.json`
- `data/candidate-markets.csv`
- `data/suppressed-candidates.csv`
- `data/suppression-audit-enrichment.csv`
- `public/data.json`
- `public/decision-terminal.txt`
- `public/decision-whatsapp.txt`
- `public/standalone.html`
- repo-root deploy artifacts (`data.json`, `standalone.html`, `app.js`, `index.html`, `styles.css`)

## Allowed mutation order

1. Direct local wrapper or OpenClaw non-critical job updates repo/runtime state.
2. `update-live-log.sh` snapshots source mtimes.
3. `update-live-log.sh` rebuilds runtime status, grading cache, payload, suppression artifacts, standalone page.
4. `update-live-log.sh` verifies source files did not change mid-build.
5. `update-live-log.sh` syncs `public/` back to repo root, then pushes the repo-root deploy artifacts used by GitHub Pages.

If step 4 fails, the run is invalid and should be retried rather than deployed.

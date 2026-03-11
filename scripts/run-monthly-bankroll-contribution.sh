#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Append-only monthly contribution writer (idempotent by effective_month).
node scripts/monthly-bankroll-contribution.mjs "$@"

# Rebuild/publication so policy status and ledger are visible immediately.
node scripts/build-live-log.mjs
node scripts/build-standalone.mjs

if [[ -n "${LIVE_LOG_DEPLOY_REPO:-}" ]]; then
  if [[ "$LIVE_LOG_DEPLOY_REPO" != "$ROOT_DIR" ]]; then
    rsync -a --delete "$ROOT_DIR/public/" "$LIVE_LOG_DEPLOY_REPO/"
    cd "$LIVE_LOG_DEPLOY_REPO"
  else
    rsync -a "$ROOT_DIR/public/" "$ROOT_DIR/"
    cd "$ROOT_DIR"
  fi

  git add -A
  if ! git diff --cached --quiet; then
    git commit -m "Monthly bankroll contribution update $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    git push
  fi
fi

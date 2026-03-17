#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"
acquire_live_log_lock "run-monthly-bankroll-contribution.sh"

# Append-only monthly contribution writer (idempotent by effective_month).
node scripts/monthly-bankroll-contribution.mjs "$@"

# Reuse the canonical rebuild/deploy path instead of duplicating it here.
TIEREDEDGE_LOCK_HELD=1 "$ROOT_DIR/scripts/update-live-log.sh"

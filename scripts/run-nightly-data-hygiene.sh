#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"
acquire_live_log_lock "run-nightly-data-hygiene.sh"

# Nightly integrity pass:
# 1) validate and backfill recommendation log when reconstructable
# 2) rebuild through the canonical live-log path so outputs stay aligned
node scripts/recommendation-log-nightly.mjs
TIEREDEDGE_LOCK_HELD=1 "$ROOT_DIR/scripts/update-live-log.sh"

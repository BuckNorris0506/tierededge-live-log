#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"
acquire_live_log_lock "run-rejected-close-capture.sh"

node scripts/capture-rejected-closing-lines.mjs
TIEREDEDGE_LOCK_HELD=1 "$ROOT_DIR/scripts/update-live-log.sh"

#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"

if ! acquire_named_lock "rejected-close-capture" "run-rejected-close-capture.sh"; then
  node scripts/capture-rejected-closing-lines.mjs --skip-due-to-active-lock
  exit 0
fi

if ! acquire_live_log_lock "run-rejected-close-capture.sh"; then
  node scripts/capture-rejected-closing-lines.mjs --skip-due-to-active-lock
  exit 0
fi

node scripts/capture-rejected-closing-lines.mjs
"$ROOT_DIR/scripts/update-live-log.sh"

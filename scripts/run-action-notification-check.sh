#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"

if ! acquire_named_lock "action-notification" "run-action-notification-check.sh"; then
  exit 0
fi

if ! acquire_live_log_lock "run-action-notification-check.sh"; then
  exit 0
fi

NOTIFICATION_OUTPUT="$(node scripts/evaluate-action-notifications.mjs)"

if [[ -n "${NOTIFICATION_OUTPUT}" ]]; then
  "$ROOT_DIR/scripts/update-live-log.sh" >/dev/null
  printf '%s\n' "$NOTIFICATION_OUTPUT"
fi


#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"

JOB_NAME="telegram-operator-poll"
FORWARD_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-name)
      JOB_NAME="${2:-$JOB_NAME}"
      shift 2
      ;;
    *)
      FORWARD_ARGS+=("$1")
      shift
      ;;
  esac
done

STARTED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
AUTOMATION_RUN_ID="${JOB_NAME}::${STARTED_AT_UTC}"
POLL_JSON_FILE="$(mktemp)"
POLL_STDERR_FILE="$(mktemp)"

cleanup() {
  rm -f "$POLL_JSON_FILE" "$POLL_STDERR_FILE"
}
trap cleanup EXIT

if ! acquire_named_lock "telegram-operator" "run-telegram-operator-poll.sh"; then
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'skipped_due_to_active_lock',
  command_path: '$ROOT_DIR/scripts/run-telegram-operator-poll.sh',
  child_command: 'node scripts/telegram-operator-bot.mjs --json',
  lock_name: 'telegram-operator',
});
EOF
  exit 0
fi

if node scripts/telegram-operator-bot.mjs --json "${FORWARD_ARGS[@]}" >"$POLL_JSON_FILE" 2>"$POLL_STDERR_FILE"; then
  PROCESSED_UPDATES="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.processed_updates ?? 0));" "$POLL_JSON_FILE")"
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'ok',
  command_path: '$ROOT_DIR/scripts/run-telegram-operator-poll.sh',
  child_command: 'node scripts/telegram-operator-bot.mjs --json',
  processed_updates: Number('$PROCESSED_UPDATES') || 0,
});
EOF
  cat "$POLL_JSON_FILE"
  exit 0
fi

node --input-type=module <<EOF
import fs from 'node:fs';
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const stderr = fs.readFileSync('$POLL_STDERR_FILE', 'utf8').trim();
appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'failed',
  command_path: '$ROOT_DIR/scripts/run-telegram-operator-poll.sh',
  child_command: 'node scripts/telegram-operator-bot.mjs --json',
  error: stderr || 'telegram_operator_poll_failed',
});
EOF

cat "$POLL_STDERR_FILE" >&2
exit 1

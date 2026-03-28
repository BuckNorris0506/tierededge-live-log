#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"

JOB_NAME="scheduled-canonical-hunt"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-name)
      JOB_NAME="${2:-$JOB_NAME}"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

STARTED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
AUTOMATION_RUN_ID="${JOB_NAME}::${STARTED_AT_UTC}"
RUNNER_JSON_FILE="$(mktemp)"
RUNNER_STDERR_FILE="$(mktemp)"
REBUILD_STDOUT_FILE="$(mktemp)"
REBUILD_STDERR_FILE="$(mktemp)"

cleanup() {
  rm -f "$RUNNER_JSON_FILE" "$RUNNER_STDERR_FILE" "$REBUILD_STDOUT_FILE" "$REBUILD_STDERR_FILE"
}
trap cleanup EXIT

if node scripts/run-canonical-hunt.mjs --json >"$RUNNER_JSON_FILE" 2>"$RUNNER_STDERR_FILE"; then
  :
else
  node --input-type=module <<EOF
import fs from 'node:fs';
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const stderr = fs.readFileSync('$RUNNER_STDERR_FILE', 'utf8').trim();
appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'failed',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || '$ROOT_DIR/scripts/run-scheduled-canonical-hunt.sh',
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json',
  error: stderr || 'canonical_runner_failed',
});
EOF
  cat "$RUNNER_STDERR_FILE" >&2
  exit 1
fi

RUNNER_STATUS="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.status || 'ok'));" "$RUNNER_JSON_FILE")"
RUN_ID="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.run_id || payload.runId || ''));" "$RUNNER_JSON_FILE")"
LOCK_NAME="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.lock_name || ''));" "$RUNNER_JSON_FILE")"

if [[ "$RUNNER_STATUS" == "skipped_due_to_active_lock" ]]; then
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'skipped_due_to_active_lock',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || '$ROOT_DIR/scripts/run-scheduled-canonical-hunt.sh',
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json',
  run_id: '$RUN_ID',
  lock_name: '$LOCK_NAME',
});
EOF
  cat "$RUNNER_JSON_FILE"
  exit 0
fi

if "$ROOT_DIR/scripts/update-live-log.sh" >"$REBUILD_STDOUT_FILE" 2>"$REBUILD_STDERR_FILE"; then
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'ok',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || '$ROOT_DIR/scripts/run-scheduled-canonical-hunt.sh',
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json && ./scripts/update-live-log.sh',
  run_id: '$RUN_ID',
  rebuild_status: 'ok',
});
EOF
  cat "$RUNNER_JSON_FILE"
  exit 0
fi

REBUILD_STDERR="$(cat "$REBUILD_STDERR_FILE" 2>/dev/null || true)"
REBUILD_STDOUT="$(cat "$REBUILD_STDOUT_FILE" 2>/dev/null || true)"
REBUILD_COMBINED="$(printf '%s\n%s' "$REBUILD_STDERR" "$REBUILD_STDOUT" | sed '/^$/d')"

if printf '%s' "$REBUILD_COMBINED" | grep -Eiq 'failed to push some refs|remote unpack failed|early EOF|mmap failed'; then
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const combined = ${REBUILD_COMBINED@Q};
appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'complete_with_deploy_warning',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || '$ROOT_DIR/scripts/run-scheduled-canonical-hunt.sh',
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json && ./scripts/update-live-log.sh',
  run_id: '$RUN_ID',
  rebuild_status: 'ok',
  deploy_status: 'failed',
  deploy_error: combined || 'deploy_push_failed',
  error: combined || 'deploy_push_failed',
});
process.stdout.write(JSON.stringify({
  status: 'complete_with_deploy_warning',
  run_id: '$RUN_ID',
  rebuild_status: 'ok',
  deploy_status: 'failed',
  deploy_error: combined || 'deploy_push_failed',
}));
EOF
  exit 0
fi

node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const combined = ${REBUILD_COMBINED@Q};
appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'failed',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || '$ROOT_DIR/scripts/run-scheduled-canonical-hunt.sh',
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json && ./scripts/update-live-log.sh',
  run_id: '$RUN_ID',
  rebuild_status: 'failed',
  error: combined || 'update_live_log_failed',
});
EOF

cat "$REBUILD_STDERR_FILE" >&2
exit 1

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

send_scheduled_heartbeat() {
  local completed_status="$1"
  local deploy_status="${2:-}"
  local deploy_error="${3:-}"
  local -a heartbeat_args
  heartbeat_args=(
    scripts/send-scheduled-hunt-heartbeat.mjs
    --job-name "$JOB_NAME"
    --runner-json-file "$RUNNER_JSON_FILE"
    --automation-run-id "$AUTOMATION_RUN_ID"
    --started-at-utc "$STARTED_AT_UTC"
    --completed-status "$completed_status"
  )
  if [[ -n "$deploy_status" ]]; then
    heartbeat_args+=(--deploy-status "$deploy_status")
  fi
  if [[ -n "$deploy_error" ]]; then
    heartbeat_args+=(--deploy-error "$deploy_error")
  fi
  node "${heartbeat_args[@]}" >/dev/null 2>&1 || true
}

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
  send_scheduled_heartbeat "ok"
  cat "$RUNNER_JSON_FILE"
  exit 0
fi

REBUILD_STDERR="$(cat "$REBUILD_STDERR_FILE" 2>/dev/null || true)"
REBUILD_STDOUT="$(cat "$REBUILD_STDOUT_FILE" 2>/dev/null || true)"
REBUILD_COMBINED="$(printf '%s\n%s' "$REBUILD_STDERR" "$REBUILD_STDOUT" | sed '/^$/d')"

if printf '%s' "$REBUILD_COMBINED" | grep -Eiq 'failed to push some refs|remote unpack failed|early EOF|mmap failed'; then
  AUTOMATION_RUN_ID="$AUTOMATION_RUN_ID" \
  JOB_NAME="$JOB_NAME" \
  STARTED_AT_UTC="$STARTED_AT_UTC" \
  RUN_ID="$RUN_ID" \
  ROOT_DIR="$ROOT_DIR" \
  REBUILD_COMBINED="$REBUILD_COMBINED" \
  node --input-type=module <<'EOF'
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const combined = process.env.REBUILD_COMBINED || '';
appendDirectAutomationRun({
  automation_run_id: process.env.AUTOMATION_RUN_ID,
  job_name: process.env.JOB_NAME,
  started_at_utc: process.env.STARTED_AT_UTC,
  completed_at_utc: new Date().toISOString(),
  status: 'complete_with_deploy_warning',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || `${process.env.ROOT_DIR}/scripts/run-scheduled-canonical-hunt.sh`,
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json && ./scripts/update-live-log.sh',
  run_id: process.env.RUN_ID,
  rebuild_status: 'ok',
  deploy_status: 'failed',
  deploy_error: combined || 'deploy_push_failed',
  error: combined || 'deploy_push_failed',
});
process.stdout.write(JSON.stringify({
  status: 'complete_with_deploy_warning',
  run_id: process.env.RUN_ID,
  rebuild_status: 'ok',
  deploy_status: 'failed',
  deploy_error: combined || 'deploy_push_failed',
  }));
EOF
  send_scheduled_heartbeat "complete_with_deploy_warning" "failed" "$REBUILD_COMBINED"
  exit 0
fi

AUTOMATION_RUN_ID="$AUTOMATION_RUN_ID" \
JOB_NAME="$JOB_NAME" \
STARTED_AT_UTC="$STARTED_AT_UTC" \
RUN_ID="$RUN_ID" \
ROOT_DIR="$ROOT_DIR" \
REBUILD_COMBINED="$REBUILD_COMBINED" \
node --input-type=module <<'EOF'
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const combined = process.env.REBUILD_COMBINED || '';
appendDirectAutomationRun({
  automation_run_id: process.env.AUTOMATION_RUN_ID,
  job_name: process.env.JOB_NAME,
  started_at_utc: process.env.STARTED_AT_UTC,
  completed_at_utc: new Date().toISOString(),
  status: 'failed',
  command_path: process.env.TIEREDGE_SCHEDULED_HUNT_COMMAND_PATH || `${process.env.ROOT_DIR}/scripts/run-scheduled-canonical-hunt.sh`,
  child_command: process.env.TIEREDGE_SCHEDULED_HUNT_CHILD_COMMAND || 'node scripts/run-canonical-hunt.mjs --json && ./scripts/update-live-log.sh',
  run_id: process.env.RUN_ID,
  rebuild_status: 'failed',
  error: combined || 'update_live_log_failed',
});
process.stdout.write(JSON.stringify({
  status: 'failed',
  run_id: process.env.RUN_ID,
  rebuild_status: 'failed',
  error: combined || 'update_live_log_failed',
}));
EOF

cat "$REBUILD_STDERR_FILE" >&2
exit 1

#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"

JOB_NAME="rejected-close-capture"
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
CAPTURE_JSON_FILE="$(mktemp)"
CAPTURE_STDERR_FILE="$(mktemp)"
REBUILD_STDOUT_FILE="$(mktemp)"
REBUILD_STDERR_FILE="$(mktemp)"

cleanup() {
  rm -f "$CAPTURE_JSON_FILE" "$CAPTURE_STDERR_FILE" "$REBUILD_STDOUT_FILE" "$REBUILD_STDERR_FILE"
}
trap cleanup EXIT

if ! acquire_named_lock "rejected-close-capture" "run-rejected-close-capture.sh"; then
  node scripts/capture-rejected-closing-lines.mjs --skip-due-to-active-lock >"$CAPTURE_JSON_FILE"
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'skipped_due_to_active_lock',
  command_path: '$ROOT_DIR/scripts/run-rejected-close-capture.sh',
  child_command: 'node scripts/capture-rejected-closing-lines.mjs',
  lock_name: 'rejected-close-capture',
});
EOF
  cat "$CAPTURE_JSON_FILE"
  exit 0
fi

if ! acquire_live_log_lock "run-rejected-close-capture.sh"; then
  node scripts/capture-rejected-closing-lines.mjs --skip-due-to-active-lock >"$CAPTURE_JSON_FILE"
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'skipped_due_to_active_lock',
  command_path: '$ROOT_DIR/scripts/run-rejected-close-capture.sh',
  child_command: 'node scripts/capture-rejected-closing-lines.mjs',
  lock_name: 'live-log',
});
EOF
  cat "$CAPTURE_JSON_FILE"
  exit 0
fi

if node scripts/capture-rejected-closing-lines.mjs --json >"$CAPTURE_JSON_FILE" 2>"$CAPTURE_STDERR_FILE"; then
  :
else
  node --input-type=module <<EOF
import fs from 'node:fs';
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const stderr = fs.readFileSync('$CAPTURE_STDERR_FILE', 'utf8').trim();
appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'failed',
  command_path: '$ROOT_DIR/scripts/run-rejected-close-capture.sh',
  child_command: 'node scripts/capture-rejected-closing-lines.mjs --json',
  error: stderr || 'rejected_close_capture_failed',
});
EOF
  cat "$CAPTURE_STDERR_FILE" >&2
  exit 1
fi

if "$ROOT_DIR/scripts/update-live-log.sh" >"$REBUILD_STDOUT_FILE" 2>"$REBUILD_STDERR_FILE"; then
  CAPTURE_RUN_ID="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.run_id || ''));" "$CAPTURE_JSON_FILE")"
  CAPTURE_STATUS="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.status || 'ok'));" "$CAPTURE_JSON_FILE")"
  node --input-type=module <<EOF
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: '$CAPTURE_STATUS',
  command_path: '$ROOT_DIR/scripts/run-rejected-close-capture.sh',
  child_command: 'node scripts/capture-rejected-closing-lines.mjs --json && ./scripts/update-live-log.sh',
  capture_run_id: '$CAPTURE_RUN_ID',
  rebuild_status: 'ok',
});
EOF
  cat "$CAPTURE_JSON_FILE"
  exit 0
fi

node --input-type=module <<EOF
import fs from 'node:fs';
import { appendDirectAutomationRun } from './scripts/direct-automation-log-utils.mjs';

const stderr = fs.readFileSync('$REBUILD_STDERR_FILE', 'utf8').trim();
const stdout = fs.readFileSync('$REBUILD_STDOUT_FILE', 'utf8').trim();
const payload = JSON.parse(fs.readFileSync('$CAPTURE_JSON_FILE', 'utf8'));
appendDirectAutomationRun({
  automation_run_id: '$AUTOMATION_RUN_ID',
  job_name: '$JOB_NAME',
  started_at_utc: '$STARTED_AT_UTC',
  completed_at_utc: new Date().toISOString(),
  status: 'failed',
  command_path: '$ROOT_DIR/scripts/run-rejected-close-capture.sh',
  child_command: 'node scripts/capture-rejected-closing-lines.mjs --json && ./scripts/update-live-log.sh',
  capture_run_id: payload.run_id || null,
  rebuild_status: 'failed',
  error: stderr || stdout || 'update_live_log_failed',
});
EOF

cat "$REBUILD_STDERR_FILE" >&2
exit 1

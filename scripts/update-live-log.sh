#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
DEPLOY_SYNC_STATUS_PATH="$ROOT_DIR/data/deploy-sync-status.json"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"
acquire_live_log_lock "update-live-log.sh"

write_deploy_sync_status() {
  local deploy_state="$1"
  local attempts="${2:-0}"
  local push_error_file="${3:-}"
  local last_error=""
  local head_sha=""
  local upstream_sha=""
  local ahead_count="0"
  local branch_name=""

  head_sha="$(git rev-parse HEAD 2>/dev/null || true)"
  branch_name="$(git branch --show-current 2>/dev/null || true)"
  upstream_sha="$(git rev-parse origin/main 2>/dev/null || true)"
  if [[ -n "$head_sha" && -n "$upstream_sha" ]]; then
    ahead_count="$(git rev-list --count "${upstream_sha}..${head_sha}" 2>/dev/null || echo "0")"
  fi
  if [[ -n "$push_error_file" && -f "$push_error_file" ]]; then
    last_error="$(cat "$push_error_file" 2>/dev/null || true)"
  fi

  DEPLOY_STATE="$deploy_state" \
  ATTEMPTS="$attempts" \
  HEAD_SHA="$head_sha" \
  UPSTREAM_SHA="$upstream_sha" \
  AHEAD_COUNT="$ahead_count" \
  BRANCH_NAME="$branch_name" \
  LAST_ERROR="$last_error" \
  DEPLOY_SYNC_STATUS_PATH="$DEPLOY_SYNC_STATUS_PATH" \
  node --input-type=module <<'EOF'
import fs from 'node:fs';

  const payload = {
    generated_at_utc: new Date().toISOString(),
    status: process.env.DEPLOY_STATE || 'unknown',
  attempts: Number(process.env.ATTEMPTS || '0'),
  branch: process.env.BRANCH_NAME || null,
  local_head: process.env.HEAD_SHA || null,
  upstream_head: process.env.UPSTREAM_SHA || null,
  ahead_of_upstream_count: Number(process.env.AHEAD_COUNT || '0'),
  last_error: process.env.LAST_ERROR || null,
};

fs.writeFileSync(process.env.DEPLOY_SYNC_STATUS_PATH, `${JSON.stringify(payload, null, 2)}\n`);
EOF
}

attempt_live_log_push() {
  local push_error_file="$1"
  if git push >"$push_error_file.stdout" 2>"$push_error_file"; then
    return 0
  fi

  if grep -Eiq 'mmap failed|remote unpack failed|early EOF|failed to push some refs' "$push_error_file"; then
    echo "WARN: live-log push hit git pack failure; running local maintenance and retrying once." >&2
    git gc --auto || true
    git repack -d -l || true
    git prune-packed || true
    if git -c pack.window=0 -c pack.threads=1 push >>"$push_error_file.stdout" 2>>"$push_error_file"; then
      return 0
    fi
  fi

  return 1
}

node scripts/build-runtime-status.mjs
if ! node scripts/update-passed-opportunity-grades.mjs; then
  echo "WARN: passed-opportunity grading failed; continuing with existing grades."
fi
node scripts/backfill-override-log.mjs
node scripts/backfill-execution-metadata.mjs
node scripts/reconcile-grading-bankroll.mjs
node scripts/build-weekly-truth-report.mjs
node scripts/build-weekly-operator-review.mjs
snapshot_source_state \
  /Users/jaredbuckman/.openclaw/cron/jobs.json \
  /Users/jaredbuckman/.openclaw/workspace/memory/odds-api-config.md \
  "$ROOT_DIR/data/decision-ledger.jsonl" \
  "$ROOT_DIR/data/grading-ledger.jsonl" \
  "$ROOT_DIR/data/bankroll-ledger.jsonl"
node scripts/build-execution-board.mjs
if ! node scripts/validate-ledger-invariants.mjs; then
  echo "WARN: ledger validator failed prebuild; rendering blocked canonical state."
fi
node scripts/build-canonical-state.mjs
cp "$ROOT_DIR/app.js" "$ROOT_DIR/public/app.js"
cp "$ROOT_DIR/index.html" "$ROOT_DIR/public/index.html"
cp "$ROOT_DIR/styles.css" "$ROOT_DIR/public/styles.css"
node scripts/build-live-log.mjs
node scripts/build-standalone.mjs
# GitHub Pages currently serves repo-root artifacts from main, not public/.
# Keep root deploy artifacts in sync with the freshly built public outputs.
rsync -a "$ROOT_DIR/public/" "$ROOT_DIR/"
if ! node scripts/sync-hunt-job-state.mjs; then
  echo "WARN: failed to sync hunt automation state from repo truth."
fi
# Refresh guarded source fingerprints after intentional hunt-job state sync.
snapshot_source_state \
  /Users/jaredbuckman/.openclaw/cron/jobs.json \
  /Users/jaredbuckman/.openclaw/workspace/memory/odds-api-config.md \
  "$ROOT_DIR/data/decision-ledger.jsonl" \
  "$ROOT_DIR/data/grading-ledger.jsonl" \
  "$ROOT_DIR/data/bankroll-ledger.jsonl"
if ! node scripts/validate-ledger-invariants.mjs --require-output-match; then
  echo "WARN: ledger validator failed postbuild; published state remains blocked."
fi

NOTIFICATION_JSON="$(mktemp)"
node scripts/evaluate-action-notifications.mjs --json > "$NOTIFICATION_JSON"
NOTIFICATION_STATUS="$(node -e "const fs=require('fs'); const payload=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(payload.status || 'no_alert'));" "$NOTIFICATION_JSON")"
if [[ "$NOTIFICATION_STATUS" != "no_alert" ]]; then
  node scripts/build-canonical-state.mjs
  node scripts/build-live-log.mjs
  node scripts/build-standalone.mjs
  rsync -a "$ROOT_DIR/public/" "$ROOT_DIR/"
fi
rm -f "$NOTIFICATION_JSON"

assert_source_state_unchanged

echo "Live log data rebuilt (including standalone page)."
if [[ -f "$ROOT_DIR/public/decision-terminal.txt" ]]; then
  echo ""
  cat "$ROOT_DIR/public/decision-terminal.txt"
fi
if [[ -f "$ROOT_DIR/public/evening-grading-report.txt" ]]; then
  echo ""
  cat "$ROOT_DIR/public/evening-grading-report.txt"
fi

# Optional deploy sync: set LIVE_LOG_DEPLOY_REPO to a local git repo path.
# Current production Pages model publishes repo-root artifacts from main.
if [[ -n "${LIVE_LOG_DEPLOY_REPO:-}" ]]; then
  if [[ ! -d "$LIVE_LOG_DEPLOY_REPO/.git" ]]; then
    echo "LIVE_LOG_DEPLOY_REPO is set but is not a git repo: $LIVE_LOG_DEPLOY_REPO"
    exit 1
  fi

  if [[ "$LIVE_LOG_DEPLOY_REPO" != "$ROOT_DIR" ]]; then
    # External deploy repo mode: publish built public files into the target repo root.
    rsync -a --delete "$ROOT_DIR/public/" "$LIVE_LOG_DEPLOY_REPO/"
    cd "$LIVE_LOG_DEPLOY_REPO"
  else
    # In-place mode: local root sync already completed above.
    cd "$ROOT_DIR"
  fi

  # Auto-sync mode: stage all repo changes (tracked/untracked/deletions).
  git add -A
  if ! git diff --cached --quiet; then
    git commit -m "Update live bet log $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    PUSH_ERROR_FILE="$(mktemp)"
    if attempt_live_log_push "$PUSH_ERROR_FILE"; then
      write_deploy_sync_status "in_sync" "2" "$PUSH_ERROR_FILE"
      rm -f "$PUSH_ERROR_FILE" "$PUSH_ERROR_FILE.stdout"
      echo "Synced and pushed live log to: $LIVE_LOG_DEPLOY_REPO"
    else
      write_deploy_sync_status "push_failed" "2" "$PUSH_ERROR_FILE"
      cat "$PUSH_ERROR_FILE" >&2
      rm -f "$PUSH_ERROR_FILE" "$PUSH_ERROR_FILE.stdout"
      exit 1
    fi
  else
    write_deploy_sync_status "no_changes" "0"
    echo "No content changes to push."
  fi
fi

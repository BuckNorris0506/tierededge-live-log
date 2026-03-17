#!/bin/zsh

LOCK_DIR="${TMPDIR:-/tmp}/tierededge-live-log.lock"
LOCK_META_FILE="$LOCK_DIR/owner"

typeset -ga TIEREDGE_SOURCE_SNAPSHOTS

acquire_live_log_lock() {
  local owner="${1:-unknown}"
  if [[ "${TIEREDEDGE_LOCK_HELD:-0}" == "1" ]]; then
    return 0
  fi

  if mkdir "$LOCK_DIR" 2>/dev/null; then
    {
      echo "owner=$owner"
      echo "pid=$$"
      echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      echo "cwd=$(pwd)"
    } > "$LOCK_META_FILE"
    export TIEREDEDGE_LOCK_HELD=1
    trap release_live_log_lock EXIT INT TERM
    return 0
  fi

  echo "ABORT: live-log automation lock is already held." >&2
  if [[ -f "$LOCK_META_FILE" ]]; then
    echo "Lock owner metadata:" >&2
    cat "$LOCK_META_FILE" >&2
  fi
  return 1
}

release_live_log_lock() {
  if [[ "${TIEREDEDGE_LOCK_HELD:-0}" != "1" ]]; then
    return 0
  fi
  rm -f "$LOCK_META_FILE" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
  unset TIEREDEDGE_LOCK_HELD
}

snapshot_source_state() {
  local file
  local mtime
  TIEREDGE_SOURCE_SNAPSHOTS=()
  for file in "$@"; do
    if [[ -f "$file" ]]; then
      mtime="$(stat -f '%m' "$file" 2>/dev/null || echo missing)"
    else
      mtime="missing"
    fi
    TIEREDGE_SOURCE_SNAPSHOTS+=("${file}::${mtime}")
  done
}

assert_source_state_unchanged() {
  local changed=0
  local snapshot file before after
  for snapshot in "${TIEREDGE_SOURCE_SNAPSHOTS[@]}"; do
    file="${snapshot%%::*}"
    before="${snapshot##*::}"
    if [[ -f "$file" ]]; then
      after="$(stat -f '%m' "$file" 2>/dev/null || echo missing)"
    else
      after="missing"
    fi
    if [[ "$before" != "$after" ]]; then
      echo "ABORT: source changed during rebuild: $file (before=$before after=$after)" >&2
      changed=1
    fi
  done
  return "$changed"
}

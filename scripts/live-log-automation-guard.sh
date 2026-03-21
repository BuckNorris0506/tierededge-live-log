#!/bin/zsh

LOCK_DIR="${TMPDIR:-/tmp}/tierededge-live-log.lock"
LOCK_META_FILE="$LOCK_DIR/owner"
LOCK_MAX_AGE_SECONDS=900

typeset -ga TIEREDGE_SOURCE_SNAPSHOTS

lock_owner_pid() {
  if [[ -f "$LOCK_META_FILE" ]]; then
    awk -F= '/^pid=/{print $2}' "$LOCK_META_FILE" 2>/dev/null
  fi
}

lock_owner_started_at() {
  if [[ -f "$LOCK_META_FILE" ]]; then
    awk -F= '/^started_at=/{print $2}' "$LOCK_META_FILE" 2>/dev/null
  fi
}

lock_is_stale() {
  local pid started_at started_epoch now age
  pid="$(lock_owner_pid)"
  if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi

  started_at="$(lock_owner_started_at)"
  if [[ -z "$started_at" ]]; then
    return 1
  fi
  started_epoch="$(date -j -f '%Y-%m-%dT%H:%M:%SZ' "$started_at" '+%s' 2>/dev/null || echo '')"
  if [[ -z "$started_epoch" ]]; then
    return 1
  fi
  now="$(date '+%s')"
  age=$(( now - started_epoch ))
  [[ "$age" -gt "$LOCK_MAX_AGE_SECONDS" ]]
}

clear_stale_live_log_lock() {
  rm -f "$LOCK_META_FILE" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
}

acquire_live_log_lock() {
  local owner="${1:-unknown}"
  if [[ "${TIEREDEDGE_LOCK_HELD:-0}" == "1" ]]; then
    return 0
  fi

  if [[ -d "$LOCK_DIR" ]] && lock_is_stale; then
    echo "WARN: removing stale live-log lock." >&2
    if [[ -f "$LOCK_META_FILE" ]]; then
      cat "$LOCK_META_FILE" >&2
    fi
    clear_stale_live_log_lock
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

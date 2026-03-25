#!/bin/zsh

LOCK_BASE_DIR="${TMPDIR:-/tmp}/tierededge-automation-locks"
LOCK_MAX_AGE_SECONDS=900

typeset -ga TIEREDGE_SOURCE_SNAPSHOTS
typeset -ga TIEREDGE_HELD_LOCKS

lock_dir_for() {
  local lock_name="${1:-live-log}"
  echo "${LOCK_BASE_DIR}/${lock_name}.lock"
}

lock_meta_file_for() {
  local lock_name="${1:-live-log}"
  echo "$(lock_dir_for "$lock_name")/owner"
}

lock_owner_pid() {
  local lock_name="${1:-live-log}"
  local lock_meta_file
  lock_meta_file="$(lock_meta_file_for "$lock_name")"
  if [[ -f "$lock_meta_file" ]]; then
    awk -F= '/^pid=/{print $2}' "$lock_meta_file" 2>/dev/null
  fi
}

lock_owner_started_at() {
  local lock_name="${1:-live-log}"
  local lock_meta_file
  lock_meta_file="$(lock_meta_file_for "$lock_name")"
  if [[ -f "$lock_meta_file" ]]; then
    awk -F= '/^started_at=/{print $2}' "$lock_meta_file" 2>/dev/null
  fi
}

lock_is_stale() {
  local lock_name="${1:-live-log}"
  local pid started_at started_epoch now age
  pid="$(lock_owner_pid "$lock_name")"
  if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi

  started_at="$(lock_owner_started_at "$lock_name")"
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

clear_named_lock() {
  local lock_name="${1:-live-log}"
  local lock_dir lock_meta_file
  lock_dir="$(lock_dir_for "$lock_name")"
  lock_meta_file="$(lock_meta_file_for "$lock_name")"
  rm -f "$lock_meta_file" 2>/dev/null || true
  rmdir "$lock_dir" 2>/dev/null || true
}

clear_stale_live_log_lock() {
  clear_named_lock "live-log"
}

lock_is_held_here() {
  local lock_name="${1:-live-log}"
  local held
  for held in "${TIEREDGE_HELD_LOCKS[@]:-}"; do
    if [[ "$held" == "$lock_name" ]]; then
      return 0
    fi
  done
  return 1
}

acquire_named_lock() {
  local lock_name="${1:-live-log}"
  local owner="${2:-unknown}"
  local lock_dir lock_meta_file
  lock_dir="$(lock_dir_for "$lock_name")"
  lock_meta_file="$(lock_meta_file_for "$lock_name")"

  mkdir -p "$LOCK_BASE_DIR"

  if lock_is_held_here "$lock_name"; then
    return 0
  fi

  if [[ -d "$lock_dir" ]] && lock_is_stale "$lock_name"; then
    echo "WARN: removing stale ${lock_name} lock." >&2
    if [[ -f "$lock_meta_file" ]]; then
      cat "$lock_meta_file" >&2
    fi
    clear_named_lock "$lock_name"
  fi

  if mkdir "$lock_dir" 2>/dev/null; then
    {
      echo "lock_name=$lock_name"
      echo "owner=$owner"
      echo "pid=$$"
      echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      echo "cwd=$(pwd)"
    } > "$lock_meta_file"
    TIEREDGE_HELD_LOCKS+=("$lock_name")
    trap release_all_named_locks EXIT INT TERM
    return 0
  fi

  echo "ABORT: ${lock_name} lock is already held." >&2
  if [[ -f "$lock_meta_file" ]]; then
    echo "Lock owner metadata:" >&2
    cat "$lock_meta_file" >&2
  fi
  return 1
}

release_named_lock() {
  local lock_name="${1:-live-log}"
  local remaining=()
  local held
  for held in "${TIEREDGE_HELD_LOCKS[@]:-}"; do
    if [[ "$held" == "$lock_name" ]]; then
      clear_named_lock "$lock_name"
    else
      remaining+=("$held")
    fi
  done
  TIEREDGE_HELD_LOCKS=("${remaining[@]}")
}

release_all_named_locks() {
  local held_locks=("${TIEREDGE_HELD_LOCKS[@]}")
  local held
  if (( ${#TIEREDGE_HELD_LOCKS[@]} == 0 )); then
    return 0
  fi
  for held in "${held_locks[@]}"; do
    clear_named_lock "$held"
  done
  TIEREDGE_HELD_LOCKS=()
}

acquire_live_log_lock() {
  local owner="${1:-unknown}"
  acquire_named_lock "live-log" "$owner"
}

release_live_log_lock() {
  release_named_lock "live-log"
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

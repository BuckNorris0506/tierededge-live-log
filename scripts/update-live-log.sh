#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Grade passed SIT opportunities before rebuilding artifacts.
# This writes counterfactual outcomes to the OpenClaw memory cache used by build-live-log.
node scripts/update-passed-opportunity-grades.mjs

node scripts/build-live-log.mjs
node scripts/build-standalone.mjs

echo "Live log data rebuilt (including standalone page)."

# Optional deploy sync: set LIVE_LOG_DEPLOY_REPO to a local git repo path
if [[ -n "${LIVE_LOG_DEPLOY_REPO:-}" ]]; then
  if [[ ! -d "$LIVE_LOG_DEPLOY_REPO/.git" ]]; then
    echo "LIVE_LOG_DEPLOY_REPO is set but is not a git repo: $LIVE_LOG_DEPLOY_REPO"
    exit 1
  fi

  if [[ "$LIVE_LOG_DEPLOY_REPO" != "$ROOT_DIR" ]]; then
    # External deploy repo mode: publish public site files to repo root.
    rsync -a --delete "$ROOT_DIR/public/" "$LIVE_LOG_DEPLOY_REPO/"
    cd "$LIVE_LOG_DEPLOY_REPO"
  else
    # In-place mode: sync built public artifacts to repo root for GitHub Pages root deploy.
    rsync -a "$ROOT_DIR/public/" "$ROOT_DIR/"
    cd "$ROOT_DIR"
  fi

  # Auto-sync mode: stage all repo changes (tracked/untracked/deletions).
  git add -A
  if ! git diff --cached --quiet; then
    git commit -m "Update live bet log $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    git push
    echo "Synced and pushed live log to: $LIVE_LOG_DEPLOY_REPO"
  else
    echo "No content changes to push."
  fi
fi

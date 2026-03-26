#!/bin/sh
set -eu

ROOT_DIR="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"
export PATH="/usr/local/bin:/usr/bin:/bin"
export LIVE_LOG_DEPLOY_REPO="$ROOT_DIR"

cd "$ROOT_DIR"
exec /bin/zsh "$ROOT_DIR/scripts/run-telegram-operator-poll.sh" --job-name telegram-operator-poll "$@"

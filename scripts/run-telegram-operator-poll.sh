#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/load-tierededge-env.sh"
source "$ROOT_DIR/scripts/live-log-automation-guard.sh"

if ! acquire_named_lock "telegram-operator" "run-telegram-operator-poll.sh"; then
  exit 0
fi

node scripts/telegram-operator-bot.mjs "$@"

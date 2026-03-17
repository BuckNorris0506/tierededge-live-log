#!/bin/zsh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/load-tierededge-env.sh"
exec /usr/local/bin/node /usr/local/lib/node_modules/openclaw/dist/index.js gateway --port 18789

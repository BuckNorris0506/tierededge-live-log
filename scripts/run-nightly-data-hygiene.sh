#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Nightly integrity pass:
# 1) validate and backfill recommendation log when reconstructable
# 2) rebuild payload so integrity artifacts and freshness are reflected publicly
node scripts/recommendation-log-nightly.mjs
node scripts/build-live-log.mjs

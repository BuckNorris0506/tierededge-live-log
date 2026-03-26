#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CRON_BEGIN="# BEGIN TIEREDGE DIRECT AUTOMATION"
CRON_END="# END TIEREDGE DIRECT AUTOMATION"

TIEREDGE_BLOCK=$(cat <<'EOF'
# BEGIN TIEREDGE DIRECT AUTOMATION
SHELL=/bin/sh
0 6 * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /bin/zsh /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-scheduled-canonical-hunt.sh --job-name morning-edge-hunt >> /tmp/tierededge-morning-edge-hunt.log 2>&1
0 12 * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /bin/zsh /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-scheduled-canonical-hunt.sh --job-name midday-edge-hunt >> /tmp/tierededge-midday-edge-hunt.log 2>&1
0 15 * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /bin/zsh /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-scheduled-canonical-hunt.sh --job-name afternoon-edge-hunt >> /tmp/tierededge-afternoon-edge-hunt.log 2>&1
35 23 * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /bin/zsh /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-rejected-close-capture.sh --job-name rejected-close-capture-evening >> /tmp/tierededge-rejected-close-capture-evening.log 2>&1
35 2 * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /bin/zsh /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-rejected-close-capture.sh --job-name rejected-close-capture-late-night >> /tmp/tierededge-rejected-close-capture-late-night.log 2>&1
15 9 * * * export PATH=/usr/local/bin:/usr/bin:/bin; export LIVE_LOG_DEPLOY_REPO="/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log"; cd /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log && /bin/zsh /Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-rejected-close-capture.sh --job-name rejected-close-capture-morning-cleanup >> /tmp/tierededge-rejected-close-capture-morning-cleanup.log 2>&1
# END TIEREDGE DIRECT AUTOMATION
EOF
)

CURRENT_CRONTAB="$(mktemp)"
NEXT_CRONTAB="$(mktemp)"

cleanup() {
  rm -f "$CURRENT_CRONTAB" "$NEXT_CRONTAB"
}
trap cleanup EXIT

if crontab -l > "$CURRENT_CRONTAB" 2>/dev/null; then
  :
else
  : > "$CURRENT_CRONTAB"
fi

awk -v begin="$CRON_BEGIN" -v end="$CRON_END" '
  $0 == begin { in_block = 1; next }
  $0 == end { in_block = 0; next }
  !in_block { print }
' "$CURRENT_CRONTAB" > "$NEXT_CRONTAB"

{
  cat "$NEXT_CRONTAB"
  [[ -s "$NEXT_CRONTAB" ]] && printf '\n'
  printf '%s\n' "$TIEREDGE_BLOCK"
} > "${NEXT_CRONTAB}.merged"

crontab "${NEXT_CRONTAB}.merged"
mv "${NEXT_CRONTAB}.merged" "$NEXT_CRONTAB"

echo "Installed TieredEdge direct automation cron block."
cat "$NEXT_CRONTAB"

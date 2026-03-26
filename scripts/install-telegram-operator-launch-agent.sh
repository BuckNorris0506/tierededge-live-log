#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE_PLIST="$ROOT_DIR/config/launchd/com.tierededge.telegram-operator-poll.plist"
TARGET_PLIST="$HOME/Library/LaunchAgents/com.tierededge.telegram-operator-poll.plist"
LABEL="com.tierededge.telegram-operator-poll"

mkdir -p "$HOME/Library/LaunchAgents"
cp "$SOURCE_PLIST" "$TARGET_PLIST"

launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$TARGET_PLIST"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "Installed LaunchAgent: $LABEL"
echo "Plist: $TARGET_PLIST"

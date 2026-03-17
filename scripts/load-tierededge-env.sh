#!/bin/zsh

# Load only the key assignment we need for cron-driven TieredEdge jobs
# without sourcing the full interactive shell profile.

load_odds_key_from_file() {
  local file_path="$1"
  local key_line=""
  [[ -f "$file_path" ]] || return 0
  key_line="$(grep -E '^(export[[:space:]]+)?ODDS_API_KEY=' "$file_path" | tail -n 1 || true)"
  [[ -n "$key_line" ]] || return 0
  eval "$key_line"
}

load_odds_key_from_file "$HOME/.tierededge-env.zsh"
load_odds_key_from_file "$HOME/.zshrc"

export ODDS_API_KEY="${ODDS_API_KEY:-}"

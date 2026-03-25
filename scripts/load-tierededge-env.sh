#!/bin/zsh

# Load only the env assignments we need for cron-driven TieredEdge jobs
# without sourcing the full interactive shell profile.

load_env_var_from_file() {
  local file_path="$1"
  local var_name="$2"
  local key_line=""
  [[ -f "$file_path" ]] || return 0
  key_line="$(grep -E "^(export[[:space:]]+)?${var_name}=" "$file_path" | tail -n 1 || true)"
  [[ -n "$key_line" ]] || return 0
  eval "$key_line"
}

load_required_envs() {
  local file_path="$1"
  load_env_var_from_file "$file_path" "ODDS_API_KEY"
  load_env_var_from_file "$file_path" "TIEREDGE_TELEGRAM_BOT_TOKEN"
  load_env_var_from_file "$file_path" "TIEREDGE_TELEGRAM_CHAT_ID"
  load_env_var_from_file "$file_path" "TELEGRAM_BOT_TOKEN"
  load_env_var_from_file "$file_path" "TELEGRAM_CHAT_ID"
}

load_required_envs "$HOME/.tierededge-env.zsh"
load_required_envs "$HOME/.zshrc"

export ODDS_API_KEY="${ODDS_API_KEY:-}"
export TIEREDGE_TELEGRAM_BOT_TOKEN="${TIEREDGE_TELEGRAM_BOT_TOKEN:-${TELEGRAM_BOT_TOKEN:-}}"
export TIEREDGE_TELEGRAM_CHAT_ID="${TIEREDGE_TELEGRAM_CHAT_ID:-${TELEGRAM_CHAT_ID:-}}"

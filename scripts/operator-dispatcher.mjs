#!/usr/bin/env node
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { CORE_PATHS, parseNumber, readJson } from './core-ledger-utils.mjs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';

const MORNING_HUNT_FALLBACK_ID = '2766547c-e6a0-40ca-a680-972c7842579c';
const OPENCLAW_JOBS_PATH = '/Users/jaredbuckman/.openclaw/cron/jobs.json';
const LIVE_LOG_REBUILD_SCRIPT = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/update-live-log.sh';
const CANONICAL_HUNT_RUNNER = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-canonical-hunt.mjs';

export const SUPPORTED_OPERATOR_COMMANDS = [
  'HELP',
  'STATUS',
  'RUN HUNT',
  'LATEST BOARD',
  'BANKROLL',
  'HEALTH',
  'FLAGS',
];

export const LEGACY_OPERATOR_ALIASES = new Map([
  ['SHOW BOARD', 'LATEST BOARD'],
  ['SHOW BLOCK REASON', 'HEALTH'],
  ['SHOW EXECUTIONS', 'STATUS'],
  ['SHOW PASSES', 'LATEST BOARD'],
  ['SHOW RECENT BETS', 'BANKROLL'],
]);

export function normalizeOperatorCommand(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toUpperCase();
}

export function resolveOperatorCommand(value) {
  const normalized = normalizeOperatorCommand(value);
  if (SUPPORTED_OPERATOR_COMMANDS.includes(normalized)) return normalized;
  if (LEGACY_OPERATOR_ALIASES.has(normalized)) {
    return LEGACY_OPERATOR_ALIASES.get(normalized);
  }
  return null;
}

export function loadOperatorState() {
  const payload = readJson(CORE_PATHS.publicData, null);
  if (!payload) throw new Error(`missing_public_state:${CORE_PATHS.publicData}`);
  return payload;
}

function getMorningHuntId() {
  try {
    const parsed = JSON.parse(fs.readFileSync(OPENCLAW_JOBS_PATH, 'utf8'));
    const job = (parsed.jobs || []).find((entry) => entry.name === 'morning-edge-hunt');
    return job?.id || MORNING_HUNT_FALLBACK_ID;
  } catch {
    return MORNING_HUNT_FALLBACK_ID;
  }
}

function compactFlags(flags) {
  const grouped = { RED: [], YELLOW: [], INFO: [] };
  for (const flag of flags || []) {
    const level = String(flag.level || 'INFO').toUpperCase();
    if (!grouped[level]) grouped[level] = [];
    grouped[level].push(flag);
  }
  return grouped;
}

function helpText() {
  return [
    'TIERED EDGE COMMANDS',
    '',
    'HELP',
    'STATUS',
    'RUN HUNT',
    'LATEST BOARD',
    'BANKROLL',
    'HEALTH',
    'FLAGS',
    '',
    'Use exact commands.',
  ].join('\n');
}

function statusText(state) {
  const decision = state.decision_payload_v1 || {};
  const run = state.latest_canonical_hunt_run || {};
  const flags = compactFlags(state.operator_dashboard?.action_flags || []);
  return [
    'TIERED EDGE STATUS',
    '',
    `Verdict: ${decision.verdict || 'UNKNOWN'}`,
    `Latest Run: ${run.run_id || 'unknown'}`,
    `Trust: ${run.invalidated ? 'FAIL' : (run.status === 'ok' ? 'PASS' : 'WARN')}`,
    `System Health: ${decision.system_health || 'UNKNOWN'}`,
    `Snapshots: ${state.operator_dashboard?.top_level_sections?.[0]?.cards?.[2]?.metrics?.[0]?.value ?? 'N/A'} valid / ${state.operator_dashboard?.top_level_sections?.[0]?.cards?.[2]?.metrics?.[1]?.value ?? 'N/A'} invalid`,
    `Flags: ${flags.RED.length} red / ${flags.YELLOW.length} yellow / ${flags.INFO.length} info`,
  ].join('\n');
}

function formatRecommendation(row) {
  return [
    `Game: ${row.event_label || 'Unknown game'}`,
    `Play: ${row.selection || 'Unknown selection'} @ ${row.sportsbook || 'Unknown book'}`,
    `Edge: +${Number(parseNumber(row.post_conf_edge_pct) || 0).toFixed(2)}%`,
    `Start Time: ${row.event_start_time || 'Unknown'}`,
    `Start: ${Number.isFinite(parseNumber(row.minutes_to_start)) ? Math.max(0, Math.round(parseNumber(row.minutes_to_start))) : 'Unknown'} minutes (${String(row.urgency_tag || 'LATER').toUpperCase()})`,
  ].join('\n');
}

function latestBoardText(state) {
  const run = state.latest_canonical_hunt_run || {};
  const selectedRows = Array.isArray(run.selected_rows) ? run.selected_rows : [];
  const misses = Array.isArray(run.executable_closest_misses) ? run.executable_closest_misses.slice(0, 3) : [];
  const lines = ['TIERED EDGE BOARD', ''];

  if (selectedRows.length) {
    lines.push('ACTIONABLE PLAYS');
    for (const row of selectedRows) {
      lines.push('');
      lines.push(formatRecommendation(row));
    }
  } else {
    lines.push('Verdict: SIT');
    lines.push(run.plain_reason || state.decision_payload_v1?.why || 'No qualifying executable edges.');
    if (misses.length) {
      lines.push('');
      lines.push('Closest Misses');
      for (const row of misses) {
        lines.push(`- ${row.selection} @ ${row.sportsbook} | +${Number(parseNumber(row.post_conf_edge_pct) || 0).toFixed(2)}% | ${String(row.urgency_tag || 'LATER').toUpperCase()}`);
      }
    }
  }

  lines.push('');
  lines.push(`Run: ${run.run_id || 'unknown'}`);
  return lines.join('\n');
}

function bankrollText(state) {
  const current = state.current_status || {};
  const openRisk = state.open_risk_summary || {};
  return [
    'BANKROLL',
    '',
    `Current: ${current.Bankroll || 'N/A'}`,
    `Open Exposure: ${openRisk.total_stake_at_risk || '$0.00'} (${openRisk.open_exposure_pct_of_bankroll || '0%'})`,
    `Pending Bets: ${openRisk.pending_ticket_count ?? 0}`,
    `Phase: ${current.Phase || 'UNKNOWN'}`,
  ].join('\n');
}

function healthText(state) {
  const decision = state.decision_payload_v1 || {};
  const run = state.latest_canonical_hunt_run || {};
  const hidden = state.operator_dashboard?.hidden_troubleshooting || {};
  const lines = [
    'SYSTEM HEALTH',
    '',
    `Trust: ${run.invalidated ? 'FAIL' : (run.status === 'ok' ? 'PASS' : 'WARN')}`,
    `Verdict: ${decision.verdict || 'UNKNOWN'}`,
    `Reason: ${decision.why || 'No explanation available.'}`,
    `Snapshots: ${hidden.invalid_snapshot_count ?? 0} invalid / ${state.operator_dashboard?.top_level_sections?.[0]?.cards?.[2]?.metrics?.[0]?.value ?? 'N/A'} valid`,
    `Stale Markets: ${hidden.stale_market_count ?? 0}`,
  ];
  return lines.join('\n');
}

function flagsText(state) {
  const flags = compactFlags(state.operator_dashboard?.action_flags || []);
  const lines = ['ACTION FLAGS', ''];
  for (const level of ['RED', 'YELLOW', 'INFO']) {
    if (!flags[level].length) continue;
    lines.push(level);
    for (const flag of flags[level]) {
      lines.push(`- ${flag.title}: ${flag.message}`);
    }
    lines.push('');
  }
  if (lines.length === 2) {
    lines.push('No current operator flags.');
  }
  return lines.join('\n').trim();
}

function blockedHuntText(blockStatus) {
  return [
    'SYSTEM BLOCKED',
    `Reason: ${blockStatus.reason_class || 'unknown'}`,
    'Edge hunt NOT executed.',
    blockStatus.reason || 'No explanation available.',
    blockStatus.post_mortem_required ? 'Complete the required post-mortem to resume.' : 'Resolve the active integrity block to resume.',
  ].join('\n');
}

function runHuntText(stateBefore) {
  const blockStatus = readHuntBlockStatus();
  if (blockStatus.blocked) {
    return {
      response_type: 'blocked',
      run_id: stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: blockedHuntText(blockStatus),
    };
  }

  const runnerResult = spawnSync('node', [CANONICAL_HUNT_RUNNER], { encoding: 'utf8' });
  if (runnerResult.status !== 0) {
    return {
      response_type: 'run_hunt_failed',
      run_id: stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: [
        'RUN HUNT',
        'Status: FAILED',
        'Stage: canonical_runner',
        `Reason: ${(runnerResult.stderr || runnerResult.stdout || 'Canonical hunt runner failed.').trim()}`,
        `Last known verdict: ${stateBefore.decision_payload_v1?.verdict || 'UNKNOWN'}`,
      ].join('\n'),
    };
  }

  const rebuildResult = spawnSync(LIVE_LOG_REBUILD_SCRIPT, [], { encoding: 'utf8' });
  if (rebuildResult.status !== 0) {
    return {
      response_type: 'rebuild_failed',
      run_id: stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: [
        'RUN HUNT',
        'Status: FAILED',
        'Stage: rebuild',
        `Reason: ${(rebuildResult.stderr || rebuildResult.stdout || 'Live-log rebuild failed.').trim()}`,
      ].join('\n'),
    };
  }

  const freshState = loadOperatorState();
  const run = freshState.latest_canonical_hunt_run || {};
  return {
    response_type: 'run_hunt_complete',
    run_id: run.run_id || null,
    text: [
      'RUN HUNT',
      'Status: COMPLETE',
      '',
      latestBoardText(freshState),
    ].join('\n'),
  };
}

function renderKnownCommand(command, state) {
  switch (command) {
    case 'HELP':
      return { response_type: 'help', run_id: state?.latest_canonical_hunt_run?.run_id || null, text: helpText() };
    case 'STATUS':
      return { response_type: 'status', run_id: state?.latest_canonical_hunt_run?.run_id || null, text: statusText(state) };
    case 'LATEST BOARD':
      return { response_type: 'latest_board', run_id: state?.latest_canonical_hunt_run?.run_id || null, text: latestBoardText(state) };
    case 'BANKROLL':
      return { response_type: 'bankroll', run_id: state?.latest_canonical_hunt_run?.run_id || null, text: bankrollText(state) };
    case 'HEALTH':
      return { response_type: 'health', run_id: state?.latest_canonical_hunt_run?.run_id || null, text: healthText(state) };
    case 'FLAGS':
      return { response_type: 'flags', run_id: state?.latest_canonical_hunt_run?.run_id || null, text: flagsText(state) };
    default:
      return null;
  }
}

export function commandKeyboard() {
  return [
    ['STATUS', 'LATEST BOARD'],
    ['RUN HUNT', 'BANKROLL'],
    ['HEALTH', 'FLAGS'],
  ];
}

export function dispatchOperatorCommand(input, options = {}) {
  const normalized = normalizeOperatorCommand(input);
  const resolved = resolveOperatorCommand(normalized);
  const state = loadOperatorState();

  if (!resolved) {
    return {
      ok: false,
      command: normalized,
      resolved_command: null,
      response_type: 'unsupported_command',
      run_id: state?.latest_canonical_hunt_run?.run_id || null,
      text: helpText(),
      keyboard: commandKeyboard(),
      legacy_alias_used: false,
    };
  }

  if (resolved === 'RUN HUNT') {
    const result = runHuntText(state);
    return {
      ok: true,
      command: normalized,
      resolved_command: resolved,
      keyboard: commandKeyboard(),
      legacy_alias_used: normalized !== resolved,
      ...result,
    };
  }

  const rendered = renderKnownCommand(resolved, state);
  return {
    ok: true,
    command: normalized,
    resolved_command: resolved,
    keyboard: commandKeyboard(),
    legacy_alias_used: normalized !== resolved,
    ...rendered,
  };
}

export function latestOperatorAlertMetadata(state = null) {
  const payload = state || loadOperatorState();
  return {
    last_outbound_alert_time: payload.notification_summary?.last_notification_time_utc || null,
    last_outbound_alert_type: payload.notification_summary?.notification_type || null,
  };
}

function main() {
  const input = process.argv.slice(2).join(' ');
  if (!input) {
    console.log(helpText());
    process.exit(0);
  }
  const result = dispatchOperatorCommand(input);
  console.log(result.text);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

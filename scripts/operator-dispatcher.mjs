#!/usr/bin/env node
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { CORE_PATHS, parseNumber, readJson } from './core-ledger-utils.mjs';
import { ingestStructuredExecutionPlacement } from './execution-layer-utils.mjs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';

const MORNING_HUNT_FALLBACK_ID = '2766547c-e6a0-40ca-a680-972c7842579c';
const OPENCLAW_JOBS_PATH = '/Users/jaredbuckman/.openclaw/cron/jobs.json';
const LIVE_LOG_REBUILD_SCRIPT = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/update-live-log.sh';
const CANONICAL_HUNT_RUNNER = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-canonical-hunt.mjs';
const TELEGRAM_HUNT_WRAPPER = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/run-scheduled-canonical-hunt.sh';

export const SUPPORTED_OPERATOR_COMMANDS = [
  'HELP',
  'STATUS',
  'RUN HUNT',
  'LATEST BOARD',
  'BANKROLL',
  'HEALTH',
  'FLAGS',
];

const SUPPORTED_BOOK_ALIASES = new Map([
  ['DRAFTKINGS', 'DraftKings'],
  ['FANDUEL', 'FanDuel'],
  ['BETMGM', 'BetMGM'],
  ['CIRCA', 'Circa'],
  ['BET365', 'bet365'],
  ['CAESARS', 'Caesars'],
  ['BETRIVERS', 'BetRivers'],
]);

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

function renderGameLabel(row) {
  if (String(row?.event_label || '').trim()) {
    return String(row.event_label).trim();
  }
  const away = String(row?.event_away_team || '').trim();
  const home = String(row?.event_home_team || '').trim();
  if (away && home) {
    return `${away} @ ${home}`;
  }
  if (home && away) {
    return `${home} @ ${away}`;
  }
  return 'Unknown game';
}

function renderCanonicalPrice(value) {
  const price = String(value || '').trim();
  if (!price) return null;
  if (price.startsWith('+') || price.startsWith('-')) return price;
  if (/^\d+$/.test(price)) return `+${price}`;
  return price;
}

function renderLineLabel(row) {
  const price = renderCanonicalPrice(row?.odds_american);
  const lineKey = String(row?.line_key || '').trim();
  if (price && lineKey) return `${price} (${lineKey})`;
  if (price) return price;
  if (lineKey) return lineKey;
  return 'Unknown';
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

function canonicalSportsbook(value) {
  const normalized = normalizeOperatorCommand(value).replace(/\s+/g, '');
  return SUPPORTED_BOOK_ALIASES.get(normalized) || null;
}

function parseOddsToken(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^[+-]\d+$/.test(raw)) return raw;
  if (/^\d+$/.test(raw)) return `+${raw}`;
  return null;
}

function parseStakeToken(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^\$?\s*([0-9]+(?:\.[0-9]{1,2})?)$/);
  if (!match) return null;
  return Number(match[1]).toFixed(2);
}

export function parseBetPlacedMessage(input) {
  const lines = String(input || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length || normalizeOperatorCommand(lines[0]) !== 'BET PLACED') return null;

  if (lines.length === 4) {
    const selectionBookMatch = lines[1].match(/^(.*?)\s+@\s+(.+)$/);
    if (!selectionBookMatch) {
      return { ok: false, reason: 'unsupported format', details: 'Expected "[Selection] @ [Sportsbook]" on line 2.' };
    }
    const sportsbook = canonicalSportsbook(selectionBookMatch[2]);
    if (!sportsbook) {
      return { ok: false, reason: 'missing sportsbook', details: 'Sportsbook is missing or unsupported.' };
    }
    const odds = parseOddsToken(lines[2]);
    if (!odds) {
      return { ok: false, reason: 'missing odds', details: 'Odds line must be a valid American price like +170 or -120.' };
    }
    const stake = parseStakeToken(lines[3]);
    if (!stake) {
      return { ok: false, reason: 'missing stake', details: 'Stake line must be a dollar amount like $2.00.' };
    }
    return {
      ok: true,
      payload: {
        selection: selectionBookMatch[1].trim(),
        actual_sportsbook: sportsbook,
        actual_odds: odds,
        actual_stake: stake,
      },
    };
  }

  if (lines.length === 5) {
    const sportsbook = canonicalSportsbook(lines[2]);
    if (!sportsbook) {
      return { ok: false, reason: 'missing sportsbook', details: 'Sportsbook is missing or unsupported.' };
    }
    const odds = parseOddsToken(lines[3]);
    if (!odds) {
      return { ok: false, reason: 'missing odds', details: 'Odds line must be a valid American price like +170 or -120.' };
    }
    const stake = parseStakeToken(lines[4]);
    if (!stake) {
      return { ok: false, reason: 'missing stake', details: 'Stake line must be a dollar amount like $2.00.' };
    }
    return {
      ok: true,
      payload: {
        selection: lines[1],
        actual_sportsbook: sportsbook,
        actual_odds: odds,
        actual_stake: stake,
      },
    };
  }

  return {
    ok: false,
    reason: 'unsupported format',
    details: 'Use either 4 lines with "[Selection] @ [Sportsbook]" or 5 lines with sportsbook on its own line.',
  };
}

function renderExecutionLogSuccess(result) {
  const row = result.row || {};
  const lines = [
    'LOGGED ✅',
    '',
    `Selection: ${row.selection || 'Unknown'}`,
    `Sportsbook: ${row.actual_sportsbook || 'Unknown'}`,
    `Odds: ${renderCanonicalPrice(row.actual_odds) || 'Unknown'}`,
    `Stake: ${Number.isFinite(parseNumber(row.actual_stake)) ? `$${parseNumber(row.actual_stake).toFixed(2)}` : 'Unknown'}`,
    `Execution Status: ${row.execution_approval_result || 'UNKNOWN'}`,
  ];
  if (row.execution_approval_result_reason && row.execution_approval_result === 'OFF_PLAN_EXECUTION') {
    lines.push(`Reason: ${row.execution_approval_result_reason}`);
  }
  if (row.run_id) {
    lines.push(`Run ID: ${row.run_id}`);
  }
  if (row.rec_id) {
    lines.push(`Rec ID: ${row.rec_id}`);
  }
  return lines.join('\n');
}

function renderExecutionLogDuplicate(result) {
  const row = result.row || {};
  return [
    'ALREADY LOGGED ⚠️',
    '',
    `Selection: ${row.selection || 'Unknown'}`,
    `Sportsbook: ${row.actual_sportsbook || 'Unknown'}`,
    `Odds: ${renderCanonicalPrice(row.actual_odds) || 'Unknown'}`,
    `Stake: ${Number.isFinite(parseNumber(row.actual_stake)) ? `$${parseNumber(row.actual_stake).toFixed(2)}` : 'Unknown'}`,
    `Execution Status: ${row.execution_approval_result || 'UNKNOWN'}`,
    `Logged At: ${row.logged_at_utc || 'Unknown'}`,
    ...(row.run_id ? [`Run ID: ${row.run_id}`] : []),
    ...(row.rec_id ? [`Rec ID: ${row.rec_id}`] : []),
  ].join('\n');
}

function renderExecutionLogFailure(parsed, ingest = null) {
  const reason = ingest?.reason || parsed?.reason || 'other parse issue';
  const details = parsed?.details
    || (reason === 'no_matching_recommendation_found'
      ? 'No matching recommendation found for that selection/book/odds/stake.'
      : reason === 'ambiguous_recommendation_match'
        ? 'More than one recommendation matched too closely to log safely.'
        : 'Could not parse the BET PLACED message.');
  return [
    'NOT LOGGED ❌',
    '',
    `Reason: ${reason}`,
    details,
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
    `Game: ${renderGameLabel(row)}`,
    `Play: ${row.selection || 'Unknown selection'} @ ${row.sportsbook || 'Unknown book'}`,
    `Line: ${renderLineLabel(row)}`,
    `Tier: ${row.tier || 'Unknown'}`,
    `Kelly Stake: ${Number.isFinite(parseNumber(row.kelly_stake)) ? `$${parseNumber(row.kelly_stake).toFixed(2)}` : 'Unknown'}`,
    `Edge: +${Number(parseNumber(row.post_conf_edge_pct) || 0).toFixed(2)}%`,
    `Start Time: ${row.event_start_time || 'Unknown'}`,
    `Start: ${Number.isFinite(parseNumber(row.minutes_to_start)) ? Math.max(0, Math.round(parseNumber(row.minutes_to_start))) : 'Unknown'} minutes (${String(row.urgency_tag || 'LATER').toUpperCase()})`,
  ].join('\n');
}

function latestBoardText(state) {
  const run = state.latest_canonical_hunt_run || {};
  const selectedRows = Array.isArray(run.selected_rows) ? run.selected_rows : [];
  const misses = Array.isArray(run.executable_closest_misses) ? run.executable_closest_misses.slice(0, 3) : [];
  const continuity = state.actionable_board_continuity_summary || {};
  const currentContinuityMap = new Map(
    (continuity.current_actionable_rows || []).map((row) => [String(row.rec_id || '').trim(), row]),
  );
  const priorContinuityRows = Array.isArray(continuity.prior_actionable_rows)
    ? continuity.prior_actionable_rows.slice(0, 3)
    : [];
  const actionableBoardCard = state.operator_dashboard?.top_level_sections?.[1]?.cards?.[0] || {};
  const totalExposureMetric = Array.isArray(actionableBoardCard.metrics)
    ? actionableBoardCard.metrics.find((metric) => metric.label === 'total_recommended_exposure')
    : null;
  const lines = ['TIERED EDGE BOARD', ''];

  if (selectedRows.length) {
    lines.push('ACTIONABLE PLAYS');
    if (totalExposureMetric?.value) {
      lines.push(`Total Recommended Exposure: ${totalExposureMetric.value}`);
    }
    for (const row of selectedRows) {
      const continuityRow = currentContinuityMap.get(String(row.rec_id || '').trim());
      lines.push('');
      lines.push(formatRecommendation(row));
      if (continuityRow?.continuity_status) {
        lines.push(`Continuity: ${String(continuityRow.continuity_status).toUpperCase()}`);
      }
    }
  } else {
    lines.push('Verdict: SIT');
    lines.push(run.plain_reason || state.decision_payload_v1?.why || 'No qualifying executable edges.');
    if (misses.length) {
      lines.push('');
      lines.push('Closest Misses');
      for (const row of misses) {
        lines.push(`- ${row.selection} @ ${row.sportsbook} | ${renderLineLabel(row)} | +${Number(parseNumber(row.post_conf_edge_pct) || 0).toFixed(2)}% | ${String(row.urgency_tag || 'LATER').toUpperCase()}`);
      }
    }
    if (priorContinuityRows.length) {
      lines.push('');
      lines.push('PREVIOUSLY ACTIONABLE');
      for (const row of priorContinuityRows) {
        const priorEdge = Number(parseNumber(row.prior_edge_pct) || 0).toFixed(2);
        const currentEdge = Number.isFinite(parseNumber(row.current_edge_pct))
          ? `now +${Number(parseNumber(row.current_edge_pct)).toFixed(2)}%`
          : 'now N/A';
        const priorPrice = renderCanonicalPrice(row.odds_american) || 'Unknown';
        const currentPrice = renderCanonicalPrice(row.current_odds_american);
        const lineDetails = currentPrice && currentPrice !== priorPrice
          ? `${priorPrice} -> ${currentPrice}${row.line_key ? ` (${row.line_key})` : ''}`
          : `${priorPrice}${row.line_key ? ` (${row.line_key})` : ''}`;
        const reason = row.continuity_reason ? ` | reason: ${row.continuity_reason}` : '';
        lines.push(`- ${row.selection} @ ${row.sportsbook} | line: ${lineDetails} | prior +${priorEdge}% | ${currentEdge} | status: ${row.continuity_status}${reason}`);
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

  const wrapperResult = spawnSync(TELEGRAM_HUNT_WRAPPER, ['--job-name', 'telegram-operator-run-hunt'], { encoding: 'utf8' });
  let wrapperPayload = null;
  if (String(wrapperResult.stdout || '').trim()) {
    try {
      wrapperPayload = JSON.parse(wrapperResult.stdout);
    } catch {
      wrapperPayload = null;
    }
  }

  if (wrapperResult.status !== 0) {
    return {
      response_type: 'run_hunt_failed',
      run_id: wrapperPayload?.run_id || stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: [
        'RUN HUNT',
        'Status: FAILED',
        'Stage: canonical_runner_or_rebuild',
        `Reason: ${(wrapperResult.stderr || wrapperResult.stdout || 'Canonical hunt wrapper failed.').trim()}`,
        `Last known verdict: ${stateBefore.decision_payload_v1?.verdict || 'UNKNOWN'}`,
      ].join('\n'),
    };
  }

  if (wrapperPayload?.status === 'skipped_due_to_active_lock') {
    return {
      response_type: 'blocked',
      run_id: wrapperPayload?.run_id || stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: [
        'RUN HUNT',
        'Status: SKIPPED',
        'Stage: lock_guard',
        `Reason: ${wrapperPayload?.lock_name || 'canonical-hunt'} already active.`,
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
  const betPlaced = parseBetPlacedMessage(input);
  const normalized = normalizeOperatorCommand(input);
  const state = loadOperatorState();

  if (betPlaced) {
    if (!betPlaced.ok) {
      return {
        ok: false,
        command: 'BET PLACED',
        resolved_command: 'BET PLACED',
        response_type: 'execution_log_rejected',
        run_id: state?.latest_canonical_hunt_run?.run_id || null,
        text: renderExecutionLogFailure(betPlaced),
        keyboard: commandKeyboard(),
        legacy_alias_used: false,
      };
    }
    const ingest = ingestStructuredExecutionPlacement({
      ...betPlaced.payload,
      bet_slip_timestamp: new Date().toISOString(),
      logged_at_utc: new Date().toISOString(),
      source: 'telegram_operator',
    });
    if (!ingest.ok) {
      if (ingest.duplicate) {
        return {
          ok: false,
          command: 'BET PLACED',
          resolved_command: 'BET PLACED',
          response_type: 'execution_log_duplicate',
          run_id: ingest.row?.run_id || state?.latest_canonical_hunt_run?.run_id || null,
          text: renderExecutionLogDuplicate(ingest),
          keyboard: commandKeyboard(),
          legacy_alias_used: false,
        };
      }
      return {
        ok: false,
        command: 'BET PLACED',
        resolved_command: 'BET PLACED',
        response_type: 'execution_log_rejected',
        run_id: state?.latest_canonical_hunt_run?.run_id || null,
        text: renderExecutionLogFailure(betPlaced, ingest),
        keyboard: commandKeyboard(),
        legacy_alias_used: false,
      };
    }
    return {
      ok: true,
      command: 'BET PLACED',
      resolved_command: 'BET PLACED',
      response_type: 'execution_log_logged',
      run_id: ingest.row?.run_id || null,
      text: renderExecutionLogSuccess(ingest),
      keyboard: commandKeyboard(),
      legacy_alias_used: false,
    };
  }

  const resolved = resolveOperatorCommand(normalized);

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

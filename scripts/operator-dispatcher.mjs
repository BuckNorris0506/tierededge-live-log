#!/usr/bin/env node
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { appendJsonl, CORE_PATHS, parseNumber, readJson, readJsonl } from './core-ledger-utils.mjs';
import { ingestStructuredExecutionPlacement, ingestStructuredExecutionSettlement } from './execution-layer-utils.mjs';
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

function parseSettlementResultToken(value) {
  const normalized = normalizeOperatorCommand(value).replace(/\s+/g, '');
  if (normalized === 'WIN') return 'WIN';
  if (normalized === 'LOSS') return 'LOSS';
  if (normalized === 'PUSH') return 'PUSH';
  return null;
}

function parseLabeledValue(line, label) {
  const match = String(line || '').match(/^\s*([^:]+)\s*:\s*(.+?)\s*$/);
  if (!match) return null;
  if (normalizeOperatorCommand(match[1]) !== normalizeOperatorCommand(label)) return null;
  return match[2].trim();
}

function parseBoostPercentToken(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^([0-9]+(?:\.[0-9]+)?)\s*%$/);
  if (!match) return null;
  const numeric = Number(match[1]);
  return Number.isFinite(numeric) ? roundBoostPercent(numeric) : null;
}

function roundBoostPercent(value) {
  return Math.round(value * 100) / 100;
}

function parseFlexibleDateTime(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const direct = Date.parse(raw);
  if (Number.isFinite(direct)) return new Date(direct).toISOString();

  const ctMatch = raw.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*CT$/i);
  if (!ctMatch) return null;
  let hour = Number(ctMatch[2]);
  const minute = Number(ctMatch[3]);
  const meridiem = ctMatch[4].toUpperCase();
  if (meridiem === 'PM' && hour !== 12) hour += 12;
  if (meridiem === 'AM' && hour === 12) hour = 0;
  const [year, month, day] = ctMatch[1].split('-').map(Number);
  const utcMs = Date.UTC(year, month - 1, day, hour + 5, minute, 0);
  return new Date(utcMs).toISOString();
}

function boostStatusForEntry(entry) {
  const expiresAtUtc = String(entry?.expires_at_utc || '').trim();
  if (!expiresAtUtc) return 'ACTIVE';
  const expiresMs = Date.parse(expiresAtUtc);
  if (!Number.isFinite(expiresMs)) return 'ACTIVE';
  return expiresMs > Date.now() ? 'ACTIVE' : 'INACTIVE';
}

function formatBoostPercent(value) {
  const numeric = parseNumber(value);
  if (!Number.isFinite(numeric)) return 'Unknown';
  return `${Number.isInteger(numeric) ? numeric : numeric.toFixed(2)}%`;
}

function formatBoostExpiration(entry) {
  if (String(entry?.expires_raw || '').trim()) return String(entry.expires_raw).trim();
  return entry?.expires_at_utc || 'Unknown';
}

function formatBoostMoney(value) {
  const numeric = parseNumber(value);
  return Number.isFinite(numeric) ? `$${numeric.toFixed(2)}` : null;
}

function formatBoostOddsThreshold(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return null;
  if (/^[+-]\d+$/.test(raw)) return raw;
  const numeric = parseNumber(raw);
  if (!Number.isFinite(numeric)) return null;
  const rounded = Math.round(numeric);
  return rounded > 0 ? `+${rounded}` : String(rounded);
}

function readProfitBoostEntries() {
  return readJsonl(CORE_PATHS.profitBoostLog);
}

function appendProfitBoostEntry(entry) {
  appendJsonl(
    CORE_PATHS.profitBoostLog,
    entry,
    (existing) => String(existing.boost_id || '').trim(),
  );
  return entry;
}

export function parseProfitBoostMessage(input) {
  const lines = String(input || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length || normalizeOperatorCommand(lines[0]) !== 'PROFIT BOOST') return null;

  const requiredExample = [
    'PROFIT BOOST',
    'Sportsbook: DraftKings',
    'Boost: 50%',
    'Scope: MLB',
    'Expires: 2026-03-27 11:59 PM CT',
  ];
  const detailedExample = [
    'PROFIT BOOST',
    'Sportsbook: DraftKings',
    'Boost: 50%',
    'Scope: MLB',
    'Max Wager: $25',
    'Bet Types: Straight, SGP',
    'Min Total Odds: +200',
    'Expires: 2026-03-27 11:59 PM CT',
  ];

  if (lines.length < 5) {
    return {
      ok: false,
      reason: 'unsupported format',
      details: `Use at least these 5 non-blank lines:\n${requiredExample.join('\n')}`,
    };
  }

  const fieldLines = lines.slice(1);
  const labeledFields = new Map();
  const duplicateLabels = [];
  const unrecognizedLines = [];
  const allowedLabels = new Set(['SPORTSBOOK', 'BOOST', 'SCOPE', 'MAX WAGER', 'BET TYPES', 'MIN TOTAL ODDS', 'EXPIRES']);
  for (const line of fieldLines) {
    const match = String(line || '').match(/^\s*([^:]+)\s*:\s*(.+?)\s*$/);
    if (!match) {
      unrecognizedLines.push(line);
      continue;
    }
    const label = normalizeOperatorCommand(match[1]);
    const value = match[2].trim();
    if (!allowedLabels.has(label)) {
      unrecognizedLines.push(line);
      continue;
    }
    if (labeledFields.has(label)) {
      duplicateLabels.push(label);
      continue;
    }
    labeledFields.set(label, value);
  }

  if (unrecognizedLines.length || duplicateLabels.length) {
    const details = [];
    if (unrecognizedLines.length) details.push(`Unrecognized lines: ${unrecognizedLines.join(' | ')}`);
    if (duplicateLabels.length) details.push(`Duplicate fields: ${duplicateLabels.join(', ')}`);
    details.push(`Supported format:\n${detailedExample.join('\n')}`);
    return {
      ok: false,
      reason: 'unsupported format',
      details: details.join('\n'),
    };
  }

  const sportsbookRaw = labeledFields.get('SPORTSBOOK') || null;
  if (!sportsbookRaw) {
    return {
      ok: false,
      reason: 'missing sportsbook',
      details: `Use "Sportsbook: [Book]".\nExample:\n${detailedExample.join('\n')}`,
    };
  }
  const sportsbook = canonicalSportsbook(sportsbookRaw);
  if (!sportsbook) {
    return {
      ok: false,
      reason: 'unsupported sportsbook',
      details: `Sportsbook is missing or unsupported.\nUse:\n${detailedExample.join('\n')}`,
    };
  }

  const boostRaw = labeledFields.get('BOOST') || null;
  const boostPercent = parseBoostPercentToken(boostRaw);
  if (boostPercent === null) {
    return {
      ok: false,
      reason: 'invalid boost percent',
      details: `Use "Boost: [Percent]".\nExample:\n${detailedExample.join('\n')}`,
    };
  }

  const scopeRaw = labeledFields.get('SCOPE') || null;
  if (!scopeRaw) {
    return {
      ok: false,
      reason: 'missing scope',
      details: `Use "Scope: [Market/Sport or General]".\nExample:\n${detailedExample.join('\n')}`,
    };
  }

  const expiresRaw = labeledFields.get('EXPIRES') || null;
  if (!expiresRaw) {
    return {
      ok: false,
      reason: 'missing expiration',
      details: `Use "Expires: [Datetime or text]".\nExample:\n${detailedExample.join('\n')}`,
    };
  }

  const maxWagerRaw = labeledFields.get('MAX WAGER') || null;
  const maxWager = maxWagerRaw === null ? null : parseNumber(maxWagerRaw);
  if (maxWagerRaw !== null && !Number.isFinite(maxWager)) {
    return {
      ok: false,
      reason: 'invalid max wager',
      details: `Use "Max Wager: [$Amount]".\nExample:\n${detailedExample.join('\n')}`,
    };
  }

  const betTypesRaw = labeledFields.get('BET TYPES') || null;
  const betTypes = String(betTypesRaw || '').trim() || null;

  const minTotalOddsRaw = labeledFields.get('MIN TOTAL ODDS') || null;
  const minTotalOdds = minTotalOddsRaw === null ? null : formatBoostOddsThreshold(minTotalOddsRaw);
  if (minTotalOddsRaw !== null && !minTotalOdds) {
    return {
      ok: false,
      reason: 'invalid min total odds',
      details: `Use "Min Total Odds: [odds]".\nExample:\n${detailedExample.join('\n')}`,
    };
  }

  return {
    ok: true,
    payload: {
      sportsbook,
      boost_percent: boostPercent,
      scope: String(scopeRaw).trim(),
      max_wager: Number.isFinite(maxWager) ? maxWager : null,
      bet_types: betTypes,
      min_total_odds: minTotalOdds,
      expires_raw: String(expiresRaw).trim(),
      expires_at_utc: parseFlexibleDateTime(expiresRaw),
    },
  };
}

export function appendStructuredProfitBoost(payload, options = {}) {
  const now = options.created_at_utc || new Date().toISOString();
  const entry = appendProfitBoostEntry({
    boost_id: options.boost_id || `profit-boost::${Date.now()}`,
    created_at_utc: now,
    sportsbook: payload.sportsbook,
    boost_percent: roundBoostPercent(payload.boost_percent),
    scope: String(payload.scope || '').trim(),
    max_wager: Number.isFinite(parseNumber(payload.max_wager)) ? parseNumber(payload.max_wager) : null,
    bet_types: String(payload.bet_types || '').trim() || null,
    min_total_odds: formatBoostOddsThreshold(payload.min_total_odds),
    expires_raw: String(payload.expires_raw || '').trim(),
    expires_at_utc: payload.expires_at_utc || null,
    source: options.source || 'telegram_operator',
    status: boostStatusForEntry(payload),
  });
  return entry;
}

function normalizeSelectionForDisplay(value) {
  return String(value || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\s+([@:/()])/g, '$1')
    .replace(/([@:/()])\s+/g, '$1')
    .replace(/\s*@\s*/g, ' @ ');
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

export function parseBetSettledMessage(input) {
  const lines = String(input || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length || normalizeOperatorCommand(lines[0]) !== 'BET SETTLED') return null;

  const expectedLines = [
    'BET SETTLED',
    'Texas Rangers ML @ DraftKings',
    '+144',
    '$2.00',
    'WIN',
  ];
  const rawPayloadLines = lines.slice(1);
  if (rawPayloadLines.length < 4) {
    const missingLineMap = {
      0: 'missing selection and sportsbook line',
      1: 'missing odds line',
      2: 'missing stake line',
      3: 'missing result line',
    };
    return {
      ok: false,
      reason: missingLineMap[rawPayloadLines.length] || 'unsupported format',
      details: `Use:\n${expectedLines.join('\n')}`,
    };
  }
  if (rawPayloadLines.length > 4) {
    return {
      ok: false,
      reason: 'ambiguous settlement format',
      details: `Use exactly 5 non-blank lines:\n${expectedLines.join('\n')}`,
    };
  }

  const selectionBookLine = rawPayloadLines[0].replace(/\s+/g, ' ').trim();
  const separatorIndex = selectionBookLine.lastIndexOf('@');
  if (separatorIndex <= 0 || separatorIndex === selectionBookLine.length - 1) {
    return {
      ok: false,
      reason: 'missing sportsbook',
      details: `Use line 2 as "[Selection] @ [Sportsbook]".\nExample:\n${expectedLines.join('\n')}`,
    };
  }

  const selectionText = normalizeSelectionForDisplay(selectionBookLine.slice(0, separatorIndex));
  const sportsbookToken = selectionBookLine.slice(separatorIndex + 1).trim();
  if (!selectionText) {
    return {
      ok: false,
      reason: 'missing selection',
      details: `Use line 2 as "[Selection] @ [Sportsbook]".\nExample:\n${expectedLines.join('\n')}`,
    };
  }

  const sportsbook = canonicalSportsbook(sportsbookToken);
  if (!sportsbook) {
    return {
      ok: false,
      reason: 'missing sportsbook',
      details: `Sportsbook is missing or unsupported.\nUse:\n${expectedLines.join('\n')}`,
    };
  }

  const odds = parseOddsToken(rawPayloadLines[1]);
  if (!odds) {
    return {
      ok: false,
      reason: 'missing odds',
      details: `Odds line must be a valid American price like +170 or -120.\nUse:\n${expectedLines.join('\n')}`,
    };
  }

  const stake = parseStakeToken(rawPayloadLines[2]);
  if (!stake) {
    return {
      ok: false,
      reason: 'missing stake',
      details: `Stake line must be a dollar amount like $2.00.\nUse:\n${expectedLines.join('\n')}`,
    };
  }

  const result = parseSettlementResultToken(rawPayloadLines[3]);
  if (!result) {
    return {
      ok: false,
      reason: 'invalid settlement result',
      details: `Result must be WIN, LOSS, or PUSH.\nUse:\n${expectedLines.join('\n')}`,
    };
  }

  return {
    ok: true,
    payload: {
      selection: selectionText,
      actual_sportsbook: sportsbook,
      actual_odds: odds,
      actual_stake: stake,
      result,
    },
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

function renderSettlementSuccess(result) {
  const grading = result.row || {};
  const execution = result.execution_row || {};
  return [
    'SETTLED ✅',
    '',
    `Selection: ${execution.selection || grading.selection || 'Unknown'}`,
    `Sportsbook: ${execution.actual_sportsbook || grading.sportsbook || 'Unknown'}`,
    `Odds: ${renderCanonicalPrice(execution.actual_odds || grading.actual_odds) || 'Unknown'}`,
    `Stake: ${Number.isFinite(parseNumber(execution.actual_stake || grading.stake)) ? `$${parseNumber(execution.actual_stake || grading.stake).toFixed(2)}` : 'Unknown'}`,
    `Result: ${grading.result || 'UNKNOWN'}`,
    `Settlement Status: ${String(grading.settlement_status || 'unknown').toUpperCase()}`,
    ...(grading.run_id ? [`Run ID: ${grading.run_id}`] : []),
    ...(grading.rec_id ? [`Rec ID: ${grading.rec_id}`] : []),
  ].join('\n');
}

function renderSettlementDuplicate(result) {
  const grading = result.row || {};
  const execution = result.execution_row || {};
  return [
    'ALREADY SETTLED ⚠️',
    '',
    `Selection: ${execution.selection || grading.selection || 'Unknown'}`,
    `Sportsbook: ${execution.actual_sportsbook || grading.sportsbook || 'Unknown'}`,
    `Odds: ${renderCanonicalPrice(execution.actual_odds || grading.actual_odds) || 'Unknown'}`,
    `Stake: ${Number.isFinite(parseNumber(execution.actual_stake || grading.stake)) ? `$${parseNumber(execution.actual_stake || grading.stake).toFixed(2)}` : 'Unknown'}`,
    `Result: ${grading.result || 'UNKNOWN'}`,
    `Settled At: ${grading.timestamp_ct || grading.ingestion_timestamp || 'Unknown'}`,
    ...(grading.run_id ? [`Run ID: ${grading.run_id}`] : []),
    ...(grading.rec_id ? [`Rec ID: ${grading.rec_id}`] : []),
  ].join('\n');
}

function renderSettlementFailure(parsed, ingest = null) {
  const reason = ingest?.reason || parsed?.reason || 'other parse issue';
  const details = parsed?.details
    || (reason === 'no_matching_execution_found'
      ? 'No matching execution row was found for that selection/book/odds/stake.'
      : reason === 'ambiguous_execution_match'
        ? 'More than one execution matched too closely to settle safely.'
        : reason === 'existing_settlement_conflict'
          ? 'A different settlement result is already logged for that execution.'
          : 'Could not parse the BET SETTLED message.');
  return [
    'NOT SETTLED ❌',
    '',
    `Reason: ${reason}`,
    details,
  ].join('\n');
}

function renderProfitBoostSuccess(entry) {
  const lines = [
    'LOGGED ✅',
    '',
    `Sportsbook: ${entry.sportsbook || 'Unknown'}`,
    `Boost: ${formatBoostPercent(entry.boost_percent)}`,
    `Scope: ${entry.scope || 'Unknown'}`,
  ];
  const maxWager = formatBoostMoney(entry.max_wager);
  if (maxWager) lines.push(`Max Wager: ${maxWager}`);
  if (String(entry.bet_types || '').trim()) lines.push(`Bet Types: ${String(entry.bet_types).trim()}`);
  if (formatBoostOddsThreshold(entry.min_total_odds)) lines.push(`Min Total Odds: ${formatBoostOddsThreshold(entry.min_total_odds)}`);
  lines.push(`Expires: ${formatBoostExpiration(entry)}`);
  lines.push(`Status: ${boostStatusForEntry(entry)}`);
  return lines.join('\n');
}

function renderProfitBoostFailure(parsed) {
  return [
    'NOT LOGGED ❌',
    '',
    `Reason: ${parsed?.reason || 'other parse issue'}`,
    parsed?.details || 'Could not parse the PROFIT BOOST message.',
  ].join('\n');
}

function compactActiveBoosts(state) {
  const active = Array.isArray(state?.active_profit_boosts) ? state.active_profit_boosts.filter((row) => String(row.status || '').toUpperCase() === 'ACTIVE') : [];
  return active.slice(0, 3).map((row) => `${row.sportsbook} ${formatBoostPercent(row.boost_percent)} ${row.scope || 'General'}`);
}

function renderFridayFunSection(state, { compact = false } = {}) {
  const summary = state?.friday_fun_summary || null;
  if (!summary) return [];
  const todayIsFriday = summary.today_is_friday === true;
  if (!todayIsFriday && !summary.latest_run_time_ct) return [];

  const lines = ['FRIDAY FUN SGP'];
  if (summary.current_relevance === 'today' && summary.latest_run_time_ct) {
    lines.push(`Status: ${summary.latest_message_type || summary.latest_status || 'AVAILABLE'}`);
    lines.push(`Latest Fun Run: ${summary.latest_run_time_ct}`);
    if (summary.latest_plain_reason) lines.push(`Reason: ${summary.latest_plain_reason}`);
  } else if (todayIsFriday) {
    lines.push('Status: No current Friday fun output logged yet today.');
    if (summary.latest_run_time_ct) {
      lines.push(`Last Known Fun Run: ${summary.latest_run_time_ct}`);
    }
  } else {
    lines.push(`Last Known Fun Run: ${summary.latest_run_time_ct}`);
  }

  if (!compact && summary.latest_summary_excerpt) {
    lines.push('');
    lines.push(summary.latest_summary_excerpt);
  }
  return lines;
}

function statusText(state) {
  const decision = state.decision_payload_v1 || {};
  const run = state.latest_canonical_hunt_run || {};
  const flags = compactFlags(state.operator_dashboard?.action_flags || []);
  const lines = [
    'TIERED EDGE STATUS',
    '',
    `Verdict: ${decision.verdict || 'UNKNOWN'}`,
    `Latest Run: ${run.run_id || 'unknown'}`,
    `Trust: ${run.invalidated ? 'FAIL' : (run.status === 'ok' ? 'PASS' : 'WARN')}`,
    `System Health: ${decision.system_health || 'UNKNOWN'}`,
    `Snapshots: ${state.operator_dashboard?.top_level_sections?.[0]?.cards?.[2]?.metrics?.[0]?.value ?? 'N/A'} valid / ${state.operator_dashboard?.top_level_sections?.[0]?.cards?.[2]?.metrics?.[1]?.value ?? 'N/A'} invalid`,
    `Flags: ${flags.RED.length} red / ${flags.YELLOW.length} yellow / ${flags.INFO.length} info`,
  ];
  const boosts = compactActiveBoosts(state);
  if (boosts.length) {
    lines.push(`Boosts: ${boosts.join(' | ')}`);
  }
  const fridayFunLines = renderFridayFunSection(state, { compact: true });
  if (fridayFunLines.length) {
    lines.push('');
    lines.push(...fridayFunLines);
  }
  return lines.join('\n');
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
  const boosts = compactActiveBoosts(state);
  if (boosts.length) {
    lines.push(`Active Boosts: ${boosts.join(' | ')}`);
    lines.push('');
  }

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

  const fridayFunLines = renderFridayFunSection(state, { compact: false });
  if (fridayFunLines.length) {
    lines.push('');
    lines.push(...fridayFunLines);
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
  const fridayFunLines = renderFridayFunSection(stateBefore, { compact: true });
  const blockStatus = readHuntBlockStatus();
  if (blockStatus.blocked) {
    const lines = [blockedHuntText(blockStatus)];
    if (fridayFunLines.length) {
      lines.push('', ...fridayFunLines);
    }
    return {
      response_type: 'blocked',
      run_id: stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: lines.join('\n'),
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
    const lines = [
      'RUN HUNT',
      'Status: FAILED',
      'Stage: canonical_runner_or_rebuild',
      `Reason: ${(wrapperResult.stderr || wrapperResult.stdout || 'Canonical hunt wrapper failed.').trim()}`,
      `Last known verdict: ${stateBefore.decision_payload_v1?.verdict || 'UNKNOWN'}`,
    ];
    if (fridayFunLines.length) {
      lines.push('', ...fridayFunLines);
    }
    return {
      response_type: 'run_hunt_failed',
      run_id: wrapperPayload?.run_id || stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: lines.join('\n'),
    };
  }

  if (wrapperPayload?.status === 'skipped_due_to_active_lock') {
    const lines = [
      'RUN HUNT',
      'Status: SKIPPED',
      'Stage: lock_guard',
      `Reason: ${wrapperPayload?.lock_name || 'canonical-hunt'} already active.`,
    ];
    if (fridayFunLines.length) {
      lines.push('', ...fridayFunLines);
    }
    return {
      response_type: 'blocked',
      run_id: wrapperPayload?.run_id || stateBefore?.latest_canonical_hunt_run?.run_id || null,
      text: lines.join('\n'),
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
  const betSettled = parseBetSettledMessage(input);
  const profitBoost = parseProfitBoostMessage(input);
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

  if (betSettled) {
    if (!betSettled.ok) {
      return {
        ok: false,
        command: 'BET SETTLED',
        resolved_command: 'BET SETTLED',
        response_type: 'settlement_rejected',
        run_id: state?.latest_canonical_hunt_run?.run_id || null,
        text: renderSettlementFailure(betSettled),
        keyboard: commandKeyboard(),
        legacy_alias_used: false,
      };
    }
    const ingest = ingestStructuredExecutionSettlement({
      ...betSettled.payload,
      settlement_timestamp: new Date().toISOString(),
      logged_at_utc: new Date().toISOString(),
      source: 'telegram_operator',
    });
    if (!ingest.ok) {
      if (ingest.duplicate) {
        return {
          ok: false,
          command: 'BET SETTLED',
          resolved_command: 'BET SETTLED',
          response_type: 'settlement_duplicate',
          run_id: ingest.row?.run_id || ingest.execution_row?.run_id || state?.latest_canonical_hunt_run?.run_id || null,
          text: renderSettlementDuplicate(ingest),
          keyboard: commandKeyboard(),
          legacy_alias_used: false,
        };
      }
      return {
        ok: false,
        command: 'BET SETTLED',
        resolved_command: 'BET SETTLED',
        response_type: 'settlement_rejected',
        run_id: ingest.row?.run_id || ingest.execution_row?.run_id || state?.latest_canonical_hunt_run?.run_id || null,
        text: renderSettlementFailure(betSettled, ingest),
        keyboard: commandKeyboard(),
        legacy_alias_used: false,
      };
    }
    return {
      ok: true,
      command: 'BET SETTLED',
      resolved_command: 'BET SETTLED',
      response_type: 'settlement_logged',
      run_id: ingest.row?.run_id || ingest.execution_row?.run_id || null,
      text: renderSettlementSuccess(ingest),
      keyboard: commandKeyboard(),
      legacy_alias_used: false,
    };
  }

  if (profitBoost) {
    if (!profitBoost.ok) {
      return {
        ok: false,
        command: 'PROFIT BOOST',
        resolved_command: 'PROFIT BOOST',
        response_type: 'profit_boost_rejected',
        run_id: state?.latest_canonical_hunt_run?.run_id || null,
        text: renderProfitBoostFailure(profitBoost),
        keyboard: commandKeyboard(),
        legacy_alias_used: false,
      };
    }
    const entry = appendStructuredProfitBoost(profitBoost.payload, {
      source: 'telegram_operator',
    });
    return {
      ok: true,
      command: 'PROFIT BOOST',
      resolved_command: 'PROFIT BOOST',
      response_type: 'profit_boost_logged',
      run_id: state?.latest_canonical_hunt_run?.run_id || null,
      text: renderProfitBoostSuccess(entry),
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

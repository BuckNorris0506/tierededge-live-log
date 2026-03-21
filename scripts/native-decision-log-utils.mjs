import fs from 'node:fs';
import path from 'node:path';
import { CORE_PATHS } from './core-ledger-utils.mjs';
import { computeKellyBreakdown } from './tierededge-kelly-cli.mjs';

export const DECISION_STAGE_VALUES = [
  'no_raw_edge',
  'confidence_gate',
  'threshold_gate',
  'risk_gate',
  'integrity_gate',
  'state_sync_gate',
];

export const FINAL_DECISION_VALUES = ['BET', 'SIT'];

export const NATIVE_DECISION_HEADERS = [
  'run_id',
  'rec_id',
  'timestamp_ct',
  'target_date',
  'sport',
  'league',
  'event_id',
  'event_label',
  'market_type',
  'selection',
  'sportsbook',
  'odds_american',
  'odds_decimal',
  'devig_implied_prob',
  'consensus_prob',
  'pre_conf_true_prob',
  'confidence_score',
  'post_conf_true_prob',
  'raw_edge_pct',
  'post_conf_edge_pct',
  'tier_threshold_pct',
  'price_edge_pass',
  'bet_permission_pass',
  'final_decision',
  'rejection_stage',
  'rejection_reason',
  'bet_class',
  'bankroll_snapshot',
  'kelly_stake',
  'include_in_core_strategy_metrics',
  'include_in_actual_bankroll',
];

const TIER_THRESHOLDS = {
  T1: 6,
  T2: 4,
  T3: 2,
};

export const DEFAULT_NATIVE_LEDGER_DIR = path.resolve(process.cwd(), 'data');
export const DEFAULT_NATIVE_ALL_LEDGER = CORE_PATHS.decisionLedger;
export const DEFAULT_NATIVE_BETS_LEDGER = CORE_PATHS.decisionLedger;
export const DEFAULT_NATIVE_PASS_LEDGER = CORE_PATHS.decisionLedger;
export const DEFAULT_NATIVE_SUPPRESSED_LEDGER = CORE_PATHS.decisionLedger;

function round4(value) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(4));
}

function parseNumber(text) {
  if (text === null || text === undefined || text === '') return null;
  const cleaned = String(text).replace(/[^0-9.-]/g, '');
  if (!cleaned || cleaned === '-' || cleaned === '.' || cleaned === '-.') return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function normalizeBool(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['true', '1', 'yes', 'y'].includes(raw)) return true;
  if (['false', '0', 'no', 'n'].includes(raw)) return false;
  return fallback;
}

function requireField(row, field) {
  const value = row[field];
  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error(`missing_field:${field}`);
  }
  return String(value).trim();
}

function normalizeStage(value, finalDecision, edge) {
  const normalized = String(value || '').trim().toLowerCase();
  if (finalDecision === 'BET') return '';
  if (DECISION_STAGE_VALUES.includes(normalized)) return normalized;
  if (edge === null || edge <= 0) return 'no_raw_edge';
  return 'threshold_gate';
}

function normalizeDecision(value) {
  const normalized = String(value || '').trim().toUpperCase();
  if (!FINAL_DECISION_VALUES.includes(normalized)) {
    throw new Error(`invalid_final_decision:${value}`);
  }
  return normalized;
}

function normalizeBetClass(value, finalDecision) {
  const normalized = String(value || '').trim().toUpperCase();
  if (normalized) return normalized;
  return finalDecision === 'BET' ? 'EDGE_BET' : 'EDGE_BET';
}

function deriveExpectedTier(edgePct) {
  if (!Number.isFinite(edgePct)) return null;
  if (edgePct >= 6) return 'T1';
  if (edgePct >= 4) return 'T2';
  if (edgePct >= 2) return 'T3';
  return null;
}

function isTierBetClass(value) {
  return ['T1', 'T2', 'T3'].includes(String(value || '').trim().toUpperCase());
}

function approxEqual(left, right, tolerance = 0.001) {
  return Number.isFinite(left) && Number.isFinite(right) && Math.abs(left - right) <= tolerance;
}

function appendJsonl(filePath, rows) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const existing = fs.existsSync(filePath)
    ? fs.readFileSync(filePath, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line))
    : [];
  const seen = new Set(existing.map((row) => `${row.run_id}::${row.rec_id}`));
  const payload = [];
  for (const row of rows) {
    const key = `${row.run_id}::${row.rec_id}`;
    if (seen.has(key)) throw new Error(`duplicate_native_row:${key}`);
    seen.add(key);
    payload.push(JSON.stringify(row));
  }
  if (payload.length === 0) return { appended: 0 };
  fs.appendFileSync(filePath, `${payload.join('\n')}\n`, 'utf8');
  return { appended: payload.length };
}

function classifyZeroToTwoPass(row) {
  return row.final_decision === 'SIT'
    && row.post_conf_edge_pct !== null
    && row.post_conf_edge_pct > 0
    && row.post_conf_edge_pct < 2;
}

function classifySuppressed(row) {
  const threshold = row.tier_threshold_pct ?? 2;
  const preConfThresholdClear = row.pre_conf_true_prob !== null
    && row.devig_implied_prob !== null
    && ((row.pre_conf_true_prob - row.devig_implied_prob) * 100) >= threshold;
  const nearThresholdPass = row.final_decision === 'SIT'
    && row.post_conf_edge_pct !== null
    && row.post_conf_edge_pct > 0
    && row.post_conf_edge_pct < threshold;
  return row.final_decision === 'SIT' && (preConfThresholdClear || nearThresholdPass);
}

export function normalizeNativeDecisionRow(input) {
  const runId = requireField(input, 'run_id');
  const recId = requireField(input, 'rec_id');
  const timestampCt = requireField(input, 'timestamp_ct');
  const targetDate = requireField(input, 'target_date');
  const finalDecision = normalizeDecision(input.final_decision);
  const rawEdgePct = round4(parseNumber(input.raw_edge_pct));
  const postConfEdgePct = round4(parseNumber(input.post_conf_edge_pct));
  const tierThresholdPct = round4(parseNumber(input.tier_threshold_pct)) ?? 2;
  const bankrollSnapshot = parseNumber(input.bankroll_snapshot);
  const kellyStake = parseNumber(input.kelly_stake);
  const normalized = {
    run_id: runId,
    rec_id: recId,
    timestamp_ct: timestampCt,
    target_date: targetDate,
    sport: requireField(input, 'sport'),
    league: String(input.league || '').trim() || null,
    event_id: String(input.event_id || '').trim() || null,
    event_label: String(input.event_label || '').trim() || null,
    market_type: requireField(input, 'market_type'),
    selection: requireField(input, 'selection'),
    sportsbook: requireField(input, 'sportsbook'),
    odds_american: String(input.odds_american || '').trim() || null,
    odds_decimal: round4(parseNumber(input.odds_decimal)),
    devig_implied_prob: round4(parseNumber(input.devig_implied_prob)),
    consensus_prob: round4(parseNumber(input.consensus_prob)),
    pre_conf_true_prob: round4(parseNumber(input.pre_conf_true_prob)),
    confidence_score: round4(parseNumber(input.confidence_score)),
    post_conf_true_prob: round4(parseNumber(input.post_conf_true_prob)),
    raw_edge_pct: rawEdgePct,
    post_conf_edge_pct: postConfEdgePct,
    tier_threshold_pct: tierThresholdPct,
    price_edge_pass: normalizeBool(input.price_edge_pass, rawEdgePct !== null && rawEdgePct >= tierThresholdPct),
    bet_permission_pass: normalizeBool(input.bet_permission_pass, finalDecision === 'BET'),
    final_decision: finalDecision,
    rejection_stage: normalizeStage(input.rejection_stage, finalDecision, rawEdgePct),
    rejection_reason: finalDecision === 'BET' ? '' : String(input.rejection_reason || '').trim().toLowerCase(),
    bet_class: normalizeBetClass(input.bet_class, finalDecision),
    bankroll_snapshot: bankrollSnapshot,
    kelly_stake: kellyStake,
    include_in_core_strategy_metrics: normalizeBool(input.include_in_core_strategy_metrics, String(input.bet_class || '').trim().toUpperCase() !== 'FUN_SGP'),
    include_in_actual_bankroll: normalizeBool(input.include_in_actual_bankroll, finalDecision === 'BET'),
  };

  if (normalized.final_decision === 'SIT' && !DECISION_STAGE_VALUES.includes(normalized.rejection_stage)) {
    throw new Error(`invalid_rejection_stage:${normalized.rejection_stage}`);
  }

  if (normalized.final_decision === 'BET' && normalized.post_conf_edge_pct !== null) {
    const expectedTier = deriveExpectedTier(normalized.post_conf_edge_pct);
    if (!expectedTier) {
      throw new Error(`bet_below_t3_threshold:${normalized.rec_id}`);
    }

    const expectedThreshold = TIER_THRESHOLDS[expectedTier];
    if (!approxEqual(normalized.tier_threshold_pct, expectedThreshold, 0.0001)) {
      throw new Error(`tier_threshold_mismatch:${normalized.rec_id}:${normalized.tier_threshold_pct}->${expectedThreshold}`);
    }

    if (isTierBetClass(normalized.bet_class) && normalized.bet_class !== expectedTier) {
      throw new Error(`tier_bet_class_mismatch:${normalized.rec_id}:${normalized.bet_class}->${expectedTier}`);
    }

    if ((normalized.kelly_stake !== null) !== (normalized.bankroll_snapshot !== null)) {
      throw new Error(`kelly_metadata_incomplete:${normalized.rec_id}`);
    }

    if (normalized.kelly_stake !== null && normalized.bankroll_snapshot !== null) {
      const tierForKelly = isTierBetClass(normalized.bet_class) ? normalized.bet_class : expectedTier;
      const breakdown = computeKellyBreakdown({
        bankroll: normalized.bankroll_snapshot,
        american_odds: normalized.odds_american,
        true_prob: normalized.post_conf_true_prob,
        implied_prob_fair: normalized.devig_implied_prob,
        tier: tierForKelly,
      });
      if (!approxEqual(normalized.kelly_stake, breakdown.final_stake, 0.001)) {
        throw new Error(`kelly_stake_mismatch:${normalized.rec_id}:${normalized.kelly_stake}->${breakdown.final_stake}`);
      }
    }
  }

  return normalized;
}

export function readNativeDecisionLedger(filePath = DEFAULT_NATIVE_ALL_LEDGER) {
  if (!fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

export function appendNativeDecisionRows(rows, options = {}) {
  const allLedger = options.allLedger || DEFAULT_NATIVE_ALL_LEDGER;
  const normalizedRows = rows.map((row) => normalizeNativeDecisionRow(row));
  const appended = appendJsonl(allLedger, normalizedRows).appended;
  return {
    all: appended,
    bets: normalizedRows.filter((row) => row.final_decision === 'BET').length,
    passes: normalizedRows.filter((row) => classifyZeroToTwoPass(row)).length,
    suppressed: normalizedRows.filter((row) => classifySuppressed(row)).length,
  };
}

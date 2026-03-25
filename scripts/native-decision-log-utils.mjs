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
  'market_family',
  'sport',
  'league',
  'event_id',
  'event_label',
  'event_home_team',
  'event_away_team',
  'market_type',
  'selection',
  'sportsbook',
  'player_name_raw',
  'player_name_normalized',
  'player_id_canonical',
  'player_team',
  'opponent_team',
  'prop_type',
  'prop_side',
  'prop_line',
  'line_key',
  'is_alt_line',
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
  'threshold_gap_pct',
  'price_edge_pass',
  'executable_book',
  'owned_book',
  'live_feed_book',
  'actionable_book',
  'bet_permission_pass',
  'final_decision',
  'rejection_stage',
  'rejection_reason',
  'rejection_class',
  'surfaced_as_closest_miss',
  'close_capture_status',
  'closing_odds_american',
  'closing_odds_decimal',
  'closing_implied_prob',
  'closing_devig_prob',
  'closing_snapshot_time_utc',
  'closing_book',
  'clv_delta_pct',
  'clv_direction',
  'close_match_quality',
  'closing_line',
  'snapshot_status',
  'snapshot_max_spread_seconds',
  'consensus_method',
  'consensus_book_count',
  'consensus_median_prob',
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

function normalizeNullableString(value) {
  const text = String(value || '').trim();
  return text ? text : null;
}

function normalizeCloseCaptureStatus(value, finalDecision) {
  const normalized = String(value || '').trim().toLowerCase();
  const allowed = new Set([
    'pending',
    'captured',
    'failed',
    'not_available',
    'insufficient_market_match',
  ]);
  if (allowed.has(normalized)) return normalized;
  if (normalized === 'not_captured') return 'not_available';
  return finalDecision === 'SIT' ? 'pending' : 'not_available';
}

function normalizeClvDirection(value) {
  const normalized = String(value || '').trim().toLowerCase();
  const allowed = new Set(['positive', 'negative', 'neutral', 'unknown']);
  return allowed.has(normalized) ? normalized : 'unknown';
}

function normalizeCloseMatchQuality(value) {
  const normalized = String(value || '').trim().toLowerCase();
  const allowed = new Set([
    'exact_same_book_same_market',
    'exact_market_cross_book',
    'proxy_only',
    'insufficient_match',
  ]);
  return allowed.has(normalized) ? normalized : 'insufficient_match';
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
    market_family: String(input.market_family || 'main_market').trim() || 'main_market',
    sport: requireField(input, 'sport'),
    league: String(input.league || '').trim() || null,
    event_id: String(input.event_id || '').trim() || null,
    event_label: String(input.event_label || '').trim() || null,
    event_home_team: String(input.event_home_team || '').trim() || null,
    event_away_team: String(input.event_away_team || '').trim() || null,
    market_type: requireField(input, 'market_type'),
    selection: requireField(input, 'selection'),
    sportsbook: requireField(input, 'sportsbook'),
    player_name_raw: String(input.player_name_raw || '').trim() || null,
    player_name_normalized: String(input.player_name_normalized || '').trim() || null,
    player_id_canonical: String(input.player_id_canonical || '').trim() || null,
    player_team: String(input.player_team || '').trim() || null,
    opponent_team: String(input.opponent_team || '').trim() || null,
    prop_type: String(input.prop_type || '').trim() || null,
    prop_side: String(input.prop_side || '').trim().toLowerCase() || null,
    prop_line: round4(parseNumber(input.prop_line)),
    line_key: String(input.line_key || '').trim() || null,
    is_alt_line: normalizeBool(input.is_alt_line, false),
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
    threshold_gap_pct: round4(parseNumber(input.threshold_gap_pct)) ?? (rawEdgePct === null ? null : round4(tierThresholdPct - rawEdgePct)),
    price_edge_pass: normalizeBool(input.price_edge_pass, rawEdgePct !== null && rawEdgePct >= tierThresholdPct),
    executable_book: normalizeBool(input.executable_book, false),
    owned_book: normalizeBool(input.owned_book, false),
    live_feed_book: normalizeBool(input.live_feed_book, false),
    actionable_book: normalizeBool(input.actionable_book, finalDecision === 'BET'),
    bet_permission_pass: normalizeBool(input.bet_permission_pass, finalDecision === 'BET'),
    final_decision: finalDecision,
    rejection_stage: normalizeStage(input.rejection_stage, finalDecision, rawEdgePct),
    rejection_reason: finalDecision === 'BET' ? '' : String(input.rejection_reason || '').trim().toLowerCase(),
    rejection_class: finalDecision === 'BET' ? '' : String(input.rejection_class || '').trim().toLowerCase(),
    surfaced_as_closest_miss: normalizeBool(input.surfaced_as_closest_miss, false),
    close_capture_status: normalizeCloseCaptureStatus(input.close_capture_status, finalDecision),
    closing_odds_american: normalizeNullableString(input.closing_odds_american),
    closing_odds_decimal: round4(parseNumber(input.closing_odds_decimal)),
    closing_implied_prob: round4(parseNumber(input.closing_implied_prob)),
    closing_devig_prob: round4(parseNumber(input.closing_devig_prob)),
    closing_snapshot_time_utc: normalizeNullableString(input.closing_snapshot_time_utc),
    closing_book: normalizeNullableString(input.closing_book),
    clv_delta_pct: round4(parseNumber(input.clv_delta_pct)),
    clv_direction: normalizeClvDirection(input.clv_direction),
    close_match_quality: normalizeCloseMatchQuality(input.close_match_quality),
    closing_line: round4(parseNumber(input.closing_line)),
    snapshot_status: normalizeNullableString(input.snapshot_status) || 'not_validated',
    snapshot_max_spread_seconds: round4(parseNumber(input.snapshot_max_spread_seconds)),
    consensus_method: normalizeNullableString(input.consensus_method),
    consensus_book_count: parseNumber(input.consensus_book_count),
    consensus_median_prob: round4(parseNumber(input.consensus_median_prob)),
    bet_class: normalizeBetClass(input.bet_class, finalDecision),
    bankroll_snapshot: bankrollSnapshot,
    kelly_stake: kellyStake,
    include_in_core_strategy_metrics: normalizeBool(input.include_in_core_strategy_metrics, String(input.bet_class || '').trim().toUpperCase() !== 'FUN_SGP'),
    include_in_actual_bankroll: normalizeBool(input.include_in_actual_bankroll, finalDecision === 'BET'),
  };

  if (normalized.market_family === 'player_prop') {
    if (!normalized.player_name_raw || !normalized.player_name_normalized || !normalized.player_id_canonical) {
      throw new Error(`missing_prop_identity:${normalized.rec_id}`);
    }
    if (!normalized.prop_type || !normalized.prop_side || normalized.prop_line === null || !normalized.line_key) {
      throw new Error(`missing_prop_market_shape:${normalized.rec_id}`);
    }
    if (!['over', 'under'].includes(normalized.prop_side)) {
      throw new Error(`invalid_prop_side:${normalized.rec_id}:${normalized.prop_side}`);
    }
  }

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

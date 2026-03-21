#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import {
  readNativeDecisionLedger,
  DEFAULT_NATIVE_ALL_LEDGER,
  DEFAULT_NATIVE_BETS_LEDGER,
  DEFAULT_NATIVE_PASS_LEDGER,
  DEFAULT_NATIVE_SUPPRESSED_LEDGER,
} from './native-decision-log-utils.mjs';
import { CORE_PATHS, readJsonl } from './core-ledger-utils.mjs';

const DEFAULT_BETS_LEDGER = path.resolve(process.cwd(), 'data', 'bets-ledger.json');
const DEFAULT_CANONICAL_STATE = path.resolve(process.cwd(), 'data', 'canonical-state.json');
const DEFAULT_PASSED_GRADES = '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json';
const DEFAULT_PUBLIC_JSON = path.resolve(process.cwd(), 'public', 'monthly-self-audit.json');
const DEFAULT_PUBLIC_REPORT = path.resolve(process.cwd(), 'public', 'monthly-self-audit.txt');
const DEFAULT_ROOT_JSON = path.resolve(process.cwd(), 'monthly-self-audit.json');
const DEFAULT_ROOT_REPORT = path.resolve(process.cwd(), 'monthly-self-audit.txt');

function readJsonSafe(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function round2(value) {
  return Number.isFinite(value) ? Number(value.toFixed(2)) : null;
}

function round4(value) {
  return Number.isFinite(value) ? Number(value.toFixed(4)) : null;
}

function parseNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const cleaned = String(value).replace(/[^0-9.+-]/g, '');
  if (!cleaned || cleaned === '-' || cleaned === '+' || cleaned === '.' || cleaned === '-.' || cleaned === '+.') return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

function normalizeName(value) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function normalizeResult(value) {
  const normalized = normalizeName(value);
  if (!normalized) return null;
  if (normalized.includes('win')) return 'win';
  if (normalized.includes('loss') || normalized.includes('lost')) return 'loss';
  if (normalized.includes('push') || normalized.includes('cash')) return 'push';
  return normalized;
}

function average(values) {
  const nums = values.filter((value) => Number.isFinite(value));
  if (nums.length === 0) return null;
  return round4(nums.reduce((sum, value) => sum + value, 0) / nums.length);
}

function safeRate(num, denom) {
  if (!Number.isFinite(num) || !Number.isFinite(denom) || denom === 0) return null;
  return round2((num / denom) * 100);
}

function chicagoMonthKey(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
  }).formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  return `${year}-${month}`;
}

function parseMonthArg() {
  const arg = process.argv.find((entry) => entry.startsWith('--month='));
  return arg ? arg.split('=')[1] : chicagoMonthKey();
}

function monthRange(monthKey) {
  const [year, month] = String(monthKey).split('-').map(Number);
  const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0, 0));
  const end = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0));
  return {
    startKey: `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-01`,
    endExclusiveKey: `${String(end.getUTCFullYear()).padStart(4, '0')}-${String(end.getUTCMonth() + 1).padStart(2, '0')}-01`,
  };
}

function rowDateKey(row) {
  const timestamp = String(row.timestamp_ct || row.Date || row.date || '').match(/(\d{4}-\d{2}-\d{2})/);
  return timestamp ? timestamp[1] : null;
}

function inMonth(row, range) {
  const key = rowDateKey(row);
  return Boolean(key && key >= range.startKey && key < range.endExclusiveKey);
}

function buildOutcomeFromGrade(row, gradesEntries) {
  const grade = gradesEntries[String(row.rec_id || '').trim()];
  if (!grade) return {
    result: null,
    units: null,
    source: 'missing_grade',
  };
  return {
    result: normalizeResult(grade.outcome_if_bet || grade.counterfactual_result),
    units: parseNumber(grade.hypothetical_units ?? grade.counterfactual_pl ?? grade.counterfactual_pl_unit),
    source: grade.grade_source || 'passed_grades_cache',
  };
}

function exactBetMatch(nativeRow, settledRows, quantRows) {
  const nativeDate = rowDateKey(nativeRow);
  const nativeBet = normalizeName(nativeRow.selection);
  const nativeMarket = normalizeName(nativeRow.market_type);
  const nativeBook = normalizeName(nativeRow.sportsbook);
  const nativeOdds = String(nativeRow.odds_american || '').trim();

  const candidates = settledRows.filter((row) => {
    const sameDate = String(row.Date || '').trim() === nativeDate;
    const sameBet = normalizeName(row.Bet) === nativeBet;
    const sameMarket = normalizeName(row.Market) === nativeMarket;
    const sameBook = normalizeName(row.Book) === nativeBook;
    const sameOdds = String(row['Odds (US)'] || '').trim() === nativeOdds;
    return sameDate && sameBet && sameMarket && sameBook && sameOdds;
  });
  if (candidates.length !== 1) return null;
  const settled = candidates[0];
  const quant = quantRows.find((row) => (
    String(row.date || '').trim() === nativeDate
    && normalizeName(row.bet) === nativeBet
    && normalizeName(row.market) === nativeMarket
    && normalizeName(row.book) === nativeBook
  )) || null;
  return {
    result: normalizeResult(settled.Result),
    units: quant ? parseNumber(quant.profit_units) : null,
    clv: parseNumber(settled.CLV),
    actual_profit: parseNumber(settled['P/L']),
    stake: parseNumber(settled.Stake),
    stake_units: quant ? parseNumber(quant.stake_units) : null,
    source: 'exact_match_settled_bet_log',
  };
}

function deriveDecisionMetrics(row) {
  const raw = parseNumber(row.raw_edge_pct);
  const post = parseNumber(row.post_conf_edge_pct);
  const preConf = parseNumber(row.pre_conf_true_prob);
  const postConfProb = parseNumber(row.post_conf_true_prob);
  const consensus = parseNumber(row.consensus_prob);
  return {
    raw_edge_pct: raw,
    post_conf_edge_pct: post,
    confidence_penalty: Number.isFinite(preConf) && Number.isFinite(postConfProb) ? round4(preConf - postConfProb) : null,
    consensus_penalty: Number.isFinite(preConf) && Number.isFinite(consensus) ? round4(preConf - consensus) : null,
  };
}

function summarizeRows(rows, options = {}) {
  const resultValues = rows.map((row) => normalizeResult(row.result)).filter(Boolean);
  const wins = resultValues.filter((value) => value === 'win').length;
  const unitsValues = rows.map((row) => parseNumber(row.units)).filter((value) => Number.isFinite(value));
  const clvValues = rows.map((row) => parseNumber(row.clv)).filter((value) => Number.isFinite(value));
  const stakeUnitValues = rows.map((row) => parseNumber(row.stake_units)).filter((value) => Number.isFinite(value));
  const totalUnits = unitsValues.length > 0 ? round2(unitsValues.reduce((sum, value) => sum + value, 0)) : null;
  const totalStakeUnits = stakeUnitValues.length > 0 ? round2(stakeUnitValues.reduce((sum, value) => sum + value, 0)) : null;

  const biggestMisses = rows
    .filter((row) => Number.isFinite(parseNumber(row.units)) && parseNumber(row.units) > 0)
    .sort((a, b) => parseNumber(b.units) - parseNumber(a.units))
    .slice(0, 3)
    .map((row) => ({
      rec_id: row.rec_id || null,
      selection: row.selection,
      units: round2(parseNumber(row.units)),
      result: row.result || null,
      rejection_stage: row.rejection_stage || null,
      rejection_reason: row.rejection_reason || null,
    }));

  const biggestSaves = rows
    .filter((row) => Number.isFinite(parseNumber(row.units)) && parseNumber(row.units) < 0)
    .sort((a, b) => parseNumber(a.units) - parseNumber(b.units))
    .slice(0, 3)
    .map((row) => ({
      rec_id: row.rec_id || null,
      selection: row.selection,
      units: round2(parseNumber(row.units)),
      result: row.result || null,
      rejection_stage: row.rejection_stage || null,
      rejection_reason: row.rejection_reason || null,
    }));

  return {
    source: options.source || null,
    count: rows.length,
    settled_or_graded_count: resultValues.length,
    win_rate: safeRate(wins, resultValues.length),
    hypothetical_units: totalUnits,
    hypothetical_roi: Number.isFinite(totalUnits) && Number.isFinite(totalStakeUnits) && totalStakeUnits !== 0
      ? round2((totalUnits / totalStakeUnits) * 100)
      : null,
    positive_clv_rate: safeRate(clvValues.filter((value) => value > 0).length, clvValues.length),
    average_raw_edge: average(rows.map((row) => parseNumber(row.raw_edge_pct))),
    average_post_conf_edge: average(rows.map((row) => parseNumber(row.post_conf_edge_pct))),
    average_confidence_penalty: average(rows.map((row) => parseNumber(row.confidence_penalty))),
    average_consensus_penalty: average(rows.map((row) => parseNumber(row.consensus_penalty))),
    biggest_saves: biggestSaves,
    biggest_misses: biggestMisses,
    native_row_count: options.nativeRowCount ?? rows.length,
  };
}

function summarizeRejectionStages(rows) {
  const stages = ['confidence_gate', 'threshold_gate', 'risk_gate', 'integrity_gate'];
  return Object.fromEntries(stages.map((stage) => {
    const stageRows = rows.filter((row) => row.rejection_stage === stage);
    return [stage, summarizeRows(stageRows, { source: 'native_decision_ledgers', nativeRowCount: stageRows.length })];
  }));
}

function reportLines(audit) {
  const lines = [];
  lines.push(`MONTHLY SELF AUDIT — ${audit.audit_month}`);
  lines.push(`Generated: ${audit.generated_at_utc}`);
  lines.push('');
  lines.push(`Audit status: ${audit.audit_status}`);
  lines.push(`Native coverage status: ${audit.native_coverage.status}`);
  lines.push(`Decision observations: ${audit.native_coverage.counts.decision_observations}`);
  lines.push(`Native bets: ${audit.native_coverage.counts.bets}`);
  lines.push(`Native passes: ${audit.native_coverage.counts.passes_zero_to_two}`);
  lines.push(`Native suppressed: ${audit.native_coverage.counts.suppressed_candidates}`);
  lines.push(`Excluded invalid rows: ${audit.native_coverage.counts.excluded_invalid_rows ?? 0}`);
  lines.push('');
  for (const [key, bucket] of Object.entries(audit.buckets)) {
    lines.push(`${key}`);
    lines.push(`- source: ${bucket.source}`);
    lines.push(`- count: ${bucket.count}`);
    lines.push(`- settled_or_graded_count: ${bucket.settled_or_graded_count}`);
    lines.push(`- win_rate: ${bucket.win_rate ?? 'N/A'}`);
    lines.push(`- hypothetical_units: ${bucket.hypothetical_units ?? 'N/A'}`);
    lines.push(`- hypothetical_roi: ${bucket.hypothetical_roi ?? 'N/A'}`);
    lines.push(`- positive_clv_rate: ${bucket.positive_clv_rate ?? 'N/A'}`);
    lines.push(`- avg_raw_edge: ${bucket.average_raw_edge ?? 'N/A'}`);
    lines.push(`- avg_post_conf_edge: ${bucket.average_post_conf_edge ?? 'N/A'}`);
    lines.push(`- avg_confidence_penalty: ${bucket.average_confidence_penalty ?? 'N/A'}`);
    lines.push(`- avg_consensus_penalty: ${bucket.average_consensus_penalty ?? 'N/A'}`);
    lines.push(`- biggest_saves: ${bucket.biggest_saves.map((row) => `${row.selection} ${row.units}`).join('; ') || 'none'}`);
    lines.push(`- biggest_misses: ${bucket.biggest_misses.map((row) => `${row.selection} ${row.units}`).join('; ') || 'none'}`);
    lines.push('');
  }
  lines.push('REJECTION STAGE SUMMARY');
  for (const [stage, summary] of Object.entries(audit.rejection_stage_summary)) {
    lines.push(`- ${stage}: count=${summary.count}, win_rate=${summary.win_rate ?? 'N/A'}, hypothetical_units=${summary.hypothetical_units ?? 'N/A'}, avg_confidence_penalty=${summary.average_confidence_penalty ?? 'N/A'}`);
  }
  lines.push('');
  lines.push(`Learning verdict: ${audit.learning_verdict}`);
  if ((audit.notes || []).length > 0) {
    lines.push('Notes:');
    for (const note of audit.notes) lines.push(`- ${note}`);
  }
  return `${lines.join('\n')}\n`;
}

const auditMonth = parseMonthArg();
const range = monthRange(auditMonth);
const passedGrades = readJsonSafe(DEFAULT_PASSED_GRADES, { entries: {} })?.entries || {};
const canonicalState = readJsonSafe(DEFAULT_CANONICAL_STATE, {}) || {};
const quantRows = canonicalState?.public_payload?.quant_performance?.per_bet || [];
const settledBets = readJsonSafe(DEFAULT_BETS_LEDGER, {})?.rows || [];
const invalidRunIds = new Set(
  readJsonl(CORE_PATHS.huntAuditLog)
    .filter((row) => String(row.invalid_status || '').toLowerCase().includes('invalid'))
    .map((row) => String(row.run_id || '').trim())
    .filter(Boolean)
);

const nativeDecisionRows = readNativeDecisionLedger(DEFAULT_NATIVE_ALL_LEDGER).filter((row) => inMonth(row, range));
const excludedNativeRows = nativeDecisionRows.filter((row) => invalidRunIds.has(String(row.run_id || '').trim()));
const validNativeDecisionRows = nativeDecisionRows.filter((row) => !invalidRunIds.has(String(row.run_id || '').trim()));
const nativeBetRows = readNativeDecisionLedger(DEFAULT_NATIVE_BETS_LEDGER).filter((row) => inMonth(row, range) && !invalidRunIds.has(String(row.run_id || '').trim()));
const nativePassRows = readNativeDecisionLedger(DEFAULT_NATIVE_PASS_LEDGER).filter((row) => inMonth(row, range) && !invalidRunIds.has(String(row.run_id || '').trim()));
const nativeSuppressedRows = readNativeDecisionLedger(DEFAULT_NATIVE_SUPPRESSED_LEDGER).filter((row) => inMonth(row, range) && !invalidRunIds.has(String(row.run_id || '').trim()));

const passRowsZeroToOne = nativePassRows
  .filter((row) => {
    const post = parseNumber(row.post_conf_edge_pct);
    return Number.isFinite(post) && post > 0 && post < 1;
  })
  .map((row) => {
    const grade = buildOutcomeFromGrade(row, passedGrades);
    return {
      ...row,
      ...deriveDecisionMetrics(row),
      result: grade.result,
      units: grade.units,
      clv: null,
      stake_units: 1,
    };
  });

const passRowsOneToTwo = nativePassRows
  .filter((row) => {
    const post = parseNumber(row.post_conf_edge_pct);
    return Number.isFinite(post) && post >= 1 && post < 2;
  })
  .map((row) => {
    const grade = buildOutcomeFromGrade(row, passedGrades);
    return {
      ...row,
      ...deriveDecisionMetrics(row),
      result: grade.result,
      units: grade.units,
      clv: null,
      stake_units: 1,
    };
  });

const thresholdSuppressedRows = nativeSuppressedRows
  .filter((row) => {
    const raw = parseNumber(row.raw_edge_pct);
    const threshold = parseNumber(row.tier_threshold_pct) ?? 2;
    return Number.isFinite(raw) && raw >= threshold;
  })
  .map((row) => {
    const grade = buildOutcomeFromGrade(row, passedGrades);
    return {
      ...row,
      ...deriveDecisionMetrics(row),
      result: grade.result,
      units: grade.units,
      clv: null,
      stake_units: 1,
    };
  });

const nativeActualBets = nativeBetRows
  .filter((row) => String(row.bet_class || '').toUpperCase() !== 'FUN_SGP')
  .map((row) => {
    const match = exactBetMatch(row, settledBets, quantRows);
    return {
      ...row,
      ...deriveDecisionMetrics(row),
      result: match?.result || null,
      units: match?.units ?? null,
      clv: match?.clv ?? null,
      stake_units: match?.stake_units ?? null,
    };
  });

const nativeFunSgpBets = nativeBetRows
  .filter((row) => String(row.bet_class || '').toUpperCase() === 'FUN_SGP')
  .map((row) => {
    const match = exactBetMatch(row, settledBets, quantRows);
    return {
      ...row,
      ...deriveDecisionMetrics(row),
      result: match?.result || null,
      units: match?.units ?? null,
      clv: match?.clv ?? null,
      stake_units: match?.stake_units ?? null,
    };
  });

const audit = {
  generated_at_utc: new Date().toISOString(),
  audit_month: auditMonth,
  window: {
    start_date: range.startKey,
    end_exclusive_date: range.endExclusiveKey,
    timezone: 'America/Chicago',
  },
  native_coverage: {
    status: validNativeDecisionRows.length > 0 ? 'native_coverage_present' : 'no_native_decision_coverage',
    counts: {
      decision_observations: validNativeDecisionRows.length,
      bets: nativeBetRows.length,
      passes_zero_to_two: nativePassRows.length,
      suppressed_candidates: nativeSuppressedRows.length,
      excluded_invalid_rows: excludedNativeRows.length,
    },
  },
  learning_scope: {
    excluded_invalid_run_ids: Array.from(invalidRunIds),
    excluded_invalid_row_count: excludedNativeRows.length,
  },
  audit_status: validNativeDecisionRows.length > 0 ? 'ready' : 'insufficient_native_coverage',
  buckets: {
    pass_band_0_to_1: summarizeRows(passRowsZeroToOne, { source: 'native_pass_ledger', nativeRowCount: passRowsZeroToOne.length }),
    pass_band_1_to_2: summarizeRows(passRowsOneToTwo, { source: 'native_pass_ledger', nativeRowCount: passRowsOneToTwo.length }),
    threshold_clearing_suppressed: summarizeRows(thresholdSuppressedRows, { source: 'native_suppressed_ledger', nativeRowCount: thresholdSuppressedRows.length }),
    actual_bets_placed: summarizeRows(nativeActualBets, { source: 'native_bets_ledger_with_exact_settlement_match', nativeRowCount: nativeActualBets.length }),
    fun_sgp: summarizeRows(nativeFunSgpBets, { source: 'native_bets_ledger_with_exact_settlement_match', nativeRowCount: nativeFunSgpBets.length }),
  },
  rejection_stage_summary: summarizeRejectionStages(
    validNativeDecisionRows
      .filter((row) => String(row.final_decision || '').toUpperCase() === 'SIT')
      .map((row) => {
        const grade = buildOutcomeFromGrade(row, passedGrades);
        return {
          ...row,
          ...deriveDecisionMetrics(row),
          result: grade.result,
          units: grade.units,
          clv: null,
          stake_units: 1,
        };
      })
  ),
  notes: [],
};

if (validNativeDecisionRows.length === 0) {
  audit.notes.push('No native decision-time observation rows exist for this month. Pass/suppression learning is unavailable without native coverage.');
}
if (nativeBetRows.length === 0) {
  audit.notes.push('No native bet rows exist for this month. Actual-bet and FUN_SGP learning buckets are empty until native decision logging is used at hunt time.');
}
if (validNativeDecisionRows.length > 0 && thresholdSuppressedRows.length === 0) {
  audit.notes.push('No threshold-clearing suppressed native rows were recorded in this month window.');
}
if (excludedNativeRows.length > 0) {
  audit.notes.push(`Excluded ${excludedNativeRows.length} native decision rows from invalidated runs: ${Array.from(invalidRunIds).join(', ')}.`);
}

audit.learning_verdict = validNativeDecisionRows.length > 0
  ? 'TieredEdge can learn from its own native decision records for this month window.'
  : 'TieredEdge cannot yet learn from itself for this month window because native decision-time coverage is absent.';

const report = reportLines(audit);
for (const outPath of [DEFAULT_PUBLIC_JSON, DEFAULT_ROOT_JSON]) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(audit, null, 2)}\n`, 'utf8');
}
for (const outPath of [DEFAULT_PUBLIC_REPORT, DEFAULT_ROOT_REPORT]) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, report, 'utf8');
}

console.log(`Built monthly self audit: ${DEFAULT_PUBLIC_JSON}`);
console.log(`Built monthly self audit report: ${DEFAULT_PUBLIC_REPORT}`);

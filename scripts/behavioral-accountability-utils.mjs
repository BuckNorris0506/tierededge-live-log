import fs from 'node:fs';
import path from 'node:path';
import {
  CORE_PATHS,
  DATA_DIR,
  REPO_ROOT,
  appendJsonl,
  parseNumber,
  readJson,
  readJsonl,
  round2,
  toCtIsoDate,
  writeJsonl,
  writeJson,
} from './core-ledger-utils.mjs';
import { EXECUTION_LOG_PATH, readExecutionLog } from './execution-layer-utils.mjs';

export const OVERRIDE_LOG_PATH = path.join(DATA_DIR, 'override-log.jsonl');
export const POST_MORTEM_LOG_PATH = path.join(DATA_DIR, 'post-mortem-log.jsonl');
export const WEEKLY_TRUTH_REPORT_JSON_PATH = path.join(REPO_ROOT, 'weekly-truth-report.json');
export const WEEKLY_TRUTH_REPORT_TXT_PATH = path.join(REPO_ROOT, 'weekly-truth-report.txt');
export const PUBLIC_WEEKLY_TRUTH_REPORT_JSON_PATH = path.join(REPO_ROOT, 'public', 'weekly-truth-report.json');
export const PUBLIC_WEEKLY_TRUTH_REPORT_TXT_PATH = path.join(REPO_ROOT, 'public', 'weekly-truth-report.txt');

const DEFAULT_POST_MORTEM_POLICY = {
  realized_loss_streak_threshold: -3,
  losing_streak_bets_threshold: 5,
};

function normalize(value) {
  return String(value || '').trim().toLowerCase();
}

function unique(values) {
  return [...new Set((values || []).filter(Boolean))];
}

function readBehaviorPolicy() {
  const raw = readJson(path.join(REPO_ROOT, 'config', 'behavioral-accountability.json'), {});
  return {
    ...DEFAULT_POST_MORTEM_POLICY,
    ...(raw || {}),
  };
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value === null || value === undefined || value === '') return [];
  return [value];
}

function joinFreeform(row) {
  const chunks = [
    row.override_justification,
    row.freeform_justification,
    row.override_reason,
    ...asArray(row.notes),
    ...asArray(row.warnings),
  ].filter((value) => value !== null && value !== undefined && String(value).trim() !== '');
  return unique(chunks.map((value) => String(value).trim())).join(' | ');
}

function isRejectedExecution(row) {
  return normalize(row.execution_approval_result) === 'reject_execution';
}

function isBlockedRun(row) {
  return normalize(row.override_reason) === 'blocked_run'
    || normalize(row.notes).includes('blocked_run')
    || normalize(row.execution_approval_result_reason) === 'blocked_run';
}

function stakeChanged(row) {
  const recommended = parseNumber(row.recommended_stake);
  const actual = parseNumber(row.actual_stake);
  if (recommended === null || actual === null) return false;
  return Math.abs(actual - recommended) > 0.009;
}

function lineOrPriceChanged(row) {
  const drift = String(row.line_price_drift || '').trim().toLowerCase();
  if (!drift || drift === 'n/a' || drift === '0 cents' || drift === '0' || drift === '0 points') return false;
  return true;
}

function isOffModel(row) {
  return !row.rec_id || ['unmatched_manual_bet', 'ambiguous_match'].includes(normalize(row.match_status));
}

function operatorSource(row) {
  return row.confirmation_source || row.operator_channel_source || row.ingestion_channel || row.source || 'unknown';
}

function baseOverrideEvent(row) {
  const justification = joinFreeform(row);
  return {
    execution_id: row.execution_id || row.execution_log_id || null,
    rec_id: row.rec_id || null,
    timestamp_utc: row.logged_at_utc || row.ingestion_timestamp || new Date().toISOString(),
    reason_required: true,
    freeform_justification: justification || null,
    operator_channel_source: operatorSource(row),
    during_blocked_run: isBlockedRun(row),
    changed_stake: stakeChanged(row),
    changed_line_price: lineOrPriceChanged(row),
    off_model: isOffModel(row),
  };
}

export function deriveOverrideEventsFromExecution(row) {
  if (!row || typeof row !== 'object') return [];
  const base = baseOverrideEvent(row);
  const events = [];
  const executionId = base.execution_id || `unknown-execution::${base.timestamp_utc}`;

  const maybePush = (overrideType) => {
    events.push({
      override_id: `override::${executionId}::${overrideType}`,
      override_type: overrideType,
      ...base,
    });
  };

  if (base.during_blocked_run) maybePush('blocked_run_bet_placed');
  if (Boolean(row.manual_override_flag)) maybePush('manual_override');
  if (isRejectedExecution(row)) maybePush('rejected_execution_placed');
  if (base.changed_stake) maybePush('stake_changed_from_recommendation');
  if (base.changed_line_price) maybePush('line_or_price_changed_at_execution');
  if (base.off_model) maybePush('off_model_bet_placed');

  return events;
}

export function readOverrideLog() {
  return readJsonl(OVERRIDE_LOG_PATH);
}

export function appendOverrideEventsForExecution(row) {
  const events = deriveOverrideEventsFromExecution(row);
  if (!events.length) return [];
  const missingJustification = events.filter((event) => !String(event.freeform_justification || '').trim());
  if (missingJustification.length) {
    throw new Error(`missing_override_justification:${missingJustification.map((event) => event.override_type).join(',')}`);
  }

  const existing = new Set(readOverrideLog().map((event) => String(event.override_id || '')));
  const next = events.filter((event) => !existing.has(String(event.override_id || '')));
  if (!next.length) return [];
  appendJsonl(OVERRIDE_LOG_PATH, next, (event) => String(event.override_id || ''));
  return next;
}

export function backfillOverrideLogFromExecutions() {
  const rows = readExecutionLog();
  const rebuilt = [];
  const seen = new Set();
  for (const row of rows) {
    for (const event of deriveOverrideEventsFromExecution(row)) {
      if (!event.freeform_justification) continue;
      const key = String(event.override_id || '');
      if (!key || seen.has(key)) continue;
      seen.add(key);
      rebuilt.push(event);
    }
  }
  writeJsonl(OVERRIDE_LOG_PATH, rebuilt);
  return {
    execution_rows_scanned: rows.length,
    override_events_appended: rebuilt.length,
    override_events_total: rebuilt.length,
  };
}

function toSortKey(row) {
  const date = String(row.date || '').trim() || toCtIsoDate(row.timestamp_ct) || '';
  const time = String(row.timestamp_ct || '').trim();
  return `${date} ${time}`;
}

function isFinalBetRow(row) {
  const type = normalize(row.grading_type);
  if (type === 'pass') return false;
  const status = normalize(row.result || row.settlement_status);
  return ['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout', 'cash out', 'cashed out'].includes(status);
}

function normalizedLossish(row) {
  const status = normalize(row.result || row.settlement_status);
  const profit = parseNumber(row.profit_loss) || 0;
  if (status === 'loss') return true;
  if (status === 'cashed_out' || status === 'cash out' || status === 'partial_cashout') return profit < 0;
  return profit < 0;
}

function buildTriggerId(kind, row, metricValue) {
  return `postmortem::${kind}::${row.grading_id || row.execution_log_id || row.ref_id || row.selection || 'unknown'}::${metricValue}`;
}

export function readPostMortemLog() {
  return readJsonl(POST_MORTEM_LOG_PATH);
}

export function appendPostMortemReview(row) {
  const required = [
    'trigger_id',
    'was_this_loss_model_consistent',
    'was_price_still_good_at_execution',
    'clv_status_observed',
    'process_break_detected',
    'override_involved',
    'emotional_state_proxy',
    'short_freeform_note',
  ];
  const missing = required.filter((field) => row[field] === undefined || row[field] === null || String(row[field]).trim() === '');
  if (missing.length) throw new Error(`missing_post_mortem_fields:${missing.join(',')}`);
  const review = {
    review_id: row.review_id || `review::${row.trigger_id}::${new Date().toISOString()}`,
    submitted_at_utc: row.submitted_at_utc || new Date().toISOString(),
    ...row,
  };
  appendJsonl(POST_MORTEM_LOG_PATH, review, (entry) => String(entry.review_id || ''));
  return review;
}

export function getPostMortemStatus(gradingRows, reviewRows = readPostMortemLog()) {
  const policy = readBehaviorPolicy();
  const settled = gradingRows
    .filter((row) => normalize(row.grading_type) === 'bet' || normalize(row.grading_type) === 'reconciliation')
    .filter(isFinalBetRow)
    .slice()
    .sort((a, b) => String(toSortKey(a)).localeCompare(String(toSortKey(b))));

  let realizedLossRun = 0;
  let losingBetRun = 0;
  let activeLossRows = [];
  let activeLosingRows = [];
  const triggers = [];

  for (const row of settled) {
    const profit = parseNumber(row.profit_loss) || 0;
    const lossish = normalizedLossish(row);

    if (lossish) {
      realizedLossRun = round2(realizedLossRun + profit) || realizedLossRun + profit;
      activeLossRows.push(row);
      losingBetRun += normalize(row.result || row.settlement_status) === 'loss' ? 1 : 0;
      if (normalize(row.result || row.settlement_status) === 'loss') {
        activeLosingRows.push(row);
      }
    } else {
      realizedLossRun = 0;
      losingBetRun = 0;
      activeLossRows = [];
      activeLosingRows = [];
    }

    if (realizedLossRun <= policy.realized_loss_streak_threshold) {
      triggers.push({
        trigger_id: buildTriggerId('realized_loss_streak', row, realizedLossRun),
        trigger_type: 'realized_loss_streak',
        threshold_value: policy.realized_loss_streak_threshold,
        streak_value: realizedLossRun,
        triggered_at_sort_key: toSortKey(row),
        triggered_at_ct: toSortKey(row),
        supporting_rows: activeLossRows.map((entry) => ({
          grading_id: entry.grading_id,
          selection: entry.selection,
          result: entry.result || entry.settlement_status,
          profit_loss: entry.profit_loss,
        })),
      });
      realizedLossRun = 0;
      activeLossRows = [];
    }

    if (losingBetRun >= policy.losing_streak_bets_threshold) {
      triggers.push({
        trigger_id: buildTriggerId('losing_streak_bets', row, losingBetRun),
        trigger_type: 'losing_streak_bets',
        threshold_value: policy.losing_streak_bets_threshold,
        streak_value: losingBetRun,
        triggered_at_sort_key: toSortKey(row),
        triggered_at_ct: toSortKey(row),
        supporting_rows: activeLosingRows.map((entry) => ({
          grading_id: entry.grading_id,
          selection: entry.selection,
          result: entry.result || entry.settlement_status,
          profit_loss: entry.profit_loss,
        })),
      });
      losingBetRun = 0;
      activeLosingRows = [];
    }
  }

  const reviewByTrigger = new Map(
    reviewRows
      .filter((row) => row.trigger_id)
      .map((row) => [String(row.trigger_id), row])
  );
  const unresolved = triggers.filter((trigger) => !reviewByTrigger.has(String(trigger.trigger_id)));
  const latestTrigger = unresolved.at(-1) || null;
  return {
    policy,
    required: Boolean(latestTrigger),
    current_status: latestTrigger ? 'POST_MORTEM_REQUIRED' : 'CLEAR',
    latest_trigger: latestTrigger,
    total_triggers: triggers.length,
    unresolved_trigger_count: unresolved.length,
    latest_review: latestTrigger ? null : (reviewRows.at(-1) || null),
  };
}

function bucketStake(stake) {
  if (stake < 1) return '<1';
  if (stake < 5) return '1-5';
  if (stake < 20) return '5-20';
  return '20+';
}

function bucketTimeOfDay(timestampCt) {
  const match = String(timestampCt || '').match(/(\d{1,2}):(\d{2})/);
  if (!match) return 'UNKNOWN';
  const hour = Number(match[1]);
  if (hour < 6) return 'Overnight';
  if (hour < 12) return 'Morning';
  if (hour < 17) return 'Afternoon';
  if (hour < 22) return 'Evening';
  return 'Late';
}

function buildDecisionLookup() {
  const decisions = readJsonl(CORE_PATHS.decisionLedger);
  const byRecId = new Map();
  const bySelectionDate = new Map();
  for (const row of decisions) {
    if (row.rec_id) byRecId.set(String(row.rec_id), row);
    const key = `${String(row.selection || '')}::${String(row.target_date || '')}`;
    if (!bySelectionDate.has(key)) bySelectionDate.set(key, row);
  }
  return { byRecId, bySelectionDate };
}

function findDecision(row, lookup) {
  const recId = String(row.rec_id || row.ref_id || '');
  if (recId && lookup.byRecId.has(recId)) return lookup.byRecId.get(recId);
  const key = `${String(row.selection || '')}::${String(row.date || '')}`;
  return lookup.bySelectionDate.get(key) || null;
}

function findExecutionForGrade(grade, executionRows) {
  const byExecutionId = new Map();
  const byRecId = new Map();
  const bySelection = new Map();
  for (const row of executionRows) {
    if (row.execution_id) byExecutionId.set(String(row.execution_id), row);
    if (row.rec_id) byRecId.set(String(row.rec_id), row);
    if (row.selection) bySelection.set(String(row.selection), row);
  }
  if (grade.execution_log_id && byExecutionId.has(String(grade.execution_log_id))) return byExecutionId.get(String(grade.execution_log_id));
  if (grade.rec_id && byRecId.has(String(grade.rec_id))) return byRecId.get(String(grade.rec_id));
  if (grade.ref_id && byRecId.has(String(grade.ref_id))) return byRecId.get(String(grade.ref_id));
  if (grade.selection && bySelection.has(String(grade.selection))) return bySelection.get(String(grade.selection));
  return null;
}

function aggregateSliceRows(rows) {
  const settled = rows.filter((row) => ['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout'].includes(normalize(row.result)));
  const betsForHitRate = settled.filter((row) => ['win', 'loss'].includes(normalize(row.result)));
  const wins = betsForHitRate.filter((row) => normalize(row.result) === 'win').length;
  const unitsRisked = round2(rows.reduce((sum, row) => sum + (parseNumber(row.stake) || 0), 0)) || 0;
  const realized = round2(rows.reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0;
  const roi = unitsRisked > 0 ? round2((realized / unitsRisked) * 100) : null;
  const edgeValues = rows.map((row) => parseNumber(row.edge_at_placement)).filter((value) => value !== null);
  const clvValues = rows.map((row) => parseNumber(row.clv_price_delta)).filter((value) => value !== null);
  const clvCovered = rows.filter((row) => ['exact_close_found', 'proxy_close_found'].includes(normalize(row.clv_status))).length;
  return {
    bet_count: rows.length,
    units_risked: unitsRisked,
    realized_pl: realized,
    roi_pct: roi,
    hit_rate_pct: betsForHitRate.length ? round2((wins / betsForHitRate.length) * 100) : null,
    average_edge_at_placement: edgeValues.length ? round2(edgeValues.reduce((sum, value) => sum + value, 0) / edgeValues.length) : null,
    clv_coverage_pct: rows.length ? round2((clvCovered / rows.length) * 100) : null,
    clv_average: clvValues.length ? round2(clvValues.reduce((sum, value) => sum + value, 0) / clvValues.length) : null,
  };
}

function topCategories(sliceGroups, direction) {
  return Object.entries(sliceGroups)
    .map(([label, rows]) => ({ label, ...aggregateSliceRows(rows) }))
    .filter((row) => row.bet_count > 0)
    .filter((row) => direction === 'worst' ? row.realized_pl < 0 : row.realized_pl > 0)
    .sort((a, b) => direction === 'worst' ? a.realized_pl - b.realized_pl : b.realized_pl - a.realized_pl)
    .slice(0, 3);
}

function addToGroup(map, key, row) {
  if (!map.has(key)) map.set(key, []);
  map.get(key).push(row);
}

export function buildWeeklyTruthReport() {
  const gradingRows = readJsonl(CORE_PATHS.gradingLedger);
  const executionRows = readExecutionLog();
  const overrideRows = readOverrideLog();
  const lookup = buildDecisionLookup();
  const now = Date.now();
  const sevenDaysAgo = now - (7 * 24 * 60 * 60 * 1000);

  const settledRows = gradingRows
    .filter((row) => normalize(row.grading_type) === 'bet')
    .filter(isFinalBetRow)
    .filter((row) => {
      const dateKey = row.date ? `${row.date}T00:00:00Z` : row.timestamp_ct;
      const ts = Date.parse(String(dateKey || '').replace(' CT', ''));
      return Number.isFinite(ts) && ts >= sevenDaysAgo && ts <= now;
    })
    .map((row) => {
      const decision = findDecision(row, lookup);
      const execution = findExecutionForGrade(row, executionRows);
      const overrideLinked = execution
        ? overrideRows.filter((event) => String(event.execution_id || '') === String(execution.execution_id || ''))
        : [];
      const clvStatus = row.clv_status || 'missing_clv_source';
      return {
        grading_id: row.grading_id,
        selection: row.selection,
        result: normalize(row.result || row.settlement_status).toUpperCase(),
        profit_loss: parseNumber(row.profit_loss) || 0,
        stake: parseNumber(row.stake) || 0,
        sport: decision?.sport || execution?.sport || 'UNKNOWN',
        league: decision?.league || execution?.league || decision?.sport || execution?.sport || 'UNKNOWN',
        bet_class: row.bet_class || decision?.bet_class || 'UNKNOWN',
        stake_bucket: bucketStake(parseNumber(row.stake) || 0),
        time_of_day: bucketTimeOfDay(row.timestamp_ct),
        manual_override_bucket: execution?.manual_override_flag ? 'manual_override' : 'normal_execution',
        blocked_run_bucket: overrideLinked.some((event) => event.during_blocked_run) ? 'blocked_run_override' : 'normal_run',
        clv_bucket: clvStatus === 'missing_clv_source'
          ? 'clv_missing'
          : ((parseNumber(row.clv_price_delta) || 0) >= 0 ? 'clv_positive_or_flat' : 'clv_negative'),
        approval_bucket: normalize(execution?.execution_approval_result) === 'reject_execution' ? 'rejected_but_placed' : 'approved_or_unknown',
        edge_at_placement: parseNumber(decision?.post_conf_edge_pct),
        clv_status: clvStatus,
        clv_price_delta: parseNumber(row.clv_price_delta),
        placement_snapshot_status: execution?.placement_snapshot_status || 'snapshot_missing',
      };
    });

  const groups = {
    sport_league: new Map(),
    bet_class: new Map(),
    stake_size_bucket: new Map(),
    time_of_day: new Map(),
    manual_override_vs_normal: new Map(),
    blocked_run_override_vs_normal: new Map(),
    clv_bucket: new Map(),
    approval_bucket: new Map(),
  };

  for (const row of settledRows) {
    addToGroup(groups.sport_league, `${row.sport} / ${row.league}`, row);
    addToGroup(groups.bet_class, row.bet_class, row);
    addToGroup(groups.stake_size_bucket, row.stake_bucket, row);
    addToGroup(groups.time_of_day, row.time_of_day, row);
    addToGroup(groups.manual_override_vs_normal, row.manual_override_bucket, row);
    addToGroup(groups.blocked_run_override_vs_normal, row.blocked_run_bucket, row);
    addToGroup(groups.clv_bucket, row.clv_bucket, row);
    addToGroup(groups.approval_bucket, row.approval_bucket, row);
  }

  const report = {
    generated_at_utc: new Date().toISOString(),
    window: 'last_7_days',
    settled_bet_count: settledRows.length,
    expected_vs_realized_summary: {
      average_edge_at_placement: (() => {
        const values = settledRows.map((row) => parseNumber(row.edge_at_placement)).filter((value) => value !== null);
        return values.length ? round2(values.reduce((sum, value) => sum + value, 0) / values.length) : null;
      })(),
      realized_pl: round2(settledRows.reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0,
      units_risked: round2(settledRows.reduce((sum, row) => sum + (parseNumber(row.stake) || 0), 0)) || 0,
    },
    override_totals: {
      total_override_events: overrideRows.length,
      blocked_run_override_count: overrideRows.filter((row) => row.during_blocked_run).length,
      off_model_override_count: overrideRows.filter((row) => row.off_model).length,
      monthly_override_count: overrideRows.filter((row) => {
        const ts = Date.parse(String(row.timestamp_utc || ''));
        if (!Number.isFinite(ts)) return false;
        const nowDate = new Date();
        const thenDate = new Date(ts);
        return nowDate.getUTCFullYear() === thenDate.getUTCFullYear() && nowDate.getUTCMonth() === thenDate.getUTCMonth();
      }).length,
    },
    missing_coverage: {
      missing_snapshot_count: executionRows.filter((row) => normalize(row.placement_snapshot_status) === 'snapshot_missing').length,
      missing_clv_count: settledRows.filter((row) => normalize(row.clv_status) === 'missing_clv_source').length,
    },
    slices: Object.fromEntries(
      Object.entries(groups).map(([key, map]) => [
        key,
        Object.fromEntries(
          Array.from(map.entries()).map(([label, rows]) => [label, aggregateSliceRows(rows)])
        ),
      ])
    ),
    top_bleeding_categories: [
      ...topCategories(Object.fromEntries(groups.sport_league), 'worst'),
      ...topCategories(Object.fromEntries(groups.manual_override_vs_normal), 'worst'),
      ...topCategories(Object.fromEntries(groups.approval_bucket), 'worst'),
    ].slice(0, 3),
    top_strongest_categories: [
      ...topCategories(Object.fromEntries(groups.sport_league), 'best'),
      ...topCategories(Object.fromEntries(groups.bet_class), 'best'),
      ...topCategories(Object.fromEntries(groups.clv_bucket), 'best'),
    ].slice(0, 3),
  };

  const textLines = [
    'WEEKLY TRUTH REPORT',
    `Generated: ${report.generated_at_utc}`,
    `Settled bets: ${report.settled_bet_count}`,
    `Average edge at placement: ${report.expected_vs_realized_summary.average_edge_at_placement ?? 'N/A'} | Realized P/L: ${report.expected_vs_realized_summary.realized_pl}`,
    `Override events: ${report.override_totals.total_override_events} | Blocked-run: ${report.override_totals.blocked_run_override_count} | Off-model: ${report.override_totals.off_model_override_count}`,
    `Missing snapshot count: ${report.missing_coverage.missing_snapshot_count} | Missing CLV count: ${report.missing_coverage.missing_clv_count}`,
    '',
    'Top Bleeding Categories:',
    ...(report.top_bleeding_categories.length
      ? report.top_bleeding_categories.map((row, index) => `${index + 1}. ${row.label} | bets=${row.bet_count} | P/L=${row.realized_pl} | ROI=${row.roi_pct ?? 'N/A'}%`)
      : ['none']),
    '',
    'Top Strongest Categories:',
    ...(report.top_strongest_categories.length
      ? report.top_strongest_categories.map((row, index) => `${index + 1}. ${row.label} | bets=${row.bet_count} | P/L=${row.realized_pl} | ROI=${row.roi_pct ?? 'N/A'}%`)
      : ['none']),
  ];

  return { report, text: textLines.join('\n') };
}

export function writeWeeklyTruthReport() {
  const { report, text } = buildWeeklyTruthReport();
  writeJson(WEEKLY_TRUTH_REPORT_JSON_PATH, report);
  writeJson(PUBLIC_WEEKLY_TRUTH_REPORT_JSON_PATH, report);
  writeJson(path.join(DATA_DIR, 'weekly-truth-report.json'), report);
  fs.writeFileSync(WEEKLY_TRUTH_REPORT_TXT_PATH, `${text}\n`, 'utf8');
  fs.writeFileSync(PUBLIC_WEEKLY_TRUTH_REPORT_TXT_PATH, `${text}\n`, 'utf8');
  fs.writeFileSync(path.join(DATA_DIR, 'weekly-truth-report.txt'), `${text}\n`, 'utf8');
  writeJson(path.join(DATA_DIR, 'behavioral-accountability.json'), {
    override_log_path: OVERRIDE_LOG_PATH,
    post_mortem_log_path: POST_MORTEM_LOG_PATH,
    weekly_truth_report_path: WEEKLY_TRUTH_REPORT_JSON_PATH,
  });
  return { report, text };
}

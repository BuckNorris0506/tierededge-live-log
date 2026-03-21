#!/usr/bin/env node
import fs from 'node:fs';
import { CORE_PATHS, readJson, readJsonl, writeJson, parseNumber, round2, formatMoney } from './core-ledger-utils.mjs';
import { EXECUTION_BOARD_PATH, EXECUTION_LOG_PATH, readExecutionLog } from './execution-layer-utils.mjs';
import { validateLedgerInvariants } from './validate-ledger-invariants.mjs';
import { OVERRIDE_LOG_PATH, POST_MORTEM_LOG_PATH, getPostMortemStatus, readOverrideLog, readPostMortemLog, buildWeeklyTruthReport } from './behavioral-accountability-utils.mjs';
import { getLatestBankrollAnnotatedGrade } from './bankroll-reconciliation-utils.mjs';
import { formatCtTimestamp } from './openclaw-runtime-utils.mjs';

const HUNT_AUDIT_LOG_PATH = CORE_PATHS.huntAuditLog;

function parsePhase(bankroll) {
  if (!Number.isFinite(bankroll)) return 'UNKNOWN';
  if (bankroll < 250) return 'AGGRESSIVE';
  if (bankroll < 5000) return 'GROWTH';
  return 'CONSERVATIVE';
}

function parsePercentString(value) {
  const num = parseNumber(value);
  return num === null ? null : num;
}

function normalizeProbability(value) {
  const num = parseNumber(value);
  if (num === null) return null;
  return num > 1 ? num / 100 : num;
}

function parseDailyExposure(summary) {
  const match = String(summary || '').match(/Daily Exposure Used:\s*([0-9.]+)%/i);
  return match ? `${match[1]}%` : 'N/A';
}

function formatPercent(value) {
  return value === null || value === undefined || !Number.isFinite(value) ? 'N/A' : `${round2(value)}%`;
}

function parseRunClassification({ runtimeStatus, freshnessHours, bankrollDiff, stateSyncGap, ledgerValidation, postMortemStatus }) {
  if (ledgerValidation && !ledgerValidation.passed) return ledgerValidation.run_classification || 'ledger_integrity_failure';
  if (postMortemStatus?.required) return 'post_mortem_required';
  const latest = runtimeStatus?.latest_hunt_current || null;
  if (latest?.data_failure_codes?.includes('auth_failure')) return 'auth_failure';
  if (latest?.data_failure_codes?.includes('runtime_gateway_failure')) return 'runtime_gateway_failure';
  if (freshnessHours !== null && freshnessHours > 36) return 'stale_state';
  if (Math.abs(bankrollDiff || 0) > 5) return 'bankroll_integrity_failure';
  if (stateSyncGap) return 'state_sync_failure';
  if (latest?.message_type === 'BET') return 'bet_ready';
  return 'true_no_edge_sit';
}

function explainBlockedRun(runClassification, freshnessHours, bankrollDifference, stateSyncGap, ledgerValidation, postMortemStatus) {
  const reasons = [];
  if (ledgerValidation && !ledgerValidation.passed) {
    reasons.push(`Ledger validation failed: ${(ledgerValidation.failure_classes || []).join(', ')}`);
  }
  if (runClassification === 'post_mortem_required' && postMortemStatus?.latest_trigger) {
    reasons.push(`Post-mortem required after ${postMortemStatus.latest_trigger.trigger_type} (${postMortemStatus.latest_trigger.streak_value}).`);
  }
  if (runClassification === 'auth_failure') reasons.push('Odds API authentication failed in the latest runtime context.');
  if (runClassification === 'runtime_gateway_failure') reasons.push('The latest hunt did not complete cleanly in the OpenClaw runtime.');
  if (freshnessHours !== null && freshnessHours > 36) reasons.push('Latest validated canonical state is stale.');
  if (Math.abs(bankrollDifference || 0) > 5) reasons.push('Bankroll state does not reconcile cleanly.');
  if (stateSyncGap) reasons.push('Canonical state sync failed.');
  return reasons.join(' ');
}

function buildDecisionTexts(payload) {
  const verdict = payload.decision_payload_v1?.verdict || 'BLOCKED';
  const classification = payload.decision_payload_v1?.run_classification || 'unknown';
  const why = payload.decision_payload_v1?.why || 'No explanation available.';
  const health = payload.decision_payload_v1?.system_health || 'UNKNOWN';
  const terminal = [
    'TIEREDGE DECISION',
    `Verdict: ${verdict}`,
    `Run classification: ${classification}`,
    `Why: ${why}`,
    `System health: ${health}`,
  ].join('\n');
  const whatsapp = [
    `*TIEREDGE ${verdict}*`,
    `Classification: ${classification}`,
    `Why: ${why}`,
    `Health: ${health}`,
  ].join('\n');
  return { terminal, whatsapp, evening: terminal };
}

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function buildPendingBets({ executionLog, betGrades, reconciliationEvents }) {
  const settledStatuses = new Set(['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout']);
  const settledReconciliations = reconciliationEvents.filter((row) => settledStatuses.has(normalizeText(row.settlement_status || row.result)));
  const settledExecutionIds = new Set(
    settledReconciliations
      .map((row) => normalizeText(row.execution_log_id || row.execution_id))
      .filter(Boolean)
  );

  const settledRecIds = new Set(
    settledReconciliations
      .map((row) => normalizeText(row.rec_id))
      .filter(Boolean)
  );

  const settledSelections = new Set(
    betGrades
      .filter((row) => settledStatuses.has(normalizeText(row.result)))
      .map((row) => normalizeText(row.selection))
      .filter(Boolean)
  );

  return executionLog
    .filter((row) => row && typeof row === 'object')
    .filter((row) => {
      const executionId = normalizeText(row.execution_id || row.execution_log_id);
      const recId = normalizeText(row.rec_id);
      const selection = normalizeText(row.selection || row.bet || row.event);
      if (executionId && settledExecutionIds.has(executionId)) return false;
      if (recId && settledRecIds.has(recId)) return false;
      if (selection && settledSelections.has(selection)) return false;
      return true;
    })
    .map((row) => ({
      execution_id: row.execution_id || null,
      rec_id: row.rec_id || null,
      sport: row.sport || null,
      league: row.league || null,
      normalized_event: row.normalized_event || null,
      event: row.event || row.event_label || null,
      market: row.market || row.market_type || null,
      selection: row.selection || row.bet || null,
      sportsbook: row.actual_sportsbook || row.recommended_sportsbook || null,
      actual_odds: row.actual_odds || null,
      actual_stake: row.actual_stake || null,
      bet_slip_timestamp: row.bet_slip_timestamp || row.logged_at_utc || null,
      status: 'PENDING',
      execution_approval_result: row.execution_approval_result || null,
      manual_override_flag: Boolean(row.manual_override_flag),
      match_status: row.match_status || (row.rec_id ? 'matched_to_recommendation' : 'unmatched_manual_bet'),
      notes: row.override_reason || row.notes || null,
    }))
    .sort((a, b) => String(b.bet_slip_timestamp || '').localeCompare(String(a.bet_slip_timestamp || '')));
}

function inferBetClass(row) {
  const explicit = normalizeText(row.bet_class);
  if (explicit === 'fun_sgp') return 'FUN_SGP';
  if (explicit === 'edge_bet') return 'EDGE_BET';
  const market = normalizeText(row.market || row.market_type);
  const selection = normalizeText(row.selection || row.event);
  if (market.includes('sgp') || market.includes('parlay') || selection.includes('sgp') || selection.includes('parlay')) {
    return 'FUN_SGP';
  }
  if (row.rec_id || row.recommended_odds || row.recommended_stake) return 'EDGE_BET';
  return 'MANUAL_OTHER';
}

function inferSport(row, decisionIndex) {
  if (row.sport) return row.sport;
  const exactByRec = decisionIndex.byRecId.get(normalizeText(row.rec_id));
  if (exactByRec?.sport) return exactByRec.sport;
  const exactBySelection = decisionIndex.bySelection.get(normalizeText(row.selection || row.bet));
  if (exactBySelection?.sport) return exactBySelection.sport;
  return 'UNKNOWN';
}

function buildDecisionIndex(decisions) {
  return {
    byRecId: new Map(
      decisions
        .filter((row) => row.rec_id)
        .map((row) => [normalizeText(row.rec_id), row])
    ),
    bySelection: new Map(
      decisions
        .filter((row) => row.selection)
        .map((row) => [normalizeText(row.selection), row])
    ),
    bySelectionDate: new Map(
      decisions
        .filter((row) => row.selection && row.target_date)
        .map((row) => [`${row.target_date}::${normalizeText(row.selection)}`, row])
    ),
  };
}

function buildInvalidRunScope(huntAuditLog, decisions) {
  const invalidRuns = (huntAuditLog || []).filter((row) => String(row.invalid_status || '').toLowerCase().includes('invalid'));
  const invalidRunIds = Array.from(new Set(invalidRuns.map((row) => String(row.run_id || '').trim()).filter(Boolean)));
  const invalidRunIdSet = new Set(invalidRunIds);
  const excludedRows = (decisions || []).filter((row) => invalidRunIdSet.has(String(row.run_id || '').trim()));
  return {
    invalid_runs: invalidRuns,
    invalid_run_ids: invalidRunIds,
    invalid_run_id_set: invalidRunIdSet,
    excluded_rows: excludedRows,
    excluded_row_count: excludedRows.length,
  };
}

function buildExecutionIndex(executionLog) {
  return {
    byExecutionId: new Map(
      executionLog
        .filter((row) => row.execution_id)
        .map((row) => [normalizeText(row.execution_id), row])
    ),
    byRecId: new Map(
      executionLog
        .filter((row) => row.rec_id)
        .map((row) => [normalizeText(row.rec_id), row])
    ),
    bySelectionDate: new Map(
      executionLog
        .filter((row) => row.selection && row.recommendation_timestamp)
        .map((row) => {
          const date = String(row.recommendation_timestamp).match(/(\d{4}-\d{2}-\d{2})/)?.[1] || String(row.bet_slip_timestamp || '').match(/(\d{4}-\d{2}-\d{2})/)?.[1] || null;
          return date ? [`${date}::${normalizeText(row.selection)}`, row] : null;
        })
        .filter(Boolean)
    ),
  };
}

function sampleSizeStatus(count) {
  if (!Number.isFinite(count) || count <= 0) return 'insufficient_sample';
  if (count < 10) return 'insufficient_sample';
  if (count < 30) return 'low_sample';
  if (count < 75) return 'moderate_sample';
  return 'meaningful_sample';
}

function sampleSizeNote(count, label = 'sample') {
  const status = sampleSizeStatus(count);
  if (status === 'insufficient_sample') return `Very small ${label}. Treat this as directional only.`;
  if (status === 'low_sample') return `Small ${label}. Early results can still swing materially.`;
  if (status === 'moderate_sample') return `Moderate ${label}. Trends are more useful, but still not settled truth.`;
  return `Larger ${label}. Conclusions are more stable, though variance still matters.`;
}

function americanToDecimal(odds) {
  const value = parseNumber(odds);
  if (!Number.isFinite(value) || value === 0) return null;
  return value > 0 ? 1 + (value / 100) : 1 + (100 / Math.abs(value));
}

function americanToImpliedProb(odds) {
  const decimal = americanToDecimal(odds);
  return Number.isFinite(decimal) ? 1 / decimal : null;
}

function wilsonIntervalLabel(successes, total) {
  if (!Number.isFinite(successes) || !Number.isFinite(total) || total <= 0) return null;
  const z = 1.96;
  const phat = successes / total;
  const denominator = 1 + ((z * z) / total);
  const centre = phat + ((z * z) / (2 * total));
  const margin = z * Math.sqrt(((phat * (1 - phat)) / total) + ((z * z) / (4 * total * total)));
  const lower = ((centre - margin) / denominator) * 100;
  const upper = ((centre + margin) / denominator) * 100;
  return `${round2(lower)}% to ${round2(upper)}%`;
}

function buildOpenRiskSummary({ pendingBets, bankroll, decisionIndex }) {
  const normalizedPending = pendingBets.map((row) => {
    const stake = round2(parseNumber(row.actual_stake) || 0) || 0;
    const betClass = inferBetClass(row);
    const sport = inferSport(row, decisionIndex);
    return {
      ...row,
      stake,
      bet_class: betClass,
      sport,
    };
  });

  const totalStakeAtRisk = round2(normalizedPending.reduce((sum, row) => sum + row.stake, 0)) || 0;
  const exposurePct = bankroll > 0 ? round2((totalStakeAtRisk / bankroll) * 100) : null;
  const byClass = ['EDGE_BET', 'FUN_SGP', 'MANUAL_OTHER'].map((betClass) => {
    const rows = normalizedPending.filter((row) => row.bet_class === betClass);
    const stake = round2(rows.reduce((sum, row) => sum + row.stake, 0)) || 0;
    return {
      bet_class: betClass,
      pending_ticket_count: rows.length,
      total_stake_at_risk: stake,
      exposure_pct_of_bankroll: bankroll > 0 ? round2((stake / bankroll) * 100) : null,
    };
  });

  const bySportMap = new Map();
  normalizedPending.forEach((row) => {
    const key = row.sport || 'UNKNOWN';
    const existing = bySportMap.get(key) || { sport: key, pending_ticket_count: 0, total_stake_at_risk: 0 };
    existing.pending_ticket_count += 1;
    existing.total_stake_at_risk = round2(existing.total_stake_at_risk + row.stake) || 0;
    bySportMap.set(key, existing);
  });
  const bySport = Array.from(bySportMap.values()).map((row) => ({
    ...row,
    exposure_pct_of_bankroll: bankroll > 0 ? round2((row.total_stake_at_risk / bankroll) * 100) : null,
  })).sort((a, b) => b.total_stake_at_risk - a.total_stake_at_risk);

  const manualOverrideStake = round2(
    normalizedPending
      .filter((row) => row.manual_override_flag)
      .reduce((sum, row) => sum + row.stake, 0)
  ) || 0;

  return {
    pending_ticket_count: normalizedPending.length,
    total_stake_at_risk: formatMoney(totalStakeAtRisk),
    total_stake_at_risk_value: totalStakeAtRisk,
    open_exposure_pct_of_bankroll: formatPercent(exposurePct),
    open_exposure_pct_of_bankroll_value: exposurePct,
    manual_override_ticket_count: normalizedPending.filter((row) => row.manual_override_flag).length,
    manual_override_stake_at_risk: formatMoney(manualOverrideStake),
    manual_override_stake_at_risk_value: manualOverrideStake,
    by_bet_class: byClass.map((row) => ({
      ...row,
      total_stake_at_risk: formatMoney(row.total_stake_at_risk),
      open_exposure_pct_of_bankroll: formatPercent(row.exposure_pct_of_bankroll),
    })),
    by_sport: bySport.map((row) => ({
      ...row,
      total_stake_at_risk: formatMoney(row.total_stake_at_risk),
      open_exposure_pct_of_bankroll: formatPercent(row.exposure_pct_of_bankroll),
    })),
  };
}

function buildPlacementSnapshotSummary(executionLog) {
  const rows = executionLog.filter((row) => row && typeof row === 'object');
  const coveredStatuses = new Set(['exact_snapshot_captured', 'proxy_snapshot_captured', 'screenshot_only_snapshot']);
  const missing = rows.filter((row) => !coveredStatuses.has(normalizeText(row.placement_snapshot_status)));
  const covered = rows.filter((row) => coveredStatuses.has(normalizeText(row.placement_snapshot_status)));
  const coveragePct = rows.length ? round2((covered.length / rows.length) * 100) : null;
  return {
    total_execution_rows: rows.length,
    snapshot_anchored_count: covered.length,
    snapshot_missing_count: missing.length,
    snapshot_coverage_pct: coveragePct,
    snapshot_coverage_pct_label: formatPercent(coveragePct),
    recent_bets_missing_snapshot: missing
      .slice()
      .reverse()
      .slice(0, 10)
      .map((row) => ({
        execution_id: row.execution_id || null,
        selection: row.selection || row.event || null,
        sportsbook: row.actual_sportsbook || row.recommended_sportsbook || null,
        actual_odds: row.actual_odds || null,
        actual_stake: row.actual_stake || null,
        placement_snapshot_status: row.placement_snapshot_status || 'snapshot_missing',
        placement_snapshot_warning: row.placement_snapshot_warning || null,
      })),
  };
}

function buildClvCoverageSummary(betGrades) {
  const finalStatuses = new Set(['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout', 'cash out', 'cashed out']);
  const settled = betGrades.filter((row) => finalStatuses.has(normalizeText(row.result || row.settlement_status)));
  const coveredStatuses = new Set(['exact_close_found', 'proxy_close_found']);
  const covered = settled.filter((row) => coveredStatuses.has(normalizeText(row.clv_status)));
  const missing = settled.filter((row) => !coveredStatuses.has(normalizeText(row.clv_status)));
  const coveragePct = settled.length ? round2((covered.length / settled.length) * 100) : null;
  return {
    settled_bet_count: settled.length,
    clv_anchored_count: covered.length,
    clv_missing_count: missing.length,
    clv_coverage_pct: coveragePct,
    clv_coverage_pct_label: formatPercent(coveragePct),
    recent_settled_bets_missing_clv: missing
      .slice()
      .reverse()
      .slice(0, 10)
      .map((row) => ({
        grading_id: row.grading_id || null,
        selection: row.selection || null,
        result: row.result || null,
        clv_status: row.clv_status || 'missing_clv_source',
        clv_warning: row.clv_warning || null,
      })),
  };
}

function buildSettledPerformanceSummary(rows) {
  const settled = rows.filter((row) => row && row.grading_type === 'BET');
  const winCount = settled.filter((row) => normalizeText(row.result) === 'win').length;
  const lossCount = settled.filter((row) => normalizeText(row.result) === 'loss').length;
  const pushCount = settled.filter((row) => normalizeText(row.result) === 'push').length;
  const voidCount = settled.filter((row) => normalizeText(row.result) === 'void').length;
  const cashoutCount = settled.filter((row) => ['cashed_out', 'partial_cashout', 'cash out', 'cashed out'].includes(normalizeText(row.result))).length;
  const totalStake = round2(settled.reduce((sum, row) => sum + (parseNumber(row.stake) || 0), 0)) || 0;
  const realizedProfit = round2(settled.reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0;
  const roiPct = totalStake > 0 ? round2((realizedProfit / totalStake) * 100) : null;
  const winRateBase = winCount + lossCount;
  const winRatePct = winRateBase > 0 ? round2((winCount / winRateBase) * 100) : null;
  return {
    settled_bet_count: settled.length,
    win_count: winCount,
    loss_count: lossCount,
    push_count: pushCount,
    void_count: voidCount,
    cashout_count: cashoutCount,
    total_stake: formatMoney(totalStake),
    total_stake_value: totalStake,
    realized_profit: formatMoney(realizedProfit),
    realized_profit_value: realizedProfit,
    roi_pct: roiPct,
    roi_pct_label: formatPercent(roiPct),
    win_rate_pct: winRatePct,
    win_rate_pct_label: formatPercent(winRatePct),
    sample_size_status: sampleSizeStatus(settled.length),
    reliability_note: sampleSizeNote(settled.length, 'settled-bet sample'),
    unit_metrics_status: 'insufficient_data',
    unit_metrics_reason: 'No canonical unit size is stored in the ledgers, so unit-based performance is hidden.',
  };
}

function buildClvAnalyticsSummary(rows) {
  const coveredStatuses = new Set(['exact_close_found', 'proxy_close_found']);
  const settled = rows.filter((row) => row && row.grading_type === 'BET');
  const anchored = settled.filter((row) => coveredStatuses.has(normalizeText(row.clv_status)));
  const clvPriceValues = anchored.map((row) => parseNumber(row.clv_price_delta)).filter((value) => value !== null);
  const clvProbValues = anchored.map((row) => parseNumber(row.clv_prob_delta)).filter((value) => value !== null);
  const positiveCount = clvPriceValues.filter((value) => value > 0).length;
  const averagePriceDelta = clvPriceValues.length ? round2(clvPriceValues.reduce((sum, value) => sum + value, 0) / clvPriceValues.length) : null;
  const averageProbDelta = clvProbValues.length ? round2(clvProbValues.reduce((sum, value) => sum + value, 0) / clvProbValues.length) : null;
  const positiveRate = clvPriceValues.length ? round2((positiveCount / clvPriceValues.length) * 100) : null;
  const coveragePct = settled.length ? round2((anchored.length / settled.length) * 100) : null;
  return {
    status: 'computed',
    eligible_settled_bet_count: settled.length,
    anchored_bet_count: anchored.length,
    missing_clv_bet_count: settled.length - anchored.length,
    coverage_pct: coveragePct,
    coverage_pct_label: formatPercent(coveragePct),
    coverage_status: coveragePct === null ? 'insufficient_data' : (coveragePct < 50 ? 'low_coverage' : (coveragePct < 80 ? 'partial_coverage' : 'strong_coverage')),
    sample_size_status: sampleSizeStatus(anchored.length),
    average_clv_price_delta: averagePriceDelta,
    average_clv_price_delta_label: averagePriceDelta === null ? 'Insufficient data' : `${averagePriceDelta}%`,
    positive_clv_rate: positiveRate,
    positive_clv_rate_label: formatPercent(positiveRate),
    average_clv_prob_delta: averageProbDelta,
    average_clv_prob_delta_label: averageProbDelta === null ? 'Insufficient data' : `${averageProbDelta}%`,
    source_warning: anchored.length === 0
      ? 'No settled bets currently have anchored closing-line data.'
      : (coveragePct !== null && coveragePct < 50 ? 'CLV coverage is still too low to trust strong conclusions.' : null),
    sample_note: sampleSizeNote(anchored.length, 'CLV-anchor sample'),
  };
}

function parseLineDriftCents(value) {
  const match = String(value || '').match(/(-?\d+(?:\.\d+)?)\s*cents/i);
  return match ? Number(match[1]) : null;
}

function buildExecutionQualitySummary(executionLog, clvCoverageSummary) {
  const rows = executionLog.filter((row) => row && typeof row === 'object');
  const matchedCount = rows.filter((row) => normalizeText(row.match_status) === 'matched_to_recommendation').length;
  const matchedRate = rows.length ? round2((matchedCount / rows.length) * 100) : null;
  const snapshotCount = rows.filter((row) => ['exact_snapshot_captured', 'proxy_snapshot_captured', 'screenshot_only_snapshot'].includes(normalizeText(row.placement_snapshot_status))).length;
  const sameBookCount = rows.filter((row) => row.placement_same_book_quote && row.actual_sportsbook).length;
  const drifts = rows.map((row) => parseLineDriftCents(row.line_price_drift)).filter((value) => value !== null);
  const avgDrift = drifts.length ? round2(drifts.reduce((sum, value) => sum + value, 0) / drifts.length) : null;
  const avgAbsDrift = drifts.length ? round2(drifts.reduce((sum, value) => sum + Math.abs(value), 0) / drifts.length) : null;
  return {
    status: 'computed',
    execution_row_count: rows.length,
    matched_to_recommendation_count: matchedCount,
    matched_to_recommendation_rate: matchedRate,
    matched_to_recommendation_rate_label: formatPercent(matchedRate),
    snapshot_coverage_pct: rows.length ? round2((snapshotCount / rows.length) * 100) : null,
    snapshot_coverage_pct_label: formatPercent(rows.length ? round2((snapshotCount / rows.length) * 100) : null),
    same_book_quote_coverage_pct: rows.length ? round2((sameBookCount / rows.length) * 100) : null,
    same_book_quote_coverage_pct_label: formatPercent(rows.length ? round2((sameBookCount / rows.length) * 100) : null),
    average_price_drift_cents: avgDrift,
    average_price_drift_cents_label: avgDrift === null ? 'Insufficient data' : `${avgDrift} cents`,
    average_absolute_price_drift_cents: avgAbsDrift,
    average_absolute_price_drift_cents_label: avgAbsDrift === null ? 'Insufficient data' : `${avgAbsDrift} cents`,
    clv_coverage_pct_label: clvCoverageSummary.clv_coverage_pct_label,
    sample_size_status: sampleSizeStatus(rows.length),
    sample_note: sampleSizeNote(rows.length, 'execution sample'),
  };
}

function buildSettledEdgeValidationRows({ betGrades, decisionIndex, executionIndex }) {
  return betGrades
    .filter((row) => row && row.grading_type === 'BET')
    .map((row) => {
      const recIdKey = normalizeText(row.rec_id);
      const selectionKey = normalizeText(row.selection);
      const selectionDateKey = row.date && row.selection ? `${row.date}::${selectionKey}` : null;
      const decision = decisionIndex.byRecId.get(recIdKey)
        || (selectionDateKey ? decisionIndex.bySelectionDate.get(selectionDateKey) : null)
        || decisionIndex.bySelection.get(selectionKey)
        || null;
      const execution = executionIndex.byExecutionId.get(normalizeText(row.execution_log_id))
        || executionIndex.byRecId.get(recIdKey)
        || (selectionDateKey ? executionIndex.bySelectionDate.get(selectionDateKey) : null)
        || null;

      const trueProb = normalizeProbability(execution?.true_probability_at_bet ?? decision?.post_conf_true_prob);
      const impliedProb = normalizeProbability(execution?.implied_probability_at_bet ?? decision?.devig_implied_prob);
      const edgePct = parseNumber(execution?.edge_pct_at_bet ?? decision?.post_conf_edge_pct);
      const recommendedOdds = execution?.recommended_odds_at_bet ?? execution?.recommended_odds ?? decision?.odds_american ?? null;
      const actualOdds = execution?.actual_odds ?? null;
      const oddsForExpected = actualOdds ?? recommendedOdds;
      const oddsSource = actualOdds ? 'execution' : (recommendedOdds ? 'recommended_proxy' : 'missing');
      const stakeValue = parseNumber(row.stake);
      const decimalOdds = americanToDecimal(oddsForExpected);
      const expectedProfit = Number.isFinite(trueProb) && Number.isFinite(stakeValue) && Number.isFinite(decimalOdds)
        ? round2((trueProb * stakeValue * (decimalOdds - 1)) - ((1 - trueProb) * stakeValue))
        : null;
      const breakevenProb = americanToImpliedProb(oddsForExpected);
      const evCaptured = [trueProb, impliedProb, edgePct].every((value) => Number.isFinite(value));

      return {
        grading_id: row.grading_id || null,
        execution_id: row.execution_log_id || execution?.execution_id || null,
        rec_id: row.rec_id || execution?.rec_id || decision?.rec_id || null,
        date: row.date || null,
        selection: row.selection || null,
        result: row.result || null,
        stake: stakeValue,
        profit_loss: parseNumber(row.profit_loss),
        bet_class: execution?.bet_class || decision?.bet_class || row.bet_class || null,
        market: execution?.market || execution?.market_type || decision?.market_type || null,
        sport: execution?.sport || decision?.sport || null,
        league: execution?.league || decision?.league || decision?.sport || null,
        execution_timestamp: execution?.bet_slip_timestamp || execution?.recommendation_timestamp || row.timestamp_ct || null,
        true_probability_at_bet: Number.isFinite(trueProb) ? round2(trueProb) : null,
        implied_probability_at_bet: Number.isFinite(impliedProb) ? round2(impliedProb) : null,
        edge_pct_at_bet: Number.isFinite(edgePct) ? round2(edgePct) : null,
        recommended_odds_at_bet: recommendedOdds,
        actual_odds_taken: actualOdds,
        odds_for_expected_calc: oddsForExpected,
        odds_source: oddsSource,
        breakeven_probability: Number.isFinite(breakevenProb) ? round2(breakevenProb) : null,
        ev_at_bet_status: evCaptured ? 'captured' : 'missing_recommendation_ev',
        expected_profit: expectedProfit,
        clv_status: row.clv_status || null,
      };
    });
}

function buildExpectationSummary(settledValidationRows) {
  const eligible = settledValidationRows.filter((row) => row.ev_at_bet_status === 'captured' && Number.isFinite(row.expected_profit));
  if (!eligible.length) {
    return {
      status: 'insufficient_data',
      reason: 'No settled bets currently have both canonical EV-at-bet fields and a usable placement price source.',
    };
  }
  const expectedProfit = round2(eligible.reduce((sum, row) => sum + row.expected_profit, 0)) || 0;
  const realizedProfit = round2(eligible.reduce((sum, row) => sum + (row.profit_loss || 0), 0)) || 0;
  const divergence = round2(realizedProfit - expectedProfit) || 0;
  return {
    status: 'computed',
    eligible_bet_count: eligible.length,
    expected_profit: formatMoney(expectedProfit),
    expected_profit_value: expectedProfit,
    realized_profit: formatMoney(realizedProfit),
    realized_profit_value: realizedProfit,
    divergence_from_expected: formatMoney(divergence),
    divergence_from_expected_value: divergence,
    sample_size_status: sampleSizeStatus(eligible.length),
    note: eligible.length < 10
      ? 'Expected vs actual is based on a very small EV-backed sample. Variance likely dominates.'
      : 'Expected vs actual is computed only where EV-at-bet history exists.',
  };
}

function buildEdgeValidationSummary(settledValidationRows, clvCoverageSummary, expectationSummary) {
  const settledCount = settledValidationRows.length;
  const evCovered = settledValidationRows.filter((row) => row.ev_at_bet_status === 'captured');
  const evCoveragePct = settledCount ? round2((evCovered.length / settledCount) * 100) : null;
  const averageEdge = evCovered.length
    ? round2(evCovered.reduce((sum, row) => sum + (row.edge_pct_at_bet || 0), 0) / evCovered.length)
    : null;
  const winLossRows = settledValidationRows.filter((row) => ['win', 'loss'].includes(normalizeText(row.result)));
  const wins = winLossRows.filter((row) => normalizeText(row.result) === 'win').length;
  const observedWinRate = winLossRows.length ? round2((wins / winLossRows.length) * 100) : null;
  const breakevenRows = winLossRows.filter((row) => Number.isFinite(row.breakeven_probability));
  const breakevenRate = breakevenRows.length
    ? round2((breakevenRows.reduce((sum, row) => sum + row.breakeven_probability, 0) / breakevenRows.length) * 100)
    : null;
  const varianceContext = (() => {
    if (!winLossRows.length) return 'No settled win/loss sample yet.';
    if (winLossRows.length < 10) return 'Small sample. Results can still be driven heavily by variance.';
    if (expectationSummary.status !== 'computed') return 'Settled sample exists, but EV history is too incomplete for expected-vs-actual comparison.';
    const diff = expectationSummary.divergence_from_expected_value || 0;
    if (diff > 0) return 'Results are above EV-backed expectation so far, but variance still matters.';
    if (diff < 0) return 'Results are below EV-backed expectation so far, but the edge is not yet falsified by this sample.';
    return 'Results are close to EV-backed expectation so far.';
  })();
  return {
    status: settledCount ? 'computed' : 'insufficient_data',
    settled_bet_sample_size: settledCount,
    settled_sample_status: sampleSizeStatus(settledCount),
    settled_sample_note: sampleSizeNote(settledCount, 'settled-bet sample'),
    ev_covered_bet_count: evCovered.length,
    ev_missing_bet_count: settledCount - evCovered.length,
    ev_coverage_pct: evCoveragePct,
    ev_coverage_pct_label: formatPercent(evCoveragePct),
    average_edge_at_bet_pct: averageEdge,
    average_edge_at_bet_pct_label: averageEdge === null ? 'Insufficient data' : `${averageEdge}%`,
    observed_win_rate_pct: observedWinRate,
    observed_win_rate_pct_label: formatPercent(observedWinRate),
    breakeven_win_rate_pct: breakevenRate,
    breakeven_win_rate_pct_label: formatPercent(breakevenRate),
    win_rate_interval_95_label: wilsonIntervalLabel(wins, winLossRows.length) || 'Insufficient data',
    variance_context: varianceContext,
    reliability_label: sampleSizeStatus(Math.min(settledCount, evCovered.length || 0)),
    unresolved_missing_data_count: (settledCount - evCovered.length) + (clvCoverageSummary.clv_missing_count || 0),
    recent_missing_ev_bets: settledValidationRows
      .filter((row) => row.ev_at_bet_status !== 'captured')
      .slice()
      .reverse()
      .slice(0, 10)
      .map((row) => ({
        grading_id: row.grading_id,
        selection: row.selection,
        result: row.result,
        ev_at_bet_status: row.ev_at_bet_status,
      })),
    recent_missing_clv_bets: clvCoverageSummary.recent_settled_bets_missing_clv || [],
  };
}

function main() {
  const decisions = readJsonl(CORE_PATHS.decisionLedger);
  const grading = readJsonl(CORE_PATHS.gradingLedger);
  const bankrollEntries = readJsonl(CORE_PATHS.bankrollLedger);
  const runtimeStatus = readJson(CORE_PATHS.runtimeStatus, {});
  const executionBoard = readJson(EXECUTION_BOARD_PATH, {
    counts: { candidates: 0, approved: 0, rejected: 0 },
    recommendations: [],
    operator_summary: [],
  });
  const executionLog = readExecutionLog();
  const overrideLog = readOverrideLog();
  const huntAuditLog = readJsonl(HUNT_AUDIT_LOG_PATH);
  const ledgerValidation = validateLedgerInvariants({ requireOutputMatch: false });
  const generatedAtUtc = new Date().toISOString();
  const invalidRunScope = buildInvalidRunScope(huntAuditLog, decisions);
  const validLearningDecisions = decisions.filter((row) => !invalidRunScope.invalid_run_id_set.has(String(row.run_id || '').trim()));
  const decisionIndex = buildDecisionIndex(validLearningDecisions);
  const rawDecisionIndex = buildDecisionIndex(decisions);
  const executionIndex = buildExecutionIndex(executionLog);

  const betDecisions = validLearningDecisions.filter((row) => row.decision_kind === 'BET');
  const passBand = validLearningDecisions.filter((row) => row.decision_kind === 'PASS');
  const suppressed = validLearningDecisions.filter((row) => row.decision_kind === 'SUPPRESSED');
  const settledBetRows = grading.filter((row) => ['BET', 'RECONCILIATION'].includes(String(row.grading_type || '').toUpperCase()));
  const betGrades = settledBetRows;
  const passGrades = grading.filter((row) => row.grading_type === 'PASS');
  const reconciliationEvents = grading.filter((row) => row.grading_type === 'RECONCILIATION');

  const startingBankroll = bankrollEntries
    .filter((row) => row.entry_type === 'STARTING_BANKROLL')
    .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0);
  const contributions = bankrollEntries
    .filter((row) => row.entry_type === 'CONTRIBUTION')
    .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0);
  const realizedProfit = round2(settledBetRows.reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0;
  const actualBankroll = round2(startingBankroll + contributions + realizedProfit) || 0;
  const latestBankrollGrade = getLatestBankrollAnnotatedGrade(settledBetRows);
  const lastRecordedBankroll = round2(parseNumber(latestBankrollGrade?.bankroll_after)) || actualBankroll;
  const bankrollDifference = round2(lastRecordedBankroll - actualBankroll) || 0;
  const latestRuntime = runtimeStatus.latest_hunt_current || runtimeStatus.latest_successful_hunt || null;
  const payloadBuildMs = Date.now();
  const freshnessHours = 0;
  const stateSyncGap = Boolean(runtimeStatus?.state_sync?.blocking_sync_gap);
  const postMortemStatus = getPostMortemStatus(grading, readPostMortemLog());
  const invalidHuntRuns = huntAuditLog
    .filter((row) => String(row.invalid_status || '').toLowerCase().includes('invalid'))
    .slice()
    .reverse();
  const latestInvalidHunt = invalidHuntRuns[0] || null;
  const weeklyTruth = buildWeeklyTruthReport().report;
  const now = new Date();
  const monthlyOverrideCount = overrideLog.filter((row) => {
    const ts = Date.parse(String(row.timestamp_utc || ''));
    if (!Number.isFinite(ts)) return false;
    const then = new Date(ts);
    return then.getUTCFullYear() === now.getUTCFullYear() && then.getUTCMonth() === now.getUTCMonth();
  }).length;
  const runClassification = parseRunClassification({ runtimeStatus, freshnessHours, bankrollDiff: bankrollDifference, stateSyncGap, ledgerValidation, postMortemStatus });
  const verdict = runClassification === 'bet_ready' ? 'BET' : (runClassification === 'true_no_edge_sit' ? 'SIT' : 'BLOCKED');

  const wins = settledBetRows.filter((row) => String(row.result || '').toUpperCase() === 'WIN').length;
  const losses = settledBetRows.filter((row) => String(row.result || '').toUpperCase() === 'LOSS').length;
  const totalGraded = settledBetRows.filter((row) => ['WIN', 'LOSS'].includes(String(row.result || '').toUpperCase())).length;
  const totalStake = round2(settledBetRows.reduce((sum, row) => sum + (parseNumber(row.stake) || 0), 0)) || 0;
  const roi = totalStake > 0 ? round2((realizedProfit / totalStake) * 100) : null;
  const avgClv = (() => {
    const values = settledBetRows.map((row) => parsePercentString(row.clv)).filter((value) => value !== null);
    return values.length ? round2(values.reduce((sum, value) => sum + value, 0) / values.length) : null;
  })();

  const latestDate = latestRuntime?.date_key || settledBetRows.at(-1)?.date || null;
  const todaysBets = settledBetRows.filter((row) => row.date === latestDate).map((row) => ({
    'Timestamp (CT)': row.timestamp_ct,
    Sport: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.sport || '',
    Market: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.market_type || '',
    Bet: row.selection,
    'Odds (US)': decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.odds_american || '',
    'Odds (Dec)': decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.odds_decimal || '',
    Book: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.sportsbook || '',
    Stake: formatMoney(parseNumber(row.stake) || 0),
    Tier: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.bet_class === 'FUN_SGP' ? 'FUN' : 'T3',
    Result: row.result,
    'P/L': formatMoney(parseNumber(row.profit_loss) || 0),
    CLV: row.clv || 'N/A',
    bet_class: row.bet_class,
  }));

  const passGradeResolved = passGrades.filter((row) => String(row.result || '').toLowerCase() !== 'ungraded');
  const passWins = passGradeResolved.filter((row) => String(row.result || '').toLowerCase() === 'win').length;
  const passLosses = passGradeResolved.filter((row) => String(row.result || '').toLowerCase() === 'loss').length;
  const passNet = round2(passGradeResolved.reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0;
  const pendingBets = buildPendingBets({ executionLog, betGrades: settledBetRows, reconciliationEvents });
  const openRiskSummary = buildOpenRiskSummary({ pendingBets, bankroll: lastRecordedBankroll, decisionIndex: rawDecisionIndex });
  const placementSnapshotSummary = buildPlacementSnapshotSummary(executionLog);
  const clvCoverageSummary = buildClvCoverageSummary(settledBetRows);
  const settledValidationRows = buildSettledEdgeValidationRows({ betGrades: settledBetRows, decisionIndex, executionIndex });
  const settledPerformanceOverall = buildSettledPerformanceSummary(settledBetRows);
  const settledPerformanceCore = buildSettledPerformanceSummary(settledBetRows.filter((row) => row.bet_class === 'EDGE_BET'));
  const settledPerformanceFun = buildSettledPerformanceSummary(settledBetRows.filter((row) => row.bet_class === 'FUN_SGP'));
  const clvAnalytics = buildClvAnalyticsSummary(settledBetRows);
  const executionQualitySummary = buildExecutionQualitySummary(executionLog, clvCoverageSummary);
  const expectationSummary = buildExpectationSummary(settledValidationRows);
  const edgeValidationSummary = buildEdgeValidationSummary(settledValidationRows, clvCoverageSummary, expectationSummary);
  const latestModelExposure = parseDailyExposure(latestRuntime?.summary);

  const decisionPayload = {
    verdict,
    run_classification: runClassification,
    why: runClassification === 'bet_ready'
      ? 'Latest scheduled hunt produced actionable bets.'
      : runClassification === 'true_no_edge_sit'
        ? 'Latest scheduled hunt found no qualifying edges.'
        : `Integrity gate failed: ${explainBlockedRun(runClassification, freshnessHours, bankrollDifference, stateSyncGap, ledgerValidation, postMortemStatus)}`,
    system_health: verdict === 'BLOCKED' ? 'FAIL' : 'PASS',
  };

  const renderers = buildDecisionTexts({ decision_payload_v1: decisionPayload });

  const payload = {
    schema: 'tierededge_canonical_v2',
    generated_at_utc: generatedAtUtc,
    last_updated_ct: formatCtTimestamp(payloadBuildMs),
    current_status: {
      Bankroll: formatMoney(lastRecordedBankroll),
      Phase: parsePhase(lastRecordedBankroll),
      'Open Exposure Used': openRiskSummary.open_exposure_pct_of_bankroll,
      'Open Tickets': openRiskSummary.pending_ticket_count,
      'Snapshot Coverage': placementSnapshotSummary.snapshot_coverage_pct_label,
      'CLV Coverage': clvCoverageSummary.clv_coverage_pct_label,
      'Override Count (month)': monthlyOverrideCount,
      'Post-Mortem Status': postMortemStatus.current_status,
      'Invalid Hunt Runs': invalidHuntRuns.length,
      'Model Daily Exposure (latest hunt)': latestModelExposure,
      'Circuit Breaker': 'OFF',
    },
    lifetime_stats: {
      'Total Bets': settledBetRows.length,
      'Win Rate': totalGraded ? `${round2((wins / totalGraded) * 100)}% (${wins}-${losses})` : 'N/A',
      'Overall ROI': roi === null ? 'N/A' : `${roi}%`,
      'Average CLV': avgClv === null ? 'N/A' : `${avgClv}%`,
    },
    bet_log: settledBetRows.slice().reverse().map((row) => ({
      Date: row.date,
      'Timestamp (CT)': row.timestamp_ct,
      Sport: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.sport || '',
      Market: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.market_type || '',
      Bet: row.selection,
      'Odds (US)': decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.odds_american || '',
      'Odds (Dec)': decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.odds_decimal || '',
      Book: decisions.find((entry) => entry.selection === row.selection && entry.target_date === row.date)?.sportsbook || '',
      Stake: formatMoney(parseNumber(row.stake) || 0),
      Tier: row.bet_class === 'FUN_SGP' ? 'FUN' : 'T3',
      Result: row.result,
      'P/L': formatMoney(parseNumber(row.profit_loss) || 0),
      CLV: row.clv || 'N/A',
      'CLV Status': row.clv_status || 'missing_clv_source',
      'Closing Odds': row.closing_odds || 'N/A',
      Bankroll: formatMoney(parseNumber(row.bankroll_after) || 0),
      bet_class: row.bet_class,
    })),
    todays_bets: todaysBets,
    pending_count: pendingBets.length,
    pending_bets: pendingBets,
    open_risk_summary: openRiskSummary,
    market_truth_summary: {
      placement_snapshot: placementSnapshotSummary,
      clv_anchor: clvCoverageSummary,
    },
    behavioral_accountability: {
      overrides: {
        monthly_override_count: monthlyOverrideCount,
        blocked_run_override_count: overrideLog.filter((row) => row.during_blocked_run).length,
        off_model_override_count: overrideLog.filter((row) => row.off_model).length,
        recent_overrides: overrideLog.slice().reverse().slice(0, 25),
      },
      post_mortem: postMortemStatus,
      weekly_truth_report_summary: {
        generated_at_utc: weeklyTruth.generated_at_utc,
        settled_bet_count: weeklyTruth.settled_bet_count,
        top_bleeding_categories: weeklyTruth.top_bleeding_categories,
        top_strongest_categories: weeklyTruth.top_strongest_categories,
        override_totals: weeklyTruth.override_totals,
        missing_coverage: weeklyTruth.missing_coverage,
      },
    },
    recommendation_learning_scope: {
      excluded_invalid_run_count: invalidRunScope.invalid_run_ids.length,
      excluded_invalid_row_count: invalidRunScope.excluded_row_count,
      excluded_run_ids: invalidRunScope.invalid_run_ids,
      recent_excluded_rows: invalidRunScope.excluded_rows.slice().reverse().slice(0, 10),
    },
    hunt_audit_summary: {
      invalid_run_count: invalidHuntRuns.length,
      latest_invalid_run: latestInvalidHunt,
      recent_invalid_runs: invalidHuntRuns.slice(0, 10),
    },
    pass_band: passBand,
    raw_pass_band_history: decisions.filter((row) => row.decision_kind === 'PASS').slice().reverse().slice(0, 25),
    suppressed_candidates: suppressed,
    raw_suppressed_history: decisions.filter((row) => row.decision_kind === 'SUPPRESSED').slice().reverse().slice(0, 25),
    passed_opportunity_tracker: {
      total_count: passBand.length,
      graded_count: passGradeResolved.length,
      ungraded_count: passGrades.filter((row) => String(row.result || '').toLowerCase() === 'ungraded').length,
      record_if_bet: `${passWins}-${passLosses}`,
      net_counterfactual_pl_if_bet: passNet,
    },
    daily_rejection_summary: {
      'Total Markets Checked': validLearningDecisions.filter((row) => row.target_date === latestDate).length,
      'Total Rejected': validLearningDecisions.filter((row) => row.target_date === latestDate && row.final_decision === 'SIT').length,
      'no_edge': validLearningDecisions.filter((row) => row.target_date === latestDate && row.rejection_reason === 'no_edge').length,
      'low_confidence': validLearningDecisions.filter((row) => row.target_date === latestDate && row.rejection_reason === 'low_confidence').length,
      'Plays Recommended': validLearningDecisions.filter((row) => row.target_date === latestDate && row.final_decision === 'BET').length,
      'Excluded Invalid Rows': invalidRunScope.excluded_rows.filter((row) => row.target_date === latestDate).length,
    },
    overall_betting_results: {
      count: settledPerformanceOverall.settled_bet_count,
      profit_loss: settledPerformanceOverall.realized_profit,
      total_staked: settledPerformanceOverall.total_stake,
      roi: settledPerformanceOverall.roi_pct_label,
      win_rate: settledPerformanceOverall.win_rate_pct_label,
      units_status: settledPerformanceOverall.unit_metrics_status,
    },
    core_strategy_results: {
      count: settledPerformanceCore.settled_bet_count,
      profit_loss: settledPerformanceCore.realized_profit,
      total_staked: settledPerformanceCore.total_stake,
      roi: settledPerformanceCore.roi_pct_label,
      win_rate: settledPerformanceCore.win_rate_pct_label,
      units_status: settledPerformanceCore.unit_metrics_status,
    },
    fun_sgp_results: {
      count: settledPerformanceFun.settled_bet_count,
      profit_loss: settledPerformanceFun.realized_profit,
      total_staked: settledPerformanceFun.total_stake,
      roi: settledPerformanceFun.roi_pct_label,
      win_rate: settledPerformanceFun.win_rate_pct_label,
      units_status: settledPerformanceFun.unit_metrics_status,
    },
    analytics_summary: {
      settled_performance: {
        overall: settledPerformanceOverall,
        edge_bet: settledPerformanceCore,
        fun_sgp: settledPerformanceFun,
      },
      clv_analytics: clvAnalytics,
      execution_quality: executionQualitySummary,
      performance_vs_expectation: expectationSummary,
    },
    edge_validation: {
      summary: edgeValidationSummary,
      clv_coverage: {
        ...clvCoverageSummary,
        coverage_status: clvAnalytics.coverage_status,
        sample_size_status: clvAnalytics.sample_size_status,
        coverage_warning: clvAnalytics.source_warning,
      },
      actual_vs_expected: expectationSummary,
      settled_rows: settledValidationRows.slice().reverse().slice(0, 50),
    },
    decision_quality: {
      status: 'insufficient_data',
      reason: 'Legacy decision-quality composite metrics are hidden until they are backed by canonical math.',
    },
    execution_quality: executionQualitySummary,
    quant_performance: expectationSummary,
    weekly_running_totals: settledPerformanceOverall,
    weekly_performance_review: {
      status: 'insufficient_data',
      reason: 'Legacy weekly review fields are not used for the live site until they are rebuilt from ledger truth.',
    },
    bankroll_contribution_policy: {
      status: 'insufficient_data',
      reason: 'No current contribution-policy artifact is loaded into canonical state.',
    },
    live_execution: {
      counts: executionBoard.counts || { candidates: 0, approved: 0, rejected: 0 },
      recommendations: executionBoard.recommendations || [],
      operator_summary: executionBoard.operator_summary || [],
      operator_output_text: (executionBoard.operator_summary || []).join('\n\n'),
      recent_execution_log: executionLog.slice().reverse().slice(0, 25),
    },
    recent_execution_log: executionLog.slice().reverse().slice(0, 25),
    recent_reconciliation_events: reconciliationEvents.slice().reverse().slice(0, 25),
    ledger_validation: ledgerValidation,
    bankroll_summary: {
      starting_bankroll: formatMoney(startingBankroll),
      contributions: formatMoney(contributions),
      realized_profit: formatMoney(realizedProfit),
      actual_bankroll: formatMoney(actualBankroll),
      last_recorded_bankroll: formatMoney(lastRecordedBankroll),
      bankroll_difference: formatMoney(bankrollDifference),
    },
    decision_payload_v1: decisionPayload,
    decision_renderers: {
      terminal_text: renderers.terminal,
      whatsapp_text: renderers.whatsapp,
      evening_grading_report_text: renderers.evening,
    },
    runtime_status: runtimeStatus,
    canonical_truth: {
      decision_ledger_path: CORE_PATHS.decisionLedger,
      grading_ledger_path: CORE_PATHS.gradingLedger,
      bankroll_ledger_path: CORE_PATHS.bankrollLedger,
      execution_board_path: EXECUTION_BOARD_PATH,
      execution_log_path: EXECUTION_LOG_PATH,
      override_log_path: OVERRIDE_LOG_PATH,
      post_mortem_log_path: POST_MORTEM_LOG_PATH,
      hunt_audit_log_path: HUNT_AUDIT_LOG_PATH,
      ledger_validation_path: '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/data/ledger-validator.json',
      canonical_state_path: CORE_PATHS.canonicalState,
      public_data_path: CORE_PATHS.publicData,
      markdown_is_operational_truth: false,
    },
  };

  writeJson(CORE_PATHS.canonicalState, payload);
  console.log(`Built canonical state: ${CORE_PATHS.canonicalState}`);
}

main();

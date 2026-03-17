#!/usr/bin/env node
import fs from 'node:fs';
import { CORE_PATHS, readJson, readJsonl, writeJson, parseNumber, round2, formatMoney } from './core-ledger-utils.mjs';
import { EXECUTION_BOARD_PATH, EXECUTION_LOG_PATH, readExecutionLog } from './execution-layer-utils.mjs';
import { validateLedgerInvariants } from './validate-ledger-invariants.mjs';
import { OVERRIDE_LOG_PATH, POST_MORTEM_LOG_PATH, getPostMortemStatus, readOverrideLog, buildWeeklyTruthReport } from './behavioral-accountability-utils.mjs';

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
  };
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
  const finalStatuses = new Set(['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout']);
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
  const ledgerValidation = validateLedgerInvariants({ requireOutputMatch: false });
  const generatedAtUtc = new Date().toISOString();
  const decisionIndex = buildDecisionIndex(decisions);

  const betDecisions = decisions.filter((row) => row.decision_kind === 'BET');
  const passBand = decisions.filter((row) => row.decision_kind === 'PASS');
  const suppressed = decisions.filter((row) => row.decision_kind === 'SUPPRESSED');
  const betGrades = grading.filter((row) => row.grading_type === 'BET');
  const passGrades = grading.filter((row) => row.grading_type === 'PASS');
  const reconciliationEvents = grading.filter((row) => row.grading_type === 'RECONCILIATION');

  const startingBankroll = bankrollEntries
    .filter((row) => row.entry_type === 'STARTING_BANKROLL')
    .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0);
  const contributions = bankrollEntries
    .filter((row) => row.entry_type === 'CONTRIBUTION')
    .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0);
  const realizedProfit = round2(betGrades.reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0;
  const actualBankroll = round2(startingBankroll + contributions + realizedProfit) || 0;
  const lastRecordedBankroll = round2(parseNumber(betGrades.at(-1)?.bankroll_after)) || actualBankroll;
  const bankrollDifference = round2(lastRecordedBankroll - actualBankroll) || 0;
  const latestRuntime = runtimeStatus.latest_hunt_current || runtimeStatus.latest_successful_hunt || null;
  const freshnessAnchorMs = runtimeStatus?.freshness_anchor?.timestamp_ms || null;
  const freshnessHours = Number.isFinite(freshnessAnchorMs) ? round2((Date.now() - freshnessAnchorMs) / 36e5) : null;
  const stateSyncGap = Boolean(runtimeStatus?.state_sync?.blocking_sync_gap);
  const postMortemStatus = getPostMortemStatus(grading);
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

  const wins = betGrades.filter((row) => String(row.result || '').toUpperCase() === 'WIN').length;
  const losses = betGrades.filter((row) => String(row.result || '').toUpperCase() === 'LOSS').length;
  const totalGraded = betGrades.filter((row) => ['WIN', 'LOSS'].includes(String(row.result || '').toUpperCase())).length;
  const totalStake = round2(betGrades.reduce((sum, row) => sum + (parseNumber(row.stake) || 0), 0)) || 0;
  const roi = totalStake > 0 ? round2((realizedProfit / totalStake) * 100) : null;
  const avgClv = (() => {
    const values = betGrades.map((row) => parsePercentString(row.clv)).filter((value) => value !== null);
    return values.length ? round2(values.reduce((sum, value) => sum + value, 0) / values.length) : null;
  })();

  const latestDate = latestRuntime?.date_key || betGrades.at(-1)?.date || null;
  const todaysBets = betGrades.filter((row) => row.date === latestDate).map((row) => ({
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
  const pendingBets = buildPendingBets({ executionLog, betGrades, reconciliationEvents });
  const openRiskSummary = buildOpenRiskSummary({ pendingBets, bankroll: lastRecordedBankroll, decisionIndex });
  const placementSnapshotSummary = buildPlacementSnapshotSummary(executionLog);
  const clvCoverageSummary = buildClvCoverageSummary(betGrades);
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
    last_updated_ct: runtimeStatus?.freshness_anchor?.timestamp_ct || latestRuntime?.run_at_ct || null,
    current_status: {
      Bankroll: formatMoney(lastRecordedBankroll),
      Phase: parsePhase(lastRecordedBankroll),
      'Open Exposure Used': openRiskSummary.open_exposure_pct_of_bankroll,
      'Open Tickets': openRiskSummary.pending_ticket_count,
      'Snapshot Coverage': placementSnapshotSummary.snapshot_coverage_pct_label,
      'CLV Coverage': clvCoverageSummary.clv_coverage_pct_label,
      'Override Count (month)': monthlyOverrideCount,
      'Post-Mortem Status': postMortemStatus.current_status,
      'Model Daily Exposure (latest hunt)': latestModelExposure,
      'Circuit Breaker': 'OFF',
    },
    lifetime_stats: {
      'Total Bets': betGrades.length,
      'Win Rate': totalGraded ? `${round2((wins / totalGraded) * 100)}% (${wins}-${losses})` : 'N/A',
      'Overall ROI': roi === null ? 'N/A' : `${roi}%`,
      'Average CLV': avgClv === null ? 'N/A' : `${avgClv}%`,
    },
    bet_log: betGrades.slice().reverse().map((row) => ({
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
    pass_band: passBand,
    suppressed_candidates: suppressed,
    passed_opportunity_tracker: {
      total_count: passBand.length,
      graded_count: passGradeResolved.length,
      ungraded_count: passGrades.filter((row) => String(row.result || '').toLowerCase() === 'ungraded').length,
      record_if_bet: `${passWins}-${passLosses}`,
      net_counterfactual_pl_if_bet: passNet,
    },
    daily_rejection_summary: {
      'Total Markets Checked': decisions.filter((row) => row.target_date === latestDate).length,
      'Total Rejected': decisions.filter((row) => row.target_date === latestDate && row.final_decision === 'SIT').length,
      'no_edge': decisions.filter((row) => row.target_date === latestDate && row.rejection_reason === 'no_edge').length,
      'low_confidence': decisions.filter((row) => row.target_date === latestDate && row.rejection_reason === 'low_confidence').length,
      'Plays Recommended': decisions.filter((row) => row.target_date === latestDate && row.final_decision === 'BET').length,
    },
    overall_betting_results: {
      count: betGrades.length,
      profit_loss: formatMoney(realizedProfit),
      total_staked: formatMoney(totalStake),
      roi: roi === null ? 'N/A' : `${roi}%`,
    },
    core_strategy_results: {
      count: betGrades.filter((row) => row.bet_class === 'EDGE_BET').length,
      profit_loss: formatMoney(round2(betGrades.filter((row) => row.bet_class === 'EDGE_BET').reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0),
    },
    fun_sgp_results: {
      count: betGrades.filter((row) => row.bet_class === 'FUN_SGP').length,
      profit_loss: formatMoney(round2(betGrades.filter((row) => row.bet_class === 'FUN_SGP').reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0)) || 0),
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

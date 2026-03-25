#!/usr/bin/env node
import process from 'node:process';
import { CORE_PATHS, parseNumber, readJsonl, round2, writeJson } from './core-ledger-utils.mjs';

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function buildInvalidRunIdSet(huntAuditRows) {
  return new Set(
    (huntAuditRows || [])
      .filter((row) => normalizeText(row.invalid_status).includes('invalid'))
      .map((row) => String(row.run_id || '').trim())
      .filter(Boolean)
  );
}

function isCleanWindowRow(row) {
  return row
    && typeof row === 'object'
    && String(row.consensus_method || '').trim() === 'trimmed_mean_with_median_guard'
    && Object.prototype.hasOwnProperty.call(row, 'snapshot_status');
}

function compareCtTimestamp(a, b) {
  return String(a || '').localeCompare(String(b || ''));
}

function summarizeRows(rows) {
  const runIds = new Set(rows.map((row) => String(row.run_id || '').trim()).filter(Boolean));
  const validSnapshots = rows.filter((row) => row.snapshot_status === 'valid').length;
  const invalidSnapshotCount = rows.filter((row) => row.snapshot_status !== 'valid' || row.rejection_reason === 'invalid_snapshot').length;
  const totalBets = rows.filter((row) => row.final_decision === 'BET').length;
  const totalSits = rows.filter((row) => row.final_decision === 'SIT').length;
  const betEdges = rows
    .filter((row) => row.final_decision === 'BET')
    .map((row) => parseNumber(row.post_conf_edge_pct))
    .filter(Number.isFinite);
  const avgEdgeBet = betEdges.length
    ? round2(betEdges.reduce((sum, value) => sum + value, 0) / betEdges.length)
    : null;
  const nearMissCount = rows.filter((row) => {
    const edge = parseNumber(row.post_conf_edge_pct);
    return row.final_decision === 'SIT' && Number.isFinite(edge) && edge >= 1.5 && edge < 2;
  }).length;
  const rejectionReasonDistribution = rows
    .filter((row) => row.final_decision === 'SIT')
    .reduce((acc, row) => {
      const key = row.rejection_reason || 'unknown';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});

  return {
    total_runs: runIds.size,
    valid_snapshots: validSnapshots,
    invalid_snapshot_count: invalidSnapshotCount,
    total_bets: totalBets,
    total_sits: totalSits,
    avg_edge_bet: avgEdgeBet,
    near_miss_count: nearMissCount,
    rejection_reason_distribution: rejectionReasonDistribution,
  };
}

export function buildCleanRunSummary({
  decisions = readJsonl(CORE_PATHS.decisionLedger),
  huntAuditRows = readJsonl(CORE_PATHS.huntAuditLog),
} = {}) {
  const invalidRunIds = buildInvalidRunIdSet(huntAuditRows);
  const candidateRows = (decisions || [])
    .filter((row) => !invalidRunIds.has(String(row.run_id || '').trim()))
    .filter(isCleanWindowRow)
    .sort((left, right) => compareCtTimestamp(left.timestamp_ct, right.timestamp_ct));

  if (!candidateRows.length) {
    return {
      schema: 'tierededge_clean_run_summary_v1',
      clean_window: {
        start_date: null,
        start_run_id: null,
        invalidated_runs_excluded: invalidRunIds.size,
      },
      daily_breakdown: [],
      rolling_7_day_summary: {
        total_runs: 0,
        valid_snapshots: 0,
        invalid_snapshot_count: 0,
        total_bets: 0,
        total_sits: 0,
        avg_edge_bet: null,
        near_miss_count: 0,
        rejection_reason_distribution: {},
      },
    };
  }

  const startRow = candidateRows[0];
  const startRunId = String(startRow.run_id || '').trim();
  const startDate = String(startRow.target_date || '').trim() || null;
  const cleanRows = candidateRows.filter((row) => compareCtTimestamp(row.timestamp_ct, startRow.timestamp_ct) >= 0);

  const byDate = new Map();
  for (const row of cleanRows) {
    const dateKey = String(row.target_date || '').trim() || 'unknown';
    if (!byDate.has(dateKey)) byDate.set(dateKey, []);
    byDate.get(dateKey).push(row);
  }

  const dailyBreakdown = [...byDate.entries()]
    .sort((left, right) => left[0].localeCompare(right[0]))
    .map(([date, rows]) => ({
      date,
      ...summarizeRows(rows),
    }));

  const rollingRows = dailyBreakdown.slice(-7).flatMap((entry) => byDate.get(entry.date) || []);

  return {
    schema: 'tierededge_clean_run_summary_v1',
    clean_window: {
      start_date: startDate,
      start_run_id: startRunId,
      invalidated_runs_excluded: invalidRunIds.size,
    },
    daily_breakdown: dailyBreakdown,
    rolling_7_day_summary: summarizeRows(rollingRows),
  };
}

function main() {
  const payload = buildCleanRunSummary();
  writeJson(CORE_PATHS.cleanRunSummary, payload);
  console.log(JSON.stringify(payload, null, 2));
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  main();
}

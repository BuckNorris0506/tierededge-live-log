#!/usr/bin/env node
import crypto from 'node:crypto';
import { CORE_PATHS, appendJsonl, parseNumber, readJson, readJsonl, round2 } from './core-ledger-utils.mjs';

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2).replace(/-/g, '_');
    args[key] = true;
  }
  return args;
}

function latestRunRows(state) {
  const runId = state?.latest_canonical_hunt_run?.run_id;
  const all = readJsonl(CORE_PATHS.decisionLedger);
  if (!runId) return [];
  return all.filter((row) => String(row.run_id || '').trim() === String(runId).trim());
}

function nativeAppendStatus(state) {
  const run = state?.latest_canonical_hunt_run;
  if (!run) return 'missing_run_artifact';
  if (run.status !== 'ok') return 'runner_failed';
  if (Number.isFinite(parseNumber(run.native_rows_appended))) return 'succeeded';
  return 'append_missing';
}

function baselineSpike(latestCount, rollingCount, totalRuns, ratio = 2) {
  if (!Number.isFinite(latestCount) || !Number.isFinite(rollingCount) || !Number.isFinite(totalRuns) || totalRuns <= 0) return false;
  const baseline = rollingCount / totalRuns;
  if (baseline <= 0) return latestCount >= 10;
  return latestCount >= Math.max(10, Math.ceil(baseline * ratio));
}

function buildActionableCandidate(state, rows) {
  const run = state?.latest_canonical_hunt_run;
  if (!run || run.invalidated) return null;
  if (nativeAppendStatus(state) !== 'succeeded') return null;
  const selectedRows = (run.selected_rows || rows.filter((row) => row.final_decision === 'BET')) || [];
  if (!selectedRows.length) return null;
  const actionable = selectedRows.filter((row) =>
    row.actionable_book !== false
    && row.executable_book !== false
    && String(row.rejection_reason || '').trim() === ''
    && !['stale_market', 'invalid_snapshot'].includes(String(row.rejection_reason || '').trim().toLowerCase())
  );
  if (!actionable.length) return null;
  const selections = actionable.slice(0, 5).map((row) =>
    `${row.selection} @ ${row.odds_american} | ${row.sportsbook} | ${round2(parseNumber(row.post_conf_edge_pct) || 0)}% edge | $${Number(parseNumber(row.kelly_stake) || 0).toFixed(2)}`
  );
  return {
    type: 'actionable',
    reason: 'actionable_recommendations_available',
    run_id: run.run_id,
    message: [
      `TIEREDGE ACTIONABLE BOARD — ${run.run_at_ct || state.last_updated_ct || 'unknown'}`,
      `Run: ${run.run_id}`,
      `Actionable recommendations: ${actionable.length}`,
      ...selections,
    ].join('\n'),
  };
}

function buildWarningCandidate(state, rows) {
  const run = state?.latest_canonical_hunt_run;
  const clean = state?.clean_run_summary?.rolling_7_day_summary || {};
  const invalidSnapshotCount = rows.filter((row) => String(row.rejection_reason || '').trim().toLowerCase() === 'invalid_snapshot').length;
  const staleMarketCount = rows.filter((row) => String(row.rejection_reason || '').trim().toLowerCase() === 'stale_market').length;
  const totalRuns = parseNumber(clean.total_runs) || 0;
  const reasons = [];

  if (run?.invalidated) reasons.push('latest_run_invalidated');
  if (nativeAppendStatus(state) !== 'succeeded') reasons.push(`native_append_${nativeAppendStatus(state)}`);
  if (!state?.ledger_validation?.passed) reasons.push('validator_failed');
  if (Math.abs(parseNumber(state?.ledger_validation?.summary?.bankroll?.difference) || 0) > 0.009) reasons.push('bankroll_mismatch');
  if (baselineSpike(invalidSnapshotCount, parseNumber(clean.invalid_snapshot_count) || 0, totalRuns)) reasons.push('invalid_snapshot_spike');
  if (baselineSpike(staleMarketCount, countReason(clean.rejection_reason_distribution, 'stale_market'), totalRuns)) reasons.push('stale_market_spike');

  if (!reasons.length) return null;
  return {
    type: 'warning',
    reason: reasons.join(','),
    run_id: run?.run_id || null,
    message: [
      `TIEREDGE WARNING — ${run?.run_at_ct || state.last_updated_ct || 'unknown'}`,
      `Run: ${run?.run_id || 'unknown'}`,
      `Reasons: ${reasons.join(', ')}`,
    ].join('\n'),
  };
}

function countReason(distribution, key) {
  return parseNumber(distribution?.[key]) || 0;
}

function buildFingerprint(candidate) {
  return crypto.createHash('sha256')
    .update(JSON.stringify({
      type: candidate.type,
      reason: candidate.reason,
      run_id: candidate.run_id,
      message: candidate.message,
    }))
    .digest('hex')
    .slice(0, 16);
}

function main() {
  const args = parseArgs(process.argv);
  const state = readJson(CORE_PATHS.publicData, {});
  const rows = latestRunRows(state);
  const notificationEvents = readJsonl(CORE_PATHS.notificationEvents);
  const now = new Date().toISOString();

  const candidate = buildWarningCandidate(state, rows) || buildActionableCandidate(state, rows);
  if (!candidate) {
    if (args.json) {
      console.log(JSON.stringify({ status: 'no_alert' }, null, 2));
    }
    return;
  }

  const fingerprint = buildFingerprint(candidate);
  const alreadySent = notificationEvents.some((row) =>
    String(row.status || '').trim().toLowerCase() === 'sent'
    && String(row.fingerprint || '').trim() === fingerprint
  );

  const event = {
    notification_id: `notify::${now}`,
    created_at_utc: now,
    status: alreadySent ? 'skipped_duplicate' : 'sent',
    notification_type: candidate.type,
    triggering_reason: candidate.reason,
    run_id: candidate.run_id,
    fingerprint,
    message: candidate.message,
  };
  appendJsonl(CORE_PATHS.notificationEvents, event, (row) => String(row.notification_id || '').trim());

  if (args.json) {
    console.log(JSON.stringify(event, null, 2));
    return;
  }
  if (!alreadySent) {
    console.log(candidate.message);
  }
}

main();


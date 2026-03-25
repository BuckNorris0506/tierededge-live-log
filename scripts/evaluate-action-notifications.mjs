#!/usr/bin/env node
import crypto from 'node:crypto';
import { CORE_PATHS, appendJsonl, parseNumber, readJson, readJsonl, round2 } from './core-ledger-utils.mjs';
import { sendTelegramMessage } from './telegram-alert-utils.mjs';

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

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
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

function countReason(distribution, key) {
  return parseNumber(distribution?.[key]) || 0;
}

function baselineSpike(latestCount, rollingCount, totalRuns, ratio = 2) {
  if (!Number.isFinite(latestCount) || !Number.isFinite(rollingCount) || !Number.isFinite(totalRuns) || totalRuns <= 0) return false;
  const baseline = rollingCount / totalRuns;
  if (baseline <= 0) return latestCount >= 10;
  return latestCount >= Math.max(10, Math.ceil(baseline * ratio));
}

function formatMinutes(value) {
  const num = parseNumber(value);
  return Number.isFinite(num) ? Math.max(0, Math.round(num)) : null;
}

function actionableRowsForNotification(state, rows) {
  const run = state?.latest_canonical_hunt_run;
  if (!run || run.invalidated) return [];
  if (nativeAppendStatus(state) !== 'succeeded') return [];
  const selectedRows = Array.isArray(run.selected_rows) && run.selected_rows.length
    ? run.selected_rows
    : rows.filter((row) => row.final_decision === 'BET');
  return selectedRows.filter((row) => {
    const urgency = String(row.urgency_tag || '').toUpperCase();
    const rejectionReason = normalizeText(row.rejection_reason);
    return row.actionable_book !== false
      && row.executable_book !== false
      && ['NOW', 'SOON'].includes(urgency)
      && !['stale_market', 'invalid_snapshot'].includes(rejectionReason);
  });
}

function buildActionableCandidate(state, rows) {
  const run = state?.latest_canonical_hunt_run;
  const actionable = actionableRowsForNotification(state, rows);
  if (!run || !actionable.length) return null;

  const boardSignature = actionable.map((row) => ({
    rec_id: row.rec_id || null,
    event_id: row.event_id || null,
    event_label: row.event_label || null,
    selection: row.selection || null,
    sportsbook: row.sportsbook || null,
    odds_american: parseNumber(row.odds_american),
    edge_pct: round2(parseNumber(row.post_conf_edge_pct) || 0),
    minutes_to_start: formatMinutes(row.minutes_to_start),
    urgency_tag: String(row.urgency_tag || 'LATER').toUpperCase(),
  }));

  const lines = [
    'TIERED EDGE — ACTION REQUIRED',
    '',
    `Run: ${run.run_id}`,
  ];

  for (const row of boardSignature) {
    lines.push('');
    lines.push(`Game: ${row.event_label || 'Unknown game'}`);
    lines.push(`Play: ${row.selection} @ ${row.sportsbook}`);
    lines.push(`Edge: +${Number(row.edge_pct || 0).toFixed(2)}%`);
    lines.push(`Start: ${row.minutes_to_start ?? 'Unknown'} minutes (${row.urgency_tag})`);
  }

  return {
    tier: 'A',
    type: 'actionable',
    reason: 'actionable_board_changed',
    trigger: 'new_actionable_board',
    run_id: run.run_id,
    channel_preference: 'telegram',
    board_signature: boardSignature,
    message: lines.join('\n'),
  };
}

function buildWarningCandidate(state, rows) {
  const run = state?.latest_canonical_hunt_run;
  const clean = state?.clean_run_summary?.rolling_7_day_summary || {};
  const invalidSnapshotCount = rows.filter((row) => normalizeText(row.rejection_reason) === 'invalid_snapshot').length;
  const staleMarketCount = rows.filter((row) => normalizeText(row.rejection_reason) === 'stale_market').length;
  const totalRuns = parseNumber(clean.total_runs) || 0;
  const reasons = [];

  if (run?.invalidated) reasons.push('latest_run_invalidated');
  const appendStatus = nativeAppendStatus(state);
  if (appendStatus !== 'succeeded') reasons.push(`native_append_${appendStatus}`);
  if (!state?.ledger_validation?.passed) reasons.push('validator_failed');
  if (Math.abs(parseNumber(state?.ledger_validation?.summary?.bankroll?.difference) || 0) > 0.009) reasons.push('bankroll_mismatch');
  if (baselineSpike(invalidSnapshotCount, parseNumber(clean.invalid_snapshot_count) || 0, totalRuns)) reasons.push('invalid_snapshot_spike');
  if (baselineSpike(staleMarketCount, countReason(clean.rejection_reason_distribution, 'stale_market'), totalRuns)) reasons.push('stale_market_spike');

  if (!reasons.length) return null;

  return {
    tier: 'B',
    type: 'warning',
    reason: reasons.join(','),
    trigger: reasons[0],
    run_id: run?.run_id || null,
    channel_preference: 'log_only',
    board_signature: null,
    message: [
      'TIERED EDGE — WARNING',
      '',
      `Run: ${run?.run_id || 'unknown'}`,
      `Reasons: ${reasons.join(', ')}`,
    ].join('\n'),
  };
}

function buildFingerprint(candidate) {
  return crypto.createHash('sha256')
    .update(JSON.stringify({
      tier: candidate.tier,
      type: candidate.type,
      trigger: candidate.trigger,
      run_id: candidate.run_id,
      board_signature: candidate.board_signature,
      message: candidate.message,
    }))
    .digest('hex')
    .slice(0, 20);
}

function existingEventForFingerprint(notificationEvents, fingerprint, status) {
  return notificationEvents.find((row) =>
    String(row.fingerprint || '').trim() === fingerprint
    && normalizeText(row.status) === normalizeText(status)
  ) || null;
}

async function deliverCandidate(candidate) {
  if (candidate.channel_preference !== 'telegram') {
    return { status: 'sent', channel_used: 'log_only', delivery_error: null };
  }
  const result = await sendTelegramMessage(candidate.message);
  return {
    status: result.ok ? 'sent' : 'failed',
    channel_used: result.ok ? 'telegram' : 'telegram_failed',
    delivery_error: result.ok ? null : (result.error || 'telegram_delivery_failed'),
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const state = readJson(CORE_PATHS.publicData, {});
  const rows = latestRunRows(state);
  const notificationEvents = readJsonl(CORE_PATHS.notificationEvents);
  const now = new Date().toISOString();

  const candidate = buildActionableCandidate(state, rows) || buildWarningCandidate(state, rows);
  if (!candidate) {
    if (args.json) {
      console.log(JSON.stringify({ status: 'no_alert' }, null, 2));
    }
    return;
  }

  const fingerprint = buildFingerprint(candidate);
  const alreadySent = existingEventForFingerprint(notificationEvents, fingerprint, 'sent');
  const alreadySkipped = existingEventForFingerprint(notificationEvents, fingerprint, 'skipped_duplicate');

  let event;
  if (alreadySent) {
    event = {
      notification_id: `notify::${now}`,
      created_at_utc: now,
      status: 'skipped_duplicate',
      notification_type: candidate.type,
      notification_tier: candidate.tier,
      triggering_reason: candidate.reason,
      trigger: candidate.trigger,
      run_id: candidate.run_id,
      fingerprint,
      channel_used: candidate.channel_preference,
      message: candidate.message,
    };
    if (!alreadySkipped) {
      appendJsonl(CORE_PATHS.notificationEvents, event, (row) => String(row.notification_id || '').trim());
    }
  } else {
    const delivery = await deliverCandidate(candidate);
    event = {
      notification_id: `notify::${now}`,
      created_at_utc: now,
      status: delivery.status,
      notification_type: candidate.type,
      notification_tier: candidate.tier,
      triggering_reason: candidate.reason,
      trigger: candidate.trigger,
      run_id: candidate.run_id,
      fingerprint,
      channel_used: delivery.channel_used,
      delivery_error: delivery.delivery_error,
      message: candidate.message,
    };
    appendJsonl(CORE_PATHS.notificationEvents, event, (row) => String(row.notification_id || '').trim());
  }

  if (args.json) {
    console.log(JSON.stringify(event, null, 2));
    return;
  }

  if (event.status === 'sent' && event.channel_used === 'telegram') {
    console.log(candidate.message);
  }
}

await main();

import crypto from 'node:crypto';
import fs from 'node:fs';
import { CORE_PATHS, appendJsonl, readJson, readJsonl } from './core-ledger-utils.mjs';
import { sendTelegramMessage } from './telegram-alert-utils.mjs';

const SCHEDULED_HUNT_JOB_NAMES = new Set([
  'morning-edge-hunt',
  'midmorning-edge-hunt',
  'midday-edge-hunt',
  'afternoon-edge-hunt',
  'evening-edge-hunt',
]);

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function parseArgs(argv) {
  const args = {
    job_name: null,
    run_id: null,
    runner_json_file: null,
    automation_run_id: null,
    started_at_utc: null,
    completed_status: 'ok',
    deploy_status: null,
    deploy_error: null,
    dry_run: false,
    force: false,
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--job-name') args.job_name = argv[++index] || null;
    else if (arg === '--run-id') args.run_id = argv[++index] || null;
    else if (arg === '--runner-json-file') args.runner_json_file = argv[++index] || null;
    else if (arg === '--automation-run-id') args.automation_run_id = argv[++index] || null;
    else if (arg === '--started-at-utc') args.started_at_utc = argv[++index] || null;
    else if (arg === '--completed-status') args.completed_status = argv[++index] || 'ok';
    else if (arg === '--deploy-status') args.deploy_status = argv[++index] || null;
    else if (arg === '--deploy-error') args.deploy_error = argv[++index] || null;
    else if (arg === '--dry-run') args.dry_run = true;
    else if (arg === '--force') args.force = true;
    else throw new Error(`unknown_arg:${arg}`);
  }
  return args;
}

function loadRunPayload(args) {
  if (args.runner_json_file) {
    const payload = readJson(args.runner_json_file, null);
    if (payload) return payload;
  }
  if (!args.run_id) {
    throw new Error('missing_run_payload');
  }
  const rows = readJsonl(CORE_PATHS.canonicalHuntRuns);
  const match = rows.find((row) => String(row.run_id || '').trim() === String(args.run_id || '').trim());
  if (!match) {
    throw new Error(`missing_run_id:${args.run_id}`);
  }
  return match;
}

function resolveHeartbeatEnabled() {
  const state = readJson(CORE_PATHS.publicData, {}) || {};
  if (typeof state.scheduled_hunt_heartbeat_enabled === 'boolean') {
    return state.scheduled_hunt_heartbeat_enabled;
  }
  const envValue = normalizeText(process.env.TIEREDGE_SCHEDULED_HUNT_HEARTBEAT_ENABLED || 'true');
  return envValue !== 'false' && envValue !== '0' && envValue !== 'off';
}

function extractScheduledTime(runPayload) {
  const runAtCt = String(runPayload.run_at_ct || '').trim();
  const runAtMatch = runAtCt.match(/\b(\d{2}:\d{2})\b/);
  if (runAtMatch) return `${runAtMatch[1]} CT`;
  const runIdMatch = String(runPayload.run_id || '').match(/::(\d{2})(\d{2})$/);
  if (runIdMatch) return `${runIdMatch[1]}:${runIdMatch[2]} CT`;
  return 'Unknown CT';
}

function inferActionableCount(runPayload) {
  if (Array.isArray(runPayload.selected_rows)) {
    const selectedCount = runPayload.selected_rows.length;
    if (selectedCount > 0) return selectedCount;
  }
  const nativeBets = Number(runPayload.native_bets_appended);
  if (Number.isFinite(nativeBets) && nativeBets > 0) return nativeBets;
  if (runPayload.has_actionable_bets === true) return 1;
  return 0;
}

function buildHeartbeatText(runPayload, args) {
  const verdict = String(runPayload.message_type || runPayload.status || 'UNKNOWN').trim().toUpperCase();
  const actionableCount = inferActionableCount(runPayload);
  const lines = [
    'SCHEDULED HUNT COMPLETE',
    `Time: ${extractScheduledTime(runPayload)}`,
    `Verdict: ${verdict}`,
    `Run: ${runPayload.run_id || args.run_id || 'unknown'}`,
  ];
  if (verdict === 'BET' && actionableCount > 0) {
    lines.push(`Actionable Plays: ${actionableCount}`);
  }
  if (normalizeText(args.completed_status) === 'complete_with_deploy_warning') {
    lines.push('Deploy: WARNING');
  }
  return {
    verdict,
    actionable_count: actionableCount,
    text: lines.join('\n'),
  };
}

function buildFingerprint(jobName, runId) {
  return crypto.createHash('sha256')
    .update(JSON.stringify({
      type: 'scheduled_hunt_heartbeat',
      job_name: jobName,
      run_id: runId,
    }))
    .digest('hex')
    .slice(0, 20);
}

function findExistingHeartbeat(fingerprint) {
  return readJsonl(CORE_PATHS.notificationEvents).find((row) =>
    String(row.fingerprint || '').trim() === fingerprint
    && ['sent', 'skipped_duplicate'].includes(normalizeText(row.status))
  ) || null;
}

async function main() {
  const args = parseArgs(process.argv);
  const jobName = String(args.job_name || '').trim();
  if (!SCHEDULED_HUNT_JOB_NAMES.has(jobName)) {
    throw new Error(`unsupported_scheduled_hunt_job:${jobName || 'missing'}`);
  }

  const enabled = resolveHeartbeatEnabled();
  const runPayload = loadRunPayload(args);
  const runId = String(runPayload.run_id || args.run_id || '').trim();
  if (!runId) {
    throw new Error('missing_run_id');
  }

  const heartbeat = buildHeartbeatText(runPayload, args);
  const fingerprint = buildFingerprint(jobName, runId);

  if (!enabled) {
    console.log(JSON.stringify({
      status: 'disabled',
      notification_type: 'scheduled_hunt_heartbeat',
      job_name: jobName,
      run_id: runId,
      text: heartbeat.text,
    }));
    return;
  }

  if (!args.force) {
    const existing = findExistingHeartbeat(fingerprint);
    if (existing) {
      console.log(JSON.stringify({
        status: 'skipped_duplicate',
        notification_type: 'scheduled_hunt_heartbeat',
        job_name: jobName,
        run_id: runId,
        fingerprint,
        text: heartbeat.text,
      }));
      return;
    }
  }

  if (args.dry_run) {
    console.log(JSON.stringify({
      status: 'dry_run',
      notification_type: 'scheduled_hunt_heartbeat',
      job_name: jobName,
      run_id: runId,
      fingerprint,
      verdict: heartbeat.verdict,
      actionable_count: heartbeat.actionable_count,
      text: heartbeat.text,
    }, null, 2));
    return;
  }

  const delivery = await sendTelegramMessage(heartbeat.text);
  const createdAtUtc = new Date().toISOString();
  const event = {
    notification_id: `heartbeat::${createdAtUtc}::${jobName}`,
    created_at_utc: createdAtUtc,
    status: delivery.ok ? 'sent' : 'failed',
    notification_type: 'scheduled_hunt_heartbeat',
    notification_tier: 'info',
    triggering_reason: 'scheduled_hunt_complete',
    trigger: 'scheduled_hunt_complete',
    job_name: jobName,
    run_id: runId,
    fingerprint,
    channel_used: delivery.ok ? 'telegram' : 'telegram_failed',
    delivery_error: delivery.ok ? null : (delivery.error || 'telegram_delivery_failed'),
    message: heartbeat.text,
    verdict: heartbeat.verdict,
    actionable_play_count: heartbeat.actionable_count,
    completed_status: args.completed_status,
    deploy_status: args.deploy_status || null,
    deploy_error: args.deploy_error || null,
    automation_run_id: args.automation_run_id || null,
    started_at_utc: args.started_at_utc || null,
  };
  appendJsonl(CORE_PATHS.notificationEvents, event, (row) => String(row.notification_id || '').trim());
  console.log(JSON.stringify(event, null, 2));
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exit(1);
});

#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { acquireNamedLock, clearNamedLock } from './automation-lock-utils.mjs';
import { appendDirectAutomationRun } from './direct-automation-log-utils.mjs';
import { appendJsonl, CORE_PATHS } from './core-ledger-utils.mjs';
import { runAutomaticSettlementPass } from './run-automatic-settlement.mjs';

const __filename = fileURLToPath(import.meta.url);
const ROOT_DIR = path.resolve(path.dirname(__filename), '..');

function loadEnvVarFromFile(filePath, varName) {
  if (!fs.existsSync(filePath)) return;
  const raw = fs.readFileSync(filePath, 'utf8');
  const line = raw
    .split('\n')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .filter((entry) => !entry.startsWith('#'))
    .find((entry) => new RegExp(`^(export\\s+)?${varName}=`).test(entry));
  if (!line) return;
  const [, value = ''] = line.split(/=(.*)/s);
  let normalized = value.trim();
  if ((normalized.startsWith('"') && normalized.endsWith('"')) || (normalized.startsWith("'") && normalized.endsWith("'"))) {
    normalized = normalized.slice(1, -1);
  }
  if (!process.env[varName]) {
    process.env[varName] = normalized;
  }
}

function loadRequiredEnvs() {
  const home = os.homedir();
  const candidates = [
    path.join(home, '.tierededge-env.zsh'),
    path.join(home, '.zshrc'),
  ];
  for (const filePath of candidates) {
    loadEnvVarFromFile(filePath, 'ODDS_API_KEY');
  }
  process.env.ODDS_API_KEY = process.env.ODDS_API_KEY || '';
}

function parseArgs(argv) {
  let jobName = 'nightly-automatic-settlement';
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === '--job-name') {
      jobName = argv[index + 1] || jobName;
      index += 1;
    }
  }
  return { jobName };
}

function appendAutomaticSettlementRun(row) {
  appendJsonl(CORE_PATHS.automaticSettlementRuns, row, (entry) => String(entry.automation_run_id || '').trim());
  return row;
}

function rebuildPublicState() {
  const child = spawnSync('/bin/zsh', ['scripts/update-live-log.sh'], {
    cwd: ROOT_DIR,
    env: process.env,
    encoding: 'utf8',
  });
  if (child.stdout) process.stdout.write(child.stdout);
  if (child.stderr) process.stderr.write(child.stderr);
  return {
    ok: (child.status || 0) === 0,
    status: child.status || 0,
  };
}

async function main() {
  const { jobName } = parseArgs(process.argv.slice(2));
  const startedAtUtc = new Date().toISOString();
  const automationRunId = `${jobName}::${startedAtUtc}`;

  loadRequiredEnvs();

  const lock = await acquireNamedLock('automatic-settlement', 'run-automatic-settlement-launcher.mjs');
  if (!lock.acquired) {
    const row = {
      automation_run_id: automationRunId,
      job_name: jobName,
      started_at_utc: startedAtUtc,
      completed_at_utc: new Date().toISOString(),
      status: 'skipped_due_to_active_lock',
      scheduler_type: 'system_cron',
      execution_mode: 'direct_local_command',
      model_backed_scheduler_in_path: false,
      command_path: path.join(ROOT_DIR, 'scripts', 'run-automatic-settlement-launcher.mjs'),
      child_command: 'node scripts/run-automatic-settlement.mjs',
      lock_name: 'automatic-settlement',
      rows_scanned: 0,
      rows_settled: 0,
      rows_unresolved: 0,
      unresolved_reason_breakdown: {},
    };
    appendAutomaticSettlementRun(row);
    appendDirectAutomationRun(row);
    process.exit(0);
  }

  try {
    const settlementResult = await runAutomaticSettlementPass({ started_at_utc: startedAtUtc });
    const rebuildResult = rebuildPublicState();
    const completedAtUtc = new Date().toISOString();
    const row = {
      automation_run_id: automationRunId,
      job_name: jobName,
      started_at_utc: startedAtUtc,
      completed_at_utc: completedAtUtc,
      status: rebuildResult.ok ? 'ok' : 'rebuild_failed',
      scheduler_type: 'system_cron',
      execution_mode: 'direct_local_command',
      model_backed_scheduler_in_path: false,
      command_path: path.join(ROOT_DIR, 'scripts', 'run-automatic-settlement-launcher.mjs'),
      child_command: 'node scripts/run-automatic-settlement.mjs',
      lock_name: 'automatic-settlement',
      rebuild_status: rebuildResult.ok ? 'ok' : 'failed',
      rows_scanned: settlementResult.rows_scanned || 0,
      rows_settled: settlementResult.rows_settled || 0,
      rows_unresolved: settlementResult.rows_unresolved || 0,
      rows_skipped_already_settled: settlementResult.rows_skipped_already_settled || 0,
      unresolved_reason_breakdown: settlementResult.unresolved_reason_breakdown || {},
      scores_cache_hits: settlementResult.scores_cache_hits || 0,
      scores_api_calls: settlementResult.scores_api_calls || 0,
      settled_rows: settlementResult.settled_rows || [],
    };
    appendAutomaticSettlementRun(row);
    appendDirectAutomationRun(row);
    if (rebuildResult.ok) {
      rebuildPublicState();
    }
    process.exit(rebuildResult.ok ? 0 : 1);
  } catch (error) {
    const completedAtUtc = new Date().toISOString();
    const row = {
      automation_run_id: automationRunId,
      job_name: jobName,
      started_at_utc: startedAtUtc,
      completed_at_utc: completedAtUtc,
      status: 'failed',
      scheduler_type: 'system_cron',
      execution_mode: 'direct_local_command',
      model_backed_scheduler_in_path: false,
      command_path: path.join(ROOT_DIR, 'scripts', 'run-automatic-settlement-launcher.mjs'),
      child_command: 'node scripts/run-automatic-settlement.mjs',
      lock_name: 'automatic-settlement',
      error: error?.message || String(error),
      transport_diagnostics: error?.transport_diagnostics || null,
      rows_scanned: 0,
      rows_settled: 0,
      rows_unresolved: 0,
      unresolved_reason_breakdown: {},
    };
    appendAutomaticSettlementRun(row);
    appendDirectAutomationRun(row);
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  } finally {
    await clearNamedLock('automatic-settlement');
  }
}

main();

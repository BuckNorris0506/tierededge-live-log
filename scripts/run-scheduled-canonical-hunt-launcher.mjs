#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { acquireNamedLock, clearNamedLock } from './automation-lock-utils.mjs';
import { appendDirectAutomationRun } from './direct-automation-log-utils.mjs';

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
  if (
    (normalized.startsWith('"') && normalized.endsWith('"'))
    || (normalized.startsWith("'") && normalized.endsWith("'"))
  ) {
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
    loadEnvVarFromFile(filePath, 'TIEREDGE_TELEGRAM_BOT_TOKEN');
    loadEnvVarFromFile(filePath, 'TIEREDGE_TELEGRAM_CHAT_ID');
    loadEnvVarFromFile(filePath, 'TELEGRAM_BOT_TOKEN');
    loadEnvVarFromFile(filePath, 'TELEGRAM_CHAT_ID');
  }
  process.env.TIEREDGE_TELEGRAM_BOT_TOKEN =
    process.env.TIEREDGE_TELEGRAM_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN || '';
  process.env.TIEREDGE_TELEGRAM_CHAT_ID =
    process.env.TIEREDGE_TELEGRAM_CHAT_ID || process.env.TELEGRAM_CHAT_ID || '';
  process.env.ODDS_API_KEY = process.env.ODDS_API_KEY || '';
}

function parseArgs(argv) {
  let jobName = 'scheduled-canonical-hunt';
  const forwardArgs = [];
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--job-name') {
      jobName = argv[index + 1] || jobName;
      index += 1;
      continue;
    }
    forwardArgs.push(token);
  }
  return { jobName, forwardArgs };
}

async function main() {
  const { jobName, forwardArgs } = parseArgs(process.argv.slice(2));
  const startedAtUtc = new Date().toISOString();
  const automationRunId = `${jobName}::${startedAtUtc}`;

  loadRequiredEnvs();
  process.env.LIVE_LOG_DEPLOY_REPO = process.env.LIVE_LOG_DEPLOY_REPO || ROOT_DIR;

  const lock = await acquireNamedLock('scheduled-canonical-hunt-launcher', 'run-scheduled-canonical-hunt-launcher.mjs');
  if (!lock.acquired) {
    appendDirectAutomationRun({
      automation_run_id: automationRunId,
      scheduler_type: 'system_cron',
      execution_mode: 'direct_local_command',
      model_backed_scheduler_in_path: false,
      job_name: jobName,
      started_at_utc: startedAtUtc,
      completed_at_utc: new Date().toISOString(),
      status: 'skipped_due_to_active_lock',
      command_path: path.join(ROOT_DIR, 'scripts', 'run-scheduled-canonical-hunt-launcher.mjs'),
      child_command: 'node scripts/run-scheduled-canonical-hunt.sh',
      lock_name: 'scheduled-canonical-hunt-launcher',
    });
    process.exit(0);
  }

  try {
    const child = spawnSync(
      '/bin/zsh',
      ['scripts/run-scheduled-canonical-hunt.sh', '--job-name', jobName, ...forwardArgs],
      {
        cwd: ROOT_DIR,
        env: process.env,
        encoding: 'utf8',
      },
    );

    if (child.stdout) process.stdout.write(child.stdout);
    if (child.stderr) process.stderr.write(child.stderr);
    process.exit(child.status || 0);
  } finally {
    await clearNamedLock('scheduled-canonical-hunt-launcher');
  }
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exit(1);
});

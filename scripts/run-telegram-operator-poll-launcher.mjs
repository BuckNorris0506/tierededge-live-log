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
    (normalized.startsWith('"') && normalized.endsWith('"')) ||
    (normalized.startsWith("'") && normalized.endsWith("'"))
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
  const forwardArgs = [];
  let jobName = 'telegram-operator-poll';
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

  const lock = await acquireNamedLock('telegram-operator', 'run-telegram-operator-poll-launcher.mjs');
  if (!lock.acquired) {
    appendDirectAutomationRun({
      automation_run_id: automationRunId,
      scheduler_type: 'launchd',
      job_name: jobName,
      started_at_utc: startedAtUtc,
      completed_at_utc: new Date().toISOString(),
      status: 'skipped_due_to_active_lock',
      command_path: path.join(ROOT_DIR, 'scripts', 'run-telegram-operator-poll-launcher.mjs'),
      child_command: 'node scripts/telegram-operator-bot.mjs --json',
      lock_name: 'telegram-operator',
    });
    process.exit(0);
  }

  try {
    const child = spawnSync(
      process.execPath,
      ['scripts/telegram-operator-bot.mjs', '--json', ...forwardArgs],
      {
        cwd: ROOT_DIR,
        env: process.env,
        encoding: 'utf8',
      },
    );

    if (child.status === 0) {
      const payload = JSON.parse(child.stdout || '{}');
      appendDirectAutomationRun({
        automation_run_id: automationRunId,
        scheduler_type: 'launchd',
        job_name: jobName,
        started_at_utc: startedAtUtc,
        completed_at_utc: new Date().toISOString(),
        status: 'ok',
        command_path: path.join(ROOT_DIR, 'scripts', 'run-telegram-operator-poll-launcher.mjs'),
        child_command: 'node scripts/telegram-operator-bot.mjs --json',
        processed_updates: Number(payload.processed_updates || 0),
      });
      process.stdout.write(child.stdout || '');
      process.exit(0);
    }

    appendDirectAutomationRun({
      automation_run_id: automationRunId,
      scheduler_type: 'launchd',
      job_name: jobName,
      started_at_utc: startedAtUtc,
      completed_at_utc: new Date().toISOString(),
      status: 'failed',
      command_path: path.join(ROOT_DIR, 'scripts', 'run-telegram-operator-poll-launcher.mjs'),
      child_command: 'node scripts/telegram-operator-bot.mjs --json',
      error: (child.stderr || child.stdout || '').trim() || `child_exit_${child.status}`,
    });
    if (child.stdout) process.stdout.write(child.stdout);
    if (child.stderr) process.stderr.write(child.stderr);
    process.exit(child.status || 1);
  } finally {
    await clearNamedLock('telegram-operator');
  }
}

main().catch((error) => {
  console.error(error?.stack || error?.message || String(error));
  process.exit(1);
});

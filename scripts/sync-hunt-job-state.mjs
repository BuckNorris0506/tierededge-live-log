#!/usr/bin/env node
import fs from 'node:fs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';

const OPENCLAW_JOBS_PATH = '/Users/jaredbuckman/.openclaw/cron/jobs.json';
const HUNT_JOB_NAMES = new Set(['morning-edge-hunt', 'friday-sgp']);

function main() {
  const block = readHuntBlockStatus();
  if (!block.ok) {
    throw new Error(block.reason);
  }

  const raw = JSON.parse(fs.readFileSync(OPENCLAW_JOBS_PATH, 'utf8'));
  let changed = 0;

  for (const job of raw.jobs || []) {
    if (!HUNT_JOB_NAMES.has(String(job.name || ''))) continue;
    const shouldEnable = !block.blocked;
    if (Boolean(job.enabled) !== shouldEnable) {
      job.enabled = shouldEnable;
      changed += 1;
    }
  }

  if (changed > 0) {
    fs.writeFileSync(OPENCLAW_JOBS_PATH, `${JSON.stringify(raw, null, 2)}\n`, 'utf8');
  }

  console.log(JSON.stringify({
    blocked: block.blocked,
    reason_class: block.reason_class,
    changed,
    hunt_jobs: (raw.jobs || [])
      .filter((job) => HUNT_JOB_NAMES.has(String(job.name || '')))
      .map((job) => ({ name: job.name, enabled: Boolean(job.enabled) })),
  }, null, 2));
}

main();

#!/usr/bin/env node
import fs from 'node:fs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';

const OPENCLAW_JOBS_PATH = '/Users/jaredbuckman/.openclaw/cron/jobs.json';
const DIRECT_LOCAL_HUNT_JOB_NAMES = new Set(['morning-edge-hunt', 'midday-edge-hunt', 'afternoon-edge-hunt']);
const DISABLED_OPENCLAW_HUNT_JOB_NAMES = new Set(['friday-sgp']);

function main() {
  const block = readHuntBlockStatus();
  if (!block.ok) {
    throw new Error(block.reason);
  }

  const raw = JSON.parse(fs.readFileSync(OPENCLAW_JOBS_PATH, 'utf8'));
  let changed = 0;

  for (const job of raw.jobs || []) {
    const jobName = String(job.name || '');
    if (DIRECT_LOCAL_HUNT_JOB_NAMES.has(jobName)) {
      if (job.enabled !== false) {
        job.enabled = false;
        changed += 1;
      }
      continue;
    }
    if (!DISABLED_OPENCLAW_HUNT_JOB_NAMES.has(jobName)) continue;
    if (job.enabled !== false) {
      job.enabled = false;
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
      .filter((job) => DIRECT_LOCAL_HUNT_JOB_NAMES.has(String(job.name || '')) || DISABLED_OPENCLAW_HUNT_JOB_NAMES.has(String(job.name || '')))
      .map((job) => ({ name: job.name, enabled: Boolean(job.enabled) })),
  }, null, 2));
}

main();

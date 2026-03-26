#!/usr/bin/env node
import path from 'node:path';
import { CORE_PATHS, appendJsonl, readJson, readJsonl } from './core-ledger-utils.mjs';

const DIRECT_AUTOMATION_CONFIG_PATH = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..', 'config', 'direct-automation-jobs.json');

export function loadDirectAutomationConfig() {
  return readJson(DIRECT_AUTOMATION_CONFIG_PATH, {
    schema: 'tierededge_direct_automation_jobs_v1',
    scheduler: {
      type: 'system_cron',
      timezone: 'America/Chicago',
    },
    jobs: [],
  });
}

export function readDirectAutomationRuns() {
  return readJsonl(CORE_PATHS.directAutomationRuns);
}

export function appendDirectAutomationRun(row) {
  const nextRow = {
    automation_run_id: row.automation_run_id || `${row.job_name || 'unknown'}::${row.started_at_utc || new Date().toISOString()}::${row.status || 'unknown'}`,
    scheduler_type: row.scheduler_type || 'system_cron',
    execution_mode: row.execution_mode || 'direct_local_command',
    model_backed_scheduler_in_path: false,
    ...row,
  };
  appendJsonl(
    CORE_PATHS.directAutomationRuns,
    nextRow,
    (existing) => String(existing.automation_run_id || '').trim(),
  );
  return nextRow;
}

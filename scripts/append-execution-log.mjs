#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendExecutionLogRow } from './execution-layer-utils.mjs';

const REQUIRED_FIELDS = [
  'rec_id',
  'run_id',
  'event',
  'market',
  'actual_sportsbook',
  'actual_odds',
  'actual_stake',
  'execution_approval_result',
  'bet_slip_timestamp',
];

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error('Usage: node scripts/append-execution-log.mjs \'<json-row>\'');
    process.exit(1);
  }

  let row;
  if (input.startsWith('@')) {
    row = JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  } else {
    row = JSON.parse(input);
  }

  if (!row.execution_id) {
    const recId = String(row.rec_id || 'manual');
    const ts = String(row.bet_slip_timestamp || row.logged_at_utc || new Date().toISOString());
    row.execution_id = `execution::${recId}::${ts}`;
  }
  const missing = REQUIRED_FIELDS.filter((field) => row[field] === undefined || row[field] === null || String(row[field]).trim() === '');
  if (missing.length) {
    throw new Error(`missing_required_fields:${missing.join(',')}`);
  }
  row.logged_at_utc = row.logged_at_utc || new Date().toISOString();
  row.manual_override_flag = Boolean(row.manual_override_flag);
  if ((row.manual_override_flag || String(row.execution_approval_result || '').trim() === 'REJECT_EXECUTION')
    && !String(row.override_justification || row.freeform_justification || row.override_reason || '').trim()) {
    throw new Error('missing_override_justification');
  }
  appendExecutionLogRow(row);
  console.log(`Appended execution log row: ${row.execution_id}`);
}

main().catch((error) => {
  console.error(`append-execution-log failed: ${error.message}`);
  process.exit(1);
});

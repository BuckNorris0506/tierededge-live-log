#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendJsonl, CORE_PATHS } from './core-ledger-utils.mjs';

const REQUIRED_FIELDS = ['entry_id', 'entry_type'];

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error('Usage: node scripts/append-bankroll-event.mjs \'<json-row-or-json-array>\'');
    process.exit(1);
  }

  let parsed;
  if (input.startsWith('@')) {
    parsed = JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  } else {
    parsed = JSON.parse(input);
  }

  const rows = Array.isArray(parsed) ? parsed : [parsed];
  for (const row of rows) {
    if (!row.entry_id && row.id) row.entry_id = row.id;
    const missing = REQUIRED_FIELDS.filter((field) => row[field] === undefined || row[field] === null || String(row[field]).trim() === '');
    if (missing.length) throw new Error(`missing_required_fields:${missing.join(',')}`);
  }

  appendJsonl(CORE_PATHS.bankrollLedger, rows, (entry) => String(entry.entry_id || entry.id || ''));
  console.log(`Appended bankroll events: ${rows.length}`);
}

main().catch((error) => {
  console.error(`append-bankroll-event failed: ${error.message}`);
  process.exit(1);
});

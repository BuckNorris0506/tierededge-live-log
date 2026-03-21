#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendJsonl, CORE_PATHS } from './core-ledger-utils.mjs';

const REQUIRED_FIELDS = ['entry_id', 'entry_type'];
const ALLOWED_ENTRY_TYPES = new Set(['STARTING_BANKROLL', 'CONTRIBUTION']);

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
    if (!row.entry_type && row.event_type) row.entry_type = row.event_type;
    if (!row.entry_id && row.id) row.entry_id = row.id;
    if (!row.entry_id && row.entry_type) {
      const date = String(row.date || row.timestamp_ct || 'unknown-date').trim();
      const key = String(row.key || row.rec_id || row.note || 'entry').trim().replace(/\s+/g, '_');
      row.entry_id = `bankroll::${date}::${key}`;
    }
    const missing = REQUIRED_FIELDS.filter((field) => row[field] === undefined || row[field] === null || String(row[field]).trim() === '');
    if (missing.length) throw new Error(`missing_required_fields:${missing.join(',')}`);
    row.entry_type = String(row.entry_type).trim().toUpperCase();
    if (!ALLOWED_ENTRY_TYPES.has(row.entry_type)) {
      throw new Error(`invalid_entry_type:${row.entry_type}:bankroll_ledger_only_accepts_starting_bankroll_or_contribution`);
    }
  }

  appendJsonl(CORE_PATHS.bankrollLedger, rows, (entry) => String(entry.entry_id || entry.id || ''));
  console.log(`Appended bankroll events: ${rows.length}`);
}

main().catch((error) => {
  console.error(`append-bankroll-event failed: ${error.message}`);
  process.exit(1);
});

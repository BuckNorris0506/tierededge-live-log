#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import { CORE_PATHS, appendJsonl } from './core-ledger-utils.mjs';

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2).replace(/-/g, '_');
    const next = argv[i + 1];
    if (next == null || next.startsWith('--')) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

async function readInput(input) {
  if (!input) throw new Error('Missing --input JSON or @file path.');
  if (input.startsWith('@')) {
    return JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  }
  return JSON.parse(input);
}

function normalizeArray(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value == null) return [];
  return [value].filter(Boolean);
}

async function main() {
  const args = parseArgs(process.argv);
  const parsed = await readInput(args.input);
  const row = {
    audit_id: parsed.audit_id || `hunt-audit::${parsed.run_id}::${crypto.createHash('sha1').update(JSON.stringify(parsed)).digest('hex').slice(0, 10)}`,
    audit_timestamp_utc: parsed.audit_timestamp_utc || new Date().toISOString(),
    audit_type: parsed.audit_type || 'RUN_INVALIDATED',
    run_id: String(parsed.run_id || '').trim(),
    session_id: parsed.session_id || null,
    session_path: parsed.session_path || null,
    affected_rec_ids: normalizeArray(parsed.affected_rec_ids),
    affected_decision_row_refs: normalizeArray(parsed.affected_decision_row_refs),
    invalid_status: parsed.invalid_status || 'invalid_recommendation_truth',
    reasons: normalizeArray(parsed.reasons).map((reason) => String(reason).trim()).filter(Boolean),
    notes: normalizeArray(parsed.notes).map((note) => String(note).trim()).filter(Boolean),
    preserved_history: parsed.preserved_history !== false,
    source: parsed.source || 'repo_audit',
  };

  if (!row.run_id) throw new Error('Missing run_id.');
  if (row.reasons.length === 0) throw new Error('At least one invalidation reason is required.');

  appendJsonl(CORE_PATHS.huntAuditLog, row, (existing) => existing.audit_id);
  console.log(`Appended hunt audit row: ${row.audit_id}`);
}

main().catch((error) => {
  console.error(`append-hunt-audit-log failed: ${error.message}`);
  process.exit(1);
});

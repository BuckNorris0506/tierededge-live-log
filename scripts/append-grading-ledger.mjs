#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendJsonl, CORE_PATHS, readJsonl } from './core-ledger-utils.mjs';
import { enrichGradingRowWithClv } from './grading-market-truth-utils.mjs';
import { isBankrollRelevantGrade, reconcileGradingBankrollAnnotations } from './bankroll-reconciliation-utils.mjs';

const REQUIRED_FIELDS = ['grading_id', 'grading_type', 'ref_id', 'selection', 'result', 'source'];

function synthesizeRefId(row) {
  const date = String(row.date || row.event_date || '').trim();
  const timestamp = String(row.timestamp_ct || '').trim();
  const selection = String(row.selection || row.bet || '').trim();
  if (row.rec_id) return `decision::${row.rec_id}`;
  return `betlog::${date || 'unknown-date'}::${timestamp || 'unknown-time'}::${selection || 'unknown-selection'}`;
}

function normalizeLegacyRow(row) {
  const normalized = { ...row };
  if (!normalized.date && normalized.event_date) normalized.date = normalized.event_date;
  if (!normalized.market && normalized.market_type) normalized.market = normalized.market_type;
  if (!normalized.market_type && normalized.market) normalized.market_type = normalized.market;
  if (!normalized.sportsbook && normalized.book) normalized.sportsbook = normalized.book;
  if (normalized.profit_loss === undefined && normalized.profit !== undefined) normalized.profit_loss = normalized.profit;
  if (normalized.clv_prob_delta === undefined && normalized.clv_pp !== undefined) normalized.clv_prob_delta = normalized.clv_pp;
  if (!normalized.grading_type) normalized.grading_type = 'BET';
  if (!normalized.grading_id) {
    const date = String(normalized.date || '').trim() || 'unknown-date';
    const timestamp = String(normalized.timestamp_ct || '').trim() || 'unknown-time';
    const selection = String(normalized.selection || normalized.bet || '').trim() || 'unknown-selection';
    normalized.grading_id = `bet::${date}::${timestamp}::${selection}`;
  }
  if (!normalized.ref_id) normalized.ref_id = synthesizeRefId(normalized);
  if (!normalized.source) normalized.source = 'legacy_grading_append_reconciliation';
  return normalized;
}

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error('Usage: node scripts/append-grading-ledger.mjs \'<json-row-or-json-array>\'');
    process.exit(1);
  }

  let parsed;
  if (input.startsWith('@')) {
    parsed = JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  } else {
    parsed = JSON.parse(input);
  }

  const rows = (Array.isArray(parsed) ? parsed : [parsed]).map(normalizeLegacyRow);
  for (const row of rows) {
    const missing = REQUIRED_FIELDS.filter((field) => row[field] === undefined || row[field] === null || String(row[field]).trim() === '');
    if (missing.length) {
      throw new Error(`missing_required_fields:${missing.join(',')}`);
    }
  }

  const enrichedRows = rows.map((row) => enrichGradingRowWithClv(row));
  const existingRows = readJsonl(CORE_PATHS.gradingLedger);
  const reconciled = reconcileGradingBankrollAnnotations([...existingRows, ...enrichedRows], readJsonl(CORE_PATHS.bankrollLedger));
  const annotationById = new Map(reconciled.rows.map((row) => [row.grading_id, row]));
  const finalRows = enrichedRows.map((row) => isBankrollRelevantGrade(row) ? (annotationById.get(row.grading_id) || row) : row);
  appendJsonl(CORE_PATHS.gradingLedger, finalRows, (entry) => String(entry.grading_id || ''));
  console.log(`Appended grading rows: ${rows.length}`);
}

main().catch((error) => {
  console.error(`append-grading-ledger failed: ${error.message}`);
  process.exit(1);
});

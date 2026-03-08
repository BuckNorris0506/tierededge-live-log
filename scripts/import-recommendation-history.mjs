#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import {
  appendRecommendationRows,
  DEFAULT_RECOMMENDATION_LOG,
  REQUIRED_RECOMMENDATION_COLUMNS,
  isTimestampFormatValid,
} from './recommendation-log-utils.mjs';

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === ',' && !inQuotes) {
      out.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out.map((v) => String(v).trim());
}

function parseTableMarkdown(markdown) {
  const lines = markdown
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|'));
  if (lines.length < 2) return [];
  const headers = lines[0].split('|').map((s) => s.trim()).filter(Boolean);
  const rows = [];
  for (let i = 2; i < lines.length; i += 1) {
    const parts = lines[i].split('|').map((s) => s.trim()).filter(Boolean);
    if (parts.length !== headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = parts[j];
    rows.push(row);
  }
  return rows;
}

function parseCsv(text) {
  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const values = parseCsvLine(lines[i]);
    if (values.length !== headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = values[j];
    rows.push(row);
  }
  return rows;
}

function normalizeRows(rows) {
  return rows.map((row) => {
    const normalized = {};
    for (const key of REQUIRED_RECOMMENDATION_COLUMNS) {
      normalized[key] = row[key] ?? '';
    }
    return normalized;
  });
}

function validateIncomingRows(rows) {
  const problems = [];
  for (const row of rows) {
    const recId = String(row.rec_id || '').trim();
    if (!recId) problems.push('missing rec_id');
    if (!isTimestampFormatValid(row.timestamp_ct)) problems.push(`bad timestamp for ${recId || 'unknown'}`);
  }
  return [...new Set(problems)];
}

async function loadRows(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const raw = await fs.readFile(filePath, 'utf8');
  if (ext === '.json') {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [parsed];
  }
  if (ext === '.csv') return parseCsv(raw);
  if (ext === '.md') return parseTableMarkdown(raw);
  throw new Error(`unsupported_extension:${ext}`);
}

async function main() {
  const source = process.argv[2];
  const mode = process.argv[3] || '--dry-run';
  if (!source) {
    console.error('Usage: node scripts/import-recommendation-history.mjs <file.(json|csv|md)> [--dry-run|--apply]');
    process.exit(1);
  }

  const rowsRaw = await loadRows(source);
  const normalized = normalizeRows(rowsRaw);
  const issues = validateIncomingRows(normalized);
  if (issues.length > 0) {
    throw new Error(`validation_failed:${issues.join('; ')}`);
  }

  if (mode !== '--apply') {
    console.log(`Dry run only. Parsed rows: ${normalized.length}`);
    console.log(`Source: ${source}`);
    console.log('No rows written. Re-run with --apply to append.');
    return;
  }

  const result = await appendRecommendationRows(normalized, DEFAULT_RECOMMENDATION_LOG);
  console.log(`Appended historical rows: ${result.appended}`);
}

main().catch((error) => {
  console.error(`Historical import failed: ${error.message}`);
  process.exit(1);
});

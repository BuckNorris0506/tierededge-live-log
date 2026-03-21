import fs from 'node:fs';
import path from 'node:path';

const SCRIPT_DIR = path.dirname(new URL(import.meta.url).pathname);
export const REPO_ROOT = path.resolve(SCRIPT_DIR, '..');
export const DATA_DIR = path.resolve(REPO_ROOT, 'data');

export const CORE_PATHS = {
  decisionLedger: path.join(DATA_DIR, 'decision-ledger.jsonl'),
  gradingLedger: path.join(DATA_DIR, 'grading-ledger.jsonl'),
  bankrollLedger: path.join(DATA_DIR, 'bankroll-ledger.jsonl'),
  huntAuditLog: path.join(DATA_DIR, 'hunt-audit-log.jsonl'),
  canonicalHuntRun: path.join(DATA_DIR, 'canonical-hunt-run.json'),
  canonicalState: path.join(DATA_DIR, 'canonical-state.json'),
  runtimeStatus: path.join(DATA_DIR, 'openclaw-runtime-status.json'),
  publicData: path.join(REPO_ROOT, 'public', 'data.json'),
};

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

export function readJson(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

export function writeJson(filePath, value) {
  ensureDir(filePath);
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

export function readJsonl(filePath) {
  if (!fs.existsSync(filePath)) return [];
  return fs.readFileSync(filePath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

export function writeJsonl(filePath, rows) {
  ensureDir(filePath);
  const body = rows.map((row) => JSON.stringify(row)).join('\n');
  fs.writeFileSync(filePath, body ? `${body}\n` : '', 'utf8');
}

export function appendJsonl(filePath, row, dedupeKeyFn = null) {
  const rows = readJsonl(filePath);
  const nextRows = Array.isArray(row) ? row : [row];
  if (dedupeKeyFn) {
    const existingKeys = new Set(rows.map((existing) => dedupeKeyFn(existing)));
    const newKeys = new Set();
    for (const nextRow of nextRows) {
      const nextKey = dedupeKeyFn(nextRow);
      if (existingKeys.has(nextKey) || newKeys.has(nextKey)) {
        throw new Error(`duplicate_row:${nextKey}`);
      }
      newKeys.add(nextKey);
    }
  }
  rows.push(...nextRows);
  writeJsonl(filePath, rows);
}

export function parseNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const cleaned = String(value).replace(/[^0-9.-]/g, '');
  if (!cleaned || cleaned === '-' || cleaned === '.' || cleaned === '-.') return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

export function round2(value) {
  return Number.isFinite(value) ? Math.round(value * 100) / 100 : null;
}

export function parsePercent(value) {
  const num = parseNumber(value);
  return num === null ? null : num;
}

export function formatMoney(value) {
  return Number.isFinite(value) ? `$${value.toFixed(2)}` : null;
}

export function extractDateKey(value) {
  const match = String(value || '').match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

export function toCtIsoDate(dateInput) {
  const date = new Date(dateInput);
  if (Number.isNaN(date.getTime())) return null;
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

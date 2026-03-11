#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';

const LEDGER_PATH = path.resolve(process.cwd(), 'data', 'bankroll-contributions.csv');
const REQUIRED_HEADERS = [
  'contribution_date',
  'effective_month',
  'contribution_amount',
  'basis_months_used',
  'realized_profit_values_used',
  'rolling_average_profit',
  'notes',
];

function parseArg(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1 || idx + 1 >= process.argv.length) return '';
  return String(process.argv[idx + 1] || '').trim();
}

function csvEscape(value) {
  const text = String(value ?? '');
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function parseCsvLine(line) {
  const out = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out;
}

function parseNumber(text) {
  const clean = String(text || '').replace(/[^0-9.-]/g, '');
  if (!clean || clean === '-' || clean === '.' || clean === '-.') return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '').trim());
}

function validMonth(value) {
  return /^\d{4}-\d{2}$/.test(String(value || '').trim());
}

async function ensureLedger() {
  try {
    await fs.access(LEDGER_PATH);
  } catch {
    await fs.mkdir(path.dirname(LEDGER_PATH), { recursive: true });
    await fs.writeFile(LEDGER_PATH, `${REQUIRED_HEADERS.join(',')}\n`, 'utf8');
  }
}

async function loadRows() {
  const raw = await fs.readFile(LEDGER_PATH, 'utf8');
  const lines = raw.split('\n').map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0) return { headers: REQUIRED_HEADERS, rows: [] };
  const headers = parseCsvLine(lines[0]).map((h) => String(h || '').trim());
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length < headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = cols[j];
    rows.push(row);
  }
  return { headers, rows };
}

async function main() {
  await ensureLedger();
  const contributionDate = parseArg('--contribution-date');
  const effectiveMonth = parseArg('--effective-month');
  const contributionAmount = parseArg('--contribution-amount');
  const basisMonthsUsed = parseArg('--basis-months-used');
  const realizedProfitValuesUsed = parseArg('--realized-profit-values-used');
  const rollingAverageProfit = parseArg('--rolling-average-profit');
  const notes = parseArg('--notes');

  if (!validDate(contributionDate)) {
    throw new Error('invalid contribution_date, expected YYYY-MM-DD');
  }
  if (!validMonth(effectiveMonth)) {
    throw new Error('invalid effective_month, expected YYYY-MM');
  }
  if (parseNumber(contributionAmount) === null) {
    throw new Error('invalid contribution_amount');
  }
  if (parseNumber(basisMonthsUsed) === null) {
    throw new Error('invalid basis_months_used');
  }
  if (parseNumber(rollingAverageProfit) === null) {
    throw new Error('invalid rolling_average_profit');
  }

  const { headers, rows } = await loadRows();
  const missing = REQUIRED_HEADERS.filter((h) => !headers.includes(h));
  if (missing.length > 0) throw new Error(`missing_required_headers:${missing.join(',')}`);

  const duplicate = rows.find((row) =>
    String(row.contribution_date).trim() === contributionDate
    && String(row.effective_month).trim() === effectiveMonth
  );
  if (duplicate) throw new Error(`duplicate_entry:${contributionDate}:${effectiveMonth}`);

  const lastDate = rows.length > 0 ? String(rows[rows.length - 1].contribution_date || '').trim() : null;
  if (lastDate && contributionDate < lastDate) {
    throw new Error(`out_of_order_append:${contributionDate}<${lastDate}`);
  }

  const row = {
    contribution_date: contributionDate,
    effective_month: effectiveMonth,
    contribution_amount: parseNumber(contributionAmount).toFixed(2),
    basis_months_used: String(Math.trunc(parseNumber(basisMonthsUsed))),
    realized_profit_values_used: realizedProfitValuesUsed || '[]',
    rolling_average_profit: parseNumber(rollingAverageProfit).toFixed(2),
    notes: notes || '',
  };
  const line = `${headers.map((h) => csvEscape(row[h] ?? '')).join(',')}\n`;
  await fs.appendFile(LEDGER_PATH, line, 'utf8');
  console.log(`Appended contribution entry: ${contributionDate} ${effectiveMonth} $${row.contribution_amount}`);
}

main().catch((error) => {
  console.error(`append-bankroll-contribution failed: ${error.message}`);
  process.exit(1);
});

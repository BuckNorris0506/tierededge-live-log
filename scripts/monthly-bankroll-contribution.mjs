#!/usr/bin/env node
import fs from 'node:fs/promises';
import fssync from 'node:fs';
import path from 'node:path';
import { CORE_PATHS, readJsonl, appendJsonl } from './core-ledger-utils.mjs';

const ROOT = process.cwd();
const LEDGER_PATH = CORE_PATHS.bankrollLedger;
const STATUS_PATH = path.resolve(ROOT, 'data', 'bankroll-contribution-status.json');

function nowCtDateParts() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));
  return { year: map.year, month: map.month, day: map.day };
}

function currentCtIso() {
  return new Date().toLocaleString('sv-SE', { timeZone: 'America/Chicago', hour12: false }).replace(' ', 'T');
}

function parseNumber(text) {
  const clean = String(text || '').replace(/[^0-9.-]/g, '');
  if (!clean || clean === '-' || clean === '.' || clean === '-.') return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

function round2(value) {
  return Number.isFinite(value) ? Math.round(value * 100) / 100 : null;
}

function parseMonthKey(dateText) {
  const match = String(dateText || '').match(/^(\d{4})-(\d{2})/);
  return match ? `${match[1]}-${match[2]}` : null;
}

function monthToIndex(monthKey) {
  const match = String(monthKey || '').match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  return (Number(match[1]) * 12) + (Number(match[2]) - 1);
}

function indexToMonth(index) {
  if (!Number.isFinite(index)) return null;
  const year = Math.floor(index / 12);
  const month = (index % 12) + 1;
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}`;
}

function previousMonth(monthKey) {
  const idx = monthToIndex(monthKey);
  return Number.isFinite(idx) ? indexToMonth(idx - 1) : null;
}

function monthsInclusive(startMonth, endMonth) {
  const start = monthToIndex(startMonth);
  const end = monthToIndex(endMonth);
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end) return [];
  const out = [];
  for (let i = start; i <= end; i += 1) out.push(indexToMonth(i));
  return out;
}

async function loadContributionLedger() {
  if (!fssync.existsSync(LEDGER_PATH)) return [];
  return readJsonl(LEDGER_PATH).filter((row) => row.entry_type === 'CONTRIBUTION');
}

async function writeStatus(status) {
  await fs.mkdir(path.dirname(STATUS_PATH), { recursive: true });
  await fs.writeFile(STATUS_PATH, `${JSON.stringify(status, null, 2)}\n`, 'utf8');
}

async function main() {
  const force = process.argv.includes('--force');
  const { year, month, day } = nowCtDateParts();
  const effectiveMonth = `${year}-${month}`;
  const contributionDate = `${year}-${month}-${day}`;
  const priorCompletedMonth = previousMonth(effectiveMonth);
  const nextExpectedCycle = `${indexToMonth(monthToIndex(effectiveMonth) + 1)}-01`;

  if (!force && day !== '01') {
    const status = {
      status: 'skipped',
      reason: 'not_first_day_of_month',
      last_run_ct: currentCtIso(),
      effective_month: effectiveMonth,
      appended: false,
      next_expected_cycle: nextExpectedCycle,
    };
    await writeStatus(status);
    console.log('Skipped: not first day of month.');
    return;
  }

  const existingRows = await loadContributionLedger();
  const duplicate = existingRows.find((row) => String(row.effective_month || '').trim() === effectiveMonth);
  if (duplicate) {
    const status = {
      status: 'skipped',
      reason: `duplicate_effective_month:${effectiveMonth}`,
      last_run_ct: currentCtIso(),
      effective_month: effectiveMonth,
      appended: false,
      next_expected_cycle: nextExpectedCycle,
    };
    await writeStatus(status);
    console.log(`No-op: contribution already exists for ${effectiveMonth}.`);
    return;
  }

  const gradingRows = readJsonl(CORE_PATHS.gradingLedger).filter((row) => row.grading_type === 'BET');
  const realizedByMonth = {};
  for (const row of gradingRows) {
    const result = String(row.result || '').trim().toLowerCase();
    if (!result || result === 'pending') continue;
    const pl = parseNumber(row.profit_loss);
    const monthKey = parseMonthKey(row.date);
    if (pl === null || !monthKey) continue;
    realizedByMonth[monthKey] = (realizedByMonth[monthKey] || 0) + pl;
  }

  const knownMonths = [
    ...Object.keys(realizedByMonth),
    ...readJsonl(CORE_PATHS.bankrollLedger).map((row) => parseMonthKey(row.contribution_date || row.date)).filter(Boolean),
  ].sort();
  const firstKnownMonth = knownMonths[0];
  if (!priorCompletedMonth || !firstKnownMonth) {
    const status = {
      status: 'failed',
      reason: 'missing_source_data_for_completed_months',
      last_run_ct: currentCtIso(),
      effective_month: effectiveMonth,
      appended: false,
      next_expected_cycle: nextExpectedCycle,
    };
    await writeStatus(status);
    throw new Error('Missing source data for completed months.');
  }

  const completedMonths = monthsInclusive(firstKnownMonth, priorCompletedMonth);
  if (completedMonths.length === 0) {
    const status = {
      status: 'failed',
      reason: 'no_completed_months_available',
      last_run_ct: currentCtIso(),
      effective_month: effectiveMonth,
      appended: false,
      next_expected_cycle: nextExpectedCycle,
    };
    await writeStatus(status);
    throw new Error('No completed months available for contribution basis.');
  }

  const basisMonthCount = Math.min(3, completedMonths.length);
  const basisMonths = completedMonths.slice(-basisMonthCount);
  const realizedValues = basisMonths.map((m) => round2(realizedByMonth[m] || 0));
  const rollingAverage = round2(realizedValues.reduce((a, b) => a + b, 0) / realizedValues.length);
  const contributionAmount = round2(Math.max(0, rollingAverage));

  const notes = `${basisMonthCount}-month rolling average contribution`;
  appendJsonl(LEDGER_PATH, [{
    entry_id: `bankroll::${contributionDate}::${effectiveMonth}`,
    entry_type: 'CONTRIBUTION',
    amount: contributionAmount,
    contribution_date: contributionDate,
    effective_month: effectiveMonth,
    basis_month_count: basisMonthCount,
    basis_months_used: basisMonths,
    realized_profit_values_used: realizedValues,
    rolling_average_realized_profit: rollingAverage,
    entry_source: 'auto_monthly_policy',
    notes,
    created_at_ct: currentCtIso(),
  }], (entry) => String(entry.entry_id || entry.id || ''));

  const status = {
    status: 'success',
    reason: 'appended',
    last_run_ct: currentCtIso(),
    effective_month: effectiveMonth,
    appended: true,
    contribution_date: contributionDate,
    contribution_amount: contributionAmount,
    basis_month_count: basisMonthCount,
    basis_months_used: basisMonths,
    realized_profit_values_used: realizedValues,
    rolling_average_realized_profit: rollingAverage,
    entry_source: 'auto_monthly_policy',
    next_expected_cycle: `${indexToMonth(monthToIndex(effectiveMonth) + 1)}-01`,
  };
  await writeStatus(status);
  console.log(`Appended monthly contribution for ${effectiveMonth}: $${contributionAmount.toFixed(2)}`);
}

main().catch(async (error) => {
  try {
    const { year, month } = nowCtDateParts();
    await writeStatus({
      status: 'failed',
      reason: error.message,
      last_run_ct: currentCtIso(),
      effective_month: `${year}-${month}`,
      appended: false,
      next_expected_cycle: null,
    });
  } catch {}
  console.error(`monthly-bankroll-contribution failed: ${error.message}`);
  process.exit(1);
});

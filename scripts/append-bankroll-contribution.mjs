#!/usr/bin/env node
import { appendJsonl, CORE_PATHS, parseNumber, toCtIsoDate } from './core-ledger-utils.mjs';

function parseArg(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1 || idx + 1 >= process.argv.length) return '';
  return String(process.argv[idx + 1] || '').trim();
}

function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || '').trim());
}

function validMonth(value) {
  return /^\d{4}-\d{2}$/.test(String(value || '').trim());
}

async function main() {
  const contributionDate = parseArg('--contribution-date');
  const effectiveMonth = parseArg('--effective-month');
  const contributionAmount = parseArg('--contribution-amount');
  const basisMonthsUsed = parseArg('--basis-months-used');
  const basisMonthCount = parseArg('--basis-month-count');
  const realizedProfitValuesUsed = parseArg('--realized-profit-values-used');
  const rollingAverageRealizedProfit = parseArg('--rolling-average-realized-profit');
  const entrySource = parseArg('--entry-source');
  const notes = parseArg('--notes');

  if (!validDate(contributionDate)) throw new Error('invalid contribution_date, expected YYYY-MM-DD');
  if (!validMonth(effectiveMonth)) throw new Error('invalid effective_month, expected YYYY-MM');
  if (parseNumber(contributionAmount) === null) throw new Error('invalid contribution_amount');
  if (parseNumber(basisMonthCount) === null) throw new Error('invalid basis_month_count');
  if (parseNumber(rollingAverageRealizedProfit) === null) throw new Error('invalid rolling_average_realized_profit');
  if (!entrySource) throw new Error('missing entry_source');

  appendJsonl(CORE_PATHS.bankrollLedger, [{
    entry_id: `bankroll::${contributionDate}::${effectiveMonth}`,
    entry_type: 'CONTRIBUTION',
    amount: Number(parseNumber(contributionAmount).toFixed(2)),
    contribution_date: contributionDate,
    effective_month: effectiveMonth,
    basis_month_count: Math.trunc(parseNumber(basisMonthCount)),
    basis_months_used: basisMonthsUsed ? basisMonthsUsed.split(',').map((value) => value.trim()).filter(Boolean) : [],
    realized_profit_values_used: realizedProfitValuesUsed || '[]',
    rolling_average_realized_profit: Number(parseNumber(rollingAverageRealizedProfit).toFixed(2)),
    entry_source: entrySource,
    notes: notes || '',
    created_at_ct: toCtIsoDate(),
  }], (entry) => String(entry.entry_id || entry.id || ''));

  console.log(`Appended bankroll contribution: ${contributionDate} ${effectiveMonth} $${Number(parseNumber(contributionAmount).toFixed(2))}`);
}

main().catch((error) => {
  console.error(`append-bankroll-contribution failed: ${error.message}`);
  process.exit(1);
});

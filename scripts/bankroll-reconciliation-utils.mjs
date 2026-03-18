import { parseNumber, round2 } from './core-ledger-utils.mjs';

export const FINAL_BANKROLL_STATUSES = new Set([
  'win',
  'loss',
  'void',
  'push',
  'cashed_out',
  'cashed out',
  'cash out',
  'partial_cashout',
  'partial cashout',
]);

function normalize(value) {
  return String(value || '').trim().toLowerCase();
}

export function isBankrollRelevantGrade(row) {
  const type = String(row?.grading_type || '').trim().toUpperCase();
  const status = normalize(row?.settlement_status || row?.result);
  return (type === 'BET' || type === 'RECONCILIATION') && FINAL_BANKROLL_STATUSES.has(status);
}

export function computeFinalProfitLoss(row) {
  const explicit = parseNumber(row?.profit_loss);
  if (explicit !== null) return explicit;
  const status = normalize(row?.settlement_status || row?.result);
  const stake = parseNumber(row?.actual_stake || row?.stake);
  const payout = parseNumber(row?.settlement_payout || row?.payout || row?.return_amount);
  if (stake === null) return null;
  if (status === 'loss') return round2(-stake);
  if (status === 'push' || status === 'void') return 0;
  if (['win', 'cashed_out', 'cashed out', 'cash out', 'partial_cashout', 'partial cashout'].includes(status) && payout !== null) {
    return round2(payout - stake);
  }
  return null;
}

function sortKey(row) {
  const date = String(row?.date || '').trim();
  const time = String(row?.timestamp_ct || '').trim() || '00:00';
  return `${date} ${time}`;
}

export function getLatestBankrollAnnotatedGrade(gradingRows) {
  return gradingRows
    .filter((row) => isBankrollRelevantGrade(row) && parseNumber(row.bankroll_after) !== null)
    .map((row, index) => ({ row, index }))
    .sort((a, b) => {
      const cmp = sortKey(a.row).localeCompare(sortKey(b.row));
      return cmp !== 0 ? cmp : a.index - b.index;
    })
    .at(-1)?.row || null;
}

export function reconcileGradingBankrollAnnotations(gradingRows, bankrollRows, options = {}) {
  const reconciledAtUtc = options.reconciledAtUtc || new Date().toISOString();
  const startingBankroll = round2(
    bankrollRows
      .filter((row) => normalize(row.entry_type) === 'starting_bankroll')
      .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0)
  ) || 0;

  const contributionEvents = bankrollRows
    .filter((row) => normalize(row.entry_type) === 'contribution')
    .map((row) => ({
      kind: 'bankroll',
      date: row.date || '',
      timestamp_ct: '00:00',
      amount: parseNumber(row.amount) || 0,
      entry_id: row.entry_id || null,
    }));

  const gradeEvents = gradingRows
    .map((row, index) => ({ row, index }))
    .filter(({ row }) => isBankrollRelevantGrade(row))
    .map(({ row, index }) => ({
      kind: 'grade',
      date: row.date || '',
      timestamp_ct: row.timestamp_ct || '00:00',
      index,
      grading_id: row.grading_id || null,
      profit_loss: computeFinalProfitLoss(row),
    }));

  const events = [...contributionEvents, ...gradeEvents].sort((a, b) => {
    const cmp = `${a.date} ${a.timestamp_ct}`.localeCompare(`${b.date} ${b.timestamp_ct}`);
    if (cmp !== 0) return cmp;
    if (a.kind !== b.kind) return a.kind === 'bankroll' ? -1 : 1;
    return (a.index || 0) - (b.index || 0);
  });

  let running = startingBankroll;
  const annotations = new Map();
  const trace = [
    {
      date: bankrollRows.find((row) => normalize(row.entry_type) === 'starting_bankroll')?.date || null,
      timestamp_ct: '00:00',
      event_type: 'STARTING_BANKROLL',
      source_file: 'bankroll-ledger',
      entry_id: bankrollRows.find((row) => normalize(row.entry_type) === 'starting_bankroll')?.entry_id || null,
      bankroll_before: 0,
      bankroll_after: startingBankroll,
      included_in_derived_bankroll: true,
      included_in_displayed_bankroll: false,
    },
  ];

  for (const event of events) {
    const before = running;
    if (event.kind === 'bankroll') {
      running = round2(running + (event.amount || 0)) || 0;
      trace.push({
        date: event.date,
        timestamp_ct: event.timestamp_ct,
        event_type: 'CONTRIBUTION',
        source_file: 'bankroll-ledger',
        entry_id: event.entry_id,
        bankroll_before: before,
        bankroll_after: running,
        realized_pl_contribution: event.amount || 0,
        included_in_derived_bankroll: true,
        included_in_displayed_bankroll: false,
      });
      continue;
    }

    const profitLoss = Number.isFinite(event.profit_loss) ? event.profit_loss : 0;
    running = round2(running + profitLoss) || 0;
    annotations.set(event.grading_id, {
      bankroll_before: before,
      bankroll_after: running,
      bankroll_after_source: 'deterministic_ledger_reconciliation',
      bankroll_after_reconciled_at_utc: reconciledAtUtc,
      realized_pl_contribution: profitLoss,
    });
    trace.push({
      date: event.date,
      timestamp_ct: event.timestamp_ct,
      event_type: 'GRADE',
      source_file: 'grading-ledger',
      grading_id: event.grading_id,
      bankroll_before: before,
      bankroll_after: running,
      realized_pl_contribution: profitLoss,
      included_in_derived_bankroll: true,
      included_in_displayed_bankroll: true,
    });
  }

  const rows = gradingRows.map((row) => {
    const annotation = annotations.get(row.grading_id);
    if (!annotation) return row;
    const next = { ...row };
    const current = parseNumber(row.bankroll_after);
    if (current !== null && Math.abs(current - annotation.bankroll_after) > 0.009 && next.legacy_bankroll_after_import === undefined) {
      next.legacy_bankroll_after_import = current;
    }
    next.bankroll_before = annotation.bankroll_before;
    next.bankroll_after = annotation.bankroll_after;
    next.bankroll_after_source = annotation.bankroll_after_source;
    next.bankroll_after_reconciled_at_utc = annotation.bankroll_after_reconciled_at_utc;
    return next;
  });

  return {
    rows,
    trace,
    ending_bankroll: running,
    annotated_count: annotations.size,
  };
}

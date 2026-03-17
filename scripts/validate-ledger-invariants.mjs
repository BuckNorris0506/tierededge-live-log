#!/usr/bin/env node
import path from 'node:path';
import { CORE_PATHS, parseNumber, readJson, readJsonl, round2, writeJson } from './core-ledger-utils.mjs';
import { readExecutionLog } from './execution-layer-utils.mjs';

const FINAL_STATUSES = new Set(['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout']);
const ALLOWED_BANKROLL_ENTRY_TYPES = new Set(['STARTING_BANKROLL', 'CONTRIBUTION']);
const DEFAULT_OUTPUT_PATH = path.join(path.dirname(CORE_PATHS.canonicalState), 'ledger-validator.json');

function normalize(value) {
  return String(value || '').trim().toLowerCase();
}

function parseArgs(argv) {
  return {
    requireOutputMatch: argv.includes('--require-output-match'),
    outputPath: (() => {
      const idx = argv.indexOf('--output');
      return idx !== -1 && argv[idx + 1] ? argv[idx + 1] : DEFAULT_OUTPUT_PATH;
    })(),
  };
}

function issue(kind, details = {}) {
  return { kind, ...details };
}

function computeFinalProfitLoss(row) {
  const explicit = parseNumber(row.profit_loss);
  if (explicit !== null) return explicit;
  const status = normalize(row.settlement_status || row.result);
  const stake = parseNumber(row.actual_stake || row.stake);
  const payout = parseNumber(row.settlement_payout || row.payout || row.return_amount);
  if (stake === null) return null;
  if (status === 'loss') return round2(-stake);
  if (status === 'push' || status === 'void') return 0;
  if ((status === 'win' || status === 'cashed_out' || status === 'partial_cashout') && payout !== null) {
    return round2(payout - stake);
  }
  return null;
}

function chooseRunClassification(failureClasses) {
  if (failureClasses.includes('invalid_bankroll_math')) return 'invalid_bankroll_math';
  if (failureClasses.includes('open_risk_mismatch')) return 'open_risk_mismatch';
  if (failureClasses.includes('duplicate_final_grade')) return 'duplicate_final_grade';
  if (failureClasses.includes('unresolved_execution_state')) return 'unresolved_execution_state';
  if (failureClasses.includes('missing_execution_id')) return 'missing_execution_id';
  if (failureClasses.includes('duplicate_execution_id')) return 'duplicate_execution_id';
  if (failureClasses.includes('missing_grading_link')) return 'missing_grading_link';
  if (failureClasses.includes('orphaned_bankroll_event')) return 'orphaned_bankroll_event';
  return failureClasses[0] || 'ledger_integrity_failure';
}

export function validateLedgerInvariants({ requireOutputMatch = false, outputPath = DEFAULT_OUTPUT_PATH } = {}) {
  const executions = readExecutionLog();
  const gradingRows = readJsonl(CORE_PATHS.gradingLedger);
  const bankrollRows = readJsonl(CORE_PATHS.bankrollLedger);
  const canonicalState = readJson(CORE_PATHS.canonicalState, null);
  const publicData = readJson(CORE_PATHS.publicData, null);
  const issues = [];

  const executionIds = new Map();
  for (const row of executions) {
    const id = String(row.execution_id || '').trim();
    if (!id) {
      issues.push(issue('missing_execution_id', { row }));
      continue;
    }
    executionIds.set(id, (executionIds.get(id) || 0) + 1);
  }
  for (const [executionId, count] of executionIds.entries()) {
    if (count > 1) issues.push(issue('duplicate_execution_id', { execution_id: executionId, count }));
  }

  const finalGradesByExecution = new Map();
  const finalGradesByRecId = new Map();
  for (const row of gradingRows) {
    const gradingId = String(row.grading_id || '').trim();
    if (!gradingId) continue;
    const gradingType = String(row.grading_type || '').trim().toUpperCase();
    const status = normalize(row.settlement_status || row.result);
    const isFinal = gradingType === 'RECONCILIATION'
      ? FINAL_STATUSES.has(status)
      : (gradingType === 'BET' && FINAL_STATUSES.has(status));
    if (!isFinal) continue;

    const executionId = String(row.execution_log_id || row.execution_id || '').trim();
    const recId = String(row.rec_id || '').trim();
    if (!executionId && !recId && gradingType === 'RECONCILIATION') {
      issues.push(issue('missing_grading_link', { grading_id: gradingId }));
      continue;
    }
    if (executionId) {
      const bucket = finalGradesByExecution.get(executionId) || [];
      bucket.push(row);
      finalGradesByExecution.set(executionId, bucket);
    }
    if (recId) {
      const bucket = finalGradesByRecId.get(recId) || [];
      bucket.push(row);
      finalGradesByRecId.set(recId, bucket);
    }
  }

  for (const [executionId, rows] of finalGradesByExecution.entries()) {
    if (!executionIds.has(executionId)) {
      issues.push(issue('missing_grading_link', { execution_id: executionId, grading_ids: rows.map((row) => row.grading_id) }));
    }
    if (rows.length > 1) {
      issues.push(issue('duplicate_final_grade', { execution_id: executionId, grading_ids: rows.map((row) => row.grading_id) }));
    }
  }

  const pendingExecutions = [];
  for (const row of executions) {
    const executionId = String(row.execution_id || '').trim();
    const recId = String(row.rec_id || '').trim();
    const linkedFinalGrades = [
      ...(executionId ? (finalGradesByExecution.get(executionId) || []) : []),
      ...(recId ? (finalGradesByRecId.get(recId) || []) : []),
    ];
    const deduped = new Map(linkedFinalGrades.map((grade) => [grade.grading_id, grade]));
    if (deduped.size > 1) {
      issues.push(issue('duplicate_final_grade', { execution_id: executionId || null, rec_id: recId || null, grading_ids: Array.from(deduped.keys()) }));
      continue;
    }
    if (deduped.size === 1) continue;
    pendingExecutions.push(row);
  }

  const bankrollIds = new Map();
  for (const row of bankrollRows) {
    const entryId = String(row.entry_id || row.id || '').trim();
    if (!entryId) {
      issues.push(issue('orphaned_bankroll_event', { reason: 'missing_entry_id', row }));
      continue;
    }
    bankrollIds.set(entryId, (bankrollIds.get(entryId) || 0) + 1);
    if (!ALLOWED_BANKROLL_ENTRY_TYPES.has(String(row.entry_type || '').trim().toUpperCase())) {
      issues.push(issue('orphaned_bankroll_event', { reason: 'unknown_entry_type', entry_id: entryId, entry_type: row.entry_type || null }));
    }
  }
  for (const [entryId, count] of bankrollIds.entries()) {
    if (count > 1) issues.push(issue('orphaned_bankroll_event', { reason: 'duplicate_entry_id', entry_id: entryId, count }));
  }

  const startingBankroll = round2(bankrollRows.filter((row) => normalize(row.entry_type) === 'starting_bankroll').reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0)) || 0;
  const contributions = round2(bankrollRows.filter((row) => normalize(row.entry_type) === 'contribution').reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0)) || 0;
  const realizedProfit = round2(
    gradingRows
      .filter((row) => {
        const type = String(row.grading_type || '').toUpperCase();
        const status = normalize(row.settlement_status || row.result);
        return (type === 'BET' || type === 'RECONCILIATION') && FINAL_STATUSES.has(status);
      })
      .reduce((sum, row) => sum + (computeFinalProfitLoss(row) || 0), 0)
  ) || 0;
  const derivedBankroll = round2(startingBankroll + contributions + realizedProfit) || 0;
  const lastRecordedBankroll = round2(
    gradingRows
      .filter((row) => {
        const status = normalize(row.settlement_status || row.result);
        return FINAL_STATUSES.has(status) && parseNumber(row.bankroll_after) !== null;
      })
      .reduce((latest, row) => row, null)?.bankroll_after
      ? parseNumber(
        gradingRows
          .filter((row) => {
            const status = normalize(row.settlement_status || row.result);
            return FINAL_STATUSES.has(status) && parseNumber(row.bankroll_after) !== null;
          })
          .reduce((latest, row) => row).bankroll_after
      )
      : derivedBankroll
  ) || derivedBankroll;
  const bankrollDifference = round2(lastRecordedBankroll - derivedBankroll) || 0;
  if (Math.abs(bankrollDifference) > 0.009) {
    issues.push(issue('invalid_bankroll_math', {
      starting_bankroll: startingBankroll,
      contributions,
      realized_profit: realizedProfit,
      derived_bankroll: derivedBankroll,
      last_recorded_bankroll: lastRecordedBankroll,
      difference: bankrollDifference,
    }));
  }

  const pendingTicketCount = pendingExecutions.length;
  const totalStakeAtRisk = round2(pendingExecutions.reduce((sum, row) => sum + (parseNumber(row.actual_stake || row.stake) || 0), 0)) || 0;
  const openExposurePct = lastRecordedBankroll > 0 ? round2((totalStakeAtRisk / lastRecordedBankroll) * 100) : 0;

  if (requireOutputMatch) {
    const outputs = [
      { name: 'canonical_state', payload: canonicalState },
      { name: 'public_data', payload: publicData },
    ];
    for (const output of outputs) {
      if (!output.payload) continue;
      const summary = output.payload.open_risk_summary || {};
      const outputCount = Number(summary.pending_ticket_count ?? output.payload.pending_bets?.length ?? 0);
      const outputStake = round2(parseNumber(summary.total_stake_at_risk) || 0) || 0;
      const outputPct = round2(parseNumber(summary.open_exposure_pct_of_bankroll) || 0) || 0;
      if (outputCount !== pendingTicketCount || Math.abs(outputStake - totalStakeAtRisk) > 0.009 || Math.abs(outputPct - openExposurePct) > 0.009) {
        issues.push(issue('open_risk_mismatch', {
          output: output.name,
          expected_pending_ticket_count: pendingTicketCount,
          actual_pending_ticket_count: outputCount,
          expected_total_stake_at_risk: totalStakeAtRisk,
          actual_total_stake_at_risk: outputStake,
          expected_open_exposure_pct: openExposurePct,
          actual_open_exposure_pct: outputPct,
        }));
      }
    }
  }

  const failureClasses = Array.from(new Set(issues.map((entry) => entry.kind)));
  const result = {
    generated_at_utc: new Date().toISOString(),
    require_output_match: requireOutputMatch,
    passed: failureClasses.length === 0,
    run_classification: failureClasses.length === 0 ? 'ledger_integrity_pass' : chooseRunClassification(failureClasses),
    failure_classes: failureClasses,
    summary: {
      executions_total: executions.length,
      executions_pending: pendingTicketCount,
      final_execution_grades: Array.from(finalGradesByExecution.values()).reduce((sum, rows) => sum + rows.length, 0),
      bankroll: {
        starting_bankroll: startingBankroll,
        contributions,
        realized_profit: realizedProfit,
        derived_bankroll: derivedBankroll,
        last_recorded_bankroll: lastRecordedBankroll,
        difference: bankrollDifference,
      },
      open_risk: {
        pending_ticket_count: pendingTicketCount,
        total_stake_at_risk: totalStakeAtRisk,
        open_exposure_pct_of_bankroll: openExposurePct,
      },
    },
    issues,
  };

  writeJson(outputPath, result);
  return result;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const result = validateLedgerInvariants(args);
  console.log(JSON.stringify(result, null, 2));
  if (!result.passed) process.exit(2);
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  try {
    main();
  } catch (error) {
    console.error(`validate-ledger-invariants failed: ${error.message}`);
    process.exit(1);
  }
}

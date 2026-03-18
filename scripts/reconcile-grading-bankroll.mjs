#!/usr/bin/env node
import { CORE_PATHS, readJsonl, writeJsonl, writeJson } from './core-ledger-utils.mjs';
import { reconcileGradingBankrollAnnotations } from './bankroll-reconciliation-utils.mjs';

const TRACE_PATH = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/data/bankroll-reconciliation-trace.json';

function main() {
  const gradingRows = readJsonl(CORE_PATHS.gradingLedger);
  const bankrollRows = readJsonl(CORE_PATHS.bankrollLedger);
  const result = reconcileGradingBankrollAnnotations(gradingRows, bankrollRows);
  writeJsonl(CORE_PATHS.gradingLedger, result.rows);
  writeJson(TRACE_PATH, {
    generated_at_utc: new Date().toISOString(),
    ending_bankroll: result.ending_bankroll,
    annotated_count: result.annotated_count,
    trace: result.trace,
  });
  console.log(JSON.stringify({
    ending_bankroll: result.ending_bankroll,
    annotated_count: result.annotated_count,
    trace_path: TRACE_PATH,
  }, null, 2));
}

main();

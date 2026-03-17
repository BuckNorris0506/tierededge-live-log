#!/usr/bin/env node
import { CORE_PATHS, readJson, readJsonl } from './core-ledger-utils.mjs';
import { buildExecutionBoard } from './execution-layer-utils.mjs';

async function main() {
  const canonicalState = readJson(CORE_PATHS.canonicalState, {});
  const runtimeStatus = readJson(CORE_PATHS.runtimeStatus, {});
  const decisions = readJsonl(CORE_PATHS.decisionLedger);
  const grading = readJsonl(CORE_PATHS.gradingLedger);
  const bankroll = readJsonl(CORE_PATHS.bankrollLedger);
  const board = await buildExecutionBoard({
    canonicalState,
    runtimeStatus,
    decisions,
    grading,
    bankrollEntries: bankroll,
  });
  console.log(`Built execution board: candidates=${board.counts.candidates} approved=${board.counts.approved} rejected=${board.counts.rejected}`);
}

main().catch((error) => {
  console.error(`build-execution-board failed: ${error.message}`);
  process.exit(1);
});

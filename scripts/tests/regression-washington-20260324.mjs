#!/usr/bin/env node
import assert from 'node:assert/strict';
import { CORE_PATHS, readJsonl } from '../core-ledger-utils.mjs';
import { loadScanCoveragePolicy } from '../scan-coverage-utils.mjs';
import {
  buildConsensusBookSet,
  buildConsensusContext,
  buildMarketRows,
  buildOwnedBookSet,
  finalizeDecisions,
  normalizeBookKey,
  resolveProbabilityPipeline,
  resolveRiskControls,
} from '../run-canonical-hunt.mjs';

const TARGET_RUN_ID = 'canonical-hunt::2026-03-24::2005';
const TARGET_EVENT_ID = 'a2ed97ee6dbc9d2dfa6fb42709ed3251';
const TARGET_EVENT_LABEL = 'Washington Capitals @ St Louis Blues';
const TARGET_SCAN_TIME_CT = '2026-03-24 20:05';
const FUTURE_EVENT_START_UTC = '2026-03-25T03:30:00.000Z';
const SNAPSHOT_LAST_UPDATE_UTC = '2026-03-25T01:05:00.000Z';
const BANKROLL_SNAPSHOT = 1396.3;

function main() {
  const historicalRows = readJsonl(CORE_PATHS.decisionLedger)
    .filter((row) => row.run_id === TARGET_RUN_ID)
    .filter((row) => row.event_id === TARGET_EVENT_ID)
    .filter((row) => row.market_type === 'ML');

  const washingtonBadRow = historicalRows.find((row) => row.selection === 'Washington Capitals ML' && row.sportsbook === 'BetMGM');
  const bluesBadRow = historicalRows.find((row) => row.selection === 'St Louis Blues ML' && row.sportsbook === 'DraftKings');

  assert.ok(washingtonBadRow, 'expected historical Washington BetMGM row in decision ledger');
  assert.ok(bluesBadRow, 'expected historical St Louis DraftKings row in decision ledger');
  assert.equal(washingtonBadRow.final_decision, 'BET', 'historical row should reflect the known false-edge failure');
  assert.ok((washingtonBadRow.post_conf_edge_pct ?? 0) >= 2, 'historical false-edge row should be above live T3 threshold');

  const policy = loadScanCoveragePolicy();
  const pipeline = resolveProbabilityPipeline(policy);
  const riskControls = resolveRiskControls(policy);
  const ownedBooks = buildOwnedBookSet(policy);
  const consensusBooks = buildConsensusBookSet(policy);

  const replayEvent = {
    id: TARGET_EVENT_ID,
    away_team: 'Washington Capitals',
    home_team: 'St Louis Blues',
    commence_time: FUTURE_EVENT_START_UTC,
    bookmakers: [
      {
        key: normalizeBookKey(washingtonBadRow.sportsbook),
        title: washingtonBadRow.sportsbook,
        last_update: SNAPSHOT_LAST_UPDATE_UTC,
        markets: [
          {
            key: 'h2h',
            last_update: SNAPSHOT_LAST_UPDATE_UTC,
            outcomes: [
              {
                name: 'Washington Capitals',
                price: Number(washingtonBadRow.odds_american),
              },
            ],
          },
        ],
      },
      {
        key: normalizeBookKey(bluesBadRow.sportsbook),
        title: bluesBadRow.sportsbook,
        last_update: SNAPSHOT_LAST_UPDATE_UTC,
        markets: [
          {
            key: 'h2h',
            last_update: SNAPSHOT_LAST_UPDATE_UTC,
            outcomes: [
              {
                name: 'St Louis Blues',
                price: Number(bluesBadRow.odds_american),
              },
            ],
          },
        ],
      },
    ],
  };

  const consensusContext = buildConsensusContext({
    event: replayEvent,
    marketKey: 'h2h',
    consensusBooks,
    pipeline,
  });

  const replayRows = replayEvent.bookmakers.flatMap((bookmaker) =>
    bookmaker.markets.flatMap((market) =>
      buildMarketRows({
        sportKey: 'icehockey_nhl',
        event: replayEvent,
        bookmaker,
        market,
        consensusContext,
        pipeline,
        scanTimeCt: TARGET_SCAN_TIME_CT,
        runId: `${TARGET_RUN_ID}::regression-replay`,
        bankrollSnapshot: BANKROLL_SNAPSHOT,
        ownedBooks,
      })
    )
  );

  const finalizedRows = finalizeDecisions(replayRows, riskControls);
  const replayWashingtonRow = finalizedRows.find((row) => row.selection === 'Washington Capitals ML' && row.sportsbook === 'BetMGM');

  assert.ok(replayWashingtonRow, 'expected replay Washington row after current pipeline replay');
  assert.equal(replayEvent.id, TARGET_EVENT_ID);
  assert.equal(replayWashingtonRow.event_label, TARGET_EVENT_LABEL);

  assert.notEqual(replayWashingtonRow.final_decision, 'BET', 'replayed Washington row must never become a BET');
  assert.ok(
    replayWashingtonRow.snapshot_status !== 'valid' || (replayWashingtonRow.post_conf_edge_pct ?? 0) < 2,
    `replayed Washington row must be invalid or <2% edge, got snapshot_status=${replayWashingtonRow.snapshot_status} edge=${replayWashingtonRow.post_conf_edge_pct}`
  );
  assert.ok(
    replayWashingtonRow.post_conf_edge_pct == null || replayWashingtonRow.post_conf_edge_pct < 2,
    `replayed Washington row edge must be null or below threshold, got ${replayWashingtonRow.post_conf_edge_pct}`
  );
  assert.equal(replayWashingtonRow.rejection_stage, 'integrity_gate', 'replayed Washington row should fail before pricing');
  assert.equal(replayWashingtonRow.rejection_reason, 'invalid_snapshot', 'replayed Washington row should fail closed as invalid_snapshot');
  assert.match(
    String(replayWashingtonRow.snapshot_status || ''),
    /invalid_/,
    `replayed Washington row should preserve invalid snapshot detail, got ${replayWashingtonRow.snapshot_status}`
  );
  assert.equal(replayWashingtonRow.consensus_prob, null, 'consensus must remain null when no valid two-sided market can be reconstructed');
  assert.equal(replayWashingtonRow.devig_implied_prob, null, 'devig implied probability must remain null when no two-sided market exists');
  assert.equal(consensusContext.snapshotStatus, 'no_valid_consensus_books', 'reconstructed 20:05 snapshot should have no valid consensus books');

  console.log(`PASS regression-washington-20260324`);
  console.log(`historical_bad_row=${washingtonBadRow.selection} @ ${washingtonBadRow.odds_american} | ${washingtonBadRow.sportsbook} edge=${washingtonBadRow.post_conf_edge_pct}% decision=${washingtonBadRow.final_decision}`);
  console.log(`replay_row=${replayWashingtonRow.selection} @ ${replayWashingtonRow.odds_american} | ${replayWashingtonRow.sportsbook} snapshot_status=${replayWashingtonRow.snapshot_status} rejection_reason=${replayWashingtonRow.rejection_reason} edge=${replayWashingtonRow.post_conf_edge_pct}`);
  console.log(`consensus_snapshot_status=${consensusContext.snapshotStatus} valid_consensus_books=${consensusContext.syncedBookKeys.size}`);
}

main();

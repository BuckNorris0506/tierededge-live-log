#!/usr/bin/env node
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { CORE_PATHS, readJson } from './core-ledger-utils.mjs';

const MORNING_HUNT_FALLBACK_ID = '2766547c-e6a0-40ca-a680-972c7842579c';
const OPENCLAW_JOBS_PATH = '/Users/jaredbuckman/.openclaw/cron/jobs.json';
const LIVE_LOG_REBUILD_SCRIPT = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/update-live-log.sh';

const COMMANDS = [
  'RUN HUNT',
  'SHOW BOARD',
  'SHOW BLOCK REASON',
  'SHOW EXECUTIONS',
  'SHOW PASSES',
  'SHOW RECENT BETS',
  'HELP',
];

function normalizeCommand(value) {
  return String(value || '').trim().replace(/\s+/g, ' ').toUpperCase();
}

function loadState() {
  const payload = readJson(CORE_PATHS.publicData, null);
  if (!payload) throw new Error(`missing_public_state:${CORE_PATHS.publicData}`);
  return payload;
}

function getMorningHuntId() {
  try {
    const parsed = JSON.parse(fs.readFileSync(OPENCLAW_JOBS_PATH, 'utf8'));
    const job = (parsed.jobs || []).find((entry) => entry.name === 'morning-edge-hunt');
    return job?.id || MORNING_HUNT_FALLBACK_ID;
  } catch {
    return MORNING_HUNT_FALLBACK_ID;
  }
}

function boardText(state) {
  const decision = state.decision_payload_v1 || {};
  const current = state.current_status || {};
  const live = state.live_execution || {};
  const openRisk = state.open_risk_summary || {};
  const marketTruth = state.market_truth_summary || {};
  const accountability = state.behavioral_accountability || {};
  const lines = [];
  lines.push(`BOARD - ${decision.verdict || 'UNKNOWN'}`);
  lines.push(`Class: ${decision.run_classification || 'unknown'}`);
  lines.push(`Why: ${decision.why || 'No explanation available.'}`);
  lines.push(`Health: ${decision.system_health || 'UNKNOWN'}`);
  lines.push(`Bankroll: ${current.Bankroll || 'N/A'} | Open risk: ${openRisk.total_stake_at_risk || 'N/A'} (${openRisk.open_exposure_pct_of_bankroll || 'N/A'}) | Breaker: ${current['Circuit Breaker'] || 'N/A'}`);
  lines.push(`Pending tickets: ${openRisk.pending_ticket_count ?? 0} | Manual overrides: ${openRisk.manual_override_ticket_count ?? 0}`);
  lines.push(`Snapshot coverage: ${marketTruth.placement_snapshot?.snapshot_coverage_pct_label || 'N/A'} | CLV coverage: ${marketTruth.clv_anchor?.clv_coverage_pct_label || 'N/A'}`);
  lines.push(`Overrides this month: ${accountability.overrides?.monthly_override_count ?? 0} | Post-mortem: ${accountability.post_mortem?.current_status || 'UNKNOWN'}`);
  lines.push(`Executable: ${(live.counts || {}).approved ?? 0} approved / ${(live.counts || {}).candidates ?? 0} candidates`);
  const approved = (live.recommendations || [])
    .filter((item) => item.execution?.execution_status === 'APPROVED_TO_BET')
    .slice(0, 5);
  if (!approved.length) {
    lines.push('Top plays: none');
  } else {
    lines.push('Top plays:');
    approved.forEach((item, index) => {
      lines.push(`${index + 1}. ${item.selection} | ${item.recommended_book || 'N/A'} ${item.recommended_odds_american ?? 'N/A'} | stake $${Number(item.execution?.final_executable_stake || 0).toFixed(2)}`);
    });
  }
  return lines.join('\n');
}

function blockReasonText(state) {
  const decision = state.decision_payload_v1 || {};
  const current = state.current_status || {};
  if (decision.verdict !== 'BLOCKED') {
    return [
      'BLOCK REASON',
      `System is not blocked. Verdict: ${decision.verdict || 'UNKNOWN'}`,
      `Class: ${decision.run_classification || 'unknown'}`,
      `Bankroll: ${current.Bankroll || 'N/A'}`,
    ].join('\n');
  }
  return [
    'BLOCK REASON',
    `Class: ${decision.run_classification || 'unknown'}`,
    `Why: ${decision.why || 'No explanation available.'}`,
    `Health: ${decision.system_health || 'UNKNOWN'}`,
  ].join('\n');
}

function executionsText(state) {
  const rows = (state.live_execution?.recent_execution_log || []).slice(0, 5);
  const snapshotSummary = state.market_truth_summary?.placement_snapshot || {};
  const overrides = state.behavioral_accountability?.overrides || {};
  const lines = ['RECENT EXECUTIONS'];
  lines.push(`Snapshot coverage: ${snapshotSummary.snapshot_coverage_pct_label || 'N/A'} (${snapshotSummary.snapshot_anchored_count ?? 0}/${snapshotSummary.total_execution_rows ?? 0})`);
  lines.push(`Recent overrides: ${overrides.recent_overrides?.length ?? 0} shown in system log`);
  if (!rows.length) {
    lines.push('No execution rows logged.');
    return lines.join('\n');
  }
  rows.forEach((row, index) => {
    lines.push(`${index + 1}. ${row.selection || row.event || 'UNKNOWN'} | ${row.actual_sportsbook || 'N/A'} ${row.actual_odds || 'N/A'} | $${row.actual_stake || '0.00'} | ${row.placement_snapshot_status || 'snapshot_missing'}`);
  });
  return lines.join('\n');
}

function passesText(state) {
  const tracker = state.passed_opportunity_tracker || {};
  const passBand = (state.pass_band || []).slice(-5).reverse();
  const lines = ['PASSES'];
  lines.push(`Total: ${tracker.total_count ?? 0} | Graded: ${tracker.graded_count ?? 0} | Ungraded: ${tracker.ungraded_count ?? 0}`);
  lines.push(`Record if bet: ${tracker.record_if_bet || 'N/A'} | Net: ${tracker.net_counterfactual_pl_if_bet ?? 'N/A'}`);
  if (!passBand.length) {
    lines.push('Recent pass-band rows: none');
    return lines.join('\n');
  }
  lines.push('Recent pass-band rows:');
  passBand.forEach((row, index) => {
    lines.push(`${index + 1}. ${row.selection || 'UNKNOWN'} | edge ${row.post_conf_edge_pct ?? 'N/A'}% | ${row.rejection_reason || row.rejection_stage || 'sit'}`);
  });
  return lines.join('\n');
}

function recentBetsText(state) {
  const rows = (state.bet_log || []).slice(0, 5);
  const clvSummary = state.market_truth_summary?.clv_anchor || {};
  const postMortem = state.behavioral_accountability?.post_mortem || {};
  const lines = ['RECENT BETS'];
  lines.push(`CLV coverage: ${clvSummary.clv_coverage_pct_label || 'N/A'} (${clvSummary.clv_anchored_count ?? 0}/${clvSummary.settled_bet_count ?? 0})`);
  lines.push(`Post-mortem: ${postMortem.current_status || 'UNKNOWN'}`);
  if (!rows.length) {
    lines.push('No graded bets yet.');
    return lines.join('\n');
  }
  rows.forEach((row, index) => {
    lines.push(`${index + 1}. ${row.Bet || 'UNKNOWN'} | ${row.Book || 'N/A'} ${row['Odds (US)'] || 'N/A'} | ${row.Result || 'N/A'} | ${row['CLV Status'] || 'missing_clv_source'}`);
  });
  return lines.join('\n');
}

function helpText() {
  return [
    'TIEREDGE MOBILE COMMANDS',
    ...COMMANDS.map((command) => `- ${command}`),
    '',
    'Use exact command phrases.',
    'All responses are built from fresh canonical/public state.',
  ].join('\n');
}

function runHuntText(stateBefore) {
  const jobId = getMorningHuntId();
  const cronResult = spawnSync('openclaw', ['cron', 'run', jobId, '--expect-final', '--timeout', '120000'], { encoding: 'utf8' });
  if (cronResult.status !== 0) {
    return [
      'RUN HUNT',
      'Status: FAILED',
      'Stage: cron_run',
      `Reason: ${(cronResult.stderr || cronResult.stdout || 'OpenClaw cron run failed.').trim()}`,
      `Last known verdict: ${stateBefore.decision_payload_v1?.verdict || 'UNKNOWN'}`,
    ].join('\n');
  }

  const rebuildResult = spawnSync(LIVE_LOG_REBUILD_SCRIPT, [], { encoding: 'utf8' });
  if (rebuildResult.status !== 0) {
    return [
      'RUN HUNT',
      'Status: FAILED',
      'Stage: rebuild',
      `Reason: ${(rebuildResult.stderr || rebuildResult.stdout || 'Live-log rebuild failed.').trim()}`,
    ].join('\n');
  }

  const stateAfter = loadState();
  const decision = stateAfter.decision_payload_v1 || {};
  const current = stateAfter.current_status || {};
  const openRisk = stateAfter.open_risk_summary || {};
  return [
    'RUN HUNT',
    'Status: COMPLETE',
    `Verdict: ${decision.verdict || 'UNKNOWN'}`,
    `Class: ${decision.run_classification || 'unknown'}`,
    `Why: ${decision.why || 'No explanation available.'}`,
    `Bankroll: ${current.Bankroll || 'N/A'} | Open risk: ${openRisk.total_stake_at_risk || 'N/A'} (${openRisk.open_exposure_pct_of_bankroll || 'N/A'})`,
    `Executable: ${(stateAfter.live_execution?.counts || {}).approved ?? 0} approved / ${(stateAfter.live_execution?.counts || {}).candidates ?? 0} candidates`,
  ].join('\n');
}

function render(command, state) {
  switch (command) {
    case 'SHOW BOARD': return boardText(state);
    case 'SHOW BLOCK REASON': return blockReasonText(state);
    case 'SHOW EXECUTIONS': return executionsText(state);
    case 'SHOW PASSES': return passesText(state);
    case 'SHOW RECENT BETS': return recentBetsText(state);
    case 'HELP': return helpText();
    default: return null;
  }
}

function main() {
  const command = normalizeCommand(process.argv.slice(2).join(' '));
  if (!command) {
    console.error(helpText());
    process.exit(1);
  }
  const state = loadState();
  if (command === 'RUN HUNT') {
    console.log(runHuntText(state));
    return;
  }
  const rendered = render(command, state);
  if (!rendered) {
    console.error(helpText());
    process.exit(1);
  }
  console.log(rendered);
}

main();

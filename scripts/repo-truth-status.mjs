#!/usr/bin/env node
import { CORE_PATHS, readJson } from './core-ledger-utils.mjs';

function main() {
  const payload = readJson(CORE_PATHS.publicData, null);
  if (!payload) {
    console.error(`missing_public_state:${CORE_PATHS.publicData}`);
    process.exit(1);
  }

  const decision = payload.decision_payload_v1 || {};
  const current = payload.current_status || {};
  const ledger = payload.ledger_validation || {};
  const postMortem = payload.behavioral_accountability?.post_mortem || {};
  const trigger = postMortem.latest_trigger || null;

  const lines = [
    'TIEREDGE REPO TRUTH STATUS',
    `Verdict: ${decision.verdict || 'UNKNOWN'}`,
    `Class: ${decision.run_classification || 'unknown'}`,
    `Why: ${decision.why || 'No explanation available.'}`,
    `Health: ${decision.system_health || 'UNKNOWN'}`,
    `Bankroll: ${current.Bankroll || 'N/A'}`,
    `Open Tickets: ${current['Open Tickets'] ?? 'N/A'} | Open Exposure: ${current['Open Exposure Used'] || 'N/A'}`,
    `Ledger Validation: ${ledger.passed ? 'PASS' : 'FAIL'}`,
    `Post-Mortem Status: ${postMortem.current_status || 'UNKNOWN'}`,
  ];

  if (trigger) {
    lines.push(`Post-Mortem Trigger: ${trigger.trigger_type} at ${trigger.triggered_at_ct || 'unknown time'}`);
    lines.push(`Threshold: ${trigger.threshold_value} | Streak Value: ${trigger.streak_value}`);
    lines.push(`Supporting Bet: ${(trigger.supporting_rows || []).map((row) => row.selection).filter(Boolean).join('; ') || 'N/A'}`);
    lines.push('Missing Review Fields: trigger_id, was_this_loss_model_consistent, was_price_still_good_at_execution, clv_status_observed, process_break_detected, override_involved, emotional_state_proxy, short_freeform_note');
  }

  console.log(lines.join('\n'));
}

main();

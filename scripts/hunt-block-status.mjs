#!/usr/bin/env node
import { CORE_PATHS, readJson } from './core-ledger-utils.mjs';

export function readHuntBlockStatus() {
  const payload = readJson(CORE_PATHS.publicData, null);
  if (!payload) {
    return {
      ok: false,
      blocked: true,
      reason_class: 'missing_public_state',
      reason: `Missing canonical public state: ${CORE_PATHS.publicData}`,
      verdict: 'BLOCKED',
      post_mortem_required: false,
      payload: null,
    };
  }

  const decision = payload.decision_payload_v1 || {};
  const postMortem = payload.behavioral_accountability?.post_mortem || {};
  const blocked = String(decision.verdict || '').toUpperCase() === 'BLOCKED';

  return {
    ok: true,
    blocked,
    reason_class: decision.run_classification || 'unknown',
    reason: decision.why || 'No explanation available.',
    verdict: decision.verdict || 'UNKNOWN',
    post_mortem_required: Boolean(postMortem.required),
    post_mortem_status: postMortem.current_status || 'UNKNOWN',
    latest_trigger: postMortem.latest_trigger || null,
    payload,
  };
}

function main() {
  console.log(JSON.stringify(readHuntBlockStatus(), null, 2));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

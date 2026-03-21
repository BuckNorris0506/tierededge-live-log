#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendNativeDecisionRows } from './native-decision-log-utils.mjs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error('Usage: node scripts/append-native-decision-log.mjs \'<json-row-or-json-array>\'');
    process.exit(1);
  }

  let parsed;
  if (input.startsWith('@')) {
    parsed = JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  } else {
    parsed = JSON.parse(input);
  }

  const rows = Array.isArray(parsed) ? parsed : [parsed];
  const blockStatus = readHuntBlockStatus();
  if (blockStatus.blocked) {
    throw new Error(`hunt_blocked:${blockStatus.reason_class}:${blockStatus.reason}`);
  }
  const result = appendNativeDecisionRows(rows);
  console.log(`Appended native decision rows: all=${result.all} bets=${result.bets} passes=${result.passes} suppressed=${result.suppressed}`);
}

main().catch((error) => {
  console.error(`Native decision append failed: ${error.message}`);
  process.exit(1);
});

#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendRecommendationRows, DEFAULT_RECOMMENDATION_LOG } from './recommendation-log-utils.mjs';

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error('Usage: node scripts/append-recommendation-row.mjs \'<json-row-or-json-array>\'');
    process.exit(1);
  }

  let parsed;
  if (input.startsWith('@')) {
    parsed = JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  } else {
    parsed = JSON.parse(input);
  }

  const rows = Array.isArray(parsed) ? parsed : [parsed];
  const result = await appendRecommendationRows(rows, DEFAULT_RECOMMENDATION_LOG);
  console.log(`Appended recommendation rows: ${result.appended}`);
}

main().catch((error) => {
  console.error(`Append failed: ${error.message}`);
  process.exit(1);
});

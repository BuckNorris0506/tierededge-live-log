#!/usr/bin/env node
import fs from 'node:fs/promises';
import { appendPostMortemReview } from './behavioral-accountability-utils.mjs';

async function main() {
  const input = process.argv[2];
  if (!input) {
    console.error('Usage: node scripts/append-post-mortem-review.mjs \'<json-row>\'');
    process.exit(1);
  }

  const row = input.startsWith('@')
    ? JSON.parse(await fs.readFile(input.slice(1), 'utf8'))
    : JSON.parse(input);

  const review = appendPostMortemReview(row);
  console.log(`Appended post-mortem review: ${review.review_id}`);
}

main().catch((error) => {
  console.error(`append-post-mortem-review failed: ${error.message}`);
  process.exit(1);
});

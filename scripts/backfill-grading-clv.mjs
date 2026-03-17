#!/usr/bin/env node
import { backfillGradingClvFields } from './grading-market-truth-utils.mjs';

try {
  const result = backfillGradingClvFields();
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  console.error(`backfill-grading-clv failed: ${error.message}`);
  process.exit(1);
}

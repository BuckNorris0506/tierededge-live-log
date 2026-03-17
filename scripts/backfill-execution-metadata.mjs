#!/usr/bin/env node
import { backfillExecutionLogMetadata } from './execution-layer-utils.mjs';

try {
  const result = backfillExecutionLogMetadata();
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  console.error(`backfill-execution-metadata failed: ${error.message}`);
  process.exit(1);
}

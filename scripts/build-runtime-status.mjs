#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { buildRuntimeStatus, DEFAULT_RUNTIME_STATUS_SNAPSHOT } from './openclaw-runtime-utils.mjs';

const outPath = process.argv[2] || DEFAULT_RUNTIME_STATUS_SNAPSHOT;
const payload = buildRuntimeStatus();

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');

console.log(`Built runtime status: ${outPath}`);
console.log(`Freshness anchor: ${payload.freshness_anchor?.source || 'unknown'} | ${payload.freshness_anchor?.timestamp_ct || 'unknown'}`);
if ((payload.warnings || []).length > 0) {
  console.log(`Warnings: ${payload.warnings.join(', ')}`);
}

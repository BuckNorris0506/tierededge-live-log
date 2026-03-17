#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { CORE_PATHS, readJson, writeJson } from './core-ledger-utils.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const PUBLIC_DIR = path.join(ROOT, 'public');
const DECISION_TERMINAL = path.join(PUBLIC_DIR, 'decision-terminal.txt');
const DECISION_WHATSAPP = path.join(PUBLIC_DIR, 'decision-whatsapp.txt');
const EVENING_REPORT = path.join(PUBLIC_DIR, 'evening-grading-report.txt');

function writeText(filePath, contents) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${contents ?? ''}\n`, 'utf8');
}

function main() {
  const canonicalState = readJson(CORE_PATHS.canonicalState, null);
  if (!canonicalState) {
    console.error(`Missing canonical state: ${CORE_PATHS.canonicalState}`);
    process.exit(1);
  }

  writeJson(CORE_PATHS.publicData, canonicalState);
  writeText(DECISION_TERMINAL, canonicalState?.decision_renderers?.terminal_text || '');
  writeText(DECISION_WHATSAPP, canonicalState?.decision_renderers?.whatsapp_text || '');
  writeText(EVENING_REPORT, canonicalState?.decision_renderers?.evening_grading_report_text || '');

  console.log(`Rendered public artifacts from canonical state: ${CORE_PATHS.publicData}`);
}

main();

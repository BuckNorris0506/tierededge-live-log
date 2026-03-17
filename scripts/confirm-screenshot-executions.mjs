#!/usr/bin/env node
import { confirmScreenshotExecutionPreview, parseArgs, SCREENSHOT_PREVIEW_PATH } from './execution-screenshot-utils.mjs';

function usage() {
  console.error('Usage: node scripts/confirm-screenshot-executions.mjs --preview <preview.json> --confirm CONFIRM_ALL|1,3|preview_id');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const previewPath = args.previewPath || SCREENSHOT_PREVIEW_PATH;
  if (!args.confirm) {
    usage();
    process.exit(1);
  }
  if (String(args.confirm).toUpperCase() === 'REJECT') {
    console.log('Preview rejected. No execution-log rows appended.');
    return;
  }

  const result = confirmScreenshotExecutionPreview({
    previewPath,
    confirm: args.confirm,
  });

  console.log(`Appended rows: ${result.appended.length}`);
  console.log(`Skipped rows: ${result.skipped}`);
  for (const row of result.appended) {
    console.log(`- ${row.selection || row.event} | match_status=${row.match_status} | execution_id=${row.execution_id}`);
  }
}

main().catch((error) => {
  console.error(`confirm-screenshot-executions failed: ${error.message}`);
  process.exit(1);
});

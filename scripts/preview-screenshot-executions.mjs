#!/usr/bin/env node
import { buildScreenshotExecutionPreview, parseArgs, SCREENSHOT_PREVIEW_PATH } from './execution-screenshot-utils.mjs';

function usage() {
  console.error('Usage: node scripts/preview-screenshot-executions.mjs --image <path> [more-paths...] [--preview <preview.json>]');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.images.length) {
    usage();
    process.exit(1);
  }

  const preview = buildScreenshotExecutionPreview(args.images, args.previewPath || SCREENSHOT_PREVIEW_PATH);
  console.log(preview.preview_text);
  console.log(`\nPreview written: ${args.previewPath || SCREENSHOT_PREVIEW_PATH}`);
  console.log('Confirmation options: CONFIRM_ALL | CONFIRM_SELECTED | REJECT | EDIT_FIELDS');
}

main().catch((error) => {
  console.error(`preview-screenshot-executions failed: ${error.message}`);
  process.exit(1);
});

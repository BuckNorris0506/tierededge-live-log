#!/usr/bin/env node
import path from 'node:path';
import {
  createWhatsappSettlementPreview,
  extractImagePathsFromMessageText,
  handleWhatsappSettlementCommand,
} from './settled-ticket-screenshot-utils.mjs';

function usage() {
  console.error('Usage:');
  console.error('  node scripts/whatsapp-settled-ticket-ingestion.mjs preview --sender <sender> --image <path> [more-paths...]');
  console.error('  node scripts/whatsapp-settled-ticket-ingestion.mjs preview --sender <sender> --message-file <path>');
  console.error('  node scripts/whatsapp-settled-ticket-ingestion.mjs command --sender <sender> --command "CONFIRM ALL" [--rebuild]');
}

function parseArgs(argv) {
  const result = {
    mode: null,
    sender: 'whatsapp:self',
    images: [],
    command: '',
    messageText: '',
    messageFile: '',
    rebuild: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!result.mode && !arg.startsWith('--')) {
      result.mode = arg;
      continue;
    }
    if (arg === '--sender') {
      result.sender = argv[i + 1] || result.sender;
      i += 1;
    } else if (arg === '--image' || arg === '--images') {
      while (argv[i + 1] && !argv[i + 1].startsWith('--')) {
        result.images.push(argv[i + 1]);
        i += 1;
      }
    } else if (arg === '--command') {
      result.command = argv[i + 1] || '';
      i += 1;
    } else if (arg === '--message') {
      result.messageText = argv[i + 1] || '';
      i += 1;
    } else if (arg === '--message-file') {
      result.messageFile = argv[i + 1] || '';
      i += 1;
    } else if (arg === '--rebuild') {
      result.rebuild = true;
    }
  }

  return result;
}

async function loadMessageText(args) {
  if (args.messageText) return args.messageText;
  if (args.messageFile) {
    const { readFile } = await import('node:fs/promises');
    return readFile(args.messageFile, 'utf8');
  }
  return '';
}

function summarizePreview(previewEntry) {
  return {
    status: 'pending_confirmation',
    pending_id: previewEntry.pending_id,
    sender: previewEntry.sender_key,
    image_count: previewEntry.staged_images.length,
    item_count: previewEntry.preview.items.length,
    expires_at_utc: previewEntry.expires_at_utc,
    whatsapp_text: previewEntry.preview_text,
  };
}

async function maybeRebuild(enabled) {
  if (!enabled) return null;
  const { spawnSync } = await import('node:child_process');
  const scriptPath = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log/scripts/update-live-log.sh';
  const result = spawnSync(scriptPath, [], { encoding: 'utf8' });
  return {
    ok: result.status === 0,
    status: result.status,
    stdout: (result.stdout || '').trim(),
    stderr: (result.stderr || '').trim(),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.mode) {
    usage();
    process.exit(1);
  }

  if (args.mode === 'preview') {
    const messageText = await loadMessageText(args);
    const messageImages = extractImagePathsFromMessageText(messageText);
    const imagePaths = [...new Set([...args.images, ...messageImages].map((item) => path.resolve(item)))];
    if (!imagePaths.length) throw new Error('no_image_attachments_found');
    const previewEntry = createWhatsappSettlementPreview({ senderKey: args.sender, imagePaths });
    console.log(JSON.stringify(summarizePreview(previewEntry), null, 2));
    return;
  }

  if (args.mode === 'command') {
    if (!args.command.trim()) throw new Error('missing_command');
    const result = handleWhatsappSettlementCommand({ senderKey: args.sender, command: args.command });
    const output = { ...result };
    if (args.rebuild && result.status === 'confirmed' && result.confirmation?.appended?.length) {
      output.rebuild = await maybeRebuild(true);
    }
    console.log(JSON.stringify(output, null, 2));
    return;
  }

  throw new Error(`unsupported_mode:${args.mode}`);
}

main().catch((error) => {
  console.error(`whatsapp-settled-ticket-ingestion failed: ${error.message}`);
  process.exit(1);
});

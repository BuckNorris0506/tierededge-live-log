#!/usr/bin/env node
import { appendJsonl, CORE_PATHS, readJson, writeJson } from './core-ledger-utils.mjs';
import { commandKeyboard, dispatchOperatorCommand, latestOperatorAlertMetadata } from './operator-dispatcher.mjs';
import { fetchTelegramUpdates, sendTelegramMessage, telegramConfiguredChatId } from './telegram-alert-utils.mjs';

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2).replace(/-/g, '_');
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      args[key] = next;
      i += 1;
    } else {
      args[key] = true;
    }
  }
  return args;
}

function currentState() {
  return readJson(CORE_PATHS.telegramOperatorState, {
    offset: null,
    last_polled_at_utc: null,
  });
}

function messageText(update) {
  return String(update?.message?.text || '').trim();
}

function inboundChatId(update) {
  return String(update?.message?.chat?.id || '').trim();
}

function eventBase(update, now) {
  return {
    telegram_event_id: `telegram-operator::${now}::${update?.update_id ?? 'unknown'}`,
    inbound_timestamp_utc: now,
    telegram_update_id: update?.update_id ?? null,
    chat_id: inboundChatId(update) || null,
    raw_text: messageText(update) || null,
  };
}

async function processUpdate(update, allowedChatId, now) {
  const base = eventBase(update, now);
  const chatId = inboundChatId(update);
  const authOk = Boolean(chatId) && chatId === String(allowedChatId || '').trim();

  if (!authOk) {
    return {
      ...base,
      outbound_timestamp_utc: null,
      command: messageText(update) || null,
      auth_status: 'rejected',
      response_type: 'unauthorized',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: 'telegram',
      delivery_status: 'rejected_unauthorized',
    };
  }

  const result = dispatchOperatorCommand(messageText(update));
  const sentAt = new Date().toISOString();
  const delivery = await sendTelegramMessage(result.text, { chatId, keyboard: commandKeyboard() });
  const alertMeta = latestOperatorAlertMetadata();

  return {
    ...base,
    outbound_timestamp_utc: sentAt,
    command: result.command || messageText(update) || null,
    resolved_command: result.resolved_command || null,
    auth_status: 'accepted',
    response_type: result.response_type || 'unknown',
    run_id: result.run_id || null,
    notification_tier: result.notification_tier || null,
    duplicate_suppression_status: null,
    outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
    delivery_status: delivery.ok ? 'sent' : 'failed',
    delivery_error: delivery.ok ? null : delivery.error,
    legacy_alias_used: Boolean(result.legacy_alias_used),
    last_outbound_alert_time: alertMeta.last_outbound_alert_time,
    last_outbound_alert_type: alertMeta.last_outbound_alert_type,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const state = currentState();
  const configuredChatId = telegramConfiguredChatId();
  let updates = [];
  if (args.simulate_command) {
    updates = [{
      update_id: Number(Date.now()),
      message: {
        chat: { id: String(args.chat_id || configuredChatId || '') },
        text: String(args.simulate_command),
      },
    }];
  } else {
    const fetchResult = await fetchTelegramUpdates(Number.isFinite(Number(state.offset)) ? Number(state.offset) : null);
    if (!fetchResult.ok) {
      throw new Error(fetchResult.error || 'telegram_updates_failed');
    }
    updates = Array.isArray(fetchResult.data) ? fetchResult.data : [];
  }
  const events = [];

  for (const update of updates) {
    if (!update?.message) continue;
    const now = new Date().toISOString();
    const event = await processUpdate(update, configuredChatId, now);
    events.push(event);
  }

  if (events.length) {
    appendJsonl(CORE_PATHS.telegramOperatorEvents, events, (row) => String(row.telegram_event_id || '').trim());
  }

  const nextOffset = updates.length ? (Math.max(...updates.map((item) => Number(item.update_id) || 0)) + 1) : state.offset;
  if (!args.simulate_command) {
    writeJson(CORE_PATHS.telegramOperatorState, {
      offset: nextOffset ?? null,
      last_polled_at_utc: new Date().toISOString(),
      processed_count: (parseInt(state.processed_count || 0, 10) || 0) + events.length,
    });
  }

  if (args.json) {
    console.log(JSON.stringify({
      ok: true,
      processed_updates: events.length,
      next_offset: nextOffset ?? null,
      events,
    }, null, 2));
  }
}

await main();

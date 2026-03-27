#!/usr/bin/env node
import { appendJsonl, CORE_PATHS, readJson, writeJson } from './core-ledger-utils.mjs';
import { commandKeyboard, dispatchOperatorCommand, latestOperatorAlertMetadata, normalizeOperatorCommand, parseBetPlacedMessage, parseBetSettledMessage, resolveOperatorCommand } from './operator-dispatcher.mjs';
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
    processed_count: 0,
  });
}

function persistState(state) {
  writeJson(CORE_PATHS.telegramOperatorState, {
    offset: state.offset ?? null,
    last_polled_at_utc: new Date().toISOString(),
    processed_count: parseInt(state.processed_count || 0, 10) || 0,
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

function syntheticUpdateId() {
  return (Date.now() * 1000) + Math.floor(Math.random() * 1000);
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

  const rawMessage = messageText(update);
  const parsedBetPlaced = parseBetPlacedMessage(rawMessage);
  const parsedBetSettled = parseBetSettledMessage(rawMessage);
  const resolvedCommand = resolveOperatorCommand(normalizeOperatorCommand(rawMessage));
  let ackDelivery = null;
  let ackSentAt = null;
  const shouldAcknowledge = Boolean(parsedBetPlaced?.ok) || Boolean(parsedBetSettled?.ok) || resolvedCommand === 'RUN HUNT';
  if (shouldAcknowledge) {
    ackSentAt = new Date().toISOString();
    ackDelivery = await sendTelegramMessage('RECEIVED ⏳\nProcessing...', { chatId });
  }

  const result = dispatchOperatorCommand(rawMessage);
  const sentAt = new Date().toISOString();
  const delivery = await sendTelegramMessage(result.text, { chatId, keyboard: commandKeyboard() });
  const alertMeta = latestOperatorAlertMetadata();

  return {
    ...base,
    outbound_timestamp_utc: sentAt,
    command: result.command || messageText(update) || null,
    resolved_command: result.resolved_command || null,
    auth_status: 'accepted',
    acknowledgment_sent: Boolean(ackDelivery),
    acknowledgment_timestamp_utc: ackSentAt,
    acknowledgment_delivery_status: ackDelivery ? (ackDelivery.ok ? 'sent' : 'failed') : null,
    acknowledgment_delivery_error: ackDelivery && !ackDelivery.ok ? ackDelivery.error : null,
    response_type: result.response_type || 'unknown',
    run_id: result.run_id || null,
    notification_tier: result.notification_tier || null,
    duplicate_suppression_status: null,
    outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
    delivery_status: delivery.ok ? 'sent' : 'failed',
    delivery_error: delivery.ok ? null : delivery.error,
    delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
    legacy_alias_used: Boolean(result.legacy_alias_used),
    last_outbound_alert_time: alertMeta.last_outbound_alert_time,
    last_outbound_alert_type: alertMeta.last_outbound_alert_type,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const state = currentState();
  const configuredChatId = telegramConfiguredChatId();
  if (args.simulate_command) {
    const updates = [{
      update_id: syntheticUpdateId(),
      message: {
        chat: { id: String(args.chat_id || configuredChatId || '') },
        text: String(args.simulate_command),
      },
    }];
    const events = [];
    for (const update of updates) {
      const now = new Date().toISOString();
      const event = await processUpdate(update, configuredChatId, now);
      events.push(event);
    }
    if (events.length) {
      appendJsonl(CORE_PATHS.telegramOperatorEvents, events, (row) => String(row.telegram_event_id || '').trim());
    }
    if (args.json) {
      console.log(JSON.stringify({
        ok: true,
        processed_updates: events.length,
        next_offset: state.offset ?? null,
        events,
      }, null, 2));
    }
    return;
  }

  const deadlineMs = Date.now() + 50000;
  const sessionEvents = [];
  let sessionProcessedCount = 0;
  let nextOffset = Number.isFinite(Number(state.offset)) ? Number(state.offset) : null;

  while (Date.now() < deadlineMs) {
    const remainingSeconds = Math.max(1, Math.floor((deadlineMs - Date.now()) / 1000));
    const fetchTimeoutSeconds = Math.min(20, remainingSeconds);
    const fetchResult = await fetchTelegramUpdates(nextOffset, fetchTimeoutSeconds);
    if (!fetchResult.ok) {
      const error = new Error(fetchResult.error || 'telegram_updates_failed');
      error.transport_diagnostics = fetchResult.diagnostics || null;
      throw error;
    }

    const updates = Array.isArray(fetchResult.data) ? fetchResult.data : [];
    if (!updates.length) {
      if (Date.now() + 5000 >= deadlineMs) {
        break;
      }
      continue;
    }

    updates.sort((a, b) => (Number(a?.update_id) || 0) - (Number(b?.update_id) || 0));

    for (const update of updates) {
      const updateId = Number(update?.update_id);
      if (Number.isFinite(nextOffset) && Number.isFinite(updateId) && updateId < nextOffset) {
        continue;
      }
      if (!update?.message) {
        if (Number.isFinite(updateId)) {
          nextOffset = updateId + 1;
          state.offset = nextOffset;
          persistState(state);
        }
        continue;
      }

      const now = new Date().toISOString();
      const event = await processUpdate(update, configuredChatId, now);
      appendJsonl(CORE_PATHS.telegramOperatorEvents, event, (row) => String(row.telegram_event_id || '').trim());
      sessionEvents.push(event);
      sessionProcessedCount += 1;

      if (Number.isFinite(updateId)) {
        nextOffset = updateId + 1;
      }
      state.offset = nextOffset;
      state.processed_count = (parseInt(state.processed_count || 0, 10) || 0) + 1;
      persistState(state);
    }

    if (updates.length < 100) {
      break;
    }
  }

  if (!sessionProcessedCount) {
    persistState(state);
  }

  if (args.json) {
    console.log(JSON.stringify({
      ok: true,
      processed_updates: sessionProcessedCount,
      next_offset: nextOffset ?? null,
      events: sessionEvents,
    }, null, 2));
  }
}

main().catch((error) => {
  if (error?.transport_diagnostics) {
    console.error(JSON.stringify({
      error: error.message,
      transport_diagnostics: error.transport_diagnostics,
    }));
  } else {
    console.error(error instanceof Error ? error.message : 'telegram_operator_failed');
  }
  process.exit(1);
});

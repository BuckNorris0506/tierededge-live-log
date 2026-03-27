#!/usr/bin/env node
import { appendJsonl, CORE_PATHS, readJson, writeJson } from './core-ledger-utils.mjs';
import { appendStructuredProfitBoost, commandKeyboard, dispatchOperatorCommand, latestOperatorAlertMetadata, normalizeOperatorCommand, parseBetPlacedMessage, parseBetSettledMessage, parseProfitBoostMessage, resolveOperatorCommand } from './operator-dispatcher.mjs';
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
    pending_profit_boost_confirmation: null,
  });
}

function persistState(state) {
  writeJson(CORE_PATHS.telegramOperatorState, {
    offset: state.offset ?? null,
    last_polled_at_utc: new Date().toISOString(),
    processed_count: parseInt(state.processed_count || 0, 10) || 0,
    pending_profit_boost_confirmation: state.pending_profit_boost_confirmation || null,
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

function normalizeBookAlias(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  if (['draftkings', 'dk'].includes(raw)) return 'DraftKings';
  if (['fanduel', 'fd'].includes(raw)) return 'FanDuel';
  if (['betmgm', 'mgm'].includes(raw)) return 'BetMGM';
  if (['caesars', 'czr'].includes(raw)) return 'Caesars';
  if (['bet365'].includes(raw)) return 'bet365';
  if (['circa'].includes(raw)) return 'Circa';
  if (['betrivers', 'br'].includes(raw)) return 'BetRivers';
  return null;
}

function parseFlexibleBoostPercent(rawText) {
  const match = String(rawText || '').match(/(\d+(?:\.\d+)?)\s*(?:%|percent\b)/i);
  if (!match) return null;
  const numeric = Number(match[1]);
  return Number.isFinite(numeric) ? numeric : null;
}

function detectSportsbook(rawText) {
  const patterns = [
    { regex: /\b(draftkings|dk)\b/i, sportsbook: 'DraftKings' },
    { regex: /\b(fanduel|fd)\b/i, sportsbook: 'FanDuel' },
    { regex: /\b(betmgm|mgm)\b/i, sportsbook: 'BetMGM' },
    { regex: /\b(caesars|czr)\b/i, sportsbook: 'Caesars' },
    { regex: /\b(bet365)\b/i, sportsbook: 'bet365' },
    { regex: /\b(circa)\b/i, sportsbook: 'Circa' },
    { regex: /\b(betrivers|br)\b/i, sportsbook: 'BetRivers' },
  ];
  const matches = patterns.filter((entry) => entry.regex.test(String(rawText || '')));
  if (matches.length !== 1) return null;
  return matches[0].sportsbook;
}

function normalizeBoostScope(rawScope) {
  const text = String(rawScope || '').trim();
  if (!text) return null;
  const cleaned = text
    .replace(/\b(i\s+have|i['’]?ve\s+got|i\s+got)\b/gi, '')
    .replace(/\b(a|an)\b/gi, '')
    .replace(/\b(any)\b/gi, '')
    .replace(/\bprofit\s+boost\b/gi, '')
    .replace(/\bboost\b/gi, '')
    .replace(/\bon\s+(draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/gi, '')
    .replace(/\bfor\b/gi, '')
    .replace(/\bpercent\b/gi, '')
    .replace(/\b\d+\+\s*leg\b/gi, '')
    .replace(/\bleg\b/gi, '')
    .replace(/\bsgp\b/gi, '')
    .replace(/\bevents?\b/gi, '')
    .replace(/\bgames?\b/gi, '')
    .replace(/[%:,]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return null;

  const knownScopes = [
    ['GENERAL', /\bgeneral\b/i],
    ['MLB', /\bmlb\b|baseball|mlb event|mlb game/i],
    ['NBA', /\bnba\b|nba sgp/i],
    ['NHL', /\bnhl\b|hockey/i],
    ['NFL', /\bnfl\b|football/i],
    ['NCAAB', /\bncaab\b|cbb\b|college basketball|college bball|college hoops/i],
    ['NCAAF', /\bncaaf\b|college football/i],
    ['WNBA', /\bwnba\b/i],
    ['MLS', /\bmls\b|soccer/i],
    ['TENNIS', /\btennis\b/i],
    ['GOLF', /\bgolf\b/i],
  ];
  for (const [label, pattern] of knownScopes) {
    if (pattern.test(cleaned)) return label;
  }

  return cleaned
    .split(' ')
    .map((token) => token.toUpperCase() === token ? token : token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(' ');
}

function detectBoostScope(rawText) {
  const text = String(rawText || '').trim();
  const scopedPatterns = [
    /\bfor\s+([A-Za-z0-9+\/ -]+?)\s+on\s+(?:draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i,
    /\bon\s+(?:draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\s+for\s+([A-Za-z0-9+\/ -]+)\b/i,
    /\b(?:profit\s+boost|boost)\s+for\s+([A-Za-z0-9+\/ -]+?)\s+on\b/i,
    /\b\d+(?:\.\d+)?\s*(?:%|percent)\s+([A-Za-z0-9+\/ -]+?)\s+(?:profit\s+boost|boost)\s+on\s+(?:draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i,
    /\b(?:draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\s+\d+(?:\.\d+)?\s*(?:%|percent)\s+(?:profit\s+boost|boost)\s+for\s+([A-Za-z0-9+\/ -]+)\b/i,
    /\b([A-Z]{2,6}|MLB|NBA|NHL|NFL|NCAAB|CBB|GENERAL)\s+(?:profit\s+boost|boost)\b/i,
  ];
  for (const pattern of scopedPatterns) {
    const match = text.match(pattern);
    if (match?.[1]) return normalizeBoostScope(match[1]);
  }
  return null;
}

function parseNaturalLanguageProfitBoost(rawMessage) {
  const text = String(rawMessage || '').trim();
  if (!text) return null;
  if (normalizeOperatorCommand(text).startsWith('PROFIT BOOST')) return null;
  if (!/\bboost\b/i.test(text)) return null;
  if (!/\b(profit\s+boost|boost)\b/i.test(text)) return null;

  const sportsbook = detectSportsbook(text);
  const boostPercent = parseFlexibleBoostPercent(text);
  const scope = detectBoostScope(text);

  const missing = [];
  if (!sportsbook) missing.push('sportsbook');
  if (boostPercent === null) missing.push('boost percent');
  if (!scope) missing.push('scope');
  if (missing.length) {
    return {
      ok: false,
      reason: `missing ${missing.join(', ')}`,
      details: 'Say something like: "I have a 100% profit boost for MLB on DraftKings".',
    };
  }

  return {
    ok: true,
    payload: {
      sportsbook,
      boost_percent: boostPercent,
      scope,
      expires_raw: 'Not specified',
      expires_at_utc: null,
      status: 'ACTIVE',
    },
  };
}

function canonicalizeProfitBoostPayload(payload) {
  if (!payload || typeof payload !== 'object') return null;
  const sportsbook = normalizeBookAlias(payload.sportsbook);
  const boostPercent = Number(payload.boost_percent);
  const scope = normalizeBoostScope(payload.scope);
  const expiresRaw = String(payload.expires_raw || 'Not specified').trim() || 'Not specified';
  const status = String(payload.status || 'ACTIVE').trim().toUpperCase() || 'ACTIVE';
  return {
    sportsbook,
    boost_percent: Number.isFinite(boostPercent) ? boostPercent : null,
    scope,
    expires_raw: expiresRaw,
    expires_at_utc: payload.expires_at_utc || null,
    status,
  };
}

function parseProfitBoostEditFields(rawMessage) {
  const text = String(rawMessage || '').trim();
  if (!text) {
    return { ok: false, reason: 'missing edit fields' };
  }

  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    return { ok: false, reason: 'missing edit fields' };
  }

  const firstIsEdit = /^EDIT$/i.test(lines[0]);
  const editLines = firstIsEdit ? lines.slice(1) : lines;
  if (!editLines.length) {
    return { ok: false, reason: 'missing edit fields' };
  }

  const updates = {};
  const invalidFields = [];
  for (const line of editLines) {
    const match = line.match(/^([A-Za-z ]+):\s*(.+)$/);
    if (!match) {
      invalidFields.push(line);
      continue;
    }
    const label = match[1].trim().toLowerCase();
    const value = match[2].trim();
    if (!value) {
      invalidFields.push(line);
      continue;
    }

    if (label === 'sportsbook' || label === 'book') {
      const sportsbook = normalizeBookAlias(value);
      if (!sportsbook) {
        return { ok: false, reason: 'invalid sportsbook' };
      }
      updates.sportsbook = sportsbook;
      continue;
    }

    if (label === 'boost' || label === 'boost percent' || label === 'percent') {
      const boostPercent = parseFlexibleBoostPercent(value) ?? parseFlexibleBoostPercent(`${value}%`);
      if (boostPercent === null) {
        return { ok: false, reason: 'invalid boost percent' };
      }
      updates.boost_percent = boostPercent;
      continue;
    }

    if (label === 'scope') {
      const scope = normalizeBoostScope(value);
      if (!scope) {
        return { ok: false, reason: 'invalid scope' };
      }
      updates.scope = scope;
      continue;
    }

    if (label === 'expires' || label === 'expiration') {
      updates.expires_raw = value;
      updates.expires_at_utc = null;
      continue;
    }

    if (label === 'status') {
      const status = String(value).trim().toUpperCase();
      if (!status) {
        return { ok: false, reason: 'invalid status' };
      }
      updates.status = status;
      continue;
    }

    invalidFields.push(line);
  }

  if (invalidFields.length) {
    return { ok: false, reason: 'unrecognized edit fields', invalid_fields: invalidFields };
  }

  return { ok: true, updates };
}

function renderProfitBoostConfirmation(candidate) {
  return [
    'I parsed this:',
    '',
    'Profit Boost',
    `Sportsbook: ${candidate.sportsbook}`,
    `Boost: ${Number.isInteger(candidate.boost_percent) ? candidate.boost_percent : candidate.boost_percent.toFixed(2)}%`,
    `Scope: ${candidate.scope}`,
    `Expires: ${candidate.expires_raw || candidate.expires_at_utc || 'Not specified'}`,
    `Status: ${candidate.status || 'ACTIVE'}`,
    '',
    'Reply YES to confirm, EDIT to correct, or CANCEL to discard.',
  ].join('\n');
}

function renderProfitBoostLogged(entry) {
  return [
    'LOGGED ✅',
    '',
    `Sportsbook: ${entry.sportsbook}`,
    `Boost: ${Number.isInteger(Number(entry.boost_percent)) ? Number(entry.boost_percent) : Number(entry.boost_percent).toFixed(2)}%`,
    `Scope: ${entry.scope}`,
    `Expires: ${entry.expires_raw || entry.expires_at_utc || 'Not specified'}`,
    `Status: ${entry.status || 'ACTIVE'}`,
  ].join('\n');
}

function renderProfitBoostEditPrompt() {
  return [
    'EDIT MODE',
    'Reply with one or more labeled lines to update the pending boost.',
    'Fields:',
    'Sportsbook: DraftKings',
    'Boost: 100%',
    'Scope: MLB',
    'Expires: 2026-03-27 11:59 PM CT',
  ].join('\n');
}

function renderProfitBoostEditFailure(reason, invalidFields = []) {
  const lines = [
    'NOT LOGGED ❌',
    '',
    `Reason: ${reason}`,
  ];
  if (invalidFields.length) {
    lines.push(`Unrecognized: ${invalidFields.join(' | ')}`);
  }
  lines.push('Use labeled lines after EDIT, for example:');
  lines.push('EDIT');
  lines.push('Boost: 50%');
  lines.push('Scope: NBA');
  return lines.join('\n');
}

function renderProfitBoostCanceled() {
  return [
    'CANCELED',
    '',
    'Pending profit boost confirmation discarded.',
  ].join('\n');
}

async function processUpdate(update, allowedChatId, now, state) {
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
  const pendingBoost = state?.pending_profit_boost_confirmation;
  const pendingMatchesChat = pendingBoost && String(pendingBoost.chat_id || '') === chatId;
  const normalizedMessage = normalizeOperatorCommand(rawMessage);

  if (pendingMatchesChat && normalizedMessage === 'YES') {
    const entry = appendStructuredProfitBoost(pendingBoost.payload, {
      source: 'telegram_operator_nl_confirmed',
    });
    state.pending_profit_boost_confirmation = null;
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendTelegramMessage(renderProfitBoostLogged(entry), { chatId, keyboard: commandKeyboard() });
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: 'PROFIT BOOST',
      resolved_command: 'PROFIT BOOST',
      auth_status: 'accepted',
      acknowledgment_sent: false,
      acknowledgment_timestamp_utc: null,
      acknowledgment_delivery_status: null,
      acknowledgment_delivery_error: null,
      response_type: 'profit_boost_logged',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      legacy_alias_used: false,
      profit_boost_confirmation_status: 'confirmed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  if (pendingMatchesChat && normalizedMessage === 'EDIT') {
    state.pending_profit_boost_confirmation = {
      ...pendingBoost,
      awaiting_edit: true,
      updated_at_utc: now,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendTelegramMessage(renderProfitBoostEditPrompt(), { chatId, keyboard: commandKeyboard() });
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: 'PROFIT BOOST',
      resolved_command: 'PROFIT BOOST',
      auth_status: 'accepted',
      acknowledgment_sent: false,
      acknowledgment_timestamp_utc: null,
      acknowledgment_delivery_status: null,
      acknowledgment_delivery_error: null,
      response_type: 'profit_boost_edit_requested',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      legacy_alias_used: false,
      profit_boost_confirmation_status: 'awaiting_edit',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  if (pendingMatchesChat && normalizedMessage === 'CANCEL') {
    state.pending_profit_boost_confirmation = null;
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendTelegramMessage(renderProfitBoostCanceled(), { chatId, keyboard: commandKeyboard() });
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: 'PROFIT BOOST',
      resolved_command: 'PROFIT BOOST',
      auth_status: 'accepted',
      acknowledgment_sent: false,
      acknowledgment_timestamp_utc: null,
      acknowledgment_delivery_status: null,
      acknowledgment_delivery_error: null,
      response_type: 'profit_boost_confirmation_canceled',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      legacy_alias_used: false,
      profit_boost_confirmation_status: 'canceled',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const editFields = pendingMatchesChat
    ? parseProfitBoostEditFields(rawMessage)
    : null;
  const shouldApplyPendingEdit = Boolean(
    pendingMatchesChat && (
      /^EDIT(?:\s|$)/i.test(rawMessage)
      || pendingBoost?.awaiting_edit
    ),
  );

  if (shouldApplyPendingEdit) {
    if (!editFields?.ok) {
      const sentAt = new Date().toISOString();
      const delivery = await sendTelegramMessage(
        renderProfitBoostEditFailure(editFields?.reason || 'invalid edit', editFields?.invalid_fields || []),
        { chatId, keyboard: commandKeyboard() },
      );
      const alertMeta = latestOperatorAlertMetadata();
      return {
        ...base,
        outbound_timestamp_utc: sentAt,
        command: 'PROFIT BOOST',
        resolved_command: 'PROFIT BOOST',
        auth_status: 'accepted',
        acknowledgment_sent: false,
        acknowledgment_timestamp_utc: null,
        acknowledgment_delivery_status: null,
        acknowledgment_delivery_error: null,
        response_type: 'profit_boost_edit_rejected',
        run_id: null,
        notification_tier: null,
        duplicate_suppression_status: null,
        outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
        delivery_status: delivery.ok ? 'sent' : 'failed',
        delivery_error: delivery.ok ? null : delivery.error,
        delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
        legacy_alias_used: false,
        profit_boost_confirmation_status: 'edit_rejected',
        last_outbound_alert_time: alertMeta.last_outbound_alert_time,
        last_outbound_alert_type: alertMeta.last_outbound_alert_type,
      };
    }

    const mergedPayload = canonicalizeProfitBoostPayload({
      ...(pendingBoost?.payload || {}),
      ...editFields.updates,
    });
    state.pending_profit_boost_confirmation = {
      ...pendingBoost,
      payload: mergedPayload,
      awaiting_edit: false,
      updated_at_utc: now,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendTelegramMessage(renderProfitBoostConfirmation(mergedPayload), { chatId, keyboard: commandKeyboard() });
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: 'PROFIT BOOST',
      resolved_command: 'PROFIT BOOST',
      auth_status: 'accepted',
      acknowledgment_sent: false,
      acknowledgment_timestamp_utc: null,
      acknowledgment_delivery_status: null,
      acknowledgment_delivery_error: null,
      response_type: 'profit_boost_confirmation_requested',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      legacy_alias_used: false,
      profit_boost_confirmation_status: 'edit_applied',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const naturalLanguageProfitBoost = parseNaturalLanguageProfitBoost(rawMessage);
  if (naturalLanguageProfitBoost && !naturalLanguageProfitBoost.ok) {
    const sentAt = new Date().toISOString();
    const delivery = await sendTelegramMessage([
      'NOT LOGGED ❌',
      '',
      `Reason: ${naturalLanguageProfitBoost.reason}`,
      naturalLanguageProfitBoost.details || 'Could not parse the profit boost.',
    ].join('\n'), { chatId, keyboard: commandKeyboard() });
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'PROFIT BOOST_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      acknowledgment_sent: false,
      acknowledgment_timestamp_utc: null,
      acknowledgment_delivery_status: null,
      acknowledgment_delivery_error: null,
      response_type: 'profit_boost_confirmation_rejected',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      legacy_alias_used: false,
      profit_boost_confirmation_status: 'parse_failed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  if (naturalLanguageProfitBoost?.ok) {
    state.pending_profit_boost_confirmation = {
      chat_id: chatId,
      created_at_utc: now,
      source_text: rawMessage,
      payload: naturalLanguageProfitBoost.payload,
      awaiting_edit: false,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendTelegramMessage(renderProfitBoostConfirmation(naturalLanguageProfitBoost.payload), { chatId, keyboard: commandKeyboard() });
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'PROFIT BOOST_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      acknowledgment_sent: false,
      acknowledgment_timestamp_utc: null,
      acknowledgment_delivery_status: null,
      acknowledgment_delivery_error: null,
      response_type: 'profit_boost_confirmation_requested',
      run_id: null,
      notification_tier: null,
      duplicate_suppression_status: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      legacy_alias_used: false,
      profit_boost_confirmation_status: 'pending_confirmation',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const parsedBetPlaced = parseBetPlacedMessage(rawMessage);
  const parsedBetSettled = parseBetSettledMessage(rawMessage);
  const parsedStructuredProfitBoost = parseProfitBoostMessage(rawMessage);
  const resolvedCommand = resolveOperatorCommand(normalizedMessage);
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
      profit_boost_confirmation_status: parsedStructuredProfitBoost ? 'direct_structured_log' : null,
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
      const event = await processUpdate(update, configuredChatId, now, state);
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
      const event = await processUpdate(update, configuredChatId, now, state);
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

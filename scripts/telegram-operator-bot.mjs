#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { appendJsonl, CORE_PATHS, readJson, writeJson } from './core-ledger-utils.mjs';
import {
  commandKeyboard,
  dispatchOperatorCommand,
  latestOperatorAlertMetadata,
  normalizeOperatorCommand,
  parseBetPlacedMessage,
  parseBetSettledMessage,
  parseProfitBoostMessage,
  parseNoSweatTokenMessage,
  parseEarlyWinTokenMessage,
  resolveOperatorCommand,
} from './operator-dispatcher.mjs';
import { buildScreenshotExecutionPreview } from './execution-screenshot-utils.mjs';
import { buildSettledTicketPreview } from './settled-ticket-screenshot-utils.mjs';
import { buildPromoScreenshotPreview } from './promo-screenshot-utils.mjs';
import {
  downloadTelegramFile,
  fetchTelegramUpdates,
  sendTelegramMessage,
  telegramConfiguredChatId,
} from './telegram-alert-utils.mjs';

const REPO_ROOT = '/Users/jaredbuckman/Documents/Playground/TieredEdge-Live-Bet-Log';
const BUILD_CANONICAL_STATE_SCRIPT = path.join(REPO_ROOT, 'scripts', 'build-canonical-state.mjs');
const BUILD_EXECUTION_BOARD_SCRIPT = path.join(REPO_ROOT, 'scripts', 'build-execution-board.mjs');
const TELEGRAM_MEDIA_DIR = path.join(REPO_ROOT, 'data', 'telegram-media-inbox');

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
  const stored = readJson(CORE_PATHS.telegramOperatorState, {}) || {};
  const pending = stored.pending_confirmation || stored.pending_profit_boost_confirmation || null;
  return {
    offset: stored.offset ?? null,
    last_polled_at_utc: stored.last_polled_at_utc ?? null,
    processed_count: parseInt(stored.processed_count || 0, 10) || 0,
    pending_confirmation: pending,
  };
}

function persistState(state) {
  const pending = state.pending_confirmation || null;
  writeJson(CORE_PATHS.telegramOperatorState, {
    offset: state.offset ?? null,
    last_polled_at_utc: new Date().toISOString(),
    processed_count: parseInt(state.processed_count || 0, 10) || 0,
    pending_confirmation: pending,
    pending_profit_boost_confirmation: pending?.kind === 'PROFIT BOOST' ? pending : null,
  });
}

function messageText(update) {
  return String(update?.message?.text || update?.message?.caption || '').trim();
}

function inboundChatId(update) {
  return String(update?.message?.chat?.id || '').trim();
}

function eventBase(update, now) {
  const photoCount = Array.isArray(update?.message?.photo) ? update.message.photo.length : 0;
  const localCount = Array.isArray(update?.message?.__local_image_paths) ? update.message.__local_image_paths.length : 0;
  return {
    telegram_event_id: `telegram-operator::${now}::${update?.update_id ?? 'unknown'}`,
    inbound_timestamp_utc: now,
    telegram_update_id: update?.update_id ?? null,
    chat_id: inboundChatId(update) || null,
    raw_text: messageText(update) || null,
    inbound_has_media: Boolean(photoCount || localCount || update?.message?.document),
    inbound_media_count: photoCount || localCount || (update?.message?.document ? 1 : 0),
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
    .replace(/\b(no\s+sweat\s+token|early\s+win\s+token|profit\s+boost|boost)\b/gi, '')
    .replace(/\bon\s+(draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/gi, '')
    .replace(/\bfor\b/gi, '')
    .replace(/\bpercent\b/gi, '')
    .replace(/\b\d+\+?\s*leg\b/gi, '')
    .replace(/\bleg\b/gi, '')
    .replace(/\bsgp\+?\b/gi, '')
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
    ['SGP', /same game parlay|\bsgp\+?\b/i],
    ['MONEYLINE', /moneyline|\bml\b/i],
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
    /\b(?:profit\s+boost|no\s+sweat\s+token|early\s+win\s+token|boost)\s+for\s+([A-Za-z0-9+\/ -]+?)\s+on\b/i,
    /\b\d+(?:\.\d+)?\s*(?:%|percent)\s+([A-Za-z0-9+\/ -]+?)\s+(?:profit\s+boost|boost)\s+on\s+(?:draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i,
    /\b(?:draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\s+\d+(?:\.\d+)?\s*(?:%|percent)\s+(?:profit\s+boost|boost)\s+for\s+([A-Za-z0-9+\/ -]+)\b/i,
    /\b([A-Z]{2,8}|MLB|NBA|NHL|NFL|NCAAB|CBB|GENERAL)\s+(?:profit\s+boost|boost|no\s+sweat\s+token|early\s+win\s+token)\b/i,
  ];
  for (const pattern of scopedPatterns) {
    const match = text.match(pattern);
    if (match?.[1]) return normalizeBoostScope(match[1]);
  }
  return null;
}

function canonicalExecutionPromoType(value) {
  const normalized = normalizeOperatorCommand(value);
  if (normalized === 'NO SWEAT TOKEN') return 'NO SWEAT TOKEN';
  if (normalized === 'EARLY WIN TOKEN') return 'EARLY WIN TOKEN';
  if (normalized === 'PROFIT BOOST') return 'PROFIT BOOST';
  return null;
}

function detectPromoType(rawText) {
  const text = normalizeOperatorCommand(rawText);
  if (text.includes('NO SWEAT')) return 'NO SWEAT TOKEN';
  if (text.includes('EARLY WIN')) return 'EARLY WIN TOKEN';
  if (text.includes('PROFIT BOOST') || /\bBOOST\b/.test(text)) return 'PROFIT BOOST';
  return null;
}

function parseOddsToken(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^[+-]\d+$/.test(raw)) return raw;
  if (/^\d+$/.test(raw)) return `+${raw}`;
  return null;
}

function parseStakeToken(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^\$?\s*([0-9]+(?:\.[0-9]{1,2})?)$/);
  if (!match) return null;
  return Number(match[1]).toFixed(2);
}

function parseSettlementResultToken(value) {
  const normalized = normalizeOperatorCommand(value).replace(/\s+/g, '');
  if (normalized === 'WIN') return 'WIN';
  if (normalized === 'LOSS') return 'LOSS';
  if (normalized === 'PUSH') return 'PUSH';
  return null;
}

function parseMaxWager(rawText) {
  const match = String(rawText || '').match(/(?:max(?:imum)?\s+(?:wager|bet)|up to)\s*:?\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)/i);
  if (!match) return null;
  const numeric = Number(match[1]);
  return Number.isFinite(numeric) ? Number(numeric.toFixed(2)) : null;
}

function parseMinTotalOdds(rawText) {
  const match = String(rawText || '').match(/(?:min(?:imum)?\s+total\s+odds|min(?:imum)?\s+odds|odds\s+of)\s*:?\s*([+-]?\d{2,4})/i);
  if (!match) return null;
  const raw = String(match[1]);
  return raw.startsWith('+') || raw.startsWith('-') ? raw : `+${raw}`;
}

function parseBetTypes(rawText) {
  const normalized = String(rawText || '').trim();
  if (!normalized) return null;
  const labels = [];
  if (/\b3\+?\s*leg\b/i.test(normalized)) labels.push('3+ leg');
  if (/\bsgp\+?\b|same game parlay/i.test(normalized)) labels.push('SGP');
  if (/\bstraight\b/i.test(normalized)) labels.push('Straight');
  if (/\bparlay\b/i.test(normalized) && !labels.includes('SGP')) labels.push('Parlay');
  if (/moneyline|\bml\b/i.test(normalized)) labels.push('Moneyline');
  return labels.length ? labels.join(', ') : null;
}

function parseNaturalLanguageProfitBoost(rawMessage) {
  const text = String(rawMessage || '').trim();
  if (!text) return null;
  if (normalizeOperatorCommand(text).startsWith('PROFIT BOOST')) return null;
  if (!/\bboost\b/i.test(text)) return null;

  const sportsbook = detectSportsbook(text);
  const boostPercent = parseFlexibleBoostPercent(text);
  const scope = detectBoostScope(text);
  const maxWager = parseMaxWager(text);
  const minTotalOdds = parseMinTotalOdds(text);
  const betTypes = parseBetTypes(text);

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
      max_wager: maxWager,
      bet_types: betTypes,
      min_total_odds: minTotalOdds,
      expires_raw: 'Not specified',
      status: 'ACTIVE',
    },
    kind: 'PROFIT BOOST',
  };
}

function parseNaturalLanguageReward(rawMessage) {
  const text = String(rawMessage || '').trim();
  if (!text) return null;
  const promoType = detectPromoType(text);
  if (!promoType || promoType === 'PROFIT BOOST') return null;
  if (normalizeOperatorCommand(text).startsWith(promoType)) return null;
  const sportsbook = detectSportsbook(text);
  const scope = detectBoostScope(text) || normalizeBoostScope(text);
  const maxWager = parseMaxWager(text);
  const minTotalOdds = parseMinTotalOdds(text);
  const betTypes = parseBetTypes(text);
  const missing = [];
  if (!sportsbook) missing.push('sportsbook');
  if (!scope) missing.push('scope');
  if (missing.length) {
    return {
      ok: false,
      reason: `missing ${missing.join(', ')}`,
      details: `Say something like: "I have a BetMGM ${promoType.toLowerCase()} for MLB".`,
      kind: promoType,
    };
  }
  return {
    ok: true,
    kind: promoType,
    payload: {
      sportsbook,
      scope,
      max_wager: maxWager,
      bet_types: betTypes,
      min_total_odds: promoType === 'NO SWEAT TOKEN' ? minTotalOdds : null,
      expires_raw: 'Not specified',
      status: 'ACTIVE',
    },
  };
}

function parseNaturalLanguageBetPlaced(rawMessage) {
  const text = String(rawMessage || '').trim();
  if (!text || /^BET\s+PLACED/i.test(text)) return null;
  if (!/\b(placed|bet|ticket)\b/i.test(text) && !/@/.test(text)) return null;
  const sportsbook = detectSportsbook(text);
  const odds = parseOddsToken(text.match(/([+-]\d{2,4})/)?.[1] || '');
  const stake = parseStakeToken(text.match(/\$\s*([0-9]+(?:\.[0-9]{1,2})?)/)?.[1] || '');
  const promoType = detectPromoType(text);
  let selection = null;
  const atMatch = text.match(/^(.+?)\s+@\s+(draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i);
  if (atMatch?.[1]) {
    selection = atMatch[1].replace(/^(i\s+(?:placed|bet)\s+)/i, '').trim();
  }
  if (!selection) {
    const onMatch = text.match(/(?:placed|bet)\s+(.+?)\s+on\s+(draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i);
    if (onMatch?.[1]) selection = onMatch[1].trim();
  }
  const missing = [];
  if (!selection) missing.push('selection');
  if (!sportsbook) missing.push('sportsbook');
  if (!odds) missing.push('odds');
  if (!stake) missing.push('stake');
  if (missing.length) return null;
  return {
    ok: true,
    kind: 'BET PLACED',
    payload: {
      selection,
      actual_sportsbook: sportsbook,
      actual_odds: odds,
      actual_stake: stake,
      promo_type: promoType,
      promo: promoType,
      event: null,
      start_time_ct: null,
    },
  };
}

function parseNaturalLanguageBetSettled(rawMessage) {
  const text = String(rawMessage || '').trim();
  if (!text || /^BET\s+SETTLED/i.test(text)) return null;
  const result = parseSettlementResultToken(text.match(/\b(WIN|LOSS|PUSH)\b/i)?.[1] || '');
  if (!result) return null;
  const sportsbook = detectSportsbook(text);
  const odds = parseOddsToken(text.match(/([+-]\d{2,4})/)?.[1] || '');
  const stake = parseStakeToken(text.match(/\$\s*([0-9]+(?:\.[0-9]{1,2})?)/)?.[1] || '');
  let selection = null;
  const atMatch = text.match(/^(.+?)\s+@\s+(draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i);
  if (atMatch?.[1]) selection = atMatch[1].replace(/^(settled\s+)/i, '').trim();
  if (!selection) {
    const settledMatch = text.match(/settled\s+(.+?)\s+on\s+(draftkings|dk|fanduel|fd|betmgm|mgm|caesars|czr|bet365|circa|betrivers|br)\b/i);
    if (settledMatch?.[1]) selection = settledMatch[1].trim();
  }
  const missing = [];
  if (!selection) missing.push('selection');
  if (!sportsbook) missing.push('sportsbook');
  if (!odds) missing.push('odds');
  if (!stake) missing.push('stake');
  if (missing.length) return null;
  return {
    ok: true,
    kind: 'BET SETTLED',
    payload: {
      selection,
      sportsbook,
      odds,
      stake,
      result,
    },
  };
}

function pendingConfirmationMatchesChat(pending, chatId) {
  return Boolean(pending) && String(pending.chat_id || '') === String(chatId || '');
}

function formatMoney(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `$${numeric.toFixed(2)}` : 'Not specified';
}

function buildStructuredCommandFromPending(pending) {
  const payload = pending?.payload || {};
  switch (pending?.kind) {
    case 'BET PLACED': {
      const lines = [
        'BET PLACED',
        `Selection: ${payload.selection || ''}`,
        `Sportsbook: ${payload.actual_sportsbook || payload.sportsbook || ''}`,
        `Odds: ${payload.actual_odds || payload.odds || ''}`,
        `Stake: $${payload.actual_stake || payload.stake || ''}`,
      ];
      if (payload.promo_type || payload.promo) lines.push(`Promo: ${payload.promo_type || payload.promo}`);
      if (payload.event) lines.push(`Game: ${payload.event}`);
      if (payload.start_time_ct) lines.push(`Start Time: ${payload.start_time_ct}`);
      return lines.join('\n');
    }
    case 'BET SETTLED':
      return [
        'BET SETTLED',
        `${payload.selection || ''} @ ${payload.sportsbook || payload.actual_sportsbook || ''}`,
        `${payload.odds || payload.actual_odds || ''}`,
        `$${payload.stake || payload.actual_stake || ''}`,
        `${payload.result || ''}`,
      ].join('\n');
    case 'PROFIT BOOST': {
      const lines = [
        'PROFIT BOOST',
        `Sportsbook: ${payload.sportsbook || ''}`,
        `Boost: ${payload.boost_percent}%`,
        `Scope: ${payload.scope || ''}`,
      ];
      if (payload.max_wager !== null && payload.max_wager !== undefined) lines.push(`Max Wager: ${formatMoney(payload.max_wager)}`);
      if (payload.bet_types) lines.push(`Bet Types: ${payload.bet_types}`);
      if (payload.min_total_odds) lines.push(`Min Total Odds: ${payload.min_total_odds}`);
      lines.push(`Expires: ${payload.expires_raw || 'Not specified'}`);
      return lines.join('\n');
    }
    case 'NO SWEAT TOKEN': {
      const lines = [
        'NO SWEAT TOKEN',
        `Sportsbook: ${payload.sportsbook || ''}`,
        `Scope: ${payload.scope || ''}`,
      ];
      if (payload.max_wager !== null && payload.max_wager !== undefined) lines.push(`Max Wager: ${formatMoney(payload.max_wager)}`);
      if (payload.bet_types) lines.push(`Bet Types: ${payload.bet_types}`);
      if (payload.min_total_odds) lines.push(`Min Total Odds: ${payload.min_total_odds}`);
      lines.push(`Expires: ${payload.expires_raw || 'Not specified'}`);
      return lines.join('\n');
    }
    case 'EARLY WIN TOKEN': {
      const lines = [
        'EARLY WIN TOKEN',
        `Sportsbook: ${payload.sportsbook || ''}`,
        `Scope: ${payload.scope || ''}`,
      ];
      if (payload.max_wager !== null && payload.max_wager !== undefined) lines.push(`Max Wager: ${formatMoney(payload.max_wager)}`);
      if (payload.bet_types) lines.push(`Bet Types: ${payload.bet_types}`);
      lines.push(`Expires: ${payload.expires_raw || 'Not specified'}`);
      return lines.join('\n');
    }
    default:
      return String(pending?.source_text || '').trim();
  }
}

function renderPendingPreview(pending, prefix = 'I parsed this:') {
  return [
    prefix,
    '',
    buildStructuredCommandFromPending(pending),
    '',
    'Reply YES to confirm, EDIT to correct, or CANCEL to discard.',
  ].join('\n');
}

function renderPendingEditPrompt(pending) {
  const kind = pending?.kind || 'ENTRY';
  const examplesByKind = {
    'BET PLACED': ['EDIT', 'Odds: +125', 'Stake: $25', 'Promo: NO SWEAT TOKEN'],
    'BET SETTLED': ['EDIT', 'Odds: +125', 'Stake: $25', 'Result: WIN'],
    'PROFIT BOOST': ['EDIT', 'Boost: 50%', 'Scope: MLB', 'Expires: 2026-03-27 11:59 PM CT'],
    'NO SWEAT TOKEN': ['EDIT', 'Scope: MLB', 'Max Wager: $25', 'Expires: 2026-03-27 11:59 PM CT'],
    'EARLY WIN TOKEN': ['EDIT', 'Scope: MLB', 'Max Wager: $25', 'Expires: 2026-03-27 11:59 PM CT'],
  };
  return [
    'EDIT MODE',
    `Update the pending ${kind} with labeled lines.`,
    'Example:',
    ...(examplesByKind[kind] || ['EDIT', 'Scope: MLB']),
  ].join('\n');
}

function renderPendingCanceled() {
  return ['CANCELED', '', 'Pending confirmation discarded.'].join('\n');
}

function renderPendingFailure(reason, details) {
  return ['NOT LOGGED ❌', '', `Reason: ${reason}`, details].join('\n');
}

function parsePendingEditFields(rawMessage, pending) {
  const text = String(rawMessage || '').trim();
  if (!text) return { ok: false, reason: 'missing edit fields', invalid_fields: [] };
  const lines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const firstIsEdit = /^EDIT$/i.test(lines[0]);
  const editLines = firstIsEdit ? lines.slice(1) : lines;
  if (!editLines.length) return { ok: false, reason: 'missing edit fields', invalid_fields: [] };

  const updates = {};
  const invalidFields = [];
  for (const line of editLines) {
    const match = line.match(/^([^:]+):\s*(.+)$/);
    if (!match) {
      invalidFields.push(line);
      continue;
    }
    const label = normalizeOperatorCommand(match[1]);
    const value = match[2].trim();
    if (!value) {
      invalidFields.push(line);
      continue;
    }
    switch (pending?.kind) {
      case 'BET PLACED':
        if (label === 'SELECTION') updates.selection = value;
        else if (label === 'SPORTSBOOK' || label === 'BOOK') updates.actual_sportsbook = normalizeBookAlias(value);
        else if (label === 'ODDS') updates.actual_odds = parseOddsToken(value);
        else if (label === 'STAKE') updates.actual_stake = parseStakeToken(value);
        else if (label === 'PROMO') updates.promo_type = canonicalExecutionPromoType(value);
        else if (label === 'GAME') updates.event = value;
        else if (label === 'START TIME') updates.start_time_ct = value;
        else invalidFields.push(line);
        break;
      case 'BET SETTLED':
        if (label === 'SELECTION') updates.selection = value;
        else if (label === 'SPORTSBOOK' || label === 'BOOK') updates.sportsbook = normalizeBookAlias(value);
        else if (label === 'ODDS') updates.odds = parseOddsToken(value);
        else if (label === 'STAKE') updates.stake = parseStakeToken(value);
        else if (label === 'RESULT') updates.result = parseSettlementResultToken(value);
        else invalidFields.push(line);
        break;
      case 'PROFIT BOOST':
        if (label === 'SPORTSBOOK' || label === 'BOOK') updates.sportsbook = normalizeBookAlias(value);
        else if (label === 'BOOST' || label === 'BOOST PERCENT' || label === 'PERCENT') updates.boost_percent = parseFlexibleBoostPercent(value) ?? parseFlexibleBoostPercent(`${value}%`);
        else if (label === 'SCOPE') updates.scope = normalizeBoostScope(value);
        else if (label === 'MAX WAGER') updates.max_wager = parseMaxWager(`Max Wager: ${value}`) ?? parseStakeToken(value);
        else if (label === 'BET TYPES') updates.bet_types = value;
        else if (label === 'MIN TOTAL ODDS') updates.min_total_odds = parseMinTotalOdds(`Min Total Odds: ${value}`) || parseOddsToken(value);
        else if (label === 'EXPIRES' || label === 'EXPIRATION') updates.expires_raw = value;
        else invalidFields.push(line);
        break;
      case 'NO SWEAT TOKEN':
      case 'EARLY WIN TOKEN':
        if (label === 'SPORTSBOOK' || label === 'BOOK') updates.sportsbook = normalizeBookAlias(value);
        else if (label === 'SCOPE') updates.scope = normalizeBoostScope(value);
        else if (label === 'MAX WAGER') updates.max_wager = parseMaxWager(`Max Wager: ${value}`) ?? parseStakeToken(value);
        else if (label === 'BET TYPES') updates.bet_types = value;
        else if (label === 'MIN TOTAL ODDS' && pending.kind === 'NO SWEAT TOKEN') updates.min_total_odds = parseMinTotalOdds(`Min Total Odds: ${value}`) || parseOddsToken(value);
        else if (label === 'EXPIRES' || label === 'EXPIRATION') updates.expires_raw = value;
        else invalidFields.push(line);
        break;
      default:
        invalidFields.push(line);
    }
  }

  const hasInvalidValue = Object.values(updates).some((value) => value === null);
  if (invalidFields.length || hasInvalidValue) {
    return { ok: false, reason: 'unrecognized or invalid edit fields', invalid_fields: invalidFields };
  }
  return { ok: true, updates };
}

function detectPromoFromLines(lines) {
  const joined = Array.isArray(lines) ? lines.join(' ') : String(lines || '');
  return detectPromoType(joined);
}

function settlementResultFromStatus(status) {
  const normalized = normalizeOperatorCommand(status);
  if (normalized === 'WIN' || normalized === 'WON') return 'WIN';
  if (normalized === 'LOSS' || normalized === 'LOST') return 'LOSS';
  if (normalized === 'PUSH') return 'PUSH';
  return null;
}

function looksLikeBetTicketText(text) {
  const raw = String(text || '');
  return /wager:\s*\$/i.test(raw)
    || /to pay:\s*\$/i.test(raw)
    || /paid:\s*\$/i.test(raw)
    || /(^|\n)\s*(moneyline|spread)\s*($|\n)/i.test(raw);
}

function chooseBestPreviewItem(items) {
  return [...(items || [])].sort((a, b) => {
    const aScore = Number(a?.extracted_fields?.extraction_confidence || 0);
    const bScore = Number(b?.extracted_fields?.extraction_confidence || 0);
    return bScore - aScore;
  })[0] || null;
}

function buildBetPendingFromScreenshot(imagePaths, rawMessage) {
  const preview = buildScreenshotExecutionPreview(imagePaths);
  const item = chooseBestPreviewItem(preview.items);
  if (!item) {
    return { ok: false, reason: 'unreadable_screenshot', details: 'Could not extract a likely bet from the screenshot.' };
  }
  const fields = item.extracted_fields || {};
  if (!fields.selection || !fields.sportsbook || !fields.odds || !fields.stake) {
    return { ok: false, reason: 'missing screenshot fields', details: 'Selection, sportsbook, odds, or stake could not be read confidently from the screenshot.' };
  }
  return {
    ok: true,
    pending: {
      kind: 'BET PLACED',
      source_kind: 'screenshot_bet_placed',
      source_text: rawMessage,
      payload: {
        selection: fields.selection,
        actual_sportsbook: fields.sportsbook,
        actual_odds: fields.odds,
        actual_stake: fields.stake,
        promo_type: detectPromoFromLines(fields.raw_lines),
        promo: detectPromoFromLines(fields.raw_lines),
        event: fields.event || null,
        start_time_ct: fields.ticket_timestamp || null,
      },
      warnings: item.warnings || [],
      media_paths: imagePaths,
    },
    preview_prefix: 'I parsed this from your screenshot:',
  };
}

function buildSettlementPendingFromScreenshot(imagePaths, rawMessage) {
  const preview = buildSettledTicketPreview(imagePaths, path.join(REPO_ROOT, 'data', 'telegram-settlement-screenshot-preview.json'));
  const item = chooseBestPreviewItem(preview.items);
  if (!item) {
    return { ok: false, reason: 'unreadable_screenshot', details: 'Could not extract a likely settled bet from the screenshot.' };
  }
  const fields = item.extracted_fields || {};
  const result = settlementResultFromStatus(fields.settlement_status);
  if (!fields.selection || !fields.sportsbook || !fields.odds || !fields.stake || !result) {
    return { ok: false, reason: 'missing screenshot fields', details: 'Selection, sportsbook, odds, stake, or result could not be read confidently from the screenshot.' };
  }
  return {
    ok: true,
    pending: {
      kind: 'BET SETTLED',
      source_kind: 'screenshot_bet_settled',
      source_text: rawMessage,
      payload: {
        selection: fields.selection,
        sportsbook: fields.sportsbook,
        odds: fields.odds,
        stake: fields.stake,
        result,
      },
      warnings: item.warnings || [],
      media_paths: imagePaths,
    },
    preview_prefix: 'I parsed this from your screenshot:',
  };
}

function buildPromoPendingFromScreenshot(imagePaths, rawMessage) {
  const preview = buildPromoScreenshotPreview(imagePaths);
  const item = chooseBestPreviewItem(preview.items);
  if (!item) {
    return { ok: false, reason: 'unreadable_screenshot', details: 'Could not extract a likely promo or reward from the screenshot.' };
  }
  const fields = item.extracted_fields || {};
  if (!fields.promo_type || !fields.sportsbook || !fields.scope) {
    return { ok: false, reason: 'missing screenshot fields', details: 'Promo type, sportsbook, or scope could not be read confidently from the screenshot.' };
  }
  return {
    ok: true,
    pending: {
      kind: fields.promo_type,
      source_kind: 'screenshot_promo',
      source_text: rawMessage,
      payload: {
        sportsbook: fields.sportsbook,
        boost_percent: fields.boost_percent,
        scope: fields.scope,
        max_wager: fields.max_wager,
        bet_types: fields.bet_types,
        min_total_odds: fields.min_total_odds,
        expires_raw: fields.expires_raw || 'Not specified',
        status: 'ACTIVE',
      },
      warnings: item.warnings || [],
      media_paths: imagePaths,
    },
    preview_prefix: 'I parsed this from your screenshot:',
  };
}

function classifyScreenshotIntent(rawMessage, imagePaths) {
  const normalized = normalizeOperatorCommand(rawMessage);
  if (normalized.startsWith('BET SETTLED')) return 'BET SETTLED';
  if (normalized.startsWith('BET PLACED')) return 'BET PLACED';
  if (normalized.startsWith('PROFIT BOOST')) return 'PROFIT BOOST';
  if (normalized.startsWith('NO SWEAT TOKEN')) return 'NO SWEAT TOKEN';
  if (normalized.startsWith('EARLY WIN TOKEN')) return 'EARLY WIN TOKEN';
  const ocrDocs = buildScreenshotExecutionPreview(imagePaths, path.join(REPO_ROOT, 'data', 'telegram-intent-bet-preview.json'));
  const betItem = chooseBestPreviewItem(ocrDocs.items);
  const rawTicketText = betItem?.extracted_fields?.raw_lines?.join('\n') || '';
  if (looksLikeBetTicketText(rawTicketText)) {
    if (settlementResultFromStatus(rawTicketText)) return 'BET SETTLED';
    if (/\bwon\b|\blost\b|\bpush\b|paid:\s*\$/i.test(rawTicketText)) return 'BET SETTLED';
    return 'BET PLACED';
  }
  const promoPreview = buildPromoScreenshotPreview(imagePaths, path.join(REPO_ROOT, 'data', 'telegram-promo-intent-preview.json'));
  const promoItem = chooseBestPreviewItem(promoPreview.items);
  if (promoItem?.extracted_fields?.promo_type) return promoItem.extracted_fields.promo_type;
  const settledPreview = buildSettledTicketPreview(imagePaths, path.join(REPO_ROOT, 'data', 'telegram-settled-intent-preview.json'));
  const settledItem = chooseBestPreviewItem(settledPreview.items);
  if (settledItem?.extracted_fields?.settlement_status && settlementResultFromStatus(settledItem.extracted_fields.settlement_status)) {
    return 'BET SETTLED';
  }
  return 'BET PLACED';
}

async function resolveImagePaths(update) {
  if (Array.isArray(update?.message?.__local_image_paths) && update.message.__local_image_paths.length) {
    return {
      ok: true,
      imagePaths: update.message.__local_image_paths.map((filePath) => path.resolve(filePath)),
      diagnostics: null,
    };
  }

  const photoEntries = Array.isArray(update?.message?.photo) ? update.message.photo : [];
  const documentEntry = update?.message?.document && String(update.message.document.mime_type || '').startsWith('image/')
    ? update.message.document
    : null;
  const fileEntries = photoEntries.length
    ? [photoEntries.slice().sort((a, b) => (Number(b.file_size) || 0) - (Number(a.file_size) || 0))[0]]
    : (documentEntry ? [documentEntry] : []);
  if (!fileEntries.length) {
    return { ok: true, imagePaths: [], diagnostics: null };
  }

  fs.mkdirSync(TELEGRAM_MEDIA_DIR, { recursive: true });
  const imagePaths = [];
  for (const [index, entry] of fileEntries.entries()) {
    const ext = documentEntry
      ? (path.extname(documentEntry.file_name || '') || '.img')
      : '.jpg';
    const destinationPath = path.join(
      TELEGRAM_MEDIA_DIR,
      `${String(inboundChatId(update) || 'chat').replace(/[^a-zA-Z0-9_-]/g, '_')}__${update?.update_id || Date.now()}__${String(index + 1).padStart(2, '0')}${ext}`,
    );
    const download = await downloadTelegramFile(entry.file_id, destinationPath);
    if (!download.ok) {
      return {
        ok: false,
        imagePaths: [],
        diagnostics: download.diagnostics || download.error || 'telegram_media_download_failed',
      };
    }
    imagePaths.push(destinationPath);
  }
  return { ok: true, imagePaths, diagnostics: null };
}

function shouldRefreshState(result) {
  return new Set([
    'execution_log_logged',
    'settlement_logged',
    'profit_boost_logged',
    'no_sweat_token_logged',
    'early_win_token_logged',
  ]).has(String(result?.response_type || ''));
}

function refreshDerivedState() {
  const commands = [
    ['node', BUILD_CANONICAL_STATE_SCRIPT],
    ['node', BUILD_EXECUTION_BOARD_SCRIPT],
  ];
  const diagnostics = [];
  for (const command of commands) {
    const result = spawnSync(command[0], command.slice(1), {
      cwd: REPO_ROOT,
      encoding: 'utf8',
      timeout: 120000,
    });
    diagnostics.push({
      command: command.join(' '),
      status: result.status,
      stderr: String(result.stderr || '').trim() || null,
    });
    if (result.status !== 0) {
      return { ok: false, diagnostics };
    }
  }
  return { ok: true, diagnostics };
}

function buildFinalResponseForPending(pending) {
  return dispatchOperatorCommand(buildStructuredCommandFromPending(pending));
}

function compactTelegramText(text, maxChars = 3500) {
  const raw = String(text || '').trim();
  if (raw.length <= maxChars) return raw;
  const suffix = '\n\n[Message truncated for Telegram length limit]';
  return `${raw.slice(0, Math.max(0, maxChars - suffix.length)).trimEnd()}${suffix}`;
}

async function sendText(chatId, text) {
  const initial = await sendTelegramMessage(text, { chatId, keyboard: commandKeyboard() });
  const description = String(initial?.diagnostics?.response_description || '').toLowerCase();
  if (!initial.ok && initial.error === 'telegram_http_400' && description.includes('message is too long')) {
    return sendTelegramMessage(compactTelegramText(text), { chatId, keyboard: commandKeyboard() });
  }
  return initial;
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
  const normalizedMessage = normalizeOperatorCommand(rawMessage);
  const pending = state.pending_confirmation;
  const pendingMatchesChat = pendingConfirmationMatchesChat(pending, chatId);

  if (pendingMatchesChat && normalizedMessage === 'YES') {
    const result = buildFinalResponseForPending(pending);
    if (shouldRefreshState(result)) {
      refreshDerivedState();
    }
    state.pending_confirmation = null;
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, result.text);
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: pending.kind,
      resolved_command: pending.kind,
      auth_status: 'accepted',
      response_type: result.response_type || 'pending_confirmation_completed',
      run_id: result.run_id || null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'confirmed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  if (pendingMatchesChat && normalizedMessage === 'EDIT') {
    state.pending_confirmation = {
      ...pending,
      awaiting_edit: true,
      updated_at_utc: now,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingEditPrompt(pending));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: pending.kind,
      resolved_command: `${pending.kind}_PENDING_CONFIRMATION`,
      auth_status: 'accepted',
      response_type: 'pending_edit_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'awaiting_edit',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  if (pendingMatchesChat && normalizedMessage === 'CANCEL') {
    state.pending_confirmation = null;
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingCanceled());
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: pending.kind,
      resolved_command: `${pending.kind}_PENDING_CONFIRMATION`,
      auth_status: 'accepted',
      response_type: 'pending_confirmation_canceled',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'canceled',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const shouldApplyPendingEdit = Boolean(
    pendingMatchesChat && (
      /^EDIT(?:\s|$)/i.test(rawMessage)
      || pending?.awaiting_edit
    )
  );
  if (shouldApplyPendingEdit) {
    const editFields = parsePendingEditFields(rawMessage, pending);
    if (!editFields.ok) {
      const sentAt = new Date().toISOString();
      const delivery = await sendText(chatId, renderPendingFailure(editFields.reason, 'Use labeled lines after EDIT to update the pending item.'));
      const alertMeta = latestOperatorAlertMetadata();
      return {
        ...base,
        outbound_timestamp_utc: sentAt,
        command: pending.kind,
        resolved_command: `${pending.kind}_PENDING_CONFIRMATION`,
        auth_status: 'accepted',
        response_type: 'pending_edit_rejected',
        run_id: null,
        outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
        delivery_status: delivery.ok ? 'sent' : 'failed',
        delivery_error: delivery.ok ? null : delivery.error,
        delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
        pending_confirmation_status: 'edit_rejected',
        last_outbound_alert_time: alertMeta.last_outbound_alert_time,
        last_outbound_alert_type: alertMeta.last_outbound_alert_type,
      };
    }
    state.pending_confirmation = {
      ...pending,
      payload: {
        ...(pending.payload || {}),
        ...editFields.updates,
      },
      awaiting_edit: false,
      updated_at_utc: now,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingPreview(state.pending_confirmation));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: pending.kind,
      resolved_command: `${pending.kind}_PENDING_CONFIRMATION`,
      auth_status: 'accepted',
      response_type: 'pending_confirmation_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'edit_applied',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const media = await resolveImagePaths(update);
  if (!media.ok) {
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, ['NOT LOGGED ❌', '', 'Reason: media download failed', String(media.diagnostics || 'telegram_media_download_failed')].join('\n'));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || '[image]',
      resolved_command: null,
      auth_status: 'accepted',
      response_type: 'media_download_failed',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  if (media.imagePaths.length) {
    const explicitIntent = classifyScreenshotIntent(rawMessage, media.imagePaths);
    const betCandidate = buildBetPendingFromScreenshot(media.imagePaths, rawMessage);
    const settledCandidate = buildSettlementPendingFromScreenshot(media.imagePaths, rawMessage);
    const promoCandidate = buildPromoPendingFromScreenshot(media.imagePaths, rawMessage);
    let candidate = null;
    let intent = explicitIntent;
    if (explicitIntent === 'BET SETTLED') {
      candidate = settledCandidate;
    } else if (explicitIntent === 'PROFIT BOOST' || explicitIntent === 'NO SWEAT TOKEN' || explicitIntent === 'EARLY WIN TOKEN') {
      candidate = promoCandidate;
    } else if (betCandidate.ok) {
      candidate = betCandidate;
      intent = 'BET PLACED';
    } else if (settledCandidate.ok) {
      candidate = settledCandidate;
      intent = 'BET SETTLED';
    } else if (promoCandidate.ok) {
      candidate = promoCandidate;
      intent = promoCandidate.pending?.kind || 'PROMO';
    } else {
      candidate = betCandidate;
      intent = 'BET PLACED';
    }

    if (!candidate.ok) {
      const sentAt = new Date().toISOString();
      const delivery = await sendText(chatId, ['NOT LOGGED ❌', '', `Reason: ${candidate.reason}`, candidate.details].join('\n'));
      const alertMeta = latestOperatorAlertMetadata();
      return {
        ...base,
        outbound_timestamp_utc: sentAt,
        command: intent || 'SCREENSHOT',
        resolved_command: `${intent || 'SCREENSHOT'}_PARSING`,
        auth_status: 'accepted',
        response_type: 'screenshot_parse_rejected',
        run_id: null,
        outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
        delivery_status: delivery.ok ? 'sent' : 'failed',
        delivery_error: delivery.ok ? null : delivery.error,
        delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
        pending_confirmation_status: 'parse_failed',
        last_outbound_alert_time: alertMeta.last_outbound_alert_time,
        last_outbound_alert_type: alertMeta.last_outbound_alert_type,
      };
    }

    state.pending_confirmation = {
      chat_id: chatId,
      created_at_utc: now,
      updated_at_utc: now,
      awaiting_edit: false,
      ...candidate.pending,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingPreview(state.pending_confirmation, candidate.preview_prefix));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: state.pending_confirmation.kind,
      resolved_command: `${state.pending_confirmation.kind}_PENDING_CONFIRMATION`,
      auth_status: 'accepted',
      response_type: 'screenshot_confirmation_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'pending_confirmation',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const naturalLanguageProfitBoost = parseNaturalLanguageProfitBoost(rawMessage);
  if (naturalLanguageProfitBoost?.ok) {
    state.pending_confirmation = {
      chat_id: chatId,
      created_at_utc: now,
      updated_at_utc: now,
      awaiting_edit: false,
      kind: naturalLanguageProfitBoost.kind,
      source_kind: 'natural_language',
      source_text: rawMessage,
      payload: naturalLanguageProfitBoost.payload,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingPreview(state.pending_confirmation));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'PROFIT BOOST_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      response_type: 'profit_boost_confirmation_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'pending_confirmation',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }
  if (naturalLanguageProfitBoost && !naturalLanguageProfitBoost.ok) {
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, ['NOT LOGGED ❌', '', `Reason: ${naturalLanguageProfitBoost.reason}`, naturalLanguageProfitBoost.details].join('\n'));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'PROFIT BOOST_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      response_type: 'profit_boost_confirmation_rejected',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'parse_failed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const naturalReward = parseNaturalLanguageReward(rawMessage);
  if (naturalReward?.ok) {
    state.pending_confirmation = {
      chat_id: chatId,
      created_at_utc: now,
      updated_at_utc: now,
      awaiting_edit: false,
      kind: naturalReward.kind,
      source_kind: 'natural_language',
      source_text: rawMessage,
      payload: naturalReward.payload,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingPreview(state.pending_confirmation));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: `${naturalReward.kind}_PENDING_CONFIRMATION`,
      auth_status: 'accepted',
      response_type: 'reward_confirmation_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'pending_confirmation',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }
  if (naturalReward && !naturalReward.ok) {
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, ['NOT LOGGED ❌', '', `Reason: ${naturalReward.reason}`, naturalReward.details].join('\n'));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: `${naturalReward.kind || 'REWARD'}_PENDING_CONFIRMATION`,
      auth_status: 'accepted',
      response_type: 'reward_confirmation_rejected',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'parse_failed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const naturalPlaced = parseNaturalLanguageBetPlaced(rawMessage);
  if (naturalPlaced?.ok) {
    state.pending_confirmation = {
      chat_id: chatId,
      created_at_utc: now,
      updated_at_utc: now,
      awaiting_edit: false,
      kind: naturalPlaced.kind,
      source_kind: 'natural_language',
      source_text: rawMessage,
      payload: naturalPlaced.payload,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingPreview(state.pending_confirmation));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'BET PLACED_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      response_type: 'execution_confirmation_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'pending_confirmation',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }
  if (!naturalPlaced && /\b(placed|bet)\b/i.test(rawMessage) && !/^BET\s+PLACED/i.test(rawMessage)) {
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, [
      'NOT LOGGED ❌',
      '',
      'Reason: missing or ambiguous bet fields',
      'Include selection, sportsbook, odds, and stake. Example:',
      'BET PLACED',
      'Kansas City Royals ML @ BetMGM',
      '+125',
      '$25',
      'Promo: NO SWEAT TOKEN',
    ].join('\n'));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'BET PLACED_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      response_type: 'execution_confirmation_rejected',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'parse_failed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const naturalSettled = parseNaturalLanguageBetSettled(rawMessage);
  if (naturalSettled?.ok) {
    state.pending_confirmation = {
      chat_id: chatId,
      created_at_utc: now,
      updated_at_utc: now,
      awaiting_edit: false,
      kind: naturalSettled.kind,
      source_kind: 'natural_language',
      source_text: rawMessage,
      payload: naturalSettled.payload,
    };
    persistState(state);
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, renderPendingPreview(state.pending_confirmation));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'BET SETTLED_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      response_type: 'settlement_confirmation_requested',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'pending_confirmation',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }
  if (!naturalSettled && /\bsettled\b|\bwin\b|\bloss\b|\bpush\b/i.test(rawMessage) && !/^BET\s+SETTLED/i.test(rawMessage)) {
    const sentAt = new Date().toISOString();
    const delivery = await sendText(chatId, [
      'NOT SETTLED ❌',
      '',
      'Reason: missing or ambiguous settlement fields',
      'Include selection, sportsbook, odds, stake, and result. Example:',
      'BET SETTLED',
      'Kansas City Royals ML @ BetMGM',
      '+125',
      '$25',
      'WIN',
    ].join('\n'));
    const alertMeta = latestOperatorAlertMetadata();
    return {
      ...base,
      outbound_timestamp_utc: sentAt,
      command: rawMessage || null,
      resolved_command: 'BET SETTLED_PENDING_CONFIRMATION',
      auth_status: 'accepted',
      response_type: 'settlement_confirmation_rejected',
      run_id: null,
      outbound_channel: delivery.ok ? 'telegram' : 'telegram_failed',
      delivery_status: delivery.ok ? 'sent' : 'failed',
      delivery_error: delivery.ok ? null : delivery.error,
      delivery_diagnostics: delivery.ok ? null : (delivery.diagnostics || null),
      pending_confirmation_status: 'parse_failed',
      last_outbound_alert_time: alertMeta.last_outbound_alert_time,
      last_outbound_alert_type: alertMeta.last_outbound_alert_type,
    };
  }

  const parsedBetPlaced = parseBetPlacedMessage(rawMessage);
  const parsedBetSettled = parseBetSettledMessage(rawMessage);
  const parsedStructuredProfitBoost = parseProfitBoostMessage(rawMessage);
  const parsedNoSweat = parseNoSweatTokenMessage(rawMessage);
  const parsedEarlyWin = parseEarlyWinTokenMessage(rawMessage);
  const resolvedCommand = resolveOperatorCommand(normalizedMessage);
  let ackDelivery = null;
  let ackSentAt = null;
  const shouldAcknowledge = Boolean(parsedBetPlaced?.ok) || Boolean(parsedBetSettled?.ok) || resolvedCommand === 'RUN HUNT';
  if (shouldAcknowledge) {
    ackSentAt = new Date().toISOString();
    ackDelivery = await sendTelegramMessage('RECEIVED ⏳\nProcessing...', { chatId });
  }

  const result = dispatchOperatorCommand(rawMessage);
  if (shouldRefreshState(result)) {
    refreshDerivedState();
  }
  const sentAt = new Date().toISOString();
  const delivery = await sendText(chatId, result.text);
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
    pending_confirmation_status: parsedStructuredProfitBoost ? 'direct_structured_log' : (parsedNoSweat || parsedEarlyWin ? 'direct_structured_log' : null),
    last_outbound_alert_time: alertMeta.last_outbound_alert_time,
    last_outbound_alert_type: alertMeta.last_outbound_alert_type,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const state = currentState();
  const configuredChatId = telegramConfiguredChatId();
  if (args.simulate_command || args.simulate_local_image) {
    const localImages = String(args.simulate_local_image || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
    const updates = [{
      update_id: syntheticUpdateId(),
      message: {
        chat: { id: String(args.chat_id || configuredChatId || '') },
        text: args.simulate_command ? String(args.simulate_command) : undefined,
        caption: args.simulate_caption ? String(args.simulate_caption) : undefined,
        __local_image_paths: localImages,
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
      if (Date.now() + 5000 >= deadlineMs) break;
      continue;
    }

    updates.sort((a, b) => (Number(a?.update_id) || 0) - (Number(b?.update_id) || 0));

    for (const update of updates) {
      const updateId = Number(update?.update_id);
      if (Number.isFinite(nextOffset) && Number.isFinite(updateId) && updateId < nextOffset) continue;
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
        state.offset = nextOffset;
        state.processed_count = (state.processed_count || 0) + 1;
        persistState(state);
      }
    }
  }

  persistState(state);

  if (args.json) {
    console.log(JSON.stringify({
      ok: true,
      processed_updates: sessionProcessedCount,
      next_offset: nextOffset,
      events: sessionEvents,
    }, null, 2));
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : 'telegram_operator_failed');
  if (error?.transport_diagnostics) {
    console.error(JSON.stringify(error.transport_diagnostics, null, 2));
  }
  process.exit(1);
});

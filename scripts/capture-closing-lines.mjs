#!/usr/bin/env node
import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';
import { promisify } from 'node:util';
import { CORE_PATHS, appendJsonl, parseNumber, readJsonl, round2 } from './core-ledger-utils.mjs';

const execFileAsync = promisify(execFile);
const ODDS_KEY_SERVICE = 'tierededge-odds-api';
const ODDS_KEY_ACCOUNT = 'default';
const RUNTIME_SECURE_DIR = '/Users/jaredbuckman/.openclaw/workspace/memory/secure';
const RUNTIME_SECURE_KEY_FILE = path.join(RUNTIME_SECURE_DIR, 'odds-api-key.enc.json');
const PHASE1_NBA_POINTS_PROP_KEY = 'player_points';
const CLOSE_CAPTURE_PATH = CORE_PATHS.rejectedCloseCaptureLog;

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, 'and')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function normalizePlayerName(value) {
  return normalizeName(value)
    .replace(/\b(jr|sr|ii|iii|iv)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeBookKey(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  if (raw === 'draftkings') return 'draftkings';
  if (raw === 'fanduel') return 'fanduel';
  if (raw === 'betmgm') return 'betmgm';
  if (raw === 'betrivers') return 'betrivers';
  if (raw === 'bet365') return 'bet365';
  if (raw === 'caesars') return 'caesars';
  if (raw === 'circa' || raw === 'circa sports' || raw === 'circasports') return 'circa';
  return raw.replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

function americanToDecimal(odds) {
  const num = Number(odds);
  if (!Number.isFinite(num) || num === 0) return null;
  return num > 0 ? (1 + (num / 100)) : (1 + (100 / Math.abs(num)));
}

function impliedProbFromAmerican(odds) {
  const decimal = americanToDecimal(odds);
  return decimal ? (1 / decimal) : null;
}

function normalizePoint(value) {
  const num = parseNumber(value);
  return Number.isFinite(num) ? Number(num.toFixed(1)) : null;
}

function outcomeKey(marketKey, outcome) {
  const name = normalizeName(outcome?.name);
  const point = normalizePoint(outcome?.point);
  if (marketKey === 'h2h') return name;
  if (marketKey === 'spreads' || marketKey === 'totals') return `${name}::${point}`;
  return `${name}::${point ?? ''}`;
}

function computeFairProbMap(outcomes) {
  const entries = (outcomes || [])
    .map((outcome) => ({
      key: outcome.key,
      raw: impliedProbFromAmerican(outcome.price),
    }))
    .filter((entry) => Number.isFinite(entry.raw));
  const total = entries.reduce((sum, entry) => sum + entry.raw, 0);
  if (!Number.isFinite(total) || total <= 0) return new Map();
  return new Map(entries.map((entry) => [entry.key, entry.raw / total]));
}

function buildPlayerPropPairs(outcomes) {
  const grouped = new Map();
  for (const outcome of outcomes || []) {
    const playerNameRaw = String(outcome?.description || '').trim();
    const playerNameNormalized = normalizePlayerName(playerNameRaw);
    const side = String(outcome?.name || '').trim().toLowerCase();
    const line = normalizePoint(outcome?.point);
    const price = parseNumber(outcome?.price);
    if (!playerNameRaw || !playerNameNormalized || !['over', 'under'].includes(side) || line === null || !Number.isFinite(price)) continue;
    const key = `${playerNameNormalized}::${line}`;
    if (!grouped.has(key)) {
      grouped.set(key, { player_name_raw: playerNameRaw, player_name_normalized: playerNameNormalized, line, over: null, under: null });
    }
    grouped.get(key)[side] = outcome;
  }
  return [...grouped.values()].filter((pair) => pair.over && pair.under);
}

function mapSportKey(row) {
  const sport = String(row.sport || '').toUpperCase();
  if (sport === 'NBA') return 'basketball_nba';
  if (sport === 'NCAAB') return 'basketball_ncaab';
  if (sport === 'NHL') return 'icehockey_nhl';
  if (sport === 'MLB') return 'baseball_mlb';
  return null;
}

function marketKeyForRow(row) {
  if (row.market_family === 'player_prop' && String(row.prop_type || '').toLowerCase() === 'points') {
    return PHASE1_NBA_POINTS_PROP_KEY;
  }
  const type = String(row.market_type || '').toLowerCase();
  if (type === 'ml') return 'h2h';
  if (type === 'spread') return 'spreads';
  if (type === 'total') return 'totals';
  return null;
}

function isPendingRejectedClose(row) {
  if (String(row.final_decision || '').toUpperCase() !== 'SIT') return false;
  const status = String(row.close_capture_status || '').trim().toLowerCase();
  return status === 'pending';
}

async function loadOddsApiKey() {
  const envKey = String(process.env.ODDS_API_KEY || '').trim();
  if (envKey.length >= 16) return envKey;

  if (process.platform === 'darwin') {
    try {
      const { stdout } = await execFileAsync('security', [
        'find-generic-password',
        '-a',
        ODDS_KEY_ACCOUNT,
        '-s',
        ODDS_KEY_SERVICE,
        '-w',
      ]);
      const keychainKey = String(stdout || '').trim();
      if (keychainKey.length >= 16) return keychainKey;
    } catch {}
  }

  try {
    const payload = JSON.parse(await fs.readFile(RUNTIME_SECURE_KEY_FILE, 'utf8'));
    const localSecret = process.env.TIEREDGE_LOCAL_SECRET;
    const fallback = `${os.userInfo().username}@${os.hostname()}`;
    const key = crypto.createHash('sha256').update(localSecret || fallback).digest();
    const iv = Buffer.from(payload.iv, 'base64');
    const tag = Buffer.from(payload.tag, 'base64');
    const data = Buffer.from(payload.data, 'base64');
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    const decrypted = Buffer.concat([decipher.update(data), decipher.final()]).toString('utf8').trim();
    return decrypted.length >= 16 ? decrypted : null;
  } catch {
    return null;
  }
}

async function fetchEventOddsPayload({ sportKey, eventId, books, markets, apiKey }) {
  const url = new URL(`https://api.the-odds-api.com/v4/sports/${sportKey}/events/${eventId}/odds`);
  url.searchParams.set('apiKey', apiKey);
  url.searchParams.set('regions', 'us');
  url.searchParams.set('markets', markets.join(','));
  url.searchParams.set('bookmakers', books.join(','));
  url.searchParams.set('oddsFormat', 'american');
  url.searchParams.set('dateFormat', 'iso');
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`event_odds_fetch_failed:${sportKey}:${eventId}:${response.status}:${text.slice(0, 160)}`);
  }
  return response.json();
}

function buildMainMarketCloseMatch(row, bookmaker, market) {
  const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
  if (normalizedBook !== normalizeBookKey(row.sportsbook)) return null;
  const keyedOutcomes = (market.outcomes || []).map((outcome) => ({
    ...outcome,
    key: outcomeKey(market.key, outcome),
    price_num: parseNumber(outcome.price),
  }));
  if (keyedOutcomes.length !== 2 || keyedOutcomes.some((outcome) => !Number.isFinite(outcome.price_num))) {
    return { status: 'failed', reason: 'missing_two_sided_market' };
  }
  const fairProbMap = computeFairProbMap(keyedOutcomes.map((outcome) => ({ ...outcome, price: outcome.price_num })));
  if (fairProbMap.size !== 2) {
    return { status: 'failed', reason: 'missing_two_sided_market' };
  }

  let targetKey = null;
  if (market.key === 'h2h') {
    targetKey = normalizeName(String(row.selection || '').replace(/\s+ml$/i, ''));
  } else if (market.key === 'spreads') {
    const sideName = String(row.selection || '').replace(/\s+[+-]?\d+(\.\d+)?$/i, '').trim();
    const point = normalizePoint((String(row.selection || '').match(/([+-]?\d+(\.\d+)?)$/) || [])[1]);
    targetKey = `${normalizeName(sideName)}::${point}`;
  } else if (market.key === 'totals') {
    const side = String(row.selection || '').split(' ')[0];
    const point = normalizePoint((String(row.selection || '').match(/([+-]?\d+(\.\d+)?)$/) || [])[1]);
    targetKey = `${normalizeName(side)}::${point}`;
  }

  const targetOutcome = keyedOutcomes.find((outcome) => outcome.key === targetKey);
  if (!targetOutcome) return { status: 'failed', reason: 'selection_not_found' };

  return {
    status: 'captured',
    closing_odds_american: String(targetOutcome.price),
    closing_implied_prob: impliedProbFromAmerican(targetOutcome.price_num),
    closing_devig_prob: fairProbMap.get(targetOutcome.key) ?? null,
    closing_line: normalizePoint(targetOutcome.point),
  };
}

function buildPropCloseMatch(row, bookmaker, market) {
  const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
  if (normalizedBook !== normalizeBookKey(row.sportsbook)) return null;
  const targetName = normalizePlayerName(row.player_name_raw || row.player_name_normalized);
  const targetSide = String(row.prop_side || '').toLowerCase();
  const targetLine = normalizePoint(row.prop_line);
  const pair = buildPlayerPropPairs(market.outcomes || []).find((entry) =>
    entry.player_name_normalized === targetName && entry.line === targetLine
  );
  if (!pair) return { status: 'failed', reason: 'selection_not_found' };
  const fairProbMap = computeFairProbMap([
    { ...pair.over, key: `${pair.player_name_normalized}::${pair.line}::over` },
    { ...pair.under, key: `${pair.player_name_normalized}::${pair.line}::under` },
  ]);
  const outcome = pair[targetSide];
  if (!outcome || fairProbMap.size !== 2) return { status: 'failed', reason: 'missing_two_sided_market' };
  const key = `${pair.player_name_normalized}::${pair.line}::${targetSide}`;
  return {
    status: 'captured',
    closing_odds_american: String(outcome.price),
    closing_implied_prob: impliedProbFromAmerican(outcome.price),
    closing_devig_prob: fairProbMap.get(key) ?? null,
    closing_line: pair.line,
  };
}

function buildCloseCaptureRecord(row, capture) {
  const originalImplied = parseNumber(row.devig_implied_prob);
  const closingDevigProb = parseNumber(capture.closing_devig_prob);
  const clvDeltaPct = Number.isFinite(closingDevigProb) && Number.isFinite(originalImplied)
    ? round2((closingDevigProb - originalImplied) * 100)
    : null;
  const clvDirection = clvDeltaPct === null
    ? null
    : clvDeltaPct > 0.05
      ? 'positive'
      : clvDeltaPct < -0.05
        ? 'negative'
        : 'neutral';
  return {
    capture_id: `rejected-close::${row.rec_id}::${new Date().toISOString()}`,
    run_id: row.run_id,
    rec_id: row.rec_id,
    event_id: row.event_id || null,
    market_type: row.market_type || null,
    selection: row.selection || null,
    sportsbook: row.sportsbook || null,
    captured_at_utc: new Date().toISOString(),
    close_capture_status: capture.status,
    closing_odds_american: capture.closing_odds_american ?? null,
    closing_implied_prob: Number.isFinite(capture.closing_implied_prob) ? Number(capture.closing_implied_prob.toFixed(4)) : null,
    closing_devig_prob: Number.isFinite(closingDevigProb) ? Number(closingDevigProb.toFixed(4)) : null,
    closing_line: capture.closing_line ?? null,
    clv_delta_pct: clvDeltaPct,
    clv_direction: clvDirection,
    failure_reason: capture.status === 'failed' ? (capture.reason || 'close_not_found') : null,
  };
}

async function main() {
  const rows = readJsonl(CORE_PATHS.decisionLedger);
  const existingCaptures = readJsonl(CLOSE_CAPTURE_PATH);
  const capturedRecIds = new Set(existingCaptures.map((row) => String(row.rec_id || '').trim()).filter(Boolean));
  const pendingRows = rows.filter((row) => isPendingRejectedClose(row) && !capturedRecIds.has(String(row.rec_id || '').trim()));

  if (!pendingRows.length) {
    console.log(JSON.stringify({
      status: 'ok',
      pending_rows: 0,
      captured: 0,
      failed: 0,
      reason: 'No rejected rows currently pending close capture.',
    }, null, 2));
    return;
  }

  const apiKey = await loadOddsApiKey();
  if (!apiKey) {
    throw new Error('odds_api_key_unavailable_for_rejected_close_capture');
  }

  const groups = new Map();
  for (const row of pendingRows) {
    const sportKey = mapSportKey(row);
    const marketKey = marketKeyForRow(row);
    const bookKey = normalizeBookKey(row.sportsbook);
    if (!sportKey || !marketKey || !bookKey || !row.event_id) continue;
    const groupKey = `${sportKey}::${row.event_id}`;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, { sportKey, eventId: row.event_id, books: new Set(), markets: new Set(), rows: [] });
    }
    const group = groups.get(groupKey);
    group.books.add(bookKey);
    group.markets.add(marketKey);
    group.rows.push(row);
  }

  const appended = [];
  for (const group of groups.values()) {
    const payload = await fetchEventOddsPayload({
      sportKey: group.sportKey,
      eventId: group.eventId,
      books: [...group.books],
      markets: [...group.markets],
      apiKey,
    });

    for (const row of group.rows) {
      const marketKey = marketKeyForRow(row);
      let matched = null;
      for (const bookmaker of payload.bookmakers || []) {
        for (const market of bookmaker.markets || []) {
          if (market.key !== marketKey) continue;
          matched = row.market_family === 'player_prop'
            ? buildPropCloseMatch(row, bookmaker, market)
            : buildMainMarketCloseMatch(row, bookmaker, market);
          if (matched) break;
        }
        if (matched) break;
      }
      appended.push(buildCloseCaptureRecord(row, matched || { status: 'failed', reason: 'close_not_found' }));
    }
  }

  if (appended.length) {
    appendJsonl(CLOSE_CAPTURE_PATH, appended, (row) => String(row.rec_id || '').trim());
  }

  console.log(JSON.stringify({
    status: 'ok',
    pending_rows: pendingRows.length,
    appended_records: appended.length,
    captured: appended.filter((row) => row.close_capture_status === 'captured').length,
    failed: appended.filter((row) => row.close_capture_status === 'failed').length,
    output_path: CLOSE_CAPTURE_PATH,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});

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
const CLOSE_CAPTURE_RUNS_PATH = CORE_PATHS.rejectedCloseCaptureRuns;
const KNOWN_FEED_BOOKS = ['draftkings', 'fanduel', 'betmgm', 'betrivers', 'bet365', 'caesars', 'circa'];
const EXACT_CLOSE_STATUSES = new Set(['captured', 'failed', 'insufficient_market_match', 'not_available']);

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2).replace(/-/g, '_');
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function appendCloseCaptureRun(row) {
  appendJsonl(CLOSE_CAPTURE_RUNS_PATH, row, (existing) => String(existing.run_id || '').trim());
}

function todayCtDateKey(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

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

function deriveRejectionClass(row) {
  if (row.rejection_class) return String(row.rejection_class).trim().toLowerCase();
  if (row.rejection_reason === 'research_only_non_owned_book') return 'non_executable_edge';
  if (row.rejection_reason === 'sub_minimum_kelly') return 'sub_minimum_kelly';
  if (row.rejection_reason === 'invalid_snapshot') return 'stale_or_unverified_odds';
  if (row.rejection_stage === 'risk_gate') return 'risk_gate_rejected';
  if (row.rejection_reason === 'no_edge' && Number(parseNumber(row.post_conf_edge_pct)) >= 1.5) return 'near_miss';
  if (row.rejection_reason === 'no_edge') return 'no_edge';
  return 'other_meaningful_canonical';
}

function isEligibleRejectedRow(row) {
  if (String(row.final_decision || '').toUpperCase() !== 'SIT') return false;
  if (!row.event_id || !row.selection || !row.sportsbook) return false;
  if (!Number.isFinite(parseNumber(row.devig_implied_prob))) return false;
  if (!marketKeyForRow(row) || !mapSportKey(row)) return false;
  const rejectionClass = deriveRejectionClass(row);
  return new Set([
    'no_edge',
    'near_miss',
    'non_executable_edge',
    'sub_minimum_kelly',
    'risk_gate_rejected',
    'stale_or_unverified_odds',
    'other_meaningful_canonical',
    'stale_market',
  ]).has(rejectionClass);
}

function shouldProcessRow(row, existingCapture, force = false) {
  if (!isEligibleRejectedRow(row)) return false;
  if (force) return true;
  if (!existingCapture) {
    const status = String(row.close_capture_status || '').trim().toLowerCase();
    return status === 'pending' || status === '';
  }
  return !EXACT_CLOSE_STATUSES.has(String(existingCapture.close_capture_status || '').trim().toLowerCase());
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

function buildMainMarketCloseMatch(row, bookmaker, market, payload, quality) {
  const keyedOutcomes = (market.outcomes || []).map((outcome) => ({
    ...outcome,
    key: outcomeKey(market.key, outcome),
    price_num: parseNumber(outcome.price),
  }));
  if (keyedOutcomes.length !== 2 || keyedOutcomes.some((outcome) => !Number.isFinite(outcome.price_num))) {
    return { status: 'insufficient_market_match', reason: 'missing_two_sided_market', close_match_quality: 'insufficient_match' };
  }
  const fairProbMap = computeFairProbMap(keyedOutcomes.map((outcome) => ({ ...outcome, price: outcome.price_num })));
  if (fairProbMap.size !== 2) {
    return { status: 'insufficient_market_match', reason: 'missing_two_sided_market', close_match_quality: 'insufficient_match' };
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
  if (!targetOutcome) {
    return { status: 'insufficient_market_match', reason: 'selection_not_found', close_match_quality: 'insufficient_match' };
  }

  return {
    status: 'captured',
    closing_odds_american: String(targetOutcome.price),
    closing_odds_decimal: americanToDecimal(targetOutcome.price_num),
    closing_implied_prob: impliedProbFromAmerican(targetOutcome.price_num),
    closing_devig_prob: fairProbMap.get(targetOutcome.key) ?? null,
    closing_snapshot_time_utc: market.last_update || bookmaker.last_update || payload?.commence_time || null,
    closing_book: bookmaker.title || bookmaker.key || null,
    closing_line: normalizePoint(targetOutcome.point),
    close_match_quality: quality,
  };
}

function buildPropCloseMatch(row, bookmaker, market, payload, quality) {
  const targetName = normalizePlayerName(row.player_name_raw || row.player_name_normalized);
  const targetSide = String(row.prop_side || '').toLowerCase();
  const targetLine = normalizePoint(row.prop_line);
  const pair = buildPlayerPropPairs(market.outcomes || []).find((entry) =>
    entry.player_name_normalized === targetName && entry.line === targetLine
  );
  if (!pair) {
    return { status: 'insufficient_market_match', reason: 'selection_not_found', close_match_quality: 'insufficient_match' };
  }
  const fairProbMap = computeFairProbMap([
    { ...pair.over, key: `${pair.player_name_normalized}::${pair.line}::over` },
    { ...pair.under, key: `${pair.player_name_normalized}::${pair.line}::under` },
  ]);
  const outcome = pair[targetSide];
  if (!outcome || fairProbMap.size !== 2) {
    return { status: 'insufficient_market_match', reason: 'missing_two_sided_market', close_match_quality: 'insufficient_match' };
  }
  const key = `${pair.player_name_normalized}::${pair.line}::${targetSide}`;
  return {
    status: 'captured',
    closing_odds_american: String(outcome.price),
    closing_odds_decimal: americanToDecimal(outcome.price),
    closing_implied_prob: impliedProbFromAmerican(outcome.price),
    closing_devig_prob: fairProbMap.get(key) ?? null,
    closing_snapshot_time_utc: market.last_update || bookmaker.last_update || payload?.commence_time || null,
    closing_book: bookmaker.title || bookmaker.key || null,
    closing_line: pair.line,
    close_match_quality: quality,
  };
}

function matchRowToPayload(row, payload) {
  const marketKey = marketKeyForRow(row);
  const sameBookKey = normalizeBookKey(row.sportsbook);
  const matcher = row.market_family === 'player_prop' ? buildPropCloseMatch : buildMainMarketCloseMatch;
  let sawRelevantMarket = false;

  for (const bookmaker of payload.bookmakers || []) {
    if (normalizeBookKey(bookmaker.key || bookmaker.title) !== sameBookKey) continue;
    for (const market of bookmaker.markets || []) {
      if (market.key !== marketKey) continue;
      sawRelevantMarket = true;
      const match = matcher(row, bookmaker, market, payload, 'exact_same_book_same_market');
      if (match.status === 'captured') return match;
    }
  }

  for (const bookmaker of payload.bookmakers || []) {
    if (normalizeBookKey(bookmaker.key || bookmaker.title) === sameBookKey) continue;
    for (const market of bookmaker.markets || []) {
      if (market.key !== marketKey) continue;
      sawRelevantMarket = true;
      const match = matcher(row, bookmaker, market, payload, 'exact_market_cross_book');
      if (match.status === 'captured') return match;
    }
  }

  return sawRelevantMarket
    ? { status: 'insufficient_market_match', reason: 'exact_line_not_found', close_match_quality: 'insufficient_match' }
    : { status: 'failed', reason: 'close_not_found', close_match_quality: 'insufficient_match' };
}

function buildCloseCaptureRecord(row, capture, nowIso) {
  const originalDevigImplied = parseNumber(row.devig_implied_prob);
  const closingDevigProb = parseNumber(capture.closing_devig_prob);
  const clvDeltaPct = Number.isFinite(closingDevigProb) && Number.isFinite(originalDevigImplied)
    ? round2((closingDevigProb - originalDevigImplied) * 100)
    : null;
  const clvDirection = clvDeltaPct === null
    ? 'unknown'
    : clvDeltaPct > 0.05
      ? 'positive'
      : clvDeltaPct < -0.05
        ? 'negative'
        : 'neutral';

  return {
    capture_id: `rejected-close::${row.rec_id}::${nowIso}`,
    run_id: row.run_id,
    rec_id: row.rec_id,
    event_id: row.event_id || null,
    market_family: row.market_family || 'main_market',
    market_type: row.market_type || null,
    selection: row.selection || null,
    sportsbook: row.sportsbook || null,
    captured_at_utc: nowIso,
    close_capture_status: capture.status,
    closing_odds_american: capture.closing_odds_american ?? null,
    closing_odds_decimal: Number.isFinite(capture.closing_odds_decimal) ? Number(capture.closing_odds_decimal.toFixed(4)) : null,
    closing_implied_prob: Number.isFinite(capture.closing_implied_prob) ? Number(capture.closing_implied_prob.toFixed(4)) : null,
    closing_devig_prob: Number.isFinite(closingDevigProb) ? Number(closingDevigProb.toFixed(4)) : null,
    closing_snapshot_time_utc: capture.closing_snapshot_time_utc ?? null,
    closing_book: capture.closing_book ?? null,
    clv_delta_pct: clvDeltaPct,
    clv_direction: clvDirection,
    close_match_quality: capture.close_match_quality || 'insufficient_match',
    closing_line: capture.closing_line ?? null,
    failure_reason: capture.status === 'captured' ? null : (capture.reason || 'close_not_found'),
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const force = args.force === true;
  const limit = Number(parseNumber(args.limit) || 0);
  const startedAt = new Date();
  const startedAtUtc = startedAt.toISOString();

  if (args.skip_due_to_active_lock) {
    const summary = {
      status: 'skipped_due_to_active_lock',
      pending_rows: 0,
      rows_scanned: 0,
      captured: 0,
      failed: 0,
      insufficient_market_match: 0,
      rows_still_pending: 0,
      output_path: CLOSE_CAPTURE_PATH,
    };
    appendCloseCaptureRun({
      run_id: `rejected-close-run::${startedAtUtc}`,
      started_at_utc: startedAtUtc,
      completed_at_utc: new Date().toISOString(),
      status: 'skipped_due_to_active_lock',
      force,
      limit: limit || null,
      eligible_rows: 0,
      rows_scanned: 0,
      captured: 0,
      failed: 0,
      insufficient_market_match: 0,
      rows_still_pending: 0,
      output_path: CLOSE_CAPTURE_PATH,
    });
    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  const rows = readJsonl(CORE_PATHS.decisionLedger);
  const invalidatedRunIds = new Set(
    readJsonl(CORE_PATHS.huntAuditLog)
      .filter((row) => String(row.invalid_status || '').toLowerCase().includes('invalid'))
      .map((row) => String(row.run_id || '').trim())
      .filter(Boolean)
  );
  const existingCaptures = readJsonl(CLOSE_CAPTURE_PATH);
  const captureIndex = new Map(existingCaptures.map((row) => [String(row.rec_id || '').trim(), row]));
  let pendingRows = rows
    .filter((row) => !invalidatedRunIds.has(String(row.run_id || '').trim()))
    .filter((row) => shouldProcessRow(row, captureIndex.get(String(row.rec_id || '').trim()), force));
  if (limit > 0) pendingRows = pendingRows.slice(0, limit);

  if (args.dry_run) {
    console.log(JSON.stringify({
      status: 'ok',
      dry_run: true,
      eligible_rows: pendingRows.length,
      sample_rec_ids: pendingRows.slice(0, 10).map((row) => row.rec_id),
    }, null, 2));
    return;
  }

  if (!pendingRows.length) {
    const summary = {
      status: 'ok',
      pending_rows: 0,
      rows_scanned: 0,
      captured: 0,
      failed: 0,
      insufficient_market_match: 0,
      rows_still_pending: 0,
      reason: 'No rejected rows currently pending close capture.',
      output_path: CLOSE_CAPTURE_PATH,
    };
    appendCloseCaptureRun({
      run_id: `rejected-close-run::${startedAtUtc}`,
      started_at_utc: startedAtUtc,
      completed_at_utc: new Date().toISOString(),
      status: 'ok',
      force,
      limit: limit || null,
      eligible_rows: 0,
      rows_scanned: 0,
      captured: 0,
      failed: 0,
      insufficient_market_match: 0,
      rows_still_pending: 0,
      output_path: CLOSE_CAPTURE_PATH,
    });
    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  const apiKey = await loadOddsApiKey();
  if (!apiKey) {
    throw new Error('odds_api_key_unavailable_for_rejected_close_capture');
  }

  const groups = new Map();
  const currentCtDate = todayCtDateKey();
  for (const row of pendingRows) {
    const sportKey = mapSportKey(row);
    const marketKey = marketKeyForRow(row);
    if (!sportKey || !marketKey || !row.event_id) continue;
    if (String(row.target_date || '') < currentCtDate) {
      appended.push(buildCloseCaptureRecord(row, {
        status: 'not_available',
        reason: 'historical_close_unavailable',
        close_match_quality: 'insufficient_match',
      }, new Date().toISOString()));
      continue;
    }
    const groupKey = `${sportKey}::${row.event_id}`;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, { sportKey, eventId: row.event_id, books: new Set(KNOWN_FEED_BOOKS), markets: new Set(), rows: [] });
    }
    const group = groups.get(groupKey);
    group.markets.add(marketKey);
    group.rows.push(row);
  }

  const appended = [];
  for (const group of groups.values()) {
    let payload = null;
    try {
      payload = await fetchEventOddsPayload({
        sportKey: group.sportKey,
        eventId: group.eventId,
        books: [...group.books],
        markets: [...group.markets],
        apiKey,
      });
    } catch (error) {
      const nowIso = new Date().toISOString();
      const message = String(error.message || 'event_odds_fetch_failed');
      const unavailable = message.includes(':404:');
      for (const row of group.rows) {
        appended.push(buildCloseCaptureRecord(row, {
          status: unavailable ? 'not_available' : 'failed',
          reason: unavailable ? 'historical_close_unavailable' : message,
          close_match_quality: 'insufficient_match',
        }, nowIso));
      }
      continue;
    }

    for (const row of group.rows) {
      const nowIso = new Date().toISOString();
      const matched = matchRowToPayload(row, payload);
      appended.push(buildCloseCaptureRecord(row, matched, nowIso));
    }
  }

  if (appended.length) {
    appendJsonl(CLOSE_CAPTURE_PATH, appended, (row) => String(row.rec_id || '').trim());
  }

  const scannedRecIds = new Set(appended.map((row) => String(row.rec_id || '').trim()));
  const rowsStillPending = pendingRows.filter((row) => {
    const recId = String(row.rec_id || '').trim();
    if (!scannedRecIds.has(recId)) return true;
    const capture = appended.find((entry) => String(entry.rec_id || '').trim() === recId);
    return !capture || capture.close_capture_status === 'pending';
  }).length;

  const summary = {
    status: 'ok',
    pending_rows: pendingRows.length,
    rows_scanned: pendingRows.length,
    appended_records: appended.length,
    captured: appended.filter((row) => row.close_capture_status === 'captured').length,
    failed: appended.filter((row) => row.close_capture_status === 'failed').length,
    insufficient_market_match: appended.filter((row) => row.close_capture_status === 'insufficient_market_match').length,
    rows_still_pending: rowsStillPending,
    output_path: CLOSE_CAPTURE_PATH,
  };

  appendCloseCaptureRun({
    run_id: `rejected-close-run::${startedAtUtc}`,
    started_at_utc: startedAtUtc,
    completed_at_utc: new Date().toISOString(),
    status: 'ok',
    force,
    limit: limit || null,
    eligible_rows: pendingRows.length,
    rows_scanned: pendingRows.length,
    captured: summary.captured,
    failed: summary.failed,
    insufficient_market_match: summary.insufficient_market_match,
    rows_still_pending: rowsStillPending,
    output_path: CLOSE_CAPTURE_PATH,
  });

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});

#!/usr/bin/env node
import { execFile } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import process from 'node:process';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { appendNativeDecisionRows } from './native-decision-log-utils.mjs';
import { computeKellyBreakdown } from './tierededge-kelly-cli.mjs';
import { loadScanCoveragePolicy } from './scan-coverage-utils.mjs';
import { CORE_PATHS, formatMoney, parseNumber, readJson, readJsonl, round2, writeJson } from './core-ledger-utils.mjs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';
import { formatCtTimestamp } from './openclaw-runtime-utils.mjs';

const execFileAsync = promisify(execFile);
const ODDS_KEY_SERVICE = 'tierededge-odds-api';
const ODDS_KEY_ACCOUNT = 'default';
const RUNTIME_SECURE_DIR = '/Users/jaredbuckman/.openclaw/workspace/memory/secure';
const RUNTIME_SECURE_KEY_FILE = path.join(RUNTIME_SECURE_DIR, 'odds-api-key.enc.json');
const SPORT_LABELS = {
  basketball_nba: 'NBA',
  basketball_ncaab: 'NCAAB',
  icehockey_nhl: 'NHL',
  baseball_mlb: 'MLB',
};
const PHASE1_NBA_POINTS_PROP_KEY = 'player_points';
const TIER_LIMITS = {
  T1: { maxBets: 2 },
  T2: { maxBets: 4 },
  T3: { maxBets: 6 },
};
const DEFAULT_SNAPSHOT_WINDOW_SECONDS = 30;
const DEFAULT_CONSENSUS_MEDIAN_GUARD_PCT = 5;

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

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
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

function americanToDecimal(odds) {
  const num = Number(odds);
  if (!Number.isFinite(num) || num === 0) return null;
  return num > 0 ? (1 + (num / 100)) : (1 + (100 / Math.abs(num)));
}

function impliedProbFromAmerican(odds) {
  const decimal = americanToDecimal(odds);
  return decimal ? (1 / decimal) : null;
}

function asUnitProbability(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num > 1 && num <= 100) return num / 100;
  return num;
}

function todayCtDateKey(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function formatCtMinute(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date).replace(',', '');
}

function eventIsTodayCt(event, targetDateKey) {
  const commence = Date.parse(String(event?.commence_time || ''));
  if (!Number.isFinite(commence)) return false;
  const eventDay = todayCtDateKey(new Date(commence));
  return eventDay === targetDateKey;
}

function buildEventLabel(event) {
  const away = String(event?.away_team || '').trim();
  const home = String(event?.home_team || '').trim();
  return away && home ? `${away} @ ${home}` : (event?.id || 'Unknown Event');
}

function marketTypeLabel(key) {
  if (key === 'h2h') return 'ML';
  if (key === 'spreads') return 'Spread';
  if (key === 'totals') return 'Total';
  if (key === PHASE1_NBA_POINTS_PROP_KEY) return 'Player Points';
  return key;
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

function displaySelection(event, marketKey, outcome) {
  const point = normalizePoint(outcome?.point);
  const name = String(outcome?.name || '').trim();
  if (marketKey === 'h2h') return `${name} ML`;
  if (marketKey === 'spreads') return `${name} ${point > 0 ? '+' : ''}${point}`;
  if (marketKey === 'totals') return `${name} ${point}`;
  if (marketKey === PHASE1_NBA_POINTS_PROP_KEY) {
    const player = String(outcome?.description || '').trim();
    const side = String(outcome?.name || '').trim();
    return `${player} ${side} ${point} Points`;
  }
  return name;
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

function confidenceFromCoverage({ bookmakerCount, freshestMinutes }) {
  const oddsQuality = bookmakerCount >= 4 ? 0.95 : bookmakerCount >= 3 ? 0.85 : bookmakerCount >= 2 ? 0.75 : 0.45;
  const marketQuality = bookmakerCount >= 4 ? 0.85 : bookmakerCount >= 3 ? 0.75 : bookmakerCount >= 2 ? 0.65 : 0.45;
  const freshnessPenalty = freshestMinutes == null ? 0.25 : freshestMinutes <= 10 ? 0 : freshestMinutes <= 20 ? 0.1 : 0.25;
  const adjustedOddsQuality = Math.max(0, oddsQuality - freshnessPenalty);
  return round2((0.4 * adjustedOddsQuality) + (0.3 * 0.5) + (0.3 * marketQuality));
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
  return slugify(raw);
}

function buildNormalizedBookSet(values = []) {
  return new Set((values || []).map((value) => normalizeBookKey(value)).filter(Boolean));
}

function buildOwnedBookSet(policy) {
  return buildNormalizedBookSet(policy?.book_sets?.owned_books || policy?.book_sets?.executable_books || []);
}

function resolveOwnedBookSet(policy, args) {
  if (args.owned_books) {
    return buildNormalizedBookSet(String(args.owned_books).split(',').map((value) => value.trim()).filter(Boolean));
  }
  return buildOwnedBookSet(policy);
}

function eventMatchesFilter(event, filterValue) {
  const filter = normalizeName(filterValue);
  if (!filter) return true;
  const haystack = [
    event?.id,
    event?.home_team,
    event?.away_team,
    buildEventLabel(event),
  ].map((value) => normalizeName(value)).join(' ');
  return haystack.includes(filter);
}

function buildConsensusBookSet(policy) {
  return buildNormalizedBookSet(
    policy?.book_sets?.consensus_books
    || [
      ...(policy?.priority_tiers?.tier_a?.default_books || []),
      ...(policy?.priority_tiers?.tier_a?.comparison_books || []),
      ...(policy?.book_sets?.comparison_books || []),
      ...(policy?.book_sets?.owned_books || []),
    ]
  );
}

function resolveProbabilityPipeline(policy) {
  const config = policy?.probability_pipeline || {};
  return {
    snapshotWindowMs: (parseNumber(config.snapshot_window_seconds) || DEFAULT_SNAPSHOT_WINDOW_SECONDS) * 1000,
    consensusMethod: String(config.consensus_method || 'trimmed_mean_with_median_guard').trim(),
    consensusMedianGuardPct: parseNumber(config.consensus_median_guard_pct) || DEFAULT_CONSENSUS_MEDIAN_GUARD_PCT,
  };
}

function resolveRiskControls(policy) {
  const config = policy?.risk_controls || {};
  return {
    mainMarketEdgeAnomalyPct: parseNumber(config.main_market_edge_anomaly_pct) || 7,
    maxStakePctPerBet: (parseNumber(config.max_stake_pct_per_bet) || 3) / 100,
    maxTotalExposurePctPerRun: (parseNumber(config.max_total_exposure_pct_per_run) || 9) / 100,
    rejectStartedEvents: config.reject_started_events !== false,
    minimumMinutesToStart: parseNumber(config.minimum_minutes_to_start) || 15,
  };
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

function trimmedMean(values) {
  if (!values.length) return null;
  if (values.length < 4) return median(values);
  const sorted = [...values].sort((a, b) => a - b);
  const trimmed = sorted.slice(1, -1);
  return trimmed.reduce((sum, value) => sum + value, 0) / trimmed.length;
}

function computeStableConsensus(values, pipeline) {
  const medianProb = median(values);
  const consensusProb = pipeline.consensusMethod === 'median'
    ? medianProb
    : trimmedMean(values);
  const deviationPct = Number.isFinite(consensusProb) && Number.isFinite(medianProb)
    ? round2(Math.abs(consensusProb - medianProb) * 100)
    : null;
  return {
    consensusProb,
    medianProb,
    deviationPct,
    sane: deviationPct === null ? false : deviationPct <= pipeline.consensusMedianGuardPct,
  };
}

function collectObservedBooks(source, observedBooks) {
  for (const bookmaker of source?.bookmakers || []) {
    const key = normalizeBookKey(bookmaker.key || bookmaker.title);
    if (key) observedBooks.add(key);
  }
}

function resolvePhase1NbaPointsProps(policy, args) {
  const config = policy?.feature_flags?.phase1_nba_points_props || {};
  const enabledByConfig = config.enabled === true;
  const enabledByCli = args.enable_phase1_nba_points_props === true;
  return {
    configured_enabled: enabledByConfig,
    enabled_for_run: enabledByConfig || enabledByCli,
    enable_source: enabledByCli && !enabledByConfig ? 'cli_override' : 'config',
    config,
  };
}

function resolvePhase1NbaSpreads(policy, args) {
  const config = policy?.feature_flags?.phase1_nba_spreads || {};
  const enabledByConfig = config.enabled === true;
  const enabledByCli = args.enable_phase1_nba_spreads === true;
  return {
    configured_enabled: enabledByConfig,
    enabled_for_run: enabledByConfig || enabledByCli,
    enable_source: enabledByCli && !enabledByConfig ? 'cli_override' : 'config',
    config,
  };
}

function resolvePhase1MlbMoneylines(policy, args) {
  const config = policy?.feature_flags?.phase1_mlb_moneylines || {};
  const enabledByConfig = config.enabled === true;
  const enabledByCli = args.enable_phase1_mlb_moneylines === true;
  return {
    configured_enabled: enabledByConfig,
    enabled_for_run: enabledByConfig || enabledByCli,
    enable_source: enabledByCli && !enabledByConfig ? 'cli_override' : 'config',
    config,
  };
}

function buildEligibleBookSet(values = []) {
  return buildNormalizedBookSet(values);
}

function buildFeatureMarketControl({ feature, fallbackConsensusBooks, fallbackOwnedBooks }) {
  const eligibleBooks = buildEligibleBookSet(feature?.config?.eligible_books || feature?.config?.trusted_books || []);
  return {
    enabled_for_run: feature?.enabled_for_run === true,
    minBookCount: parseNumber(feature?.config?.min_book_count) || 2,
    eligibleBooks: eligibleBooks.size ? eligibleBooks : new Set(fallbackConsensusBooks || fallbackOwnedBooks || []),
  };
}

function resolveMarketControl({ sportKey, marketKey, nbaSpreadsControl, mlbMoneylineControl, consensusBooksSet }) {
  if (sportKey === 'basketball_nba' && marketKey === 'spreads' && nbaSpreadsControl.enabled_for_run) {
    return nbaSpreadsControl;
  }
  if (sportKey === 'baseball_mlb' && marketKey === 'h2h' && mlbMoneylineControl.enabled_for_run) {
    return mlbMoneylineControl;
  }
  return {
    enabled_for_run: true,
    minBookCount: 2,
    eligibleBooks: new Set(consensusBooksSet),
  };
}

function deriveTier(edgePct) {
  if (!Number.isFinite(edgePct)) return null;
  if (edgePct >= 6) return 'T1';
  if (edgePct >= 4) return 'T2';
  if (edgePct >= 2) return 'T3';
  return null;
}

function rankCandidates(a, b) {
  if ((b.post_conf_edge_pct ?? -Infinity) !== (a.post_conf_edge_pct ?? -Infinity)) {
    return (b.post_conf_edge_pct ?? -Infinity) - (a.post_conf_edge_pct ?? -Infinity);
  }
  const aOdds = parseNumber(a.odds_american) ?? -Infinity;
  const bOdds = parseNumber(b.odds_american) ?? -Infinity;
  return bOdds - aOdds;
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
    } catch {
      // fall through to encrypted local store
    }
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

async function fetchOddsPayload({ sportKey, books, markets, apiKey }) {
  const url = new URL(`https://api.the-odds-api.com/v4/sports/${sportKey}/odds`);
  url.searchParams.set('apiKey', apiKey);
  url.searchParams.set('regions', 'us');
  url.searchParams.set('markets', markets.join(','));
  url.searchParams.set('bookmakers', books.join(','));
  url.searchParams.set('oddsFormat', 'american');
  url.searchParams.set('dateFormat', 'iso');
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`odds_fetch_failed:${sportKey}:${response.status}:${text.slice(0, 160)}`);
  }
  return response.json();
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

function buildPlayerPropPairs(outcomes) {
  const grouped = new Map();
  for (const outcome of outcomes || []) {
    const playerNameRaw = String(outcome?.description || '').trim();
    const playerNameNormalized = normalizePlayerName(playerNameRaw);
    const side = String(outcome?.name || '').trim().toLowerCase();
    const line = normalizePoint(outcome?.point);
    const price = parseNumber(outcome?.price);
    if (!playerNameRaw || !playerNameNormalized || !['over', 'under'].includes(side) || line === null || !Number.isFinite(price)) {
      continue;
    }
    const key = `${playerNameNormalized}::${line}`;
    if (!grouped.has(key)) {
      grouped.set(key, {
        player_name_raw: playerNameRaw,
        player_name_normalized: playerNameNormalized,
        line,
        over: null,
        under: null,
      });
    }
    grouped.get(key)[side] = outcome;
  }
  return [...grouped.values()].filter((pair) => pair.over && pair.under);
}

function createBaseRow({
  sportKey,
  event,
  bookmaker,
  market,
  outcome,
  scanTimeCt,
  runId,
  bankrollSnapshot,
  ownedBooks,
}) {
  const bookKey = normalizeBookKey(bookmaker.key || bookmaker.title);
  const actionableBook = ownedBooks.has(bookKey);
  return {
    run_id: runId,
    rec_id: `${runId}::${sportKey}::${slugify(event.id || buildEventLabel(event))}::${market.key}::${slugify(displaySelection(event, market.key, outcome))}::${slugify(bookmaker.title || bookmaker.key)}`,
    timestamp_ct: scanTimeCt,
    target_date: todayCtDateKey(),
    market_family: 'main_market',
    sport: SPORT_LABELS[sportKey] || sportKey.toUpperCase(),
    league: SPORT_LABELS[sportKey] || sportKey.toUpperCase(),
    event_id: event.id || null,
    event_label: buildEventLabel(event),
    event_home_team: event.home_team || null,
    event_away_team: event.away_team || null,
    market_type: marketTypeLabel(market.key),
    selection: displaySelection(event, market.key, outcome),
    sportsbook: bookmaker.title || bookmaker.key || 'Unknown',
    owned_book: actionableBook,
    live_feed_book: true,
    actionable_book: actionableBook,
    odds_american: parseNumber(outcome.price) === null ? null : String(outcome.price),
    odds_decimal: round2(americanToDecimal(outcome.price)),
    devig_implied_prob: null,
    consensus_prob: null,
    pre_conf_true_prob: null,
    confidence_score: null,
    post_conf_true_prob: null,
    raw_edge_pct: null,
    post_conf_edge_pct: null,
    tier_threshold_pct: 2,
    threshold_gap_pct: null,
    price_edge_pass: false,
    executable_book: actionableBook,
    bet_permission_pass: false,
    final_decision: 'SIT',
    rejection_stage: '',
    rejection_reason: '',
    rejection_class: '',
    surfaced_as_closest_miss: false,
    close_capture_status: 'pending',
    closing_odds_american: null,
    closing_odds_decimal: null,
    closing_implied_prob: null,
    closing_devig_prob: null,
    closing_snapshot_time_utc: null,
    closing_book: null,
    clv_delta_pct: null,
    clv_direction: 'unknown',
    close_match_quality: 'insufficient_match',
    closing_line: null,
    snapshot_status: 'not_validated',
    snapshot_max_spread_seconds: null,
    consensus_method: null,
    consensus_book_count: null,
    consensus_median_prob: null,
    bet_class: 'EDGE_BET',
    bankroll_snapshot: bankrollSnapshot,
    kelly_stake: 0,
    include_in_core_strategy_metrics: true,
    include_in_actual_bankroll: false,
  };
}

function invalidSnapshotRows({
  sportKey,
  event,
  bookmaker,
  market,
  scanTimeCt,
  runId,
  bankrollSnapshot,
  ownedBooks,
  snapshotStatus,
  snapshotMaxSpreadSeconds = null,
  consensusMethod = null,
  rejectionClass = 'invalid_snapshot',
}) {
  return (market.outcomes || []).map((outcome) => ({
    ...createBaseRow({
      sportKey,
      event,
      bookmaker,
      market,
      outcome,
      scanTimeCt,
      runId,
      bankrollSnapshot,
      ownedBooks,
    }),
    rejection_stage: 'integrity_gate',
    rejection_reason: 'invalid_snapshot',
    rejection_class: rejectionClass,
    snapshot_status: snapshotStatus,
    snapshot_max_spread_seconds: snapshotMaxSpreadSeconds,
    consensus_method: consensusMethod,
  }));
}

function buildValidatedMainMarketSnapshot({ bookmaker, market }) {
  const bookKey = normalizeBookKey(bookmaker.key || bookmaker.title);
  const timestampIso = String(market?.last_update || bookmaker?.last_update || '').trim();
  const updateMs = Date.parse(timestampIso);
  if (!bookKey || !Number.isFinite(updateMs)) {
    return { valid: false, reason: 'missing_timestamp', bookKey, updateMs: null };
  }
  const keyedOutcomes = (market.outcomes || []).map((outcome) => ({
    ...outcome,
    key: outcomeKey(market.key, outcome),
    price_num: parseNumber(outcome.price),
  }));
  if (keyedOutcomes.length !== 2) {
    return { valid: false, reason: 'missing_counterpart', bookKey, updateMs };
  }
  if (keyedOutcomes.some((outcome) => !Number.isFinite(outcome.price_num))) {
    return { valid: false, reason: 'unverified_odds', bookKey, updateMs };
  }
  const uniqueKeys = new Set(keyedOutcomes.map((outcome) => outcome.key));
  if (uniqueKeys.size !== keyedOutcomes.length) {
    return { valid: false, reason: 'duplicate_outcomes', bookKey, updateMs };
  }
  const fairProbMap = computeFairProbMap(keyedOutcomes.map((outcome) => ({
    ...outcome,
    price: outcome.price_num,
  })));
  if (fairProbMap.size !== 2) {
    return { valid: false, reason: 'invalid_devig_pair', bookKey, updateMs };
  }
  return {
    valid: true,
    bookKey,
    bookTitle: bookmaker.title || bookmaker.key || 'Unknown',
    updateMs,
    updateIso: timestampIso,
    outcomesByKey: new Map(keyedOutcomes.map((outcome) => [outcome.key, {
      fairProb: fairProbMap.get(outcome.key),
      outcome,
    }])),
  };
}

function computePropConsensusMap(event, marketKey, control) {
  const grouped = new Map();
  for (const bookmaker of event.bookmakers || []) {
    const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
    if (control?.eligibleBooks?.size && !control.eligibleBooks.has(normalizedBook)) continue;
    for (const market of bookmaker.markets || []) {
      if (market.key !== marketKey) continue;
      for (const pair of buildPlayerPropPairs(market.outcomes || [])) {
        const fairProbMap = computeFairProbMap([
          { ...pair.over, key: `${pair.player_name_normalized}::${pair.line}::over` },
          { ...pair.under, key: `${pair.player_name_normalized}::${pair.line}::under` },
        ]);
        for (const side of ['over', 'under']) {
          const outcomeKeyValue = `${pair.player_name_normalized}::${pair.line}::${side}`;
          const fairProb = fairProbMap.get(outcomeKeyValue);
          if (!Number.isFinite(fairProb)) continue;
          if (!grouped.has(outcomeKeyValue)) grouped.set(outcomeKeyValue, []);
          grouped.get(outcomeKeyValue).push({
            bookmaker: bookmaker.key,
            fairProb,
            player_name_raw: pair.player_name_raw,
            line: pair.line,
          });
        }
      }
    }
  }
  const consensus = new Map();
  for (const [key, entries] of grouped.entries()) {
    if (entries.length < (control?.minBookCount || 2)) continue;
    const avgFairProb = entries.reduce((sum, entry) => sum + entry.fairProb, 0) / entries.length;
    const sample = entries[0] || {};
    consensus.set(key, {
      avgFairProb,
      bookmakerCount: entries.length,
      player_name_raw: sample.player_name_raw || null,
      line: sample.line ?? null,
    });
  }
  return consensus;
}

function buildConsensusContext({ event, marketKey, consensusBooks, pipeline, minBookCount = 2, eligibleBooks = null }) {
  const candidateSnapshots = new Map();
  const consensusSnapshots = [];
  for (const bookmaker of event.bookmakers || []) {
    const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
    if (eligibleBooks?.size && !eligibleBooks.has(normalizedBook)) continue;
    for (const market of bookmaker.markets || []) {
      if (market.key !== marketKey) continue;
      const snapshot = buildValidatedMainMarketSnapshot({ bookmaker, market });
      const bookKey = snapshot.bookKey || normalizeBookKey(bookmaker.key || bookmaker.title);
      candidateSnapshots.set(bookKey, snapshot);
      if (snapshot.valid && consensusBooks.has(bookKey)) {
        consensusSnapshots.push(snapshot);
      }
    }
  }
  if (!consensusSnapshots.length) {
    return {
      candidateSnapshots,
      consensusByOutcome: new Map(),
      maxSpreadSeconds: null,
      syncedBookKeys: new Set(),
      snapshotStatus: 'no_valid_consensus_books',
    };
  }

  const freshestMs = Math.max(...consensusSnapshots.map((snapshot) => snapshot.updateMs));
  const syncedSnapshots = consensusSnapshots.filter((snapshot) => (freshestMs - snapshot.updateMs) <= pipeline.snapshotWindowMs);
  if (syncedSnapshots.length < minBookCount) {
    return {
      candidateSnapshots,
      consensusByOutcome: new Map(),
      maxSpreadSeconds: null,
      syncedBookKeys: new Set(syncedSnapshots.map((snapshot) => snapshot.bookKey)),
      snapshotStatus: 'insufficient_valid_books',
    };
  }
  const syncedBookKeys = new Set(syncedSnapshots.map((snapshot) => snapshot.bookKey));
  const minMs = Math.min(...syncedSnapshots.map((snapshot) => snapshot.updateMs));
  const maxSpreadSeconds = round2((freshestMs - minMs) / 1000);
  const grouped = new Map();

  for (const snapshot of syncedSnapshots) {
    for (const [key, entry] of snapshot.outcomesByKey.entries()) {
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key).push({
        bookmaker: snapshot.bookKey,
        fairProb: entry.fairProb,
      });
    }
  }

  const consensusByOutcome = new Map();
  for (const [key, entries] of grouped.entries()) {
    const stable = computeStableConsensus(entries.map((entry) => entry.fairProb), pipeline);
    consensusByOutcome.set(key, {
      consensusProb: stable.consensusProb,
      medianProb: stable.medianProb,
      sanityDeviationPct: stable.deviationPct,
      sanityPass: stable.sane,
      bookmakerCount: entries.length,
      maxSpreadSeconds,
    });
  }

  return {
    candidateSnapshots,
    consensusByOutcome,
    maxSpreadSeconds,
    syncedBookKeys,
    snapshotStatus: 'valid',
  };
}

function buildMarketRows({
  sportKey,
  event,
  bookmaker,
  market,
  consensusContext,
  pipeline,
  minBookCount = 2,
  scanTimeCt,
  runId,
  bankrollSnapshot,
  ownedBooks,
}) {
  const bookKey = normalizeBookKey(bookmaker.key || bookmaker.title);
  const snapshot = consensusContext.candidateSnapshots.get(bookKey);
  if (consensusContext.snapshotStatus !== 'valid') {
    return invalidSnapshotRows({
      sportKey,
      event,
      bookmaker,
      market,
      scanTimeCt,
      runId,
      bankrollSnapshot,
      ownedBooks,
      snapshotStatus: consensusContext.snapshotStatus,
      snapshotMaxSpreadSeconds: consensusContext.maxSpreadSeconds,
      consensusMethod: pipeline.consensusMethod,
      rejectionClass: consensusContext.snapshotStatus === 'insufficient_valid_books' ? 'invalid_snapshot' : 'invalid_snapshot',
    });
  }
  if (!snapshot?.valid) {
    return invalidSnapshotRows({
      sportKey,
      event,
      bookmaker,
      market,
      scanTimeCt,
      runId,
      bankrollSnapshot,
      ownedBooks,
      snapshotStatus: `invalid_${snapshot?.reason || 'candidate_snapshot'}`,
      snapshotMaxSpreadSeconds: consensusContext.maxSpreadSeconds,
      consensusMethod: pipeline.consensusMethod,
      rejectionClass: snapshot?.reason === 'missing_counterpart' ? 'missing_two_sided_market' : 'invalid_snapshot',
    });
  }
  if (!consensusContext.syncedBookKeys.has(bookKey)) {
    return invalidSnapshotRows({
      sportKey,
      event,
      bookmaker,
      market,
      scanTimeCt,
      runId,
      bankrollSnapshot,
      ownedBooks,
      snapshotStatus: 'desynchronized_snapshot',
      snapshotMaxSpreadSeconds: consensusContext.maxSpreadSeconds,
      consensusMethod: pipeline.consensusMethod,
      rejectionClass: 'stale_market',
    });
  }

  const freshMinutes = round2((Date.now() - snapshot.updateMs) / 60000);
  const rows = [];
  for (const outcome of market.outcomes || []) {
    const key = outcomeKey(market.key, outcome);
    const candidateEntry = snapshot.outcomesByKey.get(key);
    const consensus = consensusContext.consensusByOutcome.get(key);
    if (!candidateEntry || !consensus || !Number.isFinite(consensus.consensusProb)) {
      rows.push(...invalidSnapshotRows({
        sportKey,
        event,
        bookmaker,
        market,
        scanTimeCt,
        runId,
        bankrollSnapshot,
        ownedBooks,
        snapshotStatus: 'missing_consensus_outcome',
        snapshotMaxSpreadSeconds: consensusContext.maxSpreadSeconds,
        consensusMethod: pipeline.consensusMethod,
        rejectionClass: 'invalid_snapshot',
      }));
      break;
    }
    if ((consensus.bookmakerCount || 0) < minBookCount) {
      rows.push(...invalidSnapshotRows({
        sportKey,
        event,
        bookmaker,
        market,
        scanTimeCt,
        runId,
        bankrollSnapshot,
        ownedBooks,
        snapshotStatus: 'insufficient_valid_books',
        snapshotMaxSpreadSeconds: consensus.maxSpreadSeconds,
        consensusMethod: pipeline.consensusMethod,
        rejectionClass: 'invalid_snapshot',
      }));
      break;
    }
    if (!consensus.sanityPass) {
      rows.push(...invalidSnapshotRows({
        sportKey,
        event,
        bookmaker,
        market,
        scanTimeCt,
        runId,
        bankrollSnapshot,
        ownedBooks,
        snapshotStatus: 'consensus_sanity_rejected',
        snapshotMaxSpreadSeconds: consensus.maxSpreadSeconds,
        consensusMethod: pipeline.consensusMethod,
        rejectionClass: 'invalid_snapshot',
      }));
      break;
    }

    const candidateFair = asUnitProbability(candidateEntry.fairProb);
    const consensusFairProb = asUnitProbability(consensus.consensusProb);
    const preConfTrueProb = Number(consensusFairProb.toFixed(4));
    const devigProb = Number(candidateFair.toFixed(4));
    const edgePct = round2((consensusFairProb - candidateFair) * 100);
    const confidenceScore = confidenceFromCoverage({
      bookmakerCount: consensus.bookmakerCount,
      freshestMinutes: freshMinutes,
    });
    const tier = deriveTier(edgePct);
    const kelly = tier
      ? computeKellyBreakdown({
          bankroll: bankrollSnapshot,
          american_odds: outcome.price,
          true_prob: consensusFairProb,
          implied_prob_fair: candidateFair,
          tier,
        })
      : null;
    rows.push({
      ...createBaseRow({
        sportKey,
        event,
        bookmaker,
        market,
        outcome,
        scanTimeCt,
        runId,
        bankrollSnapshot,
        ownedBooks,
      }),
      devig_implied_prob: devigProb,
      consensus_prob: Number(consensusFairProb.toFixed(4)),
      pre_conf_true_prob: preConfTrueProb,
      confidence_score: confidenceScore,
      post_conf_true_prob: preConfTrueProb,
      raw_edge_pct: edgePct,
      post_conf_edge_pct: edgePct,
      tier_threshold_pct: tier ? Number(tier.slice(1) === '1' ? 6 : tier.slice(1) === '2' ? 4 : 2) : 2,
      threshold_gap_pct: round2((tier ? Number(tier.slice(1) === '1' ? 6 : tier.slice(1) === '2' ? 4 : 2) : 2) - edgePct),
      price_edge_pass: Number.isFinite(edgePct) && edgePct >= 2,
      snapshot_status: 'valid',
      snapshot_max_spread_seconds: consensus.maxSpreadSeconds,
      consensus_method: pipeline.consensusMethod,
      consensus_book_count: consensus.bookmakerCount,
      consensus_median_prob: Number(consensus.medianProb.toFixed(4)),
      kelly_stake: kelly?.final_stake ?? 0,
      analysis_meta: {
        market_key: market.key,
        bookmaker_key: bookmaker.key,
        bookmaker_count: consensus.bookmakerCount,
        latest_market_update: snapshot.updateIso,
        event_start_utc: event.commence_time || null,
      },
    });
  }
  return rows;
}

function buildPhase1NbaPointPropRows({ event, bookmaker, market, consensusMap, scanTimeCt, runId, bankrollSnapshot, ownedBooks }) {
  const rows = [];
  const freshMinutes = (() => {
    const lastUpdate = Date.parse(String(market?.last_update || bookmaker?.last_update || event?.commence_time || ''));
    return Number.isFinite(lastUpdate) ? round2((Date.now() - lastUpdate) / 60000) : null;
  })();

  for (const pair of buildPlayerPropPairs(market.outcomes || [])) {
    const pairFairMap = computeFairProbMap([
      { ...pair.over, key: `${pair.player_name_normalized}::${pair.line}::over` },
      { ...pair.under, key: `${pair.player_name_normalized}::${pair.line}::under` },
    ]);

    for (const side of ['over', 'under']) {
      const outcome = pair[side];
      const key = `${pair.player_name_normalized}::${pair.line}::${side}`;
      const candidateFair = asUnitProbability(pairFairMap.get(key));
      const consensus = consensusMap.get(key);
      const consensusFairProb = asUnitProbability(consensus?.avgFairProb);
      if (!pair.player_name_raw || !pair.player_name_normalized || pair.line === null) continue;
      if (!Number.isFinite(candidateFair) || !consensus || !Number.isFinite(consensusFairProb)) continue;
      const preConfTrueProb = Number(consensusFairProb.toFixed(4));
      const devigProb = Number(candidateFair.toFixed(4));
      const edgePct = round2((consensusFairProb - candidateFair) * 100);
      const confidenceScore = confidenceFromCoverage({
        bookmakerCount: consensus.bookmakerCount,
        freshestMinutes: freshMinutes,
      });
      const tier = deriveTier(edgePct);
      const kelly = tier
        ? computeKellyBreakdown({
            bankroll: bankrollSnapshot,
            american_odds: outcome.price,
            true_prob: consensusFairProb,
            implied_prob_fair: candidateFair,
            tier,
          })
        : null;
      rows.push({
        run_id: runId,
        rec_id: `${runId}::basketball_nba::${slugify(event.id || buildEventLabel(event))}::player-points::${slugify(pair.player_name_raw)}::${side}::${String(pair.line).replace('.', '-')}::${slugify(bookmaker.title || bookmaker.key)}`,
        timestamp_ct: scanTimeCt,
        target_date: todayCtDateKey(),
        market_family: 'player_prop',
        sport: 'NBA',
        league: 'NBA',
        event_id: event.id || null,
        event_label: buildEventLabel(event),
        event_home_team: event.home_team || null,
        event_away_team: event.away_team || null,
        market_type: 'Player Points',
        selection: `${pair.player_name_raw} ${side === 'over' ? 'Over' : 'Under'} ${pair.line} Points`,
        sportsbook: bookmaker.title || bookmaker.key || 'Unknown',
        owned_book: ownedBooks.has(normalizeBookKey(bookmaker.key || bookmaker.title)),
        live_feed_book: true,
        actionable_book: ownedBooks.has(normalizeBookKey(bookmaker.key || bookmaker.title)),
        player_name_raw: pair.player_name_raw,
        player_name_normalized: pair.player_name_normalized,
        player_id_canonical: `basketball_nba::${event.id || 'unknown-event'}::${slugify(pair.player_name_raw)}`,
        player_team: null,
        opponent_team: null,
        prop_type: 'points',
        prop_side: side,
        prop_line: pair.line,
        line_key: `points::${side}::${pair.line}`,
        is_alt_line: false,
        odds_american: String(outcome.price),
        odds_decimal: round2(americanToDecimal(outcome.price)),
        devig_implied_prob: devigProb,
        consensus_prob: Number(consensusFairProb.toFixed(4)),
        pre_conf_true_prob: preConfTrueProb,
        confidence_score: confidenceScore,
        post_conf_true_prob: preConfTrueProb,
        raw_edge_pct: edgePct,
        post_conf_edge_pct: edgePct,
        tier_threshold_pct: tier ? Number(tier.slice(1) === '1' ? 6 : tier.slice(1) === '2' ? 4 : 2) : 2,
        threshold_gap_pct: round2((tier ? Number(tier.slice(1) === '1' ? 6 : tier.slice(1) === '2' ? 4 : 2) : 2) - edgePct),
        price_edge_pass: Number.isFinite(edgePct) && edgePct >= 2,
        executable_book: ownedBooks.has(normalizeBookKey(bookmaker.key || bookmaker.title)),
        bet_permission_pass: false,
        final_decision: 'SIT',
        rejection_stage: '',
        rejection_reason: '',
        rejection_class: '',
        surfaced_as_closest_miss: false,
        close_capture_status: 'pending',
        closing_odds_american: null,
        closing_odds_decimal: null,
        closing_implied_prob: null,
        closing_devig_prob: null,
        closing_snapshot_time_utc: null,
        closing_book: null,
        clv_delta_pct: null,
        clv_direction: 'unknown',
        close_match_quality: 'insufficient_match',
        closing_line: null,
        snapshot_status: 'valid',
        snapshot_max_spread_seconds: 0,
        consensus_method: 'mean',
        consensus_book_count: consensus.bookmakerCount,
        consensus_median_prob: Number(consensusFairProb.toFixed(4)),
        bet_class: 'EDGE_BET',
        bankroll_snapshot: bankrollSnapshot,
        kelly_stake: kelly?.final_stake ?? 0,
        include_in_core_strategy_metrics: true,
        include_in_actual_bankroll: false,
        analysis_meta: {
          market_key: market.key,
          bookmaker_key: bookmaker.key,
          bookmaker_count: consensus.bookmakerCount,
          latest_market_update: market.last_update || bookmaker.last_update || null,
          event_start_utc: event.commence_time || null,
          prop_phase: 'phase1_nba_points_only',
        },
      });
    }
  }
  return rows;
}

function minutesToStart(row) {
  const startMs = Date.parse(String(row?.analysis_meta?.event_start_utc || ''));
  if (!Number.isFinite(startMs)) return null;
  return round2((startMs - Date.now()) / 60000);
}

function finalizeDecisions(rows, riskControls) {
  const byTierCounts = { T1: 0, T2: 0, T3: 0 };
  let totalExposure = 0;
  const sorted = [...rows].sort(rankCandidates);
  for (const row of sorted) {
    if (row.snapshot_status && row.snapshot_status !== 'valid') {
      row.final_decision = 'SIT';
      row.rejection_stage = 'integrity_gate';
      row.rejection_reason = 'invalid_snapshot';
      row.rejection_class = 'stale_or_unverified_odds';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    const minutesUntilStart = minutesToStart(row);
    if (riskControls.rejectStartedEvents && minutesUntilStart !== null && minutesUntilStart <= 0) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'integrity_gate';
      row.rejection_reason = 'stale_market';
      row.rejection_class = 'stale_market';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    if (minutesUntilStart !== null && minutesUntilStart < riskControls.minimumMinutesToStart) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'integrity_gate';
      row.rejection_reason = 'stale_market';
      row.rejection_class = 'stale_market';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    const edge = row.post_conf_edge_pct;
    if (row.market_family === 'main_market' && Number.isFinite(edge) && edge > riskControls.mainMarketEdgeAnomalyPct) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'integrity_gate';
      row.rejection_reason = 'edge_anomaly';
      row.rejection_class = 'edge_anomaly';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    const tier = deriveTier(edge);
    if (!tier) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'threshold_gate';
      row.rejection_reason = 'no_edge';
      row.rejection_class = (row.post_conf_edge_pct ?? 0) >= 1.5 ? 'near_miss' : 'no_edge';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    const stakeCap = round2((parseNumber(row.bankroll_snapshot) || 0) * riskControls.maxStakePctPerBet);
    if (stakeCap > 0) {
      row.kelly_stake = round2(Math.min(parseNumber(row.kelly_stake) || 0, stakeCap));
    }
    if ((row.confidence_score ?? 0) < 0.6) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'confidence_gate';
      row.rejection_reason = 'low_confidence';
      row.rejection_class = 'other_meaningful_canonical';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    const limit = TIER_LIMITS[tier];
    if (byTierCounts[tier] >= limit.maxBets) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'risk_gate';
      row.rejection_reason = 'exposure_cap_reached';
      row.rejection_class = 'risk_gate_rejected';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    if ((parseNumber(row.kelly_stake) || 0) < 0.5) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'risk_gate';
      row.rejection_reason = 'sub_minimum_kelly';
      row.rejection_class = 'sub_minimum_kelly';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    const bankroll = parseNumber(row.bankroll_snapshot) || 0;
    const maxRunExposure = bankroll * riskControls.maxTotalExposurePctPerRun;
    const proposedTotalExposure = totalExposure + (parseNumber(row.kelly_stake) || 0);
    if (maxRunExposure > 0 && proposedTotalExposure > maxRunExposure + 0.001) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'risk_gate';
      row.rejection_reason = 'exposure_cap_reached';
      row.rejection_class = 'risk_gate_rejected';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    if (!row.actionable_book) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'risk_gate';
      row.rejection_reason = 'research_only_non_owned_book';
      row.rejection_class = 'non_executable_edge';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    row.final_decision = 'BET';
    row.rejection_stage = '';
    row.rejection_reason = '';
    row.rejection_class = '';
    row.bet_permission_pass = true;
    row.include_in_actual_bankroll = true;
    byTierCounts[tier] += 1;
    totalExposure += parseNumber(row.kelly_stake) || 0;
  }
  return sorted;
}

function summarizeRun({
  appendedRows,
  selectedRows,
  sitRows,
  bankrollSnapshot,
  runId,
  scanTimeCt,
  reason = null,
  propSummary = null,
  propFeatureFlags = null,
  ownedBooks = [],
  liveFeedBooks = [],
  actionableBooksForRun = [],
  feedUnavailableOwnedBooks = [],
  researchOnlyBooks = [],
}) {
  const grouped = { T1: [], T2: [], T3: [] };
  for (const row of selectedRows) {
    const tier = deriveTier(row.post_conf_edge_pct);
    if (tier) grouped[tier].push(row);
  }
  const researchOnlyRows = sitRows
    .filter((row) => row.rejection_reason === 'research_only_non_owned_book')
    .sort(rankCandidates)
    .slice(0, 6);
  const actionableMisses = sitRows
    .filter((row) => row.actionable_book)
    .filter((row) => (row.post_conf_edge_pct ?? 0) >= 0.5 && (row.post_conf_edge_pct ?? 0) < 2)
    .sort(rankCandidates)
    .slice(0, 6);
  const lines = [];
  lines.push(`TIERED EDGE HUNT — ${todayCtDateKey()}`);
  lines.push(`Bankroll: ${formatMoney(bankrollSnapshot)} | Phase: STANDARD | Daily Exposure Used: 0%`);
  lines.push(`Owned books: ${ownedBooks.join(', ') || 'none'}`);
  lines.push(`Live feed books this run: ${liveFeedBooks.join(', ') || 'none'}`);
  lines.push(`Actionable books this run: ${actionableBooksForRun.join(', ') || 'none'}`);
  lines.push(`Owned but unavailable in feed: ${feedUnavailableOwnedBooks.join(', ') || 'none'}`);
  lines.push(`Research-only books this run: ${researchOnlyBooks.join(', ') || 'none'}`);
  lines.push('');
  if (selectedRows.length === 0) {
    lines.push('RECOMMENDED PLAYS: None');
  } else {
    lines.push('RECOMMENDED PLAYS:');
    for (const tier of ['T1', 'T2', 'T3']) {
      if (!grouped[tier].length) continue;
      lines.push('');
      lines.push(`${tier}:`);
      for (const row of grouped[tier]) {
        lines.push(`- [ ] ${row.selection} @ ${row.odds_american} | ${row.sportsbook}`);
        lines.push(`  Timestamp (CT): ${row.timestamp_ct}`);
        lines.push(`  True Prob: ${(row.post_conf_true_prob * 100).toFixed(1)}% | Implied Prob (de-vig): ${(row.devig_implied_prob * 100).toFixed(1)}% | Edge: +${row.post_conf_edge_pct}%`);
        lines.push(`  Kelly Stake: ${formatMoney(parseNumber(row.kelly_stake) || 0)}`);
      }
    }
  }
  lines.push('');
  lines.push('EXECUTABLE CLOSE MISSES:');
  if (!actionableMisses.length) {
    lines.push('- No qualifying edges >= +2% after consensus de-vig analysis.');
  } else {
    for (const row of actionableMisses) {
      lines.push(`- ${row.selection} @ ${row.odds_american} | ${row.sportsbook}: +${row.post_conf_edge_pct}% edge — ${row.rejection_reason || 'No edge at current price'}`);
    }
  }
  if (researchOnlyRows.length) {
    lines.push('');
    lines.push('RESEARCH-ONLY NON-OWNED BOOK EDGES:');
    for (const row of researchOnlyRows) {
      lines.push(`- ${row.selection} @ ${row.odds_american} | ${row.sportsbook}: +${row.post_conf_edge_pct}% edge — research_only`);
    }
  }
  lines.push('');
  if (selectedRows.length === 0) {
    lines.push(`VERDICT: SIT — ${reason || 'No qualifying edges found after consensus de-vig analysis.'}`);
  } else {
    lines.push(`VERDICT: ${selectedRows.length} plays found | ${sitRows.length} sat out`);
  }
  return {
    schema: 'tierededge_canonical_hunt_run_v1',
    run_id: runId,
    generated_at_utc: new Date().toISOString(),
    run_at_ct: scanTimeCt,
    status: 'ok',
    message_type: selectedRows.length > 0 ? 'BET' : 'SIT',
    requires_state_sync: false,
    has_actionable_bets: selectedRows.length > 0,
    native_rows_appended: appendedRows.length,
    native_bets_appended: selectedRows.length,
    native_sits_appended: sitRows.length,
    invalidated: false,
    plain_reason: selectedRows.length > 0
      ? 'Canonical repo-owned hunt completed and appended native decision rows.'
      : (reason || 'Canonical repo-owned hunt completed with verified odds and no qualifying edges.'),
    prop_feature_flags: propFeatureFlags,
    prop_summary: propSummary,
    owned_books: ownedBooks,
    live_feed_books: liveFeedBooks,
    actionable_books_for_run: actionableBooksForRun,
    feed_unavailable_owned_books: feedUnavailableOwnedBooks,
    research_only_books: researchOnlyBooks,
    summary: lines.join('\n'),
    rows: {
      bet_rec_ids: selectedRows.map((row) => row.rec_id),
      sit_rec_ids: sitRows.slice(0, 20).map((row) => row.rec_id),
      research_only_rec_ids: researchOnlyRows.map((row) => row.rec_id),
    },
  };
}

function markSurfacedRejectedRows(rows) {
  const sitRows = rows.filter((row) => row.final_decision === 'SIT');
  const actionableMisses = sitRows
    .filter((row) => row.actionable_book)
    .filter((row) => (row.post_conf_edge_pct ?? 0) >= 0.5 && (row.post_conf_edge_pct ?? 0) < 2)
    .sort(rankCandidates)
    .slice(0, 6);
  const researchOnlyRows = sitRows
    .filter((row) => row.rejection_reason === 'research_only_non_owned_book')
    .sort(rankCandidates)
    .slice(0, 6);
  for (const row of [...actionableMisses, ...researchOnlyRows]) {
    row.surfaced_as_closest_miss = true;
    if (!row.rejection_class && row.rejection_reason === 'no_edge' && (row.post_conf_edge_pct ?? 0) >= 1.5) {
      row.rejection_class = 'near_miss';
    }
  }
}

function failureArtifact({ runId, scanTimeCt, reason }) {
  return {
    schema: 'tierededge_canonical_hunt_run_v1',
    run_id: runId,
    generated_at_utc: new Date().toISOString(),
    run_at_ct: scanTimeCt,
    status: 'failed',
    message_type: 'BLOCKED',
    requires_state_sync: false,
    has_actionable_bets: false,
    native_rows_appended: 0,
    invalidated: false,
    plain_reason: reason,
    summary: `TIERED EDGE HUNT — ${todayCtDateKey()}\nCANNOT_VERIFY_ODDS — SIT\nReason: ${reason}`,
  };
}

export {
  buildConsensusBookSet,
  buildConsensusContext,
  buildMarketRows,
  buildOwnedBookSet,
  createBaseRow,
  finalizeDecisions,
  formatCtMinute,
  normalizeBookKey,
  resolveProbabilityPipeline,
  resolveRiskControls,
  todayCtDateKey,
};

async function main() {
  const args = parseArgs(process.argv);
  const runAt = new Date();
  const runId = args.run_id || `canonical-hunt::${todayCtDateKey(runAt)}::${formatCtMinute(runAt).slice(11).replace(':', '')}`;
  const scanTimeCt = formatCtMinute(runAt);
  const blockStatus = readHuntBlockStatus();
  if (blockStatus.blocked) {
    throw new Error(`hunt_blocked:${blockStatus.reason_class}:${blockStatus.reason}`);
  }

  const apiKey = await loadOddsApiKey();
  if (!apiKey) {
    const artifact = failureArtifact({
      runId,
      scanTimeCt,
      reason: 'Odds API key unavailable in the TieredEdge runtime secure store.',
    });
    writeJson(CORE_PATHS.canonicalHuntRun, artifact);
    console.log(artifact.summary);
    return;
  }

  const publicState = readJson(CORE_PATHS.publicData, {});
  const bankrollSnapshot = parseNumber(publicState?.current_status?.Bankroll)
    ?? parseNumber(publicState?.bankroll_summary?.last_recorded_bankroll)
    ?? 0;
  const policy = loadScanCoveragePolicy();
  const ownedBooksSet = resolveOwnedBookSet(policy, args);
  const consensusBooksSet = buildConsensusBookSet(policy);
  const probabilityPipeline = resolveProbabilityPipeline(policy);
  const riskControls = resolveRiskControls(policy);
  const propFeatureFlags = resolvePhase1NbaPointsProps(policy, args);
  const nbaSpreadsFeatureFlags = resolvePhase1NbaSpreads(policy, args);
  const mlbFeatureFlags = resolvePhase1MlbMoneylines(policy, args);
  const nbaSpreadsControl = buildFeatureMarketControl({
    feature: nbaSpreadsFeatureFlags,
    fallbackConsensusBooks: consensusBooksSet,
    fallbackOwnedBooks: ownedBooksSet,
  });
  const mlbMoneylineControl = buildFeatureMarketControl({
    feature: mlbFeatureFlags,
    fallbackConsensusBooks: consensusBooksSet,
    fallbackOwnedBooks: ownedBooksSet,
  });
  const nbaPointsControl = buildFeatureMarketControl({
    feature: propFeatureFlags,
    fallbackConsensusBooks: consensusBooksSet,
    fallbackOwnedBooks: ownedBooksSet,
  });
  const targetDateKey = todayCtDateKey(runAt);
  const eventFilter = String(args.event_filter || args.team_filter || '').trim();
  const tierA = policy?.priority_tiers?.tier_a || {};
  const books = [...new Set([...(tierA.default_books || []), ...(tierA.comparison_books || []), ...ownedBooksSet, ...consensusBooksSet])];
  const markets = tierA.markets || ['h2h', 'spreads', 'totals'];
  const rows = [];
  const todaysEventsBySport = new Map();
  const observedBooks = new Set();
  const propSummary = {
    phase1_nba_spreads: {
      configured_enabled: nbaSpreadsFeatureFlags.configured_enabled,
      enabled_for_run: nbaSpreadsFeatureFlags.enabled_for_run,
      enable_source: nbaSpreadsFeatureFlags.enable_source,
      fetched_event_count: 0,
      analyzed_row_count: 0,
      selected_bet_count: 0,
      sit_count: 0,
      native_rows_appended: 0,
      min_book_count: nbaSpreadsControl.minBookCount,
      eligible_books: [...nbaSpreadsControl.eligibleBooks].sort(),
    },
    phase1_mlb_moneylines: {
      configured_enabled: mlbFeatureFlags.configured_enabled,
      enabled_for_run: mlbFeatureFlags.enabled_for_run,
      enable_source: mlbFeatureFlags.enable_source,
      fetched_event_count: 0,
      analyzed_row_count: 0,
      selected_bet_count: 0,
      sit_count: 0,
      native_rows_appended: 0,
      min_book_count: mlbMoneylineControl.minBookCount,
      eligible_books: [...mlbMoneylineControl.eligibleBooks].sort(),
    },
    phase1_nba_points_props: {
      configured_enabled: propFeatureFlags.configured_enabled,
      enabled_for_run: propFeatureFlags.enabled_for_run,
      enable_source: propFeatureFlags.enable_source,
      fetched_event_count: 0,
      analyzed_row_count: 0,
      selected_bet_count: 0,
      sit_count: 0,
      native_rows_appended: 0,
      min_book_count: nbaPointsControl.minBookCount,
      eligible_books: [...nbaPointsControl.eligibleBooks].sort(),
    },
  };

  for (const sportKey of tierA.sports || []) {
    const payload = await fetchOddsPayload({ sportKey, books, markets, apiKey });
    for (const event of payload || []) collectObservedBooks(event, observedBooks);
    const todaysEvents = (payload || [])
      .filter((event) => eventIsTodayCt(event, targetDateKey))
      .filter((event) => eventMatchesFilter(event, eventFilter));
    todaysEventsBySport.set(sportKey, todaysEvents);
    if (sportKey === 'basketball_nba' && nbaSpreadsFeatureFlags.enabled_for_run) {
      propSummary.phase1_nba_spreads.fetched_event_count = todaysEvents.length;
    }
    for (const event of todaysEvents) {
      for (const marketKey of markets) {
        const marketControl = resolveMarketControl({
          sportKey,
          marketKey,
          nbaSpreadsControl,
          mlbMoneylineControl,
          consensusBooksSet,
        });
        const consensusContext = buildConsensusContext({
          event,
          marketKey,
          consensusBooks: marketControl.eligibleBooks,
          pipeline: probabilityPipeline,
          minBookCount: marketControl.minBookCount,
          eligibleBooks: marketControl.eligibleBooks,
        });
        for (const bookmaker of event.bookmakers || []) {
          const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
          if (marketControl.eligibleBooks?.size && !marketControl.eligibleBooks.has(normalizedBook)) continue;
          for (const market of bookmaker.markets || []) {
            if (market.key !== marketKey) continue;
            rows.push(...buildMarketRows({
              sportKey,
              event,
              bookmaker,
              market,
              consensusContext,
              pipeline: probabilityPipeline,
              minBookCount: marketControl.minBookCount,
              scanTimeCt,
              runId,
              bankrollSnapshot,
              ownedBooks: ownedBooksSet,
            }));
          }
        }
      }
    }
  }

  if (mlbFeatureFlags.enabled_for_run) {
    const mlbConfig = mlbFeatureFlags.config || {};
    const mlbSportKey = mlbConfig.sport_key || 'baseball_mlb';
    const mlbMarkets = (mlbConfig.markets || ['h2h']).filter(Boolean);
    const mlbBooks = [...new Set([
      ...((mlbConfig.eligible_books || mlbConfig.trusted_books || []).filter(Boolean)),
      ...((mlbConfig.comparison_books || []).filter(Boolean)),
    ])];
    const payload = await fetchOddsPayload({ sportKey: mlbSportKey, books: mlbBooks, markets: mlbMarkets, apiKey });
    for (const event of payload || []) collectObservedBooks(event, observedBooks);
    const todaysEvents = (payload || [])
      .filter((event) => eventIsTodayCt(event, targetDateKey))
      .filter((event) => eventMatchesFilter(event, eventFilter));
    todaysEventsBySport.set(mlbSportKey, todaysEvents);
    propSummary.phase1_mlb_moneylines.fetched_event_count = todaysEvents.length;

    for (const event of todaysEvents) {
      for (const marketKey of mlbMarkets) {
        const consensusContext = buildConsensusContext({
          event,
          marketKey,
          consensusBooks: mlbMoneylineControl.eligibleBooks,
          pipeline: probabilityPipeline,
          minBookCount: mlbMoneylineControl.minBookCount,
          eligibleBooks: mlbMoneylineControl.eligibleBooks,
        });
        for (const bookmaker of event.bookmakers || []) {
          const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
          if (mlbMoneylineControl.eligibleBooks?.size && !mlbMoneylineControl.eligibleBooks.has(normalizedBook)) continue;
          for (const market of bookmaker.markets || []) {
            if (market.key !== marketKey) continue;
            rows.push(...buildMarketRows({
              sportKey: mlbSportKey,
              event,
              bookmaker,
              market,
              consensusContext,
              pipeline: probabilityPipeline,
              minBookCount: mlbMoneylineControl.minBookCount,
              scanTimeCt,
              runId,
              bankrollSnapshot,
              ownedBooks: ownedBooksSet,
            }));
          }
        }
      }
    }
  }

  if (propFeatureFlags.enabled_for_run) {
    const propConfig = propFeatureFlags.config || {};
    const propSportKey = propConfig.sport_key || 'basketball_nba';
    const propMarketKey = propConfig.market_key || PHASE1_NBA_POINTS_PROP_KEY;
    const propBooks = [...new Set([
      ...((propConfig.eligible_books || propConfig.trusted_books || books).filter(Boolean)),
    ])];
    const nbaEvents = todaysEventsBySport.get(propSportKey) || [];
    propSummary.phase1_nba_points_props.fetched_event_count = nbaEvents.length;

    for (const event of nbaEvents) {
      const propPayload = await fetchEventOddsPayload({
        sportKey: propSportKey,
        eventId: event.id,
        books: propBooks,
        markets: [propMarketKey],
        apiKey,
      });
      collectObservedBooks(propPayload, observedBooks);
      const consensusMap = computePropConsensusMap(propPayload, propMarketKey, nbaPointsControl);
      for (const bookmaker of propPayload.bookmakers || []) {
        const normalizedBook = normalizeBookKey(bookmaker.key || bookmaker.title);
        if (nbaPointsControl.eligibleBooks?.size && !nbaPointsControl.eligibleBooks.has(normalizedBook)) continue;
        for (const market of bookmaker.markets || []) {
          if (market.key !== propMarketKey) continue;
          rows.push(...buildPhase1NbaPointPropRows({
            event: propPayload,
            bookmaker,
            market,
            consensusMap,
            scanTimeCt,
            runId,
            bankrollSnapshot,
            ownedBooks: ownedBooksSet,
          }));
        }
      }
    }
  }

  const bestByOutcome = new Map();
  for (const row of rows) {
    const key = `${row.event_id}::${row.market_type}::${row.selection}`;
    const existing = bestByOutcome.get(key);
    if (!existing || rankCandidates(row, existing) < 0) {
      bestByOutcome.set(key, row);
    }
  }

  const finalizedRows = finalizeDecisions([...bestByOutcome.values()], riskControls);
  markSurfacedRejectedRows(finalizedRows);
  const propRows = finalizedRows.filter((row) => row.market_family === 'player_prop');
  const nbaSpreadRows = finalizedRows.filter((row) => row.sport === 'NBA' && row.market_family === 'main_market' && row.market_type === 'Spread');
  const mlbRows = finalizedRows.filter((row) => row.sport === 'MLB' && row.market_family === 'main_market');
  propSummary.phase1_nba_spreads.analyzed_row_count = nbaSpreadRows.length;
  propSummary.phase1_nba_spreads.selected_bet_count = nbaSpreadRows.filter((row) => row.final_decision === 'BET').length;
  propSummary.phase1_nba_spreads.sit_count = nbaSpreadRows.filter((row) => row.final_decision === 'SIT').length;
  propSummary.phase1_mlb_moneylines.analyzed_row_count = mlbRows.length;
  propSummary.phase1_mlb_moneylines.selected_bet_count = mlbRows.filter((row) => row.final_decision === 'BET').length;
  propSummary.phase1_mlb_moneylines.sit_count = mlbRows.filter((row) => row.final_decision === 'SIT').length;
  propSummary.phase1_nba_points_props.analyzed_row_count = propRows.length;
  propSummary.phase1_nba_points_props.selected_bet_count = propRows.filter((row) => row.final_decision === 'BET').length;
  propSummary.phase1_nba_points_props.sit_count = propRows.filter((row) => row.final_decision === 'SIT').length;
  appendNativeDecisionRows(finalizedRows);
  propSummary.phase1_nba_spreads.native_rows_appended = nbaSpreadRows.length;
  propSummary.phase1_mlb_moneylines.native_rows_appended = mlbRows.length;
  propSummary.phase1_nba_points_props.native_rows_appended = propRows.length;
  const selectedRows = finalizedRows.filter((row) => row.final_decision === 'BET');
  const sitRows = finalizedRows.filter((row) => row.final_decision === 'SIT');
  const ownedBooks = [...ownedBooksSet].sort();
  const liveFeedBooks = [...observedBooks].sort();
  const actionableBooksForRun = ownedBooks.filter((book) => observedBooks.has(book));
  const feedUnavailableOwnedBooks = ownedBooks.filter((book) => !observedBooks.has(book));
  const researchOnlyBooks = liveFeedBooks.filter((book) => !ownedBooksSet.has(book));
  const artifact = summarizeRun({
    appendedRows: finalizedRows,
    selectedRows,
    sitRows,
    bankrollSnapshot,
    runId,
    scanTimeCt,
    propSummary,
    propFeatureFlags,
    ownedBooks,
    liveFeedBooks,
    actionableBooksForRun,
    feedUnavailableOwnedBooks,
    researchOnlyBooks,
  });
  writeJson(CORE_PATHS.canonicalHuntRun, artifact);

  if (args.json) {
    console.log(JSON.stringify(artifact, null, 2));
    return;
  }
  console.log(artifact.summary);
}

const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isDirectRun) {
  main().catch((error) => {
    const runAt = new Date();
    const artifact = failureArtifact({
      runId: `canonical-hunt::${todayCtDateKey(runAt)}::${formatCtMinute(runAt).slice(11).replace(':', '')}`,
      scanTimeCt: formatCtMinute(runAt),
      reason: error.message,
    });
    writeJson(CORE_PATHS.canonicalHuntRun, artifact);
    console.error(error.message);
    process.exit(1);
  });
}

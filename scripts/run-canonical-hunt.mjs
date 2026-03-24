#!/usr/bin/env node
import { execFile } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import process from 'node:process';
import path from 'node:path';
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
};
const PHASE1_NBA_POINTS_PROP_KEY = 'player_points';
const TIER_LIMITS = {
  T1: { maxBets: 2 },
  T2: { maxBets: 4 },
  T3: { maxBets: 6 },
};

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

function computePropConsensusMap(event, marketKey) {
  const grouped = new Map();
  for (const bookmaker of event.bookmakers || []) {
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

function buildMarketRows({ sportKey, event, bookmaker, market, consensusMap, scanTimeCt, runId, bankrollSnapshot }) {
  const rows = [];
  const fairProbMap = computeFairProbMap((market.outcomes || []).map((outcome) => ({
    ...outcome,
    key: outcomeKey(market.key, outcome),
  })));
  const freshMinutes = (() => {
    const lastUpdate = Date.parse(String(market?.last_update || bookmaker?.last_update || event?.commence_time || ''));
    return Number.isFinite(lastUpdate) ? round2((Date.now() - lastUpdate) / 60000) : null;
  })();

  for (const outcome of market.outcomes || []) {
    const key = outcomeKey(market.key, outcome);
    const candidateFair = asUnitProbability(fairProbMap.get(key));
    const consensus = consensusMap.get(key);
    const consensusFairProb = asUnitProbability(consensus?.avgFairProb);
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
      price_edge_pass: Number.isFinite(edgePct) && edgePct >= 2,
      bet_permission_pass: false,
      final_decision: 'SIT',
      rejection_stage: '',
      rejection_reason: '',
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
      },
    });
  }
  return rows;
}

function buildPhase1NbaPointPropRows({ event, bookmaker, market, consensusMap, scanTimeCt, runId, bankrollSnapshot }) {
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
        price_edge_pass: Number.isFinite(edgePct) && edgePct >= 2,
        bet_permission_pass: false,
        final_decision: 'SIT',
        rejection_stage: '',
        rejection_reason: '',
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
          prop_phase: 'phase1_nba_points_only',
        },
      });
    }
  }
  return rows;
}

function finalizeDecisions(rows) {
  const byTierCounts = { T1: 0, T2: 0, T3: 0 };
  const sorted = [...rows].sort(rankCandidates);
  for (const row of sorted) {
    const edge = row.post_conf_edge_pct;
    const tier = deriveTier(edge);
    if (!tier) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'threshold_gate';
      row.rejection_reason = 'no_edge';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    if ((row.confidence_score ?? 0) < 0.6) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'confidence_gate';
      row.rejection_reason = 'low_confidence';
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
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    if ((parseNumber(row.kelly_stake) || 0) < 0.5) {
      row.final_decision = 'SIT';
      row.rejection_stage = 'risk_gate';
      row.rejection_reason = 'exposure_cap_reached';
      row.bet_permission_pass = false;
      row.include_in_actual_bankroll = false;
      row.kelly_stake = 0;
      continue;
    }
    row.final_decision = 'BET';
    row.rejection_stage = '';
    row.rejection_reason = '';
    row.bet_permission_pass = true;
    row.include_in_actual_bankroll = true;
    byTierCounts[tier] += 1;
  }
  return sorted;
}

function buildConsensusMap(event, marketKey) {
  const grouped = new Map();
  for (const bookmaker of event.bookmakers || []) {
    for (const market of bookmaker.markets || []) {
      if (market.key !== marketKey) continue;
      const keyedOutcomes = (market.outcomes || []).map((outcome) => ({
        ...outcome,
        key: outcomeKey(market.key, outcome),
      }));
      const fairProbMap = computeFairProbMap(keyedOutcomes);
      for (const outcome of keyedOutcomes) {
        const fair = fairProbMap.get(outcome.key);
        if (!Number.isFinite(fair)) continue;
        if (!grouped.has(outcome.key)) grouped.set(outcome.key, []);
        grouped.get(outcome.key).push({
          bookmaker: bookmaker.key,
          fairProb: fair,
        });
      }
    }
  }
  const consensus = new Map();
  for (const [key, entries] of grouped.entries()) {
    const avgFairProb = entries.reduce((sum, entry) => sum + entry.fairProb, 0) / entries.length;
    consensus.set(key, {
      avgFairProb,
      bookmakerCount: entries.length,
    });
  }
  return consensus;
}

function summarizeRun({ appendedRows, selectedRows, sitRows, bankrollSnapshot, runId, scanTimeCt, reason = null, propSummary = null, propFeatureFlags = null }) {
  const grouped = { T1: [], T2: [], T3: [] };
  for (const row of selectedRows) {
    const tier = deriveTier(row.post_conf_edge_pct);
    if (tier) grouped[tier].push(row);
  }
  const lines = [];
  lines.push(`TIERED EDGE HUNT — ${todayCtDateKey()}`);
  lines.push(`Bankroll: ${formatMoney(bankrollSnapshot)} | Phase: STANDARD | Daily Exposure Used: 0%`);
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
  lines.push('SITTING OUT:');
  const misses = sitRows
    .filter((row) => (row.post_conf_edge_pct ?? 0) >= 0.5 && (row.post_conf_edge_pct ?? 0) < 2)
    .sort(rankCandidates)
    .slice(0, 6);
  if (!misses.length) {
    lines.push('- No qualifying edges >= +2% after consensus de-vig analysis.');
  } else {
    for (const row of misses) {
      lines.push(`- ${row.selection} @ ${row.odds_american} | ${row.sportsbook}: +${row.post_conf_edge_pct}% edge — ${row.rejection_reason || 'No edge at current price'}`);
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
    summary: lines.join('\n'),
    rows: {
      bet_rec_ids: selectedRows.map((row) => row.rec_id),
      sit_rec_ids: sitRows.slice(0, 20).map((row) => row.rec_id),
    },
  };
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
  const propFeatureFlags = resolvePhase1NbaPointsProps(policy, args);
  const targetDateKey = todayCtDateKey(runAt);
  const tierA = policy?.priority_tiers?.tier_a || {};
  const books = [...new Set([...(tierA.default_books || []), ...(tierA.comparison_books || [])])];
  const markets = tierA.markets || ['h2h', 'spreads', 'totals'];
  const rows = [];
  const todaysEventsBySport = new Map();
  const propSummary = {
    phase1_nba_points_props: {
      configured_enabled: propFeatureFlags.configured_enabled,
      enabled_for_run: propFeatureFlags.enabled_for_run,
      enable_source: propFeatureFlags.enable_source,
      fetched_event_count: 0,
      analyzed_row_count: 0,
      selected_bet_count: 0,
      sit_count: 0,
      native_rows_appended: 0,
    },
  };

  for (const sportKey of tierA.sports || []) {
    const payload = await fetchOddsPayload({ sportKey, books, markets, apiKey });
    const todaysEvents = (payload || []).filter((event) => eventIsTodayCt(event, targetDateKey));
    todaysEventsBySport.set(sportKey, todaysEvents);
    for (const event of todaysEvents) {
      for (const marketKey of markets) {
        const consensusMap = buildConsensusMap(event, marketKey);
        for (const bookmaker of event.bookmakers || []) {
          for (const market of bookmaker.markets || []) {
            if (market.key !== marketKey) continue;
            rows.push(...buildMarketRows({
              sportKey,
              event,
              bookmaker,
              market,
              consensusMap,
              scanTimeCt,
              runId,
              bankrollSnapshot,
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
    const propBooks = (propConfig.trusted_books || books).filter(Boolean);
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
      const consensusMap = computePropConsensusMap(propPayload, propMarketKey);
      for (const bookmaker of propPayload.bookmakers || []) {
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

  const finalizedRows = finalizeDecisions([...bestByOutcome.values()]);
  const propRows = finalizedRows.filter((row) => row.market_family === 'player_prop');
  propSummary.phase1_nba_points_props.analyzed_row_count = propRows.length;
  propSummary.phase1_nba_points_props.selected_bet_count = propRows.filter((row) => row.final_decision === 'BET').length;
  propSummary.phase1_nba_points_props.sit_count = propRows.filter((row) => row.final_decision === 'SIT').length;
  appendNativeDecisionRows(finalizedRows);
  propSummary.phase1_nba_points_props.native_rows_appended = propRows.length;
  const selectedRows = finalizedRows.filter((row) => row.final_decision === 'BET');
  const sitRows = finalizedRows.filter((row) => row.final_decision === 'SIT');
  const artifact = summarizeRun({
    appendedRows: finalizedRows,
    selectedRows,
    sitRows,
    bankrollSnapshot,
    runId,
    scanTimeCt,
    propSummary,
    propFeatureFlags,
  });
  writeJson(CORE_PATHS.canonicalHuntRun, artifact);

  if (args.json) {
    console.log(JSON.stringify(artifact, null, 2));
    return;
  }
  console.log(artifact.summary);
}

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

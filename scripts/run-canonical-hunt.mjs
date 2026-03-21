#!/usr/bin/env node
import process from 'node:process';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { appendNativeDecisionRows } from './native-decision-log-utils.mjs';
import { computeKellyBreakdown } from './tierededge-kelly-cli.mjs';
import { loadScanCoveragePolicy } from './scan-coverage-utils.mjs';
import { CORE_PATHS, formatMoney, parseNumber, readJson, readJsonl, round2, writeJson } from './core-ledger-utils.mjs';
import { readHuntBlockStatus } from './hunt-block-status.mjs';
import { formatCtTimestamp } from './openclaw-runtime-utils.mjs';

const RUNTIME_KEY_STORE = '/Users/jaredbuckman/.openclaw/workspace/tierededge_runtime/api-key-store.mjs';
const SPORT_LABELS = {
  basketball_nba: 'NBA',
  basketball_ncaab: 'NCAAB',
  icehockey_nhl: 'NHL',
};
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
  const moduleUrl = pathToFileURL(path.resolve(RUNTIME_KEY_STORE)).href;
  const runtime = await import(moduleUrl);
  const result = await runtime.getOddsApiKey();
  return result?.value || null;
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
      sport: SPORT_LABELS[sportKey] || sportKey.toUpperCase(),
      league: SPORT_LABELS[sportKey] || sportKey.toUpperCase(),
      event_id: event.id || null,
      event_label: buildEventLabel(event),
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

function summarizeRun({ appendedRows, selectedRows, sitRows, bankrollSnapshot, runId, scanTimeCt, reason = null }) {
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
  const targetDateKey = todayCtDateKey(runAt);
  const tierA = policy?.priority_tiers?.tier_a || {};
  const books = [...new Set([...(tierA.default_books || []), ...(tierA.comparison_books || [])])];
  const markets = tierA.markets || ['h2h', 'spreads', 'totals'];
  const rows = [];

  for (const sportKey of tierA.sports || []) {
    const payload = await fetchOddsPayload({ sportKey, books, markets, apiKey });
    const todaysEvents = (payload || []).filter((event) => eventIsTodayCt(event, targetDateKey));
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

  const bestByOutcome = new Map();
  for (const row of rows) {
    const key = `${row.event_id}::${row.market_type}::${row.selection}`;
    const existing = bestByOutcome.get(key);
    if (!existing || rankCandidates(row, existing) < 0) {
      bestByOutcome.set(key, row);
    }
  }

  const finalizedRows = finalizeDecisions([...bestByOutcome.values()]);
  appendNativeDecisionRows(finalizedRows);
  const selectedRows = finalizedRows.filter((row) => row.final_decision === 'BET');
  const sitRows = finalizedRows.filter((row) => row.final_decision === 'SIT');
  const artifact = summarizeRun({
    appendedRows: finalizedRows,
    selectedRows,
    sitRows,
    bankrollSnapshot,
    runId,
    scanTimeCt,
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

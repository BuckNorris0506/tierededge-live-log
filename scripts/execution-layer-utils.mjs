import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { CORE_PATHS, appendJsonl, parseNumber, readJson, readJsonl, round2, toCtIsoDate, writeJsonl } from './core-ledger-utils.mjs';
import { computeKellyBreakdown } from './tierededge-kelly-cli.mjs';
import { appendOverrideEventsForExecution, deriveOverrideEventsFromExecution } from './behavioral-accountability-utils.mjs';
import { enrichGradingRowWithClv } from './grading-market-truth-utils.mjs';
import { isBankrollRelevantGrade, reconcileGradingBankrollAnnotations } from './bankroll-reconciliation-utils.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
export const EXECUTION_POLICY_PATH = path.join(ROOT, 'config', 'execution-policy.json');
export const EXECUTION_BOARD_PATH = path.join(ROOT, 'data', 'execution-board.json');
export const EXECUTION_LOG_PATH = path.join(ROOT, 'data', 'execution-log.jsonl');
export const EXECUTION_QUOTE_CACHE_PATH = path.join(ROOT, 'data', 'execution-quote-cache.json');

const DEFAULT_POLICY = {
  stale_recommendation_hours: 8,
  odds_stale_minutes: 10,
  promo_evaluation: {
    no_sweat: {
      refund_type: 'bonus_bet',
      bonus_bet_conversion_pct: 70,
      positive_threshold_pct: 2,
      neutral_threshold_pct: 0,
    },
  },
  hedge_management: {
    scan_frequency_per_day: 3,
    meaningful_stake_rule: {
      pct_of_bankroll: 2,
      minimum_stake: 25,
    },
    pregame_thresholds_by_scan_frequency: {
      3: {
        consider_hedge: {
          clv_cents_min: 25,
          current_edge_pct_max: 0.25,
          time_to_start_minutes_max: 240,
        },
        strong_hedge: {
          clv_cents_min: 40,
          current_edge_pct_max: 0,
          time_to_start_minutes_max: 120,
        },
      },
      6: {
        consider_hedge: {
          clv_cents_min: 20,
          current_edge_pct_max: 0.35,
          time_to_start_minutes_max: 240,
        },
        strong_hedge: {
          clv_cents_min: 35,
          current_edge_pct_max: 0,
          time_to_start_minutes_max: 120,
        },
      },
    },
    live_override: {
      state_driven_clv_cents_min: 50,
      default_decision: 'LET RIDE',
    },
  },
  price_only_tolerance_cents: { moneyline: 8, spread: 10, total: 10 },
  line_tolerance_points: { spread: 0.5, total: 0.5 },
  supported_books: {
    DraftKings: 'draftkings',
    FanDuel: 'fanduel',
    BetMGM: 'betmgm',
    Caesars: 'caesars',
    BetRivers: 'betrivers',
    bet365: 'bet365',
    LowVig: 'lowvig',
  },
  supported_sports: {
    NBA: 'basketball_nba',
    NCAAB: 'basketball_ncaab',
    CBB: 'basketball_ncaab',
    NHL: 'icehockey_nhl',
    MLB: 'baseball_mlb',
    UFC: 'mma_mixed_martial_arts',
  },
};

const STATIC_TEAM_METADATA = [
  { sport: 'NHL', league: 'NHL', teams: ['Boston Bruins', 'Montreal Canadiens', 'Minnesota Wild', 'Chicago Blackhawks', 'Buffalo Sabres', 'Vegas Golden Knights', 'Carolina Hurricanes', 'Columbus Blue Jackets', 'Winnipeg Jets', 'Nashville Predators', 'Edmonton Oilers', 'San Jose Sharks', 'Florida Panthers', 'Vancouver Canucks', 'New York Islanders', 'Toronto Maple Leafs'] },
  { sport: 'NBA', league: 'NBA', teams: ['Charlotte Hornets', 'Miami Heat', 'Detroit Pistons', 'Washington Wizards', 'Oklahoma City Thunder', 'Orlando Magic', 'New York Knicks', 'Indiana Pacers', 'Cleveland Cavaliers', 'Milwaukee Bucks', 'Phoenix Suns', 'Minnesota Timberwolves', 'Denver Nuggets', 'Philadelphia 76ers', 'San Antonio Spurs', 'Sacramento Kings', 'Los Angeles Lakers'] },
  { sport: 'NCAAB', league: 'NCAAB', teams: ['George Mason', 'Liberty', 'Yale', 'Howard', 'UMBC', 'Wichita State', 'Wyoming', 'Oklahoma State', 'Davidson', 'NC State', 'Texas', 'Miami', 'Missouri'] },
  { sport: 'MLB', league: 'MLB', teams: ['Chicago Cubs', 'St. Louis Cardinals', 'New York Yankees', 'Boston Red Sox'] },
  { sport: 'NFL', league: 'NFL', teams: ['Kansas City Chiefs', 'Buffalo Bills', 'Green Bay Packers'] },
];

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\bml\b/g, '')
    .replace(/[^\w\s@/-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeEvent(value) {
  return normalizeText(value)
    .replace(/\s+vs\.?\s+/g, ' @ ')
    .replace(/\s+v\.?\s+/g, ' @ ')
    .replace(/\s+-\s+/g, ' @ ');
}

function compactText(value) {
  return normalizeText(value).replace(/\s+/g, '');
}

function normalizeTeam(value) {
  return normalizeText(value)
    .replace(/\bst\b/g, 'state')
    .replace(/\bsaint\b/g, 'st')
    .trim();
}

function extractTeamsFromEvent(event) {
  const normalized = normalizeEvent(event);
  if (!normalized) return [];
  if (normalized.includes(' @ ')) {
    return normalized.split(' @ ').map((part) => normalizeTeam(part)).filter(Boolean);
  }
  return normalized.split('/').map((part) => normalizeTeam(part)).filter(Boolean);
}

function buildExecutionMetadataIndex() {
  const decisions = readJsonl(CORE_PATHS.decisionLedger);
  const byRecId = new Map();
  const bySelection = new Map();
  const byEvent = new Map();
  const byTeam = new Map();

  for (const row of decisions) {
    const meta = {
      sport: row.sport || null,
      league: row.league || row.sport || null,
      normalized_event: row.event_label ? normalizeEvent(row.event_label) : null,
      event_label: row.event_label || null,
      selection: row.selection || null,
    };
    if (row.rec_id) byRecId.set(normalizeText(row.rec_id), meta);
    if (row.selection) bySelection.set(normalizeText(row.selection), meta);
    if (meta.normalized_event) byEvent.set(meta.normalized_event, meta);
    for (const team of extractTeamsFromEvent(row.event_label)) {
      if (!byTeam.has(team)) byTeam.set(team, { sport: meta.sport, league: meta.league });
    }
  }

  for (const entry of STATIC_TEAM_METADATA) {
    for (const team of entry.teams) {
      const key = normalizeTeam(team);
      if (!byTeam.has(key)) byTeam.set(key, { sport: entry.sport, league: entry.league });
    }
  }

  return { byRecId, bySelection, byEvent, byTeam };
}

function inferFromTeams(event, byTeam) {
  const teams = extractTeamsFromEvent(event);
  if (!teams.length) return { sport: null, league: null };
  const candidates = teams.map((team) => byTeam.get(team)).filter(Boolean);
  if (!candidates.length) return { sport: null, league: null };
  const sports = new Set(candidates.map((row) => row.sport).filter(Boolean));
  const leagues = new Set(candidates.map((row) => row.league).filter(Boolean));
  if (sports.size === 1) {
    return {
      sport: Array.from(sports)[0] || null,
      league: leagues.size === 1 ? Array.from(leagues)[0] : Array.from(leagues)[0] || Array.from(sports)[0] || null,
    };
  }
  return { sport: null, league: null };
}

function buildPlacementSnapshot(row) {
  const actualQuote = row.actual_odds || null;
  const actualBook = row.actual_sportsbook || row.sportsbook || null;
  const recommendedQuote = row.recommended_odds || row.odds_american || null;
  const recommendedBook = row.recommended_sportsbook || null;
  const timestampUtc = row.placement_snapshot_timestamp_utc || row.logged_at_utc || row.ingestion_timestamp || new Date().toISOString();
  const sameBookQuote = row.placement_same_book_quote || (actualQuote ? {
    sportsbook: actualBook,
    odds_american: actualQuote,
    market: row.market || row.market_type || null,
    selection: row.selection || null,
  } : null);
  const consensusQuote = row.placement_consensus_quote || (recommendedQuote ? {
    sportsbook: recommendedBook,
    odds_american: recommendedQuote,
    market: row.market || row.market_type || null,
    selection: row.selection || null,
  } : null);

  let status = row.placement_snapshot_status || null;
  let source = row.placement_snapshot_source || null;
  let warning = row.placement_snapshot_warning || null;
  let snapshotJson = row.placement_market_snapshot_json || null;

  if (!status) {
    if (snapshotJson && row.placement_snapshot_source === 'api_quote') {
      status = 'exact_snapshot_captured';
      source = source || 'api_quote';
    } else if (row.screenshot_filename) {
      status = 'screenshot_only_snapshot';
      source = source || 'screenshot_extraction';
      snapshotJson = snapshotJson || { same_book_quote: sameBookQuote, screenshot_filename: row.screenshot_filename };
    } else if (sameBookQuote || consensusQuote) {
      status = 'proxy_snapshot_captured';
      source = source || 'recommendation_proxy';
      snapshotJson = snapshotJson || { same_book_quote: sameBookQuote, consensus_quote: consensusQuote };
      warning = warning || 'Exact placement-time API snapshot unavailable; proxy quote stored.';
    } else {
      status = 'snapshot_missing';
      source = source || 'none';
      warning = warning || 'No placement snapshot source available.';
    }
  }

  const hashPayload = snapshotJson ? JSON.stringify(snapshotJson) : `${status}|${row.execution_id || ''}|${sameBookQuote?.odds_american || ''}|${consensusQuote?.odds_american || ''}`;
  const hash = row.placement_market_snapshot_hash || createHash('sha256').update(hashPayload).digest('hex');

  return {
    placement_snapshot_status: status,
    placement_snapshot_source: source,
    placement_snapshot_timestamp_utc: timestampUtc,
    placement_market_snapshot_json: snapshotJson,
    placement_market_snapshot_hash: hash,
    placement_same_book_quote: sameBookQuote,
    placement_consensus_quote: consensusQuote,
    placement_snapshot_warning: warning,
  };
}

export function enrichExecutionLogRow(row, options = {}) {
  const metadataIndex = options.metadataIndex || buildExecutionMetadataIndex();
  const next = { ...row };
  next.logged_at_utc = next.logged_at_utc || new Date().toISOString();
  const warnings = Array.isArray(next.warnings) ? [...next.warnings] : [];
  const notes = Array.isArray(next.notes) ? [...next.notes] : (next.notes ? [next.notes] : []);

  const normalizedEvent = next.normalized_event || normalizeEvent(next.event || next.event_label || '');
  if (normalizedEvent) next.normalized_event = normalizedEvent;

  let inferred = null;
  if (next.rec_id) inferred = metadataIndex.byRecId.get(normalizeText(next.rec_id)) || null;
  if (!inferred && next.selection) inferred = metadataIndex.bySelection.get(normalizeText(next.selection)) || null;
  if (!inferred && normalizedEvent) inferred = metadataIndex.byEvent.get(normalizedEvent) || null;

  const teamInference = (!inferred || !inferred.sport) ? inferFromTeams(next.event || next.event_label || '', metadataIndex.byTeam) : { sport: null, league: null };

  if (!next.sport) next.sport = inferred?.sport || teamInference.sport || 'UNKNOWN';
  if (!next.league) next.league = inferred?.league || teamInference.league || (next.sport !== 'UNKNOWN' ? next.sport : null);

  if (!next.event && inferred?.event_label) next.event = inferred.event_label;
  if (next.sport === 'UNKNOWN') warnings.push('unknown_sport_metadata');
  if (!next.league && next.sport && next.sport !== 'UNKNOWN') next.league = next.sport;
  Object.assign(next, buildPlacementSnapshot(next));

  next.warnings = Array.from(new Set(warnings));
  next.notes = Array.from(new Set(notes));
  return next;
}

function asPercentProbability(value) {
  const numeric = parseNumber(value);
  if (numeric === null) return null;
  return numeric > 1 ? numeric / 100 : numeric;
}

function safeDateMs(value) {
  const ms = Date.parse(String(value || '').replace(' CT', ''));
  return Number.isFinite(ms) ? ms : null;
}

function normalizePromoType(value) {
  const normalized = String(value || '').trim().replace(/\s+/g, ' ').toUpperCase();
  if (!normalized) return null;
  if (normalized === 'NO SWEAT TOKEN') return 'NO SWEAT TOKEN';
  if (normalized === 'EARLY WIN TOKEN') return 'EARLY WIN TOKEN';
  if (normalized === 'PROFIT BOOST') return 'PROFIT BOOST';
  return null;
}

function loadActiveOperatorPromos() {
  const rows = readJsonl(CORE_PATHS.operatorPromoLog);
  return rows.filter((row) => String(row.status || '').toUpperCase() === 'ACTIVE');
}

function scopeMatchesPromo(scope, candidate) {
  const normalizedScope = String(scope || '').trim().toUpperCase();
  if (!normalizedScope || normalizedScope === 'GENERAL') return true;
  const candidateSport = String(candidate?.sport || '').trim().toUpperCase();
  const candidateMarket = String(candidate?.market_type || '').trim().toUpperCase();
  return normalizedScope === candidateSport || normalizedScope === candidateMarket;
}

function resolveExecutionPromoContext(row, candidate) {
  const promoType = normalizePromoType(row?.promo_type || row?.promo);
  if (!promoType) return null;
  const sportsbook = normalizeText(row?.actual_sportsbook);
  const activePromos = loadActiveOperatorPromos()
    .filter((entry) => normalizePromoType(entry.promo_type) === promoType)
    .filter((entry) => normalizeText(entry.sportsbook) === sportsbook)
    .filter((entry) => scopeMatchesPromo(entry.scope, candidate));
  const matchedPromo = activePromos[0] || null;
  return {
    promo_type: promoType,
    reward_type: promoType,
    matched_promo_id: matchedPromo?.promo_id || null,
    matched_promo_scope: matchedPromo?.scope || null,
    matched_promo_bet_types: matchedPromo?.bet_types || null,
    matched_promo_expires_at_utc: matchedPromo?.expires_at_utc || null,
    matched_promo_expires_raw: matchedPromo?.expires_raw || null,
    matched_promo_max_wager: Number.isFinite(parseNumber(matchedPromo?.max_wager)) ? parseNumber(matchedPromo.max_wager) : null,
  };
}

function evPctFromTrueProbAndOdds(trueProb, americanOdds) {
  const probability = asPercentProbability(trueProb);
  const decimal = Number.isFinite(parseNumber(americanOdds))
    ? (parseNumber(americanOdds) > 0
      ? 1 + (parseNumber(americanOdds) / 100)
      : 1 + (100 / Math.abs(parseNumber(americanOdds))))
    : null;
  if (!Number.isFinite(probability) || !Number.isFinite(decimal) || decimal <= 1) return null;
  const net = decimal - 1;
  return round2(((probability * net) - (1 - probability)) * 100);
}

function classifyNoSweatLabel(value, policy) {
  const positiveThreshold = parseNumber(policy?.promo_evaluation?.no_sweat?.positive_threshold_pct) ?? 2;
  const neutralThreshold = parseNumber(policy?.promo_evaluation?.no_sweat?.neutral_threshold_pct) ?? 0;
  const numeric = parseNumber(value);
  if (!Number.isFinite(numeric)) return null;
  if (numeric >= positiveThreshold) return 'NO_SWEAT_POSITIVE';
  if (numeric >= neutralThreshold) return 'NO_SWEAT_NEUTRAL';
  return 'NO_SWEAT_NOT_WORTH_IT';
}

function buildPromoEvaluationFields(row, candidate, policy) {
  const promoContext = resolveExecutionPromoContext(row, candidate);
  if (!promoContext) return {};

  const actualStake = parseNumber(row.actual_stake);
  const actualOdds = parseNumber(row.actual_odds);
  const trueProb = asPercentProbability(candidate?.post_conf_true_prob);
  const standardEvPct = evPctFromTrueProbAndOdds(trueProb, actualOdds);

  const base = {
    promo_type: promoContext.promo_type,
    reward_type: promoContext.reward_type,
    matched_promo_id: promoContext.matched_promo_id,
    matched_promo_scope: promoContext.matched_promo_scope,
    matched_promo_bet_types: promoContext.matched_promo_bet_types,
    matched_promo_expires_at_utc: promoContext.matched_promo_expires_at_utc,
    matched_promo_expires_raw: promoContext.matched_promo_expires_raw,
    standard_ev_pct: standardEvPct,
  };

  if (promoContext.promo_type !== 'NO SWEAT TOKEN') {
    return base;
  }

  const conversionPct = parseNumber(policy?.promo_evaluation?.no_sweat?.bonus_bet_conversion_pct) ?? 70;
  const maxRefund = Number.isFinite(actualStake)
    ? round2(Math.min(actualStake, Number.isFinite(promoContext.matched_promo_max_wager) ? promoContext.matched_promo_max_wager : actualStake))
    : null;
  const refundValuePctOfStake = Number.isFinite(actualStake) && actualStake > 0 && Number.isFinite(maxRefund)
    ? round2((maxRefund * (conversionPct / 100) / actualStake) * 100)
    : null;
  const adjustedEvPct = Number.isFinite(standardEvPct) && Number.isFinite(trueProb) && Number.isFinite(refundValuePctOfStake)
    ? round2(standardEvPct + ((1 - trueProb) * refundValuePctOfStake))
    : null;

  return {
    ...base,
    no_sweat_max_refund: maxRefund,
    refund_type: policy?.promo_evaluation?.no_sweat?.refund_type || 'bonus_bet',
    bonus_bet_conversion_pct: conversionPct,
    no_sweat_adjusted_ev_pct: adjustedEvPct,
    no_sweat_adjusted_label: classifyNoSweatLabel(adjustedEvPct, policy),
  };
}

function extractRuntimeRecommendationContexts(input, acc = []) {
  if (!input) return acc;
  if (Array.isArray(input)) {
    input.forEach((item) => extractRuntimeRecommendationContexts(item, acc));
    return acc;
  }
  if (typeof input !== 'object') return acc;
  if (input.summary && (input.message_type || input.session_id || input.run_at_ct)) {
    acc.push(input);
  }
  Object.values(input).forEach((value) => extractRuntimeRecommendationContexts(value, acc));
  return acc;
}

function parseRuntimeRecommendations(summary, runId, targetDate, context = {}) {
  const text = String(summary || '');
  const rows = [];
  const pattern = /- \[ \] (.+?)\s+([+-]\d{2,4}) \| ([^\n]+)\n\s+Timestamp \(CT\): ([^\n]+)\n\s+True Prob: ([0-9.]+)% \| Implied Prob \(de-vig\): ([0-9.]+)% \| Edge: \+?([0-9.]+)%\n\s+Kelly Stake: \$([0-9.]+)/g;
  let match;
  let index = 1;
  while ((match = pattern.exec(text)) !== null) {
    const selection = match[1].trim();
    const books = match[3].split('/').map((item) => item.trim()).filter(Boolean);
    const recId = `runtime-rec::${runId || 'unknown'}::${compactText(selection)}::${index}`;
    rows.push({
      rec_id: recId,
      recommendation_key: `${runId || 'runtime'}::${index}`,
      run_id: runId,
      selection,
      sportsbook: books[0] || null,
      sportsbook_options: books,
      odds_american: match[2],
      timestamp_ct: match[4].trim(),
      post_conf_true_prob: Number(match[5]),
      devig_implied_prob: Number(match[6]),
      post_conf_edge_pct: Number(match[7]),
      raw_edge_pct: null,
      kelly_stake: Number(match[8]),
      market_type: null,
      event_label: null,
      bet_class: 'EDGE_BET',
      source: 'runtime_summary',
      context_message_type: context.message_type || null,
      context_data_failure_codes: Array.isArray(context.data_failure_codes) ? context.data_failure_codes : [],
      sport: null,
      league: null,
      target_date: targetDate || null,
    });
    index += 1;
  }
  return rows;
}

function normalizeExecutionDuplicateKey(row) {
  return {
    selection: normalizeText(row.selection || ''),
    sportsbook: normalizeText(row.actual_sportsbook || row.recommended_sportsbook || ''),
    odds: String(parseNumber(row.actual_odds ?? row.recommended_odds) ?? ''),
    stake: String(round2(parseNumber(row.actual_stake ?? row.recommended_stake)) ?? ''),
    date: toCtIsoDate(row.bet_slip_timestamp || row.logged_at_utc || new Date().toISOString()),
    rec_id: String(row.rec_id || '').trim(),
  };
}

function findLikelyDuplicateExecution(row) {
  const target = normalizeExecutionDuplicateKey(row);
  const existingRows = readExecutionLog();
  for (const existing of existingRows) {
    const existingKey = normalizeExecutionDuplicateKey(existing);
    const sameCore =
      target.selection === existingKey.selection
      && target.sportsbook === existingKey.sportsbook
      && target.odds === existingKey.odds
      && target.stake === existingKey.stake
      && target.date === existingKey.date;
    if (!sameCore) continue;
    if (target.rec_id && existingKey.rec_id && target.rec_id !== existingKey.rec_id) continue;
    return existing;
  }
  return null;
}

function loadRecommendationUniverse() {
  const runtimeStatus = readJson(CORE_PATHS.runtimeStatus, {});
  const decisionRows = readJsonl(CORE_PATHS.decisionLedger)
    .filter((row) => row.final_decision === 'BET' || row.decision_kind === 'BET')
    .map((row) => ({
      rec_id: row.rec_id || null,
      recommendation_key: row.entry_id,
      run_id: row.run_id,
      sport: row.sport || null,
      league: row.league || row.sport || null,
      selection: row.selection,
      event_label: row.event_label,
      normalized_event: row.event_label ? normalizeEvent(row.event_label) : null,
      sportsbook: row.sportsbook,
      sportsbook_options: [row.sportsbook].filter(Boolean),
      odds_american: row.odds_american,
      market_type: row.market_type || null,
      timestamp_ct: row.timestamp_ct,
      post_conf_true_prob: row.post_conf_true_prob,
      devig_implied_prob: row.devig_implied_prob,
      post_conf_edge_pct: row.post_conf_edge_pct,
      raw_edge_pct: row.raw_edge_pct ?? null,
      kelly_stake: parseNumber(row.kelly_stake),
      bet_class: row.bet_class || 'EDGE_BET',
      source: row.source || 'decision_ledger',
      context_message_type: 'BET',
      context_data_failure_codes: [],
    }));

  const runtimeRows = extractRuntimeRecommendationContexts(runtimeStatus)
    .filter((context) => String(context.message_type || '').toUpperCase() === 'BET')
    .flatMap((context) => {
      const runId = context.run_id || (context.session_id ? `openclaw::morning-edge-hunt::${context.session_id}` : null);
      return parseRuntimeRecommendations(context.summary, runId, context.date_key || null, context);
    });

  return [...decisionRows, ...runtimeRows];
}

function uniqueRunIdsInOrder(recommendations) {
  const seen = new Set();
  const runIds = [];
  for (const row of recommendations) {
    const runId = String(row.run_id || '').trim();
    if (!runId || seen.has(runId)) continue;
    seen.add(runId);
    runIds.push(runId);
  }
  return runIds;
}

function extractRunDate(runId) {
  const match = String(runId || '').match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function latestCanonicalRunId() {
  const publicData = readJson(CORE_PATHS.publicData, {});
  return String(publicData?.latest_canonical_hunt_run?.run_id || '').trim() || null;
}

function buildRecommendationScopes(recommendations) {
  const latestRunId = latestCanonicalRunId();
  const orderedRunIds = uniqueRunIdsInOrder(recommendations);
  const latestRunDate = extractRunDate(latestRunId);

  const latestCandidates = latestRunId
    ? recommendations.filter((row) => String(row.run_id || '').trim() === latestRunId)
    : [];

  const recentRunIds = orderedRunIds
    .filter((runId) => runId !== latestRunId)
    .filter((runId) => {
      if (!latestRunDate) return true;
      return extractRunDate(runId) === latestRunDate;
    })
    .slice(-8);

  const recentHistoricalCandidates = recommendations.filter((row) => recentRunIds.includes(String(row.run_id || '').trim()));

  return {
    latestRunId,
    recentRunIds,
    latestCandidates,
    recentHistoricalCandidates,
  };
}

function sportsbookMatch(extractedBook, candidate) {
  const books = [candidate.sportsbook, ...(candidate.sportsbook_options || [])].filter(Boolean).map(normalizeText);
  return extractedBook && books.includes(normalizeText(extractedBook));
}

function executionRecommendationScore(row, candidate) {
  let score = 0;
  const rowRunId = normalizeText(row.run_id);
  const candidateRunId = normalizeText(candidate.run_id);
  if (rowRunId && candidateRunId && rowRunId === candidateRunId) score += 45;

  const rowEvent = normalizeEvent(row.event || row.normalized_event || '');
  const candidateEvent = normalizeEvent(candidate.event_label || candidate.normalized_event || '');
  if (rowEvent && candidateEvent && rowEvent === candidateEvent) {
    score += 30;
  } else {
    const rowTokens = new Set(rowEvent.split(' ').filter(Boolean));
    const candidateText = normalizeText(candidate.selection);
    let overlap = 0;
    for (const token of rowTokens) {
      if (token && candidateText.includes(token)) overlap += 1;
    }
    score += Math.min(18, overlap * 6);
  }

  if (row.selection && candidate.selection && normalizeText(row.selection) === normalizeText(candidate.selection)) score += 25;
  if (sportsbookMatch(row.actual_sportsbook || row.recommended_sportsbook || row.sportsbook, candidate)) score += 12;

  const rowRecommendedOdds = parseNumber(row.recommended_odds);
  const rowActualOdds = parseNumber(row.actual_odds);
  const candidateOdds = parseNumber(candidate.odds_american);
  const preferredOdds = Number.isFinite(rowRecommendedOdds) ? rowRecommendedOdds : rowActualOdds;
  if (Number.isFinite(preferredOdds) && Number.isFinite(candidateOdds)) {
    const diff = Math.abs(preferredOdds - candidateOdds);
    if (diff === 0) score += 20;
    else if (diff <= 5) score += 16;
    else if (diff <= 10) score += 10;
  }

  const rowStake = parseNumber(row.recommended_stake) ?? parseNumber(row.actual_stake);
  const candidateStake = parseNumber(candidate.kelly_stake);
  if (Number.isFinite(rowStake) && Number.isFinite(candidateStake)) {
    const diff = Math.abs(rowStake - candidateStake);
    if (diff < 0.01) score += 15;
    else if (diff <= 0.5) score += 10;
    else if (diff <= 2) score += 4;
  }

  const rowTs = safeDateMs(row.recommendation_timestamp || row.bet_slip_timestamp);
  const candidateTs = safeDateMs(candidate.timestamp_ct);
  if (Number.isFinite(rowTs) && Number.isFinite(candidateTs)) {
    const diffMinutes = Math.abs(rowTs - candidateTs) / 60000;
    if (diffMinutes <= 30) score += 10;
    else if (diffMinutes <= 120) score += 6;
    else if (diffMinutes <= 480) score += 3;
  }

  return score;
}

function classifyExecutionRecommendationMatch(row, recommendations) {
  const scored = recommendations.map((candidate) => ({ candidate, score: executionRecommendationScore(row, candidate) }))
    .sort((a, b) => b.score - a.score);
  const top = scored[0];
  const second = scored[1];
  if (!top || top.score < 65) return { match_status: 'unmatched_manual_bet', candidate: null, confidence: 'low' };
  if (second && top.score - second.score <= 5) {
    return { match_status: 'ambiguous_match', candidate: top.candidate, confidence: 'low' };
  }
  return {
    match_status: top.score >= 85 ? 'matched_to_recommendation' : 'matched_with_low_confidence',
    candidate: top.candidate,
    confidence: top.score >= 85 ? 'high' : 'medium',
  };
}

export function previewExecutionRecommendationMatch(row, recommendations = null) {
  const universe = recommendations || loadRecommendationUniverse();
  const scopes = buildRecommendationScopes(universe);

  const evaluateMatch = (match, scopeName) => {
    const candidate = match.candidate || null;
    const approved = Boolean(candidate)
      && ['matched_to_recommendation', 'matched_with_low_confidence'].includes(match.match_status)
      && String(candidate.context_message_type || '').toUpperCase() === 'BET'
      && (!Array.isArray(candidate.context_data_failure_codes) || candidate.context_data_failure_codes.length === 0);

    return {
      match_status: match.match_status,
      match_confidence: match.confidence || 'low',
      approved,
      candidate,
      match_scope: scopeName,
      latest_run_id: scopes.latestRunId,
      historical_run_ids_considered: scopes.recentRunIds,
      stale_execution: approved && scopeName === 'recent_historical_runs',
      execution_approval_result: approved
        ? (scopeName === 'recent_historical_runs' ? 'STALE_EXECUTION' : 'APPROVED_EXECUTION')
        : (match.match_status === 'ambiguous_match' ? 'AMBIGUOUS_MATCH' : 'REJECT_EXECUTION'),
      execution_approval_result_reason: approved
        ? (scopeName === 'recent_historical_runs'
            ? 'matched_recent_historical_recommendation'
            : 'matched_originating_recommendation')
        : (match.match_status === 'ambiguous_match' ? 'ambiguous_recommendation_match' : 'no_matching_recommendation_found'),
    };
  };

  if (scopes.latestCandidates.length) {
    const latestPreview = evaluateMatch(classifyExecutionRecommendationMatch(row, scopes.latestCandidates), 'latest_canonical_run');
    if (latestPreview.approved) {
      return latestPreview;
    }
  }

  if (scopes.recentHistoricalCandidates.length) {
    const historicalPreview = evaluateMatch(classifyExecutionRecommendationMatch(row, scopes.recentHistoricalCandidates), 'recent_historical_runs');
    if (historicalPreview.approved) {
      return historicalPreview;
    }
  }

  return evaluateMatch(classifyExecutionRecommendationMatch(row, universe), 'full_universe_fallback');
}

function cleanupNotesValue(value) {
  const items = Array.isArray(value) ? value : value ? [value] : [];
  return Array.from(new Set(items.filter(Boolean).map((item) => String(item).trim()).filter((item) => item && item !== 'blocked_run')));
}

function evAtBetFieldsFromCandidate(row, candidate) {
  const existingTrueProb = asPercentProbability(row.true_probability_at_bet);
  const existingImpliedProb = asPercentProbability(row.implied_probability_at_bet);
  const existingEdgePct = parseNumber(row.edge_pct_at_bet);
  const existingRawEdgePct = parseNumber(row.raw_edge_pct_at_bet);

  const trueProb = existingTrueProb ?? asPercentProbability(candidate?.post_conf_true_prob);
  const impliedProb = existingImpliedProb ?? asPercentProbability(candidate?.devig_implied_prob);
  const edgePct = existingEdgePct ?? parseNumber(candidate?.post_conf_edge_pct);
  const rawEdgePct = existingRawEdgePct ?? parseNumber(candidate?.raw_edge_pct);

  const hasCanonicalEv = [trueProb, impliedProb, edgePct].every((value) => Number.isFinite(value));
  return {
    true_probability_at_bet: Number.isFinite(trueProb) ? round2(trueProb) : null,
    implied_probability_at_bet: Number.isFinite(impliedProb) ? round2(impliedProb) : null,
    edge_pct_at_bet: Number.isFinite(edgePct) ? round2(edgePct) : null,
    raw_edge_pct_at_bet: Number.isFinite(rawEdgePct) ? round2(rawEdgePct) : null,
    recommended_odds_at_bet: row.recommended_odds_at_bet || candidate?.odds_american || row.recommended_odds || null,
    bet_class: row.bet_class || candidate?.bet_class || null,
    market_type: row.market_type || candidate?.market_type || row.market || null,
    event_label: row.event_label || candidate?.event_label || row.event || null,
    ev_at_bet_status: hasCanonicalEv ? 'captured' : 'missing_recommendation_ev',
    ev_at_bet_source: hasCanonicalEv ? candidate?.source || 'recommendation_match' : 'missing',
  };
}

function reclassifyExecutionRow(row, recommendations = null) {
  const universe = recommendations || loadRecommendationUniverse();
  const match = classifyExecutionRecommendationMatch(row, universe);
  if (!match.candidate) {
    return {
      ...row,
      ...evAtBetFieldsFromCandidate(row, null),
      match_status: row.match_status || 'unmatched_manual_bet',
    };
  }

  const candidate = match.candidate;
  const approved = ['matched_to_recommendation', 'matched_with_low_confidence'].includes(match.match_status)
    && String(candidate.context_message_type || '').toUpperCase() === 'BET'
    && (!Array.isArray(candidate.context_data_failure_codes) || candidate.context_data_failure_codes.length === 0);
  const preserveStaleExecution = String(row.execution_approval_result || '').trim() === 'STALE_EXECUTION';
  const preserveOffPlanExecution = String(row.execution_approval_result || '').trim() === 'OFF_PLAN_EXECUTION';

  const next = {
    ...row,
    rec_id: row.rec_id?.startsWith('manual-recovered::') ? candidate.rec_id : (row.rec_id || candidate.rec_id),
    run_id: row.run_id || candidate.run_id || null,
    selection: row.selection || candidate.selection || null,
    recommended_sportsbook: row.recommended_sportsbook || candidate.sportsbook || null,
    recommended_odds: row.recommended_odds || candidate.odds_american || null,
    recommended_stake: row.recommended_stake || candidate.kelly_stake || null,
    recommendation_timestamp: row.recommendation_timestamp || candidate.timestamp_ct || null,
    sport: row.sport && row.sport !== 'UNKNOWN' ? row.sport : (candidate.sport || row.sport || null),
    league: row.league || candidate.league || row.sport || null,
    match_status: match.match_status,
    match_confidence: match.confidence,
    execution_approval_result: preserveOffPlanExecution
      ? 'OFF_PLAN_EXECUTION'
      : preserveStaleExecution
      ? 'STALE_EXECUTION'
      : (approved ? 'APPROVED_EXECUTION' : (match.match_status === 'ambiguous_match' ? 'AMBIGUOUS_MATCH' : (row.execution_approval_result || 'REJECT_EXECUTION'))),
    manual_override_flag: approved ? false : Boolean(row.manual_override_flag),
    execution_approval_result_reason: preserveOffPlanExecution
      ? (row.execution_approval_result_reason || 'no_matching_recommendation_found')
      : preserveStaleExecution
      ? 'matched_recent_historical_recommendation'
      : (approved ? 'matched_originating_recommendation' : row.execution_approval_result_reason),
    override_reason: approved && normalizeText(row.override_reason) === 'blocked_run' ? null : row.override_reason,
    notes: cleanupNotesValue(row.notes),
    warnings: cleanupNotesValue(row.warnings),
    ...evAtBetFieldsFromCandidate(row, candidate),
  };
  return next;
}

export function loadExecutionPolicy() {
  const parsed = readJson(EXECUTION_POLICY_PATH, {});
  return {
    ...DEFAULT_POLICY,
    ...parsed,
    promo_evaluation: {
      ...DEFAULT_POLICY.promo_evaluation,
      ...(parsed?.promo_evaluation || {}),
      no_sweat: {
        ...DEFAULT_POLICY.promo_evaluation.no_sweat,
        ...(parsed?.promo_evaluation?.no_sweat || {}),
      },
    },
    price_only_tolerance_cents: {
      ...DEFAULT_POLICY.price_only_tolerance_cents,
      ...(parsed?.price_only_tolerance_cents || {}),
    },
    line_tolerance_points: {
      ...DEFAULT_POLICY.line_tolerance_points,
      ...(parsed?.line_tolerance_points || {}),
    },
    hedge_management: {
      ...DEFAULT_POLICY.hedge_management,
      ...(parsed?.hedge_management || {}),
      meaningful_stake_rule: {
        ...DEFAULT_POLICY.hedge_management.meaningful_stake_rule,
        ...(parsed?.hedge_management?.meaningful_stake_rule || {}),
      },
      pregame_thresholds_by_scan_frequency: {
        ...DEFAULT_POLICY.hedge_management.pregame_thresholds_by_scan_frequency,
        ...(parsed?.hedge_management?.pregame_thresholds_by_scan_frequency || {}),
      },
      live_override: {
        ...DEFAULT_POLICY.hedge_management.live_override,
        ...(parsed?.hedge_management?.live_override || {}),
      },
    },
    supported_books: {
      ...DEFAULT_POLICY.supported_books,
      ...(parsed?.supported_books || {}),
    },
    supported_sports: {
      ...DEFAULT_POLICY.supported_sports,
      ...(parsed?.supported_sports || {}),
    },
  };
}

function marketKind(row) {
  const market = String(row.market_type || '').toLowerCase();
  if (market === 'ml' || market === 'moneyline' || market === 'h2h') return 'moneyline';
  if (market.includes('spread')) return 'spread';
  if (market.includes('total')) return 'total';
  if (market.includes('sgp') || market.includes('parlay')) return 'parlay';
  return 'unknown';
}

function tierFromThreshold(threshold) {
  const value = parseNumber(threshold);
  if (value === null) return 'T3';
  if (value >= 6) return 'T1';
  if (value >= 4) return 'T2';
  return 'T3';
}

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\bml\b/g, '')
    .replace(/\s+/g, ' ')
    .replace(/[^a-z0-9+\-.\s]/g, '')
    .trim();
}

function parseSelection(row) {
  const selection = String(row.selection || '').trim();
  const kind = marketKind(row);
  if (kind === 'moneyline') {
    return { kind, outcomeName: normalizeName(selection.replace(/\s+ml$/i, '')) };
  }
  if (kind === 'spread') {
    const match = selection.match(/^(.*?)([+-]\d+(?:\.\d+)?)$/);
    if (!match) return { kind, outcomeName: normalizeName(selection), point: null };
    return {
      kind,
      outcomeName: normalizeName(match[1]),
      point: Number(match[2]),
    };
  }
  if (kind === 'total') {
    const match = selection.match(/^(Over|Under)\s+(\d+(?:\.\d+)?)$/i);
    if (!match) return { kind, side: null, point: null };
    return {
      kind,
      side: match[1].toLowerCase(),
      point: Number(match[2]),
      outcomeName: normalizeName(match[1]),
    };
  }
  return { kind, outcomeName: normalizeName(selection) };
}

function toEventLabel(event) {
  const away = String(event.away_team || '').trim();
  const home = String(event.home_team || '').trim();
  if (away && home) return `${away} @ ${home}`;
  return event.id || '';
}

function computeLineWorseAmount(kind, recommendedPoint, currentPoint, side) {
  if (!Number.isFinite(recommendedPoint) || !Number.isFinite(currentPoint)) return 0;
  if (kind === 'spread') return Math.max(0, recommendedPoint - currentPoint);
  if (kind === 'total' && side === 'over') return Math.max(0, currentPoint - recommendedPoint);
  if (kind === 'total' && side === 'under') return Math.max(0, recommendedPoint - currentPoint);
  return 0;
}

function computePriceWorseCents(recommendedOdds, currentOdds) {
  if (!Number.isFinite(recommendedOdds) || !Number.isFinite(currentOdds)) return null;
  return Math.max(0, recommendedOdds - currentOdds);
}

function loadQuoteCache() {
  return readJson(EXECUTION_QUOTE_CACHE_PATH, { entries: {} });
}

function writeQuoteCache(cache) {
  fs.writeFileSync(EXECUTION_QUOTE_CACHE_PATH, `${JSON.stringify(cache, null, 2)}\n`, 'utf8');
}

async function fetchSportOdds({ sportKey, bookKey, marketKey, policy }) {
  const cache = loadQuoteCache();
  const cacheKey = `${sportKey}::${bookKey}::${marketKey}`;
  const cacheEntry = cache.entries?.[cacheKey];
  const maxAgeMinutes = policy?.odds_stale_minutes || 10;
  if (cacheEntry?.fetched_at_ms && (Date.now() - cacheEntry.fetched_at_ms) <= (maxAgeMinutes * 60 * 1000)) {
    return { source: 'cache', payload: cacheEntry.payload };
  }

  const apiKey = process.env.ODDS_API_KEY || '';
  if (!apiKey) throw new Error('missing_api_key');

  const url = new URL(`https://api.the-odds-api.com/v4/sports/${sportKey}/odds`);
  url.searchParams.set('apiKey', apiKey);
  url.searchParams.set('regions', 'us');
  url.searchParams.set('markets', marketKey);
  url.searchParams.set('bookmakers', bookKey);
  url.searchParams.set('oddsFormat', 'american');
  url.searchParams.set('dateFormat', 'iso');

  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    if (response.status === 401) throw new Error('auth_failure');
    if (response.status === 429) throw new Error('quota_failure');
    throw new Error(`odds_fetch_failed:${response.status}:${text.slice(0, 120)}`);
  }
  const payload = await response.json();
  cache.entries = cache.entries || {};
  cache.entries[cacheKey] = {
    fetched_at_ms: Date.now(),
    payload,
  };
  writeQuoteCache(cache);
  return { source: 'network', payload };
}

function findQuoteForRecommendation({ events, row, bookKey }) {
  const parsed = parseSelection(row);
  const candidates = [];

  for (const event of events || []) {
    for (const bookmaker of event.bookmakers || []) {
      if (String(bookmaker.key || '').trim() !== bookKey) continue;
      for (const market of bookmaker.markets || []) {
        const mk = String(market.key || '').trim();
        const expectedMarket = parsed.kind === 'moneyline' ? 'h2h' : parsed.kind === 'spread' ? 'spreads' : parsed.kind === 'total' ? 'totals' : null;
        if (!expectedMarket || mk !== expectedMarket) continue;
        for (const outcome of market.outcomes || []) {
          const price = parseNumber(outcome.price);
          const point = parseNumber(outcome.point);
          const outcomeName = normalizeName(outcome.name);
          let matched = false;
          let lineWorseAmount = 0;

          if (parsed.kind === 'moneyline') {
            matched = outcomeName === parsed.outcomeName;
          } else if (parsed.kind === 'spread') {
            matched = outcomeName === parsed.outcomeName;
            lineWorseAmount = computeLineWorseAmount(parsed.kind, parsed.point, point, null);
          } else if (parsed.kind === 'total') {
            matched = outcomeName === parsed.outcomeName;
            lineWorseAmount = computeLineWorseAmount(parsed.kind, parsed.point, point, parsed.side);
          }

          if (!matched) continue;
          const recOdds = parseNumber(row.odds_american);
          const priceWorseCents = computePriceWorseCents(recOdds, price);
          const score = (lineWorseAmount * 1000) + Math.abs((price || 0) - (recOdds || 0));
          candidates.push({
            event_id: event.id || null,
            event_label: toEventLabel(event),
            commence_time: event.commence_time || null,
            book_key: bookmaker.key,
            market_key: mk,
            current_odds_american: price,
            current_odds_decimal: price === null ? null : (price > 0 ? round2(1 + (price / 100)) : round2(1 + (100 / Math.abs(price)))),
            current_point: point,
            odds_last_update: market.last_update || bookmaker.last_update || event.commence_time || null,
            line_worse_amount: lineWorseAmount,
            price_worse_cents: priceWorseCents,
            score,
          });
        }
      }
    }
  }

  if (candidates.length === 0) return { match_status: 'none', quote: null };
  candidates.sort((a, b) => a.score - b.score);
  if (candidates.length > 1 && candidates[0].score === candidates[1].score) {
    return { match_status: 'ambiguous', quote: null };
  }
  return { match_status: 'matched', quote: candidates[0] };
}

function impliedProbabilityFromAmerican(americanOdds) {
  const price = parseNumber(americanOdds);
  if (!Number.isFinite(price)) return null;
  if (price > 0) return 100 / (price + 100);
  return Math.abs(price) / (Math.abs(price) + 100);
}

function computeClvCents(originalOdds, currentOdds) {
  const original = parseNumber(originalOdds);
  const current = parseNumber(currentOdds);
  if (!Number.isFinite(original) || !Number.isFinite(current)) return null;
  return round2(original - current);
}

export function classifyHedgeDecision({
  selection,
  original_odds,
  current_odds,
  original_edge_pct,
  current_edge_pct,
  time_to_game_start,
  stake_size,
  is_live_market,
  bankroll,
  policy = DEFAULT_POLICY,
}) {
  const clvCents = computeClvCents(original_odds, current_odds);
  const originalEdge = parseNumber(original_edge_pct);
  const currentEdge = parseNumber(current_edge_pct);
  const stakeSize = parseNumber(stake_size);
  const minutesToStart = parseNumber(time_to_game_start);
  const bankrollSize = parseNumber(bankroll);
  const marketType = is_live_market ? 'LIVE' : 'PREGAME';
  const edgeDecay = Number.isFinite(originalEdge) && Number.isFinite(currentEdge)
    ? round2(originalEdge - currentEdge)
    : null;
  const hedgePolicy = policy?.hedge_management || DEFAULT_POLICY.hedge_management;
  const scanFrequency = String(parseNumber(hedgePolicy.scan_frequency_per_day) || 3);
  const activeThresholds = hedgePolicy.pregame_thresholds_by_scan_frequency?.[scanFrequency]
    || hedgePolicy.pregame_thresholds_by_scan_frequency?.['3']
    || DEFAULT_POLICY.hedge_management.pregame_thresholds_by_scan_frequency[3];
  const strongThreshold = activeThresholds?.strong_hedge || {};
  const considerThreshold = activeThresholds?.consider_hedge || {};
  const meaningfulStakeRule = hedgePolicy.meaningful_stake_rule || DEFAULT_POLICY.hedge_management.meaningful_stake_rule;
  const liveOverride = hedgePolicy.live_override || DEFAULT_POLICY.hedge_management.live_override;
  const stakePctOfBankroll = Number.isFinite(stakeSize) && Number.isFinite(bankrollSize) && bankrollSize > 0
    ? round2((stakeSize / bankrollSize) * 100)
    : null;
  const meaningfulStake = Number.isFinite(stakeSize) && (
    (Number.isFinite(stakePctOfBankroll) && stakePctOfBankroll >= parseNumber(meaningfulStakeRule.pct_of_bankroll))
    || stakeSize >= parseNumber(meaningfulStakeRule.minimum_stake)
  );
  const withinConsiderTime = Number.isFinite(minutesToStart)
    ? minutesToStart <= parseNumber(considerThreshold.time_to_start_minutes_max)
    : false;
  const withinStrongTime = Number.isFinite(minutesToStart)
    ? minutesToStart <= parseNumber(strongThreshold.time_to_start_minutes_max)
    : false;

  let decision = 'LET RIDE';
  let reasonLines = [];
  let decisionReason = 'insufficient_clv';

  if (is_live_market && Number.isFinite(clvCents) && clvCents >= parseNumber(liveOverride.state_driven_clv_cents_min)) {
    decision = 'LET RIDE';
    decisionReason = 'state_driven_live_movement';
    reasonLines = [
      'state_driven_live_movement',
      'original_edge_already_captured',
      'default_let_ride',
    ];
  } else if (is_live_market) {
    decision = 'LET RIDE';
    decisionReason = 'state_driven_live_movement';
    reasonLines = [
      'state_driven_live_movement',
      'default_let_ride',
    ];
  } else {
    if (Number.isFinite(currentEdge) && currentEdge >= 1) {
      decision = 'LET RIDE';
      decisionReason = 'edge_still_present';
      reasonLines = ['edge_still_present'];
    } else if (!meaningfulStake) {
      decision = 'LET RIDE';
      decisionReason = 'stake_too_small_to_manage';
      reasonLines = ['stake_too_small_to_manage'];
    } else if (!withinConsiderTime) {
      decision = 'LET RIDE';
      decisionReason = 'too_early_to_manage';
      reasonLines = ['too_early_to_manage'];
    } else if (
      Number.isFinite(clvCents)
      && clvCents >= parseNumber(strongThreshold.clv_cents_min)
      && (!Number.isFinite(currentEdge) || currentEdge <= parseNumber(strongThreshold.current_edge_pct_max))
      && withinStrongTime
    ) {
      decision = 'STRONG HEDGE';
      decisionReason = 'strong_edge_decay_with_large_clv';
      reasonLines = [
        'strong_edge_decay_with_large_clv',
      ];
    } else if (
      Number.isFinite(clvCents)
      && clvCents >= parseNumber(considerThreshold.clv_cents_min)
      && Number.isFinite(currentEdge)
      && currentEdge >= 0
      && currentEdge <= parseNumber(considerThreshold.current_edge_pct_max)
    ) {
      decision = 'CONSIDER HEDGE';
      decisionReason = 'edge_decayed_with_meaningful_clv';
      reasonLines = [
        'edge_decayed_with_meaningful_clv',
      ];
    } else {
      decision = 'LET RIDE';
      decisionReason = 'insufficient_clv';
      reasonLines = [
        'insufficient_clv',
      ];
    }
  }

  return {
    selection,
    original_odds: renderOddsValue(original_odds),
    current_odds: renderOddsValue(current_odds),
    clv_cents: clvCents,
    original_edge_pct: Number.isFinite(originalEdge) ? round2(originalEdge) : null,
    current_edge_pct: Number.isFinite(currentEdge) ? round2(currentEdge) : null,
    edge_decay: edgeDecay,
    time_to_game_start: Number.isFinite(minutesToStart) ? round2(minutesToStart) : null,
    stake_size: Number.isFinite(stakeSize) ? round2(stakeSize) : null,
    bankroll: Number.isFinite(bankrollSize) ? round2(bankrollSize) : null,
    stake_pct_of_bankroll: stakePctOfBankroll,
    meaningful_stake: Boolean(meaningfulStake),
    is_live_market: Boolean(is_live_market),
    market_type: marketType,
    hedge_scan_frequency_mode: Number(scanFrequency),
    hedge_thresholds_active: activeThresholds,
    meaningful_stake_rule: meaningfulStakeRule,
    decision,
    hedge_decision_reason: decisionReason,
    reason: reasonLines,
  };
}

function renderOddsValue(value) {
  const price = parseNumber(value);
  if (!Number.isFinite(price)) return null;
  return price > 0 ? `+${price}` : `${price}`;
}

function buildPositionUpdateText(item) {
  const lines = [
    'POSITION UPDATE',
    '',
    `Selection: ${item.selection || 'Unknown'}`,
    `Original Odds: ${item.original_odds || 'N/A'}`,
    `Current Odds: ${item.current_odds || 'N/A'}`,
    `CLV: ${Number.isFinite(item.clv_cents) ? `${item.clv_cents > 0 ? '+' : ''}${item.clv_cents} cents` : 'N/A'}`,
    '',
    `Original Edge: ${Number.isFinite(item.original_edge_pct) ? `${item.original_edge_pct}%` : 'N/A'}`,
    `Current Edge: ${Number.isFinite(item.current_edge_pct) ? `${item.current_edge_pct}%` : 'N/A'}`,
    '',
    `Market Type: ${item.market_type}`,
    '',
    `Decision: ${item.decision}`,
    '',
    'Reason:',
    ...item.reason.map((line) => `- ${line}`),
  ];
  return lines.join('\n');
}

function buildStakeBreakdown(row, bankroll) {
  const trueProb = asPercentProbability(row.post_conf_true_prob ?? row.pre_conf_true_prob);
  const impliedFair = asPercentProbability(row.devig_implied_prob);
  if (!Number.isFinite(bankroll) || !Number.isFinite(trueProb) || !Number.isFinite(parseNumber(row.odds_american))) {
    return null;
  }
  return computeKellyBreakdown({
    bankroll,
    american_odds: parseNumber(row.odds_american),
    true_prob: trueProb,
    implied_prob_fair: impliedFair,
    tier: row.bet_class === 'FUN_SGP' ? 'FUN' : tierFromThreshold(row.tier_threshold_pct),
    breaker_active: false,
  });
}

function buildOperatorLine(item) {
  const drift = item.execution.line_or_price_drift_label || 'N/A';
  return [
    `Play: ${item.selection}`,
    `Recommended: ${item.recommended_odds_american || 'N/A'} ${item.recommended_book || ''}`.trim(),
    `Current: ${item.execution.current_odds_american ?? 'N/A'} ${item.execution.current_book || ''}`.trim(),
    `Stake: $${Number(item.execution.final_executable_stake || 0).toFixed(2)}`,
    `Drift: ${drift}`,
    `Execution status: ${item.execution.execution_status}`,
    item.execution.rejection_reason ? `Reason: ${item.execution.rejection_reason}` : null,
  ].filter(Boolean).join('\n');
}

function buildMissedExecutionWindowRows(items) {
  return items
    .filter((item) => item.execution?.execution_window_classification === 'missed_execution_window')
    .map((item) => ({
      window_id: `missed-window::${item.run_id || 'unknown'}::${item.rec_id || 'unknown'}`,
      recorded_at_utc: new Date().toISOString(),
      rec_id: item.rec_id || null,
      run_id: item.run_id || null,
      execution_window_classification: 'missed_execution_window',
      sport: item.sport || null,
      league: item.league || null,
      market_type: item.market_type || null,
      event_id: item.event_id || null,
      event_label: item.event_label || null,
      selection: item.selection || null,
      sportsbook: item.recommended_book || null,
      recommended_odds_american: item.recommended_odds_american ?? null,
      current_odds_american: item.execution.current_odds_american ?? null,
      line_worse_amount: item.execution.line_worse_amount ?? 0,
      price_worse_cents: item.execution.price_worse_cents ?? null,
      rejection_reason: item.execution.rejection_reason || null,
      execution_status: item.execution.execution_status || null,
      original_edge_pct: item.original_edge_pct ?? null,
      tier_threshold_pct: item.tier_threshold_pct ?? null,
      original_kelly_stake: item.original_kelly_stake ?? null,
      current_execution_edge_estimate_pct: item.execution.current_execution_edge_estimate_pct ?? null,
      quote_timestamp_utc: item.execution.odds_last_update || null,
      source: 'execution_board',
    }));
}

function currentBankrollFromExecutionLog(executionLog) {
  const bankrolls = (executionLog || [])
    .map((row) => parseNumber(row.bankroll_after))
    .filter((value) => Number.isFinite(value));
  if (!bankrolls.length) return null;
  return bankrolls[bankrolls.length - 1];
}

async function buildHedgeDecisionRows({ executionLog, grading, policy }) {
  const currentBankroll = currentBankrollFromExecutionLog(executionLog);
  const settledExecutionIds = new Set(
    (grading || [])
      .map((row) => String(row.execution_id || row.execution_log_id || row.ref_id || '').trim())
      .filter(Boolean),
  );
  const openExecutions = (executionLog || []).filter((row) => !settledExecutionIds.has(String(row.execution_id || '').trim()));
  const items = [];

  for (const row of openExecutions) {
    const sportKey = policy.supported_sports[row.sport] || null;
    const bookKey = policy.supported_books[row.actual_sportsbook || row.recommended_sportsbook || row.sportsbook] || null;
    if (!sportKey || !bookKey || marketKind(row) === 'parlay' || marketKind(row) === 'unknown') {
      continue;
    }

    try {
      const marketKey = marketKind(row) === 'moneyline' ? 'h2h' : marketKind(row) === 'spread' ? 'spreads' : 'totals';
      const fetched = await fetchSportOdds({ sportKey, bookKey, marketKey, policy });
      const matchRow = {
        selection: row.selection,
        market_type: row.market_type || row.market,
        odds_american: row.actual_odds || row.recommended_odds_at_bet || row.recommended_odds,
      };
      const matched = findQuoteForRecommendation({ events: fetched.payload, row: matchRow, bookKey });
      if (matched.match_status !== 'matched') continue;

      const quote = matched.quote;
      const eventStartMs = safeDateMs(quote.commence_time);
      const minutesToStart = Number.isFinite(eventStartMs) ? round2((eventStartMs - Date.now()) / 60000) : null;
      const isLiveMarket = Number.isFinite(minutesToStart) ? minutesToStart < 0 : false;
      const currentImpliedProb = impliedProbabilityFromAmerican(quote.current_odds_american);
      const trueProb = asPercentProbability(row.true_probability_at_bet);
      const currentEdgePct = Number.isFinite(trueProb) && Number.isFinite(currentImpliedProb)
        ? round2((trueProb - currentImpliedProb) * 100)
        : null;

      const decision = classifyHedgeDecision({
        selection: row.selection,
        original_odds: row.actual_odds || row.recommended_odds_at_bet || row.recommended_odds,
        current_odds: quote.current_odds_american,
        original_edge_pct: row.edge_pct_at_bet,
        current_edge_pct: currentEdgePct,
        time_to_game_start: minutesToStart,
        stake_size: row.actual_stake,
        is_live_market: isLiveMarket,
        bankroll: row.bankroll_after || currentBankroll,
        policy,
      });

      items.push({
        execution_id: row.execution_id || null,
        run_id: row.run_id || null,
        rec_id: row.rec_id || null,
        event_label: row.event_label || row.event || quote.event_label || null,
        selection: row.selection || null,
        sportsbook: row.actual_sportsbook || row.recommended_sportsbook || null,
        execution_status: row.execution_approval_result || null,
        current_quote_source: fetched.source,
        current_book: quote.book_key || null,
        current_odds_american: renderOddsValue(quote.current_odds_american),
        current_edge_pct: currentEdgePct,
        event_start_time_utc: quote.commence_time || null,
        minutes_to_start: minutesToStart,
        ...decision,
        operator_output: buildPositionUpdateText({
          ...decision,
          selection: row.selection || null,
        }),
      });
    } catch {
      continue;
    }
  }

  const summary = {
    total_positions: items.length,
    let_ride_count: items.filter((item) => item.decision === 'LET RIDE').length,
    consider_hedge_count: items.filter((item) => item.decision === 'CONSIDER HEDGE').length,
    strong_hedge_count: items.filter((item) => item.decision === 'STRONG HEDGE').length,
  };

  return { items, summary };
}

export async function buildExecutionBoard({ canonicalState, runtimeStatus, decisions, grading, bankrollEntries }) {
  const policy = loadExecutionPolicy();
  const startingBankroll = (bankrollEntries || [])
    .filter((row) => row.entry_type === 'STARTING_BANKROLL')
    .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0);
  const contributions = (bankrollEntries || [])
    .filter((row) => row.entry_type === 'CONTRIBUTION')
    .reduce((sum, row) => sum + (parseNumber(row.amount) || 0), 0);
  const realizedProfit = (grading || [])
    .filter((row) => row.grading_type === 'BET')
    .reduce((sum, row) => sum + (parseNumber(row.profit_loss) || 0), 0);
  const currentBankroll = round2(startingBankroll + contributions + realizedProfit) || parseNumber(canonicalState?.current_status?.Bankroll) || 0;
  const latestCurrent = runtimeStatus?.latest_hunt_current || null;
  const blockingSyncGap = Boolean(runtimeStatus?.state_sync?.blocking_sync_gap);
  const runClassification = latestCurrent?.data_failure_codes?.includes('auth_failure')
    ? 'auth_failure'
    : latestCurrent?.data_failure_codes?.includes('runtime_gateway_failure')
      ? 'runtime_gateway_failure'
      : blockingSyncGap
        ? 'state_sync_failure'
        : latestCurrent?.message_type === 'BET'
          ? 'bet_ready'
          : latestCurrent?.message_type === 'SIT'
            ? 'true_no_edge_sit'
            : (canonicalState?.decision_payload_v1?.run_classification || 'unknown');
  const blockedRun = !['bet_ready', 'true_no_edge_sit'].includes(runClassification);
  const latestDate = runtimeStatus?.latest_hunt_current?.date_key || null;
  const settledSelections = new Set(
    (grading || [])
      .filter((row) => row.grading_type === 'BET')
      .map((row) => `${row.date}::${row.selection}`)
  );
  const liveCandidates = (decisions || []).filter((row) => (
    row.decision_kind === 'BET'
    && row.source === 'recommendation_log'
    && !settledSelections.has(`${row.target_date}::${row.selection}`)
    && (!latestDate || row.target_date === latestDate)
  ));

  const items = [];
  for (const row of liveCandidates) {
    const stakeBreakdown = buildStakeBreakdown(row, currentBankroll);
    const sportKey = policy.supported_sports[row.sport] || null;
    const bookKey = policy.supported_books[row.sportsbook] || null;
    const recAgeHours = (() => {
      const ms = safeDateMs(row.timestamp_ct);
      return Number.isFinite(ms) ? round2((Date.now() - ms) / 36e5) : null;
    })();
    const execution = {
      execution_status: 'REJECT_EXECUTION',
      execution_window_classification: 'not_missed_execution_window',
      rejection_reason: '',
      current_book: row.sportsbook,
      current_odds_american: null,
      current_odds_decimal: null,
      current_point: null,
      odds_last_update: null,
      odds_quote_source: null,
      line_worse_amount: 0,
      price_worse_cents: null,
      line_or_price_drift_label: 'N/A',
      bankroll_used: currentBankroll,
      sub_min_stake: !stakeBreakdown || stakeBreakdown.final_stake <= 0,
      stake_breakdown: stakeBreakdown,
      tolerance_check: null,
      current_execution_edge_estimate_pct: null,
    };

    if (blockedRun) {
      execution.rejection_reason = 'blocked_run';
    } else if (runClassification !== 'bet_ready') {
      execution.rejection_reason = 'degraded_data';
    } else if (Math.abs(parseNumber(canonicalState?.bankroll_summary?.bankroll_difference) || 0) > 5) {
      execution.rejection_reason = 'bankroll_untrusted';
    } else if (!stakeBreakdown || stakeBreakdown.final_stake <= 0) {
      execution.rejection_reason = 'sub_min_stake';
    } else if (recAgeHours !== null && recAgeHours > policy.stale_recommendation_hours) {
      execution.rejection_reason = 'stale_recommendation';
    } else if (!sportKey || !bookKey || marketKind(row) === 'parlay' || marketKind(row) === 'unknown') {
      execution.rejection_reason = 'odds_unavailable';
    } else {
      try {
        const marketKey = marketKind(row) === 'moneyline' ? 'h2h' : marketKind(row) === 'spread' ? 'spreads' : 'totals';
        const fetched = await fetchSportOdds({ sportKey, bookKey, marketKey, policy });
        const matched = findQuoteForRecommendation({ events: fetched.payload, row, bookKey });
        execution.odds_quote_source = fetched.source;
        if (matched.match_status !== 'matched') {
          execution.rejection_reason = 'odds_unavailable';
        } else {
          const quote = matched.quote;
          execution.current_odds_american = quote.current_odds_american;
          execution.current_odds_decimal = quote.current_odds_decimal;
          execution.current_point = quote.current_point;
          execution.odds_last_update = quote.odds_last_update;
          execution.line_worse_amount = quote.line_worse_amount;
          execution.price_worse_cents = quote.price_worse_cents;
          const kind = marketKind(row);
          const oddsAgeMinutes = (() => {
            const ms = safeDateMs(quote.odds_last_update);
            return Number.isFinite(ms) ? round2((Date.now() - ms) / 60000) : null;
          })();
          const lineTolerance = kind === 'spread'
            ? policy.line_tolerance_points.spread
            : kind === 'total'
              ? policy.line_tolerance_points.total
              : 0;
          const priceTolerance = kind === 'moneyline'
            ? policy.price_only_tolerance_cents.moneyline
            : kind === 'spread'
              ? policy.price_only_tolerance_cents.spread
              : policy.price_only_tolerance_cents.total;
          execution.tolerance_check = {
            odds_age_minutes: oddsAgeMinutes,
            max_odds_age_minutes: policy.odds_stale_minutes,
            line_tolerance_points: lineTolerance,
            price_tolerance_cents: priceTolerance,
          };
          execution.line_or_price_drift_label = kind === 'moneyline'
            ? `${quote.current_odds_american - parseNumber(row.odds_american)} cents`
            : Number.isFinite(quote.line_worse_amount) && quote.line_worse_amount > 0
              ? `${quote.line_worse_amount} pts / ${quote.price_worse_cents ?? 0} cents`
              : `${quote.price_worse_cents ?? 0} cents`;

          if (oddsAgeMinutes !== null && oddsAgeMinutes > policy.odds_stale_minutes) {
            execution.rejection_reason = 'odds_unavailable';
          } else if (quote.line_worse_amount > lineTolerance || (quote.price_worse_cents ?? 0) > priceTolerance) {
            execution.rejection_reason = 'line_moved_past_tolerance';
            execution.execution_window_classification = 'missed_execution_window';
            const recommendedTrueProb = asPercentProbability(row.post_conf_true_prob ?? row.pre_conf_true_prob);
            const currentImpliedProb = Number.isFinite(parseNumber(quote.current_odds_american))
              ? (parseNumber(quote.current_odds_american) > 0
                ? 100 / (parseNumber(quote.current_odds_american) + 100)
                : Math.abs(parseNumber(quote.current_odds_american)) / (Math.abs(parseNumber(quote.current_odds_american)) + 100))
              : null;
            execution.current_execution_edge_estimate_pct = Number.isFinite(recommendedTrueProb) && Number.isFinite(currentImpliedProb)
              ? round2((recommendedTrueProb - currentImpliedProb) * 100)
              : null;
          } else {
            execution.execution_status = 'APPROVED_TO_BET';
          }
        }
      } catch (error) {
        execution.rejection_reason = String(error.message || '').includes('missing_api_key')
          ? 'degraded_data'
          : 'odds_unavailable';
      }
    }

    items.push({
      rec_id: row.rec_id,
      run_id: row.run_id,
      timestamp_ct: row.timestamp_ct,
      target_date: row.target_date,
      sport: row.sport,
      league: row.league,
      event_id: row.event_id,
      event_label: row.event_label,
      market_type: row.market_type,
      selection: row.selection,
      recommended_book: row.sportsbook,
      recommended_odds_american: parseNumber(row.odds_american),
      recommended_odds_decimal: parseNumber(row.odds_decimal),
      true_probability_used: asPercentProbability(row.post_conf_true_prob ?? row.pre_conf_true_prob),
      implied_probability_used: asPercentProbability(row.devig_implied_prob),
      original_edge_pct: parseNumber(row.post_conf_edge_pct),
      tier_threshold_pct: parseNumber(row.tier_threshold_pct),
      original_kelly_stake: stakeBreakdown?.final_stake ?? parseNumber(row.kelly_stake) ?? null,
      bet_class: row.bet_class,
      execution,
      operator_output: buildOperatorLine({
        selection: row.selection,
        recommended_odds_american: row.odds_american,
        recommended_book: row.sportsbook,
        execution,
      }),
    });
  }

  const approved = items.filter((item) => item.execution.execution_status === 'APPROVED_TO_BET').length;
  const rejected = items.length - approved;
  const result = {
    generated_at_utc: new Date().toISOString(),
    run_classification: runClassification,
    policy,
    counts: {
      candidates: items.length,
      approved,
      rejected,
    },
    recommendations: items,
    operator_summary: items.map((item) => item.operator_output),
  };

  const missedWindows = buildMissedExecutionWindowRows(items);
  for (const row of missedWindows) {
    try {
      appendJsonl(CORE_PATHS.missedExecutionWindows, row, (entry) => String(entry.window_id || '').trim());
    } catch (error) {
      if (!String(error.message || '').startsWith('duplicate_row:')) throw error;
    }
  }

  const executionLog = readExecutionLog();
  const hedgeDecisions = await buildHedgeDecisionRows({
    executionLog,
    grading,
    policy,
  });
  result.hedge_decisions = hedgeDecisions.items;
  result.hedge_summary = hedgeDecisions.summary;
  result.hedge_operator_output = hedgeDecisions.items.map((item) => item.operator_output);
  result.hedge_scan_frequency_mode = policy.hedge_management?.scan_frequency_per_day || 3;
  result.hedge_thresholds_active = policy.hedge_management?.pregame_thresholds_by_scan_frequency?.[String(policy.hedge_management?.scan_frequency_per_day || 3)]
    || policy.hedge_management?.pregame_thresholds_by_scan_frequency?.[policy.hedge_management?.scan_frequency_per_day || 3]
    || null;
  result.meaningful_stake_rule = policy.hedge_management?.meaningful_stake_rule || null;
  fs.writeFileSync(EXECUTION_BOARD_PATH, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  return result;
}

export function readExecutionLog() {
  const metadataIndex = buildExecutionMetadataIndex();
  return readJsonl(EXECUTION_LOG_PATH)
    .flatMap((row) => Array.isArray(row) ? row : [row])
    .map((row) => enrichExecutionLogRow(row, { metadataIndex }));
}

export function appendExecutionLogRow(row) {
  const metadataIndex = buildExecutionMetadataIndex();
  const enriched = enrichExecutionLogRow(reclassifyExecutionRow(row), { metadataIndex });
  const overrideEvents = deriveOverrideEventsFromExecution(enriched);
  const missingJustification = overrideEvents.filter((event) => !String(event.freeform_justification || '').trim());
  if (missingJustification.length) {
    throw new Error(`missing_override_justification:${missingJustification.map((event) => event.override_type).join(',')}`);
  }
  appendJsonl(
    EXECUTION_LOG_PATH,
    enriched,
    (entry) => String((Array.isArray(entry) ? entry[0]?.execution_id : entry?.execution_id) || '')
  );
  appendOverrideEventsForExecution(enriched);
}

export function ingestStructuredExecutionPlacement(row) {
  const policy = loadExecutionPolicy();
  const preview = previewExecutionRecommendationMatch(row);
  const candidate = preview.candidate || null;
  const timestamp = row.bet_slip_timestamp || new Date().toISOString();
  const classification = preview.approved
    ? preview.execution_approval_result
    : 'OFF_PLAN_EXECUTION';
  const classificationReason = preview.approved
    ? preview.execution_approval_result_reason
    : (preview.match_status === 'ambiguous_match'
      ? 'ambiguous_recommendation_match'
      : 'no_matching_recommendation_found');

  const executionRow = {
    rec_id: candidate?.rec_id || null,
    run_id: candidate?.run_id || null,
    event: candidate?.event_label || row.event || null,
    event_label: candidate?.event_label || row.event || null,
    market: candidate?.market_type || row.market || 'Unknown',
    market_type: candidate?.market_type || row.market || null,
    selection: row.selection || candidate?.selection || null,
    recommendation_timestamp: candidate?.timestamp_ct || null,
    recommended_sportsbook: candidate?.sportsbook || null,
    recommended_odds: candidate?.odds_american || null,
    recommended_stake: candidate?.kelly_stake ?? null,
    actual_sportsbook: row.actual_sportsbook,
    actual_odds: row.actual_odds,
    actual_stake: row.actual_stake,
    promo_type: normalizePromoType(row.promo_type || row.promo),
    reward_type: normalizePromoType(row.promo_type || row.promo),
    bet_slip_timestamp: timestamp,
    execution_id: row.execution_id || `execution::telegram-operator::${Date.now()}`,
    logged_at_utc: row.logged_at_utc || new Date().toISOString(),
    source: row.source || 'telegram_operator',
    notes: Array.isArray(row.notes) ? row.notes : (row.notes ? [row.notes] : []),
    manual_override_flag: false,
    execution_approval_result: classification,
    execution_approval_result_reason: classificationReason,
    stale_execution_match: Boolean(preview.stale_execution),
    matched_from_run_scope: preview.match_scope || null,
  };

  if (classification === 'OFF_PLAN_EXECUTION') {
    executionRow.override_reason = classificationReason;
    executionRow.notes = Array.from(new Set([
      ...(executionRow.notes || []),
      'logged_real_bet_without_recommendation_match',
    ]));
  }

  const recommendedStake = parseNumber(candidate?.kelly_stake);
  const actualStake = parseNumber(executionRow.actual_stake);
  const recommendedOdds = parseNumber(candidate?.odds_american);
  const actualOdds = parseNumber(executionRow.actual_odds);
  const stakeChanged = Number.isFinite(recommendedStake) && Number.isFinite(actualStake) && Math.abs(recommendedStake - actualStake) > 0.009;
  const priceChanged = Number.isFinite(recommendedOdds) && Number.isFinite(actualOdds) && recommendedOdds !== actualOdds;
  if (!executionRow.override_reason && (classification !== 'APPROVED_EXECUTION' || stakeChanged || priceChanged)) {
    executionRow.override_reason = classificationReason || 'execution_differs_from_recommendation';
  }
  if (stakeChanged || priceChanged) {
    executionRow.notes = Array.from(new Set([
      ...(executionRow.notes || []),
      'logged_real_bet_with_execution_variance',
    ]));
  }

  Object.assign(executionRow, buildPromoEvaluationFields(executionRow, candidate, policy));

  const duplicate = findLikelyDuplicateExecution(executionRow);
  if (duplicate) {
    return {
      ok: false,
      duplicate: true,
      reason: 'duplicate_submission',
      preview,
      row: duplicate,
    };
  }

  appendExecutionLogRow(executionRow);
  const appended = readExecutionLog().find((entry) => entry.execution_id === executionRow.execution_id) || null;
  return {
    ok: true,
    reason: null,
    preview,
    row: appended || executionRow,
  };
}

function normalizeSettlementResult(value) {
  const normalized = normalizeText(value).replace(/\s+/g, '_');
  if (normalized === 'win' || normalized === 'won') return 'WIN';
  if (normalized === 'loss' || normalized === 'lost') return 'LOSS';
  if (normalized === 'push') return 'PUSH';
  return null;
}

function settlementStatusFromResult(result) {
  const normalized = normalizeSettlementResult(result);
  return normalized ? normalized.toLowerCase() : null;
}

function settlementPayoutFromOdds(result, stake, americanOdds) {
  const normalized = normalizeSettlementResult(result);
  const stakeNum = parseNumber(stake);
  const oddsNum = parseNumber(americanOdds);
  if (!Number.isFinite(stakeNum)) return null;
  if (normalized === 'LOSS') return 0;
  if (normalized === 'PUSH') return round2(stakeNum);
  if (normalized !== 'WIN' || !Number.isFinite(oddsNum)) return null;
  if (oddsNum > 0) {
    return round2(stakeNum + (stakeNum * oddsNum / 100));
  }
  return round2(stakeNum + (stakeNum * 100 / Math.abs(oddsNum)));
}

function settlementProfitLoss(result, stake, americanOdds) {
  const payout = settlementPayoutFromOdds(result, stake, americanOdds);
  const stakeNum = parseNumber(stake);
  if (!Number.isFinite(stakeNum)) return null;
  const normalized = normalizeSettlementResult(result);
  if (normalized === 'LOSS') return round2(-stakeNum);
  if (normalized === 'PUSH') return 0;
  if (!Number.isFinite(payout)) return null;
  return round2(payout - stakeNum);
}

function sameExecutionShape(a, b) {
  return normalizeText(a.selection) === normalizeText(b.selection)
    && normalizeText(a.actual_sportsbook) === normalizeText(b.actual_sportsbook)
    && parseNumber(a.actual_odds) === parseNumber(b.actual_odds)
    && Math.abs((parseNumber(a.actual_stake) || 0) - (parseNumber(b.actual_stake) || 0)) <= 0.009;
}

function findExecutionForSettlement(row) {
  const executions = readExecutionLog().slice().reverse();
  const candidates = executions.filter((entry) => sameExecutionShape(entry, row));
  if (!candidates.length) {
    return { ok: false, reason: 'no_matching_execution_found', row: null };
  }
  const exactSourceCandidates = candidates.filter((entry) => normalizeText(entry.source) === 'telegram_operator');
  const preferred = exactSourceCandidates.length ? exactSourceCandidates : candidates;
  if (preferred.length > 1) {
    return { ok: false, reason: 'ambiguous_execution_match', row: null };
  }
  return { ok: true, reason: null, row: preferred[0] };
}

function findExistingSettlement(matchExecution) {
  const gradingRows = readJsonl(CORE_PATHS.gradingLedger).slice().reverse();
  return gradingRows.find((row) => {
    const executionId = normalizeText(row.execution_log_id || row.execution_id || row.ref_id);
    const target = normalizeText(matchExecution.execution_id);
    const result = normalizeSettlementResult(row.result || row.settlement_status);
    return executionId && target && executionId === target && Boolean(result);
  }) || null;
}

function appendStructuredGradingRow(row) {
  const enriched = enrichGradingRowWithClv(row);
  const existingRows = readJsonl(CORE_PATHS.gradingLedger);
  const reconciled = reconcileGradingBankrollAnnotations([...existingRows, enriched], readJsonl(CORE_PATHS.bankrollLedger));
  const annotationById = new Map(reconciled.rows.map((entry) => [entry.grading_id, entry]));
  const finalRow = isBankrollRelevantGrade(enriched) ? (annotationById.get(enriched.grading_id) || enriched) : enriched;
  appendJsonl(CORE_PATHS.gradingLedger, finalRow, (entry) => String(entry.grading_id || ''));
  return finalRow;
}

export function ingestAutomaticExecutionSettlementForExecution(executionRow, result, options = {}) {
  const normalizedResult = normalizeSettlementResult(result);
  if (!normalizedResult) {
    return { ok: false, reason: 'invalid_settlement_result', row: null, execution_row: executionRow || null };
  }
  if (!executionRow?.execution_id) {
    return { ok: false, reason: 'missing_execution_id', row: null, execution_row: executionRow || null };
  }

  const existingSettlement = findExistingSettlement(executionRow);
  if (existingSettlement) {
    const existingResult = normalizeSettlementResult(existingSettlement.result || existingSettlement.settlement_status);
    if (existingResult === normalizedResult) {
      return {
        ok: false,
        duplicate: true,
        reason: 'duplicate_settlement_submission',
        row: existingSettlement,
        execution_row: executionRow,
      };
    }
    return {
      ok: false,
      reason: 'existing_settlement_conflict',
      row: existingSettlement,
      execution_row: executionRow,
    };
  }

  const timestamp = options.settlement_timestamp || options.logged_at_utc || new Date().toISOString();
  const gradingRow = {
    grading_id: options.grading_id || `reconciliation::${executionRow.execution_id}::${normalizeText(normalizedResult)}::${Date.now()}`,
    grading_type: 'RECONCILIATION',
    ref_id: executionRow.execution_id,
    execution_log_id: executionRow.execution_id,
    execution_id: executionRow.execution_id,
    rec_id: executionRow.rec_id || null,
    run_id: executionRow.run_id || null,
    date: toCtIsoDate(timestamp),
    timestamp_ct: timestamp,
    selection: executionRow.selection,
    sportsbook: executionRow.actual_sportsbook,
    actual_odds: executionRow.actual_odds,
    actual_stake: parseNumber(executionRow.actual_stake),
    stake: parseNumber(executionRow.actual_stake),
    settlement_status: settlementStatusFromResult(normalizedResult),
    settlement_payout: settlementPayoutFromOdds(normalizedResult, executionRow.actual_stake, executionRow.actual_odds),
    settlement_source: options.settlement_source || options.source || 'automatic_settlement_job',
    result: normalizedResult,
    profit_loss: settlementProfitLoss(normalizedResult, executionRow.actual_stake, executionRow.actual_odds),
    source: options.source || 'automatic_settlement_job',
    notes: Array.isArray(options.notes) ? options.notes : (options.notes ? [options.notes] : []),
    auto_settlement: true,
    auto_settlement_reason: options.auto_settlement_reason || null,
  };

  const appended = appendStructuredGradingRow(gradingRow);
  return {
    ok: true,
    reason: null,
    row: appended,
    execution_row: executionRow,
  };
}

export function ingestStructuredExecutionSettlement(row) {
  const normalizedResult = normalizeSettlementResult(row.result || row.settlement_result);
  if (!normalizedResult) {
    return { ok: false, reason: 'invalid_settlement_result', row: null };
  }

  const matched = findExecutionForSettlement(row);
  if (!matched.ok) {
    return { ok: false, reason: matched.reason, row: null };
  }

  const executionRow = matched.row;
  const existingSettlement = findExistingSettlement(executionRow);
  if (existingSettlement) {
    const existingResult = normalizeSettlementResult(existingSettlement.result || existingSettlement.settlement_status);
    if (existingResult === normalizedResult) {
      return {
        ok: false,
        duplicate: true,
        reason: 'duplicate_settlement_submission',
        row: existingSettlement,
        execution_row: executionRow,
      };
    }
    return {
      ok: false,
      reason: 'existing_settlement_conflict',
      row: existingSettlement,
      execution_row: executionRow,
    };
  }

  const timestamp = row.settlement_timestamp || row.logged_at_utc || new Date().toISOString();
  const payout = settlementPayoutFromOdds(normalizedResult, executionRow.actual_stake, executionRow.actual_odds);
  const gradingRow = {
    grading_id: row.grading_id || `reconciliation::${executionRow.execution_id}::${normalizeText(normalizedResult)}::${Date.now()}`,
    grading_type: 'RECONCILIATION',
    ref_id: executionRow.execution_id,
    execution_log_id: executionRow.execution_id,
    execution_id: executionRow.execution_id,
    rec_id: executionRow.rec_id || null,
    run_id: executionRow.run_id || null,
    date: toCtIsoDate(timestamp),
    timestamp_ct: timestamp,
    selection: executionRow.selection,
    sportsbook: executionRow.actual_sportsbook,
    actual_odds: executionRow.actual_odds,
    actual_stake: parseNumber(executionRow.actual_stake),
    stake: parseNumber(executionRow.actual_stake),
    settlement_status: settlementStatusFromResult(normalizedResult),
    settlement_payout: payout,
    settlement_source: row.source || 'telegram_operator',
    result: normalizedResult,
    profit_loss: settlementProfitLoss(normalizedResult, executionRow.actual_stake, executionRow.actual_odds),
    source: row.source || 'telegram_operator',
    notes: Array.isArray(row.notes) ? row.notes : [],
  };

  const appended = appendStructuredGradingRow(gradingRow);
  return {
    ok: true,
    reason: null,
    row: appended,
    execution_row: executionRow,
  };
}

export function backfillExecutionLogMetadata() {
  const metadataIndex = buildExecutionMetadataIndex();
  const recommendationUniverse = loadRecommendationUniverse();
  const rows = readJsonl(EXECUTION_LOG_PATH).flatMap((row) => Array.isArray(row) ? row : [row]);
  const enrichedRows = rows.map((row) => enrichExecutionLogRow(reclassifyExecutionRow(row, recommendationUniverse), { metadataIndex }));
  writeJsonl(EXECUTION_LOG_PATH, enrichedRows);
  const unknownCount = enrichedRows.filter((row) => String(row.sport || '') === 'UNKNOWN').length;
  return {
    total_rows: enrichedRows.length,
    unknown_count: unknownCount,
    approved_execution_count: enrichedRows.filter((row) => normalizeText(row.execution_approval_result) === 'approved_execution').length,
    manual_override_count: enrichedRows.filter((row) => Boolean(row.manual_override_flag)).length,
  };
}

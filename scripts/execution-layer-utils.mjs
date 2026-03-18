import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { CORE_PATHS, appendJsonl, parseNumber, readJson, readJsonl, round2, writeJsonl } from './core-ledger-utils.mjs';
import { computeKellyBreakdown } from './tierededge-kelly-cli.mjs';
import { appendOverrideEventsForExecution, deriveOverrideEventsFromExecution } from './behavioral-accountability-utils.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
export const EXECUTION_POLICY_PATH = path.join(ROOT, 'config', 'execution-policy.json');
export const EXECUTION_BOARD_PATH = path.join(ROOT, 'data', 'execution-board.json');
export const EXECUTION_LOG_PATH = path.join(ROOT, 'data', 'execution-log.jsonl');
export const EXECUTION_QUOTE_CACHE_PATH = path.join(ROOT, 'data', 'execution-quote-cache.json');

const DEFAULT_POLICY = {
  stale_recommendation_hours: 8,
  odds_stale_minutes: 10,
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
    execution_approval_result: approved ? 'APPROVED_EXECUTION' : (match.match_status === 'ambiguous_match' ? 'AMBIGUOUS_MATCH' : (row.execution_approval_result || 'REJECT_EXECUTION')),
    manual_override_flag: approved ? false : Boolean(row.manual_override_flag),
    execution_approval_result_reason: approved ? 'matched_originating_recommendation' : row.execution_approval_result_reason,
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
    price_only_tolerance_cents: {
      ...DEFAULT_POLICY.price_only_tolerance_cents,
      ...(parsed?.price_only_tolerance_cents || {}),
    },
    line_tolerance_points: {
      ...DEFAULT_POLICY.line_tolerance_points,
      ...(parsed?.line_tolerance_points || {}),
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

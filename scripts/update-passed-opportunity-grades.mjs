#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { CORE_PATHS, readJsonl, writeJsonl } from './core-ledger-utils.mjs';

const CACHE_FILE = '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json';
const ODDS_CONFIG = '/Users/jaredbuckman/.openclaw/workspace/memory/odds-api-config.md';
const ROOT_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SCORES_CACHE_FILE = path.join(ROOT_DIR, 'data', 'odds-api-scores-cache.json');
const SCORES_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

const SPORT_KEY_MAP = {
  NBA: 'basketball_nba',
  CBB: 'basketball_ncaab',
  NCAAB: 'basketball_ncaab',
  NHL: 'icehockey_nhl',
  MLB: 'baseball_mlb',
  NFL: 'americanfootball_nfl',
  CFB: 'americanfootball_ncaaf',
  UFC: 'mma_mixed_martial_arts',
  MMA: 'mma_mixed_martial_arts',
  EPL: 'soccer_epl',
};

const TEAM_ALIAS_BY_SPORT = {
  NBA: {
    ATL: ['atlanta hawks', 'hawks'],
    BOS: ['boston celtics', 'celtics'],
    BKN: ['brooklyn nets', 'nets'],
    CHA: ['charlotte hornets', 'hornets'],
    CHI: ['chicago bulls', 'bulls'],
    CLE: ['cleveland cavaliers', 'cavaliers', 'cavs'],
    DAL: ['dallas mavericks', 'mavericks', 'mavs'],
    DEN: ['denver nuggets', 'nuggets'],
    DET: ['detroit pistons', 'pistons'],
    GSW: ['golden state warriors', 'warriors'],
    HOU: ['houston rockets', 'rockets'],
    IND: ['indiana pacers', 'pacers'],
    LAC: ['los angeles clippers', 'clippers'],
    LAL: ['los angeles lakers', 'lakers'],
    MEM: ['memphis grizzlies', 'grizzlies'],
    MIA: ['miami heat', 'heat'],
    MIL: ['milwaukee bucks', 'bucks'],
    MIN: ['minnesota timberwolves', 'timberwolves', 'wolves'],
    NOP: ['new orleans pelicans', 'pelicans', 'pels'],
    NYK: ['new york knicks', 'knicks'],
    OKC: ['oklahoma city thunder', 'thunder'],
    ORL: ['orlando magic', 'magic'],
    PHI: ['philadelphia 76ers', '76ers', 'sixers'],
    PHX: ['phoenix suns', 'suns'],
    POR: ['portland trail blazers', 'trail blazers', 'blazers'],
    SAC: ['sacramento kings', 'kings'],
    SAS: ['san antonio spurs', 'spurs'],
    TOR: ['toronto raptors', 'raptors'],
    UTA: ['utah jazz', 'jazz'],
    WAS: ['washington wizards', 'wizards'],
  },
  NHL: {
    ANA: ['anaheim ducks', 'ducks'],
    BOS: ['boston bruins', 'bruins'],
    BUF: ['buffalo sabres', 'sabres'],
    CAR: ['carolina hurricanes', 'hurricanes', 'canes'],
    CBJ: ['columbus blue jackets', 'blue jackets', 'jackets'],
    CGY: ['calgary flames', 'flames'],
    CHI: ['chicago blackhawks', 'blackhawks', 'hawks'],
    COL: ['colorado avalanche', 'avalanche', 'avs'],
    DAL: ['dallas stars', 'stars'],
    DET: ['detroit red wings', 'red wings'],
    EDM: ['edmonton oilers', 'oilers'],
    FLA: ['florida panthers', 'panthers'],
    LAK: ['los angeles kings', 'kings'],
    MIN: ['minnesota wild', 'wild'],
    MTL: ['montreal canadiens', 'canadiens', 'habs'],
    NSH: ['nashville predators', 'predators', 'preds'],
    NJD: ['new jersey devils', 'devils'],
    NYI: ['new york islanders', 'islanders'],
    NYR: ['new york rangers', 'rangers'],
    OTT: ['ottawa senators', 'senators', 'sens'],
    PHI: ['philadelphia flyers', 'flyers'],
    PIT: ['pittsburgh penguins', 'penguins', 'pens'],
    SEA: ['seattle kraken', 'kraken'],
    SJS: ['san jose sharks', 'sharks'],
    STL: ['st. louis blues', 'st louis blues', 'blues'],
    TBL: ['tampa bay lightning', 'lightning'],
    TOR: ['toronto maple leafs', 'maple leafs', 'leafs'],
    VAN: ['vancouver canucks', 'canucks'],
    VGK: ['vegas golden knights', 'golden knights', 'knights'],
    WPG: ['winnipeg jets', 'jets'],
    WSH: ['washington capitals', 'capitals', 'caps'],
  },
};

const MONTH_INDEX = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

function parseTable(markdown) {
  const lines = markdown
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|'));
  if (lines.length < 2) return [];

  const headers = lines[0].split('|').map((s) => s.trim()).filter(Boolean);
  const rows = [];
  for (let i = 2; i < lines.length; i += 1) {
    const parts = lines[i].split('|').map((s) => s.trim()).filter(Boolean);
    if (parts.length < headers.length) continue;
    const normalizedParts = parts.slice(0, headers.length);
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = normalizedParts[j];
    rows.push(row);
  }
  return rows;
}

function parseOddsApiKey(text) {
  const m = text.match(/^API_KEY=(.+)$/m);
  return m ? m[1].trim() : null;
}

function parseOddsApiKeyEnvName(text) {
  const explicit = text.match(/^API_KEY_ENV=(.+)$/m)?.[1]?.trim();
  if (explicit) return explicit;
  const yamlish = text.match(/^\s*api_key_env:\s*(.+)$/mi)?.[1]?.trim();
  return yamlish || null;
}

function resolveOddsApiKey(text) {
  const envName = parseOddsApiKeyEnvName(text) || 'ODDS_API_KEY';
  const envValue = process.env[envName];
  if (typeof envValue === 'string' && envValue.trim()) {
    return envValue.trim();
  }
  return parseOddsApiKey(text);
}

function normalizeName(name) {
  return String(name || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function normalizeResult(value) {
  const normalized = normalizeName(value);
  if (!normalized || normalized === '-' || normalized === 'pending' || normalized === 'ungraded') return normalized || null;
  if (normalized.includes('win')) return 'win';
  if (normalized.includes('loss') || normalized.includes('lost')) return 'loss';
  if (normalized.includes('push')) return 'push';
  return normalized;
}

function parsePercent(value) {
  if (value === null || value === undefined) return null;
  const stripped = String(value).replace(/[%+,]/g, '').trim();
  const num = Number(stripped);
  return Number.isFinite(num) ? num : null;
}

function parseMoney(value) {
  if (value === null || value === undefined) return null;
  const stripped = String(value).replace(/[$,]/g, '').trim();
  const num = Number(stripped);
  return Number.isFinite(num) ? num : null;
}

function round4(value) {
  return Number.isFinite(value) ? Math.round(value * 10000) / 10000 : null;
}

function selectionCandidates(sport, selection) {
  const raw = String(selection || '').trim();
  if (!raw) return [];
  const norm = normalizeName(raw);
  const upper = raw.toUpperCase();
  const sportAliases = TEAM_ALIAS_BY_SPORT[String(sport || '').trim().toUpperCase()] || {};
  const aliases = sportAliases[upper] || [];
  return Array.from(new Set([norm, ...aliases.map(normalizeName)]));
}

function toDecimalOdds(us) {
  const n = Number(us);
  if (!Number.isFinite(n)) return null;
  if (n > 0) return 1 + (n / 100);
  return 1 + (100 / Math.abs(n));
}

function asNumber(v) {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseIsoDatePrefix(text) {
  const m = String(text || '').match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function toEpochMs(input) {
  const t = Date.parse(String(input || ''));
  return Number.isFinite(t) ? t : null;
}

function pickBestEvent(events, recTimestampCt) {
  if (!Array.isArray(events) || events.length === 0) return null;
  const recMs = toEpochMs(parseIsoDatePrefix(recTimestampCt));
  const scored = events.map((event) => {
    const evtMs = toEpochMs(event.commence_time || event.start_time);
    const distance = recMs !== null && evtMs !== null ? Math.abs(evtMs - recMs) : Number.MAX_SAFE_INTEGER;
    return {
      event,
      completedRank: event.completed === true ? 0 : 1,
      distance,
    };
  });
  scored.sort((a, b) => {
    if (a.completedRank !== b.completedRank) return a.completedRank - b.completedRank;
    return a.distance - b.distance;
  });
  return scored[0].event;
}

function parseScores(event) {
  if (Array.isArray(event.scores) && event.scores.length >= 2) {
    const a = event.scores[0];
    const b = event.scores[1];
    return [
      { name: a.name || a.team || event.home_team, score: asNumber(a.score ?? a.points) },
      { name: b.name || b.team || event.away_team, score: asNumber(b.score ?? b.points) },
    ];
  }
  if (event.home_score !== undefined && event.away_score !== undefined) {
    return [
      { name: event.home_team, score: asNumber(event.home_score) },
      { name: event.away_team, score: asNumber(event.away_score) },
    ];
  }
  return null;
}

function getCounterfactualResult(event, selection) {
  const scores = parseScores(event);
  if (!scores) return null;

  const normalizedSelection = normalizeName(selection);
  const side = scores.find((s) => normalizeName(s.name).includes(normalizedSelection) || normalizedSelection.includes(normalizeName(s.name)));
  if (!side) return null;
  const opponent = scores.find((s) => s !== side);
  if (!opponent) return null;
  if (side.score === null || opponent.score === null) return null;

  if (side.score > opponent.score) return 'win';
  if (side.score < opponent.score) return 'loss';
  return 'push';
}

function toUnitPl(result, usOdds, decOdds) {
  const normalized = normalizeResult(result);
  const decimal = decOdds ?? toDecimalOdds(usOdds);
  if (normalized === 'win' && decimal) return round4(decimal - 1);
  if (normalized === 'loss') return -1;
  if (normalized === 'push') return 0;
  return null;
}

function buildSitResultRecIdMap(rows) {
  const map = new Map();
  for (const row of rows) {
    if (String(row.decision || '').trim().toUpperCase() !== 'SIT-RESULT') continue;
    const baseRecId = String(row.rec_id || '').replace(/-GRADE$/, '').trim();
    if (!baseRecId) continue;
    map.set(baseRecId, {
      outcome_if_bet: normalizeResult(row.rejection_reason),
      event_label: null,
      settlement_date: parseIsoDatePrefix(row.timestamp_ct),
      grade_source: 'recommendation_log_sit_result',
    });
  }
  return map;
}

async function fetchScoresBySport(sportKey, apiKey) {
  const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/scores?daysFrom=3&apiKey=${encodeURIComponent(apiKey)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`scores_fetch_failed_${sportKey}_${res.status}`);
  return res.json();
}

async function readJson(file) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'));
  } catch {
    return null;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function isFreshCache(entry) {
  const fetchedAt = Date.parse(String(entry?.fetched_at || ''));
  if (!Number.isFinite(fetchedAt)) return false;
  return (Date.now() - fetchedAt) <= SCORES_CACHE_TTL_MS;
}

async function loadScoresCache() {
  const cache = (await readJson(SCORES_CACHE_FILE)) || { updated_at: null, sports: {} };
  return {
    updated_at: cache.updated_at || null,
    sports: cache.sports || {},
  };
}

async function persistScoresCache(cache) {
  await fs.writeFile(SCORES_CACHE_FILE, `${JSON.stringify({
    updated_at: nowIso(),
    sports: cache.sports || {},
  }, null, 2)}\n`, 'utf8');
}

function buildFailureEntry(existing, row, failureReason) {
  return {
    ...(existing || {}),
    rec_id: row.rec_id,
    outcome_if_bet: 'ungraded',
    counterfactual_result: 'ungraded',
    counterfactual_pl: null,
    counterfactual_pl_unit: null,
    hypothetical_units: null,
    event_id: existing?.event_id || null,
    event_label: existing?.event_label || null,
    settlement_date: existing?.settlement_date || null,
    graded_at: new Date().toISOString(),
    grade_source: existing?.grade_source || 'deterministic_backfill_failed',
    failure_reason: failureReason,
  };
}

function buildSuccessEntry(existing, row, source, result, extras = {}) {
  const unitPl = toUnitPl(result, row.recommended_odds_us, asNumber(row.recommended_odds_dec));
  return {
    ...(existing || {}),
    rec_id: row.rec_id,
    outcome_if_bet: result,
    counterfactual_result: result,
    counterfactual_pl: unitPl,
    counterfactual_pl_unit: unitPl,
    hypothetical_units: unitPl,
    event_id: extras.event_id ?? existing?.event_id ?? null,
    event_label: extras.event_label ?? existing?.event_label ?? null,
    settlement_date: extras.settlement_date ?? existing?.settlement_date ?? parseIsoDatePrefix(row.timestamp_ct),
    graded_at: new Date().toISOString(),
    grade_source: source,
    failure_reason: null,
  };
}

async function main() {
  const configRaw = await fs.readFile(ODDS_CONFIG, 'utf8');
  const apiKey = resolveOddsApiKey(configRaw);

  const decisionRows = readJsonl(CORE_PATHS.decisionLedger);
  const passRows = decisionRows
    .filter((row) => row.decision_kind === 'PASS')
    .map((row) => ({
      rec_id: row.rec_id,
      timestamp_ct: row.timestamp_ct,
      sport: row.sport,
      market: row.market_type,
      selection: row.selection,
      recommended_odds_us: row.odds_american,
      recommended_odds_dec: row.odds_decimal,
    }));
  const validPassRecIds = new Set(passRows.map((row) => row.rec_id).filter(Boolean));
  const cache = (await readJson(CACHE_FILE)) || { updated_at: null, entries: {} };
  const entries = cache.entries || {};
  const scoresCache = await loadScoresCache();

  const sitResultRecIdMap = new Map();

  const sportKeys = [...new Set(
    passRows
      .map((r) => SPORT_KEY_MAP[String(r.sport || '').trim().toUpperCase()])
      .filter(Boolean)
  )];

  const stats = {
    total_pass_rows: passRows.length,
    preserved_existing: 0,
    backfilled_local: 0,
    backfilled_api: 0,
    api_calls_made: 0,
    scores_cache_hits: 0,
    failed: 0,
    failure_reasons: {},
  };

  const scoresBySport = {};
  if (apiKey) {
    for (const sportKey of sportKeys) {
      const cached = scoresCache.sports?.[sportKey];
      if (cached?.events && isFreshCache(cached)) {
        scoresBySport[sportKey] = cached.events;
        stats.scores_cache_hits += 1;
        continue;
      }
      try {
        const events = await fetchScoresBySport(sportKey, apiKey);
        scoresBySport[sportKey] = events;
        scoresCache.sports[sportKey] = {
          fetched_at: nowIso(),
          events,
        };
        stats.api_calls_made += 1;
      } catch (error) {
        console.log(`WARN: ${String(error.message || error)}`);
      }
    }
  } else {
    console.log('WARN: No ODDS API key configured; using deterministic local backfill only.');
  }

  for (const row of passRows) {
    const recId = row.rec_id;
    if (!recId) continue;

    if (entries[recId]?.counterfactual_result && entries[recId]?.counterfactual_result !== 'ungraded') {
      stats.preserved_existing += 1;
      continue;
    }

    if (!parseIsoDatePrefix(row.timestamp_ct)) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'missing_event_identity');
      stats.failed += 1;
      stats.failure_reasons.missing_event_identity = (stats.failure_reasons.missing_event_identity || 0) + 1;
      continue;
    }
    if (!String(row.market || '').trim()) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'missing_market');
      stats.failed += 1;
      stats.failure_reasons.missing_market = (stats.failure_reasons.missing_market || 0) + 1;
      continue;
    }
    if (!String(row.selection || '').trim()) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'missing_selection');
      stats.failed += 1;
      stats.failure_reasons.missing_selection = (stats.failure_reasons.missing_selection || 0) + 1;
      continue;
    }
    if (!String(row.recommended_odds_us || '').trim() && !String(row.recommended_odds_dec || '').trim()) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'missing_line_or_odds');
      stats.failed += 1;
      stats.failure_reasons.missing_line_or_odds = (stats.failure_reasons.missing_line_or_odds || 0) + 1;
      continue;
    }

    const recIdGrade = sitResultRecIdMap.get(recId);
    if (recIdGrade?.outcome_if_bet && recIdGrade.outcome_if_bet !== 'ungraded') {
      entries[recId] = buildSuccessEntry(entries[recId], row, recIdGrade.grade_source, recIdGrade.outcome_if_bet, recIdGrade);
      stats.backfilled_local += 1;
      continue;
    }

    const sportKey = SPORT_KEY_MAP[String(row.sport || '').trim().toUpperCase()];
    const events = scoresBySport[sportKey];
    if (!sportKey || !events || !Array.isArray(events)) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'no_historical_result_found');
      stats.failed += 1;
      stats.failure_reasons.no_historical_result_found = (stats.failure_reasons.no_historical_result_found || 0) + 1;
      continue;
    }

    const selection = row.selection || '';
    const candidates = selectionCandidates(row.sport, selection);
    if (candidates.length === 0) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'missing_selection');
      stats.failed += 1;
      stats.failure_reasons.missing_selection = (stats.failure_reasons.missing_selection || 0) + 1;
      continue;
    }

    const matched = events.filter((event) => {
      const names = [event.home_team, event.away_team].map(normalizeName);
      return candidates.some((sel) => names.some((n) => n.includes(sel) || sel.includes(n)));
    });
    if (matched.length === 0) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'missing_event_identity');
      stats.failed += 1;
      stats.failure_reasons.missing_event_identity = (stats.failure_reasons.missing_event_identity || 0) + 1;
      continue;
    }

    const event = pickBestEvent(matched, row.timestamp_ct);
    if (!event) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'ambiguous_match');
      stats.failed += 1;
      stats.failure_reasons.ambiguous_match = (stats.failure_reasons.ambiguous_match || 0) + 1;
      continue;
    }
    if (event.completed !== true) {
      entries[recId] = buildFailureEntry(entries[recId], row, 'no_historical_result_found');
      stats.failed += 1;
      stats.failure_reasons.no_historical_result_found = (stats.failure_reasons.no_historical_result_found || 0) + 1;
      continue;
    }

    let result = 'ungraded';
    for (const sel of candidates) {
      const candidateResult = getCounterfactualResult(event, sel);
      if (candidateResult) {
        result = candidateResult;
        break;
      }
    }
    if (result === 'ungraded') {
      entries[recId] = buildFailureEntry(entries[recId], row, matched.length > 1 ? 'ambiguous_match' : 'legacy_incomplete_row');
      stats.failed += 1;
      const reason = matched.length > 1 ? 'ambiguous_match' : 'legacy_incomplete_row';
      stats.failure_reasons[reason] = (stats.failure_reasons[reason] || 0) + 1;
      continue;
    }

    entries[recId] = buildSuccessEntry(entries[recId], row, 'odds_api_scores', result, {
      event_id: event.id || null,
      event_label: `${event.away_team} @ ${event.home_team}`,
      settlement_date: parseIsoDatePrefix(event.commence_time || row.timestamp_ct),
    });
    stats.backfilled_api += 1;
  }

  const out = {
    updated_at: new Date().toISOString(),
    stats,
    entries,
  };
  const existingGradingRows = readJsonl(CORE_PATHS.gradingLedger);
  writeJsonl(
    CORE_PATHS.gradingLedger,
    [...existingGradingRows.filter((row) => row.grading_type !== 'PASS'), ...Object.entries(entries).filter(([recId]) => validPassRecIds.has(recId)).map(([recId, entry]) => ({
      grading_id: `pass::${recId}`,
      grading_type: 'PASS',
      ref_id: `decision::${recId}`,
      date: entry.settlement_date || null,
      timestamp_ct: entry.graded_at || null,
      selection: entry.event_label || recId,
      result: entry.counterfactual_result || entry.outcome_if_bet || 'ungraded',
      profit_loss: entry.counterfactual_pl ?? null,
      stake: null,
      bankroll_after: null,
      clv: null,
      bet_class: 'EDGE_BET',
      source: entry.grade_source || 'passed_opportunity_grades',
      failure_reason: entry.failure_reason || null,
    }))].sort((a, b) => String(a.date || a.timestamp_ct || '').localeCompare(String(b.date || b.timestamp_ct || '')) || String(a.grading_id).localeCompare(String(b.grading_id)))
  );
  await persistScoresCache(scoresCache);
  await fs.writeFile(CACHE_FILE, `${JSON.stringify(out, null, 2)}\n`, 'utf8');
  console.log(`Updated passed-opportunity grades cache: ${CACHE_FILE}`);
  console.log(JSON.stringify(stats, null, 2));
}

main().catch((error) => {
  console.error(`Passed-opportunity grading failed: ${error.message}`);
  process.exit(1);
});

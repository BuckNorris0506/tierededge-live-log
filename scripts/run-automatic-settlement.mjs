#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { CORE_PATHS, parseNumber, readJson, readJsonl, round2 } from './core-ledger-utils.mjs';
import { ingestAutomaticExecutionSettlementForExecution, readExecutionLog } from './execution-layer-utils.mjs';

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
};

const FINAL_STATUSES = new Set(['win', 'loss', 'push', 'void', 'cashed_out', 'partial_cashout']);

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\bml\b/g, '')
    .replace(/[^a-z0-9+\-.@\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeEventLabel(value) {
  return normalizeText(value)
    .replace(/\s+vs\.?\s+/g, ' @ ')
    .replace(/\s+v\.?\s+/g, ' @ ')
    .replace(/\s+-\s+/g, ' @ ');
}

function parseIsoDatePrefix(text) {
  const match = String(text || '').match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function toEpochMs(value) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) ? ms : null;
}

function currentIso() {
  return new Date().toISOString();
}

function marketKind(row) {
  const market = String(row?.market_type || row?.market || '').toLowerCase();
  if (market === 'ml' || market === 'moneyline' || market === 'h2h') return 'moneyline';
  if (market.includes('spread')) return 'spread';
  if (market.includes('total')) return 'total';
  return 'unsupported';
}

function parseSelection(row) {
  const selection = String(row?.selection || '').trim();
  const kind = marketKind(row);
  if (kind === 'moneyline') {
    return { kind, team: normalizeText(selection.replace(/\s+ml$/i, '')) };
  }
  if (kind === 'spread') {
    const match = selection.match(/^(.*?)([+-]\d+(?:\.\d+)?)$/);
    if (!match) return { kind, team: normalizeText(selection), point: null };
    return {
      kind,
      team: normalizeText(match[1]),
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
    };
  }
  return { kind };
}

function parseScores(event) {
  if (Array.isArray(event?.scores) && event.scores.length >= 2) {
    const [first, second] = event.scores;
    return [
      { name: first.name || first.team || event.home_team, score: parseNumber(first.score ?? first.points) },
      { name: second.name || second.team || event.away_team, score: parseNumber(second.score ?? second.points) },
    ];
  }
  if (event?.home_score !== undefined && event?.away_score !== undefined) {
    return [
      { name: event.home_team, score: parseNumber(event.home_score) },
      { name: event.away_team, score: parseNumber(event.away_score) },
    ];
  }
  return null;
}

function resolveTeamScore(scores, teamName) {
  const normalizedTeam = normalizeText(teamName);
  const direct = scores.find((entry) => normalizeText(entry.name) === normalizedTeam);
  if (direct) return direct;
  return scores.find((entry) => {
    const candidate = normalizeText(entry.name);
    return candidate.includes(normalizedTeam) || normalizedTeam.includes(candidate);
  }) || null;
}

function parseEventTeams(label) {
  const normalized = normalizeEventLabel(label);
  if (!normalized || !normalized.includes(' @ ')) return [];
  return normalized.split(' @ ').map((part) => part.trim()).filter(Boolean);
}

function pickBestEvent(events, referenceTime) {
  if (!Array.isArray(events) || !events.length) return null;
  const referenceMs = toEpochMs(referenceTime) ?? toEpochMs(parseIsoDatePrefix(referenceTime));
  const scored = events.map((event) => {
    const eventMs = toEpochMs(event?.commence_time || event?.start_time);
    const distance = Number.isFinite(referenceMs) && Number.isFinite(eventMs)
      ? Math.abs(referenceMs - eventMs)
      : Number.MAX_SAFE_INTEGER;
    return {
      event,
      completedRank: event?.completed === true ? 0 : 1,
      distance,
    };
  });
  scored.sort((a, b) => {
    if (a.completedRank !== b.completedRank) return a.completedRank - b.completedRank;
    return a.distance - b.distance;
  });
  return scored[0]?.event || null;
}

function matchEventForExecution(row, events) {
  const targetLabel = normalizeEventLabel(row?.event_label || row?.event || '');
  const targetTeams = parseEventTeams(row?.event_label || row?.event || '');
  if (!targetLabel || !targetTeams.length) {
    return { ok: false, reason: 'missing_event_identity', event: null };
  }

  const exact = (events || []).filter((event) => normalizeEventLabel(`${event.away_team} @ ${event.home_team}`) === targetLabel);
  if (exact.length === 1) {
    return { ok: true, reason: null, event: exact[0] };
  }
  if (exact.length > 1) {
    return { ok: true, reason: null, event: pickBestEvent(exact, row?.bet_slip_timestamp || row?.recommendation_timestamp || row?.logged_at_utc) };
  }

  const broad = (events || []).filter((event) => {
    const eventTeams = [normalizeText(event.away_team), normalizeText(event.home_team)];
    return targetTeams.every((team) => eventTeams.some((eventTeam) => eventTeam.includes(team) || team.includes(eventTeam)));
  });
  if (!broad.length) {
    return { ok: false, reason: 'event_not_found_in_scores_feed', event: null };
  }
  return {
    ok: true,
    reason: null,
    event: pickBestEvent(broad, row?.bet_slip_timestamp || row?.recommendation_timestamp || row?.logged_at_utc),
  };
}

function determineSettlementResult(row, event) {
  const kind = marketKind(row);
  if (!['moneyline', 'spread', 'total'].includes(kind)) {
    return { ok: false, reason: 'unsupported_market_type', result: null };
  }
  if (event?.completed !== true) {
    return { ok: false, reason: 'event_not_final', result: null };
  }
  const scores = parseScores(event);
  if (!scores || scores.length < 2 || scores.some((entry) => !Number.isFinite(entry.score))) {
    return { ok: false, reason: 'missing_final_score', result: null };
  }

  const parsed = parseSelection(row);
  if (kind === 'moneyline') {
    const side = resolveTeamScore(scores, parsed.team);
    const opponent = scores.find((entry) => entry !== side) || null;
    if (!side || !opponent) return { ok: false, reason: 'selection_not_resolved', result: null };
    if (side.score > opponent.score) return { ok: true, reason: 'moneyline_final_score', result: 'WIN' };
    if (side.score < opponent.score) return { ok: true, reason: 'moneyline_final_score', result: 'LOSS' };
    return { ok: true, reason: 'moneyline_final_score', result: 'PUSH' };
  }

  if (kind === 'spread') {
    const side = resolveTeamScore(scores, parsed.team);
    const opponent = scores.find((entry) => entry !== side) || null;
    if (!side || !opponent || !Number.isFinite(parsed.point)) {
      return { ok: false, reason: 'selection_not_resolved', result: null };
    }
    const spreadOutcome = round2(side.score + parsed.point - opponent.score);
    if (spreadOutcome > 0) return { ok: true, reason: 'spread_final_score', result: 'WIN' };
    if (spreadOutcome < 0) return { ok: true, reason: 'spread_final_score', result: 'LOSS' };
    return { ok: true, reason: 'spread_final_score', result: 'PUSH' };
  }

  if (!Number.isFinite(parsed.point) || !parsed.side) {
    return { ok: false, reason: 'selection_not_resolved', result: null };
  }
  const totalScore = round2((scores[0].score || 0) + (scores[1].score || 0));
  if (parsed.side === 'over') {
    if (totalScore > parsed.point) return { ok: true, reason: 'total_final_score', result: 'WIN' };
    if (totalScore < parsed.point) return { ok: true, reason: 'total_final_score', result: 'LOSS' };
    return { ok: true, reason: 'total_final_score', result: 'PUSH' };
  }
  if (totalScore < parsed.point) return { ok: true, reason: 'total_final_score', result: 'WIN' };
  if (totalScore > parsed.point) return { ok: true, reason: 'total_final_score', result: 'LOSS' };
  return { ok: true, reason: 'total_final_score', result: 'PUSH' };
}

function readScoresCache() {
  return readJson(SCORES_CACHE_FILE, { updated_at: null, sports: {} });
}

function writeScoresCache(cache) {
  fs.writeFileSync(SCORES_CACHE_FILE, `${JSON.stringify({
    updated_at: currentIso(),
    sports: cache?.sports || {},
  }, null, 2)}\n`, 'utf8');
}

function cacheFresh(entry) {
  const fetchedAt = Number(entry?.fetched_at_ms);
  return Number.isFinite(fetchedAt) && (Date.now() - fetchedAt) <= SCORES_CACHE_TTL_MS;
}

async function fetchScoresBySport(sportKey, apiKey) {
  const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/scores?daysFrom=3&apiKey=${encodeURIComponent(apiKey)}`;
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`scores_fetch_failed:${sportKey}:${response.status}:${text.slice(0, 120)}`);
  }
  return response.json();
}

function unresolvedReasonForNoScores(apiKeyPresent) {
  return apiKeyPresent ? 'scores_unavailable' : 'missing_api_key';
}

function buildExecutionSettlementGroups(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = String(row?.rec_id || row?.execution_id || '').trim() || `execution::${row.execution_id || Math.random()}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  return groups;
}

export async function runAutomaticSettlementPass(options = {}) {
  const startedAtUtc = options.started_at_utc || currentIso();
  const apiKey = String(process.env.ODDS_API_KEY || '').trim();
  const executionRows = readExecutionLog();
  const gradingRows = readJsonl(CORE_PATHS.gradingLedger);
  const settledExecutionIds = new Set(
    gradingRows
      .filter((row) => FINAL_STATUSES.has(normalizeText(row?.settlement_status || row?.result)))
      .map((row) => normalizeText(row?.execution_log_id || row?.execution_id || row?.ref_id))
      .filter(Boolean),
  );

  const candidates = executionRows.filter((row) => !settledExecutionIds.has(normalizeText(row.execution_id)));
  const stats = {
    started_at_utc: startedAtUtc,
    completed_at_utc: null,
    rows_scanned: candidates.length,
    rows_settled: 0,
    rows_unresolved: 0,
    rows_skipped_already_settled: executionRows.length - candidates.length,
    unresolved_reason_breakdown: {},
    scores_cache_hits: 0,
    scores_api_calls: 0,
    settled_rows: [],
  };

  const scoresCache = readScoresCache();
  const sportKeys = Array.from(new Set(candidates.map((row) => SPORT_KEY_MAP[String(row?.sport || row?.league || '').trim().toUpperCase()]).filter(Boolean)));
  const scoresBySport = {};
  for (const sportKey of sportKeys) {
    const cached = scoresCache?.sports?.[sportKey];
    if (cached?.events && cacheFresh(cached)) {
      scoresBySport[sportKey] = cached.events;
      stats.scores_cache_hits += 1;
      continue;
    }
    if (!apiKey) continue;
    const events = await fetchScoresBySport(sportKey, apiKey);
    scoresBySport[sportKey] = events;
    scoresCache.sports = scoresCache.sports || {};
    scoresCache.sports[sportKey] = {
      fetched_at_ms: Date.now(),
      events,
    };
    stats.scores_api_calls += 1;
  }
  if (stats.scores_api_calls > 0) {
    writeScoresCache(scoresCache);
  }

  const candidateGroups = buildExecutionSettlementGroups(candidates);
  for (const groupRows of candidateGroups.values()) {
    if (groupRows.length > 1 && groupRows.some((row) => String(row?.rec_id || '').trim())) {
      for (const row of groupRows) {
        stats.rows_unresolved += 1;
        stats.unresolved_reason_breakdown.multi_execution_cluster_manual_review =
          (stats.unresolved_reason_breakdown.multi_execution_cluster_manual_review || 0) + 1;
      }
      continue;
    }

    const [row] = groupRows;
    const kind = marketKind(row);
    const sportKey = SPORT_KEY_MAP[String(row?.sport || row?.league || '').trim().toUpperCase()] || null;
    let unresolvedReason = null;

    if (!['moneyline', 'spread', 'total'].includes(kind)) {
      unresolvedReason = 'unsupported_market_type';
    } else if (!sportKey) {
      unresolvedReason = 'unsupported_sport';
    } else if (!String(row?.event_label || row?.event || '').trim()) {
      unresolvedReason = 'missing_event_identity';
    } else if (!Array.isArray(scoresBySport[sportKey])) {
      unresolvedReason = unresolvedReasonForNoScores(Boolean(apiKey));
    }

    if (unresolvedReason) {
      stats.rows_unresolved += 1;
      stats.unresolved_reason_breakdown[unresolvedReason] = (stats.unresolved_reason_breakdown[unresolvedReason] || 0) + 1;
      continue;
    }

    const matchedEvent = matchEventForExecution(row, scoresBySport[sportKey]);
    if (!matchedEvent.ok || !matchedEvent.event) {
      const reason = matchedEvent.reason || 'event_not_found_in_scores_feed';
      stats.rows_unresolved += 1;
      stats.unresolved_reason_breakdown[reason] = (stats.unresolved_reason_breakdown[reason] || 0) + 1;
      continue;
    }

    const settlement = determineSettlementResult(row, matchedEvent.event);
    if (!settlement.ok) {
      const reason = settlement.reason || 'unresolved';
      stats.rows_unresolved += 1;
      stats.unresolved_reason_breakdown[reason] = (stats.unresolved_reason_breakdown[reason] || 0) + 1;
      continue;
    }

    const ingest = ingestAutomaticExecutionSettlementForExecution(row, settlement.result, {
      settlement_timestamp: currentIso(),
      source: 'automatic_settlement_job',
      settlement_source: 'automatic_settlement_job',
      auto_settlement_reason: settlement.reason,
      notes: ['automatic_standard_market_settlement'],
    });

    if (ingest.ok) {
      stats.rows_settled += 1;
      stats.settled_rows.push({
        execution_id: row.execution_id,
        grading_id: ingest.row?.grading_id || null,
        selection: row.selection || null,
        sportsbook: row.actual_sportsbook || null,
        result: settlement.result,
        run_id: row.run_id || null,
        event_label: row.event_label || row.event || null,
      });
      continue;
    }

    const reason = ingest.duplicate ? 'already_settled' : (ingest.reason || 'settlement_append_failed');
    stats.rows_unresolved += 1;
    stats.unresolved_reason_breakdown[reason] = (stats.unresolved_reason_breakdown[reason] || 0) + 1;
  }

  stats.completed_at_utc = currentIso();
  return stats;
}

async function main() {
  const result = await runAutomaticSettlementPass();
  console.log(JSON.stringify(result, null, 2));
}

main().catch((error) => {
  console.error(`automatic_settlement_failed: ${error?.message || error}`);
  process.exit(1);
});

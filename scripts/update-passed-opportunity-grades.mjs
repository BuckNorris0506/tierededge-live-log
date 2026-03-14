#!/usr/bin/env node
import fs from 'node:fs/promises';

const RECOMMENDATION_LOG = '/Users/jaredbuckman/.openclaw/workspace/memory/recommendation-log.md';
const CACHE_FILE = '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json';
const ODDS_CONFIG = '/Users/jaredbuckman/.openclaw/workspace/memory/odds-api-config.md';

const SPORT_KEY_MAP = {
  NBA: 'basketball_nba',
  CBB: 'basketball_ncaab',
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

function normalizeName(name) {
  return String(name || '').trim().toLowerCase().replace(/\s+/g, ' ');
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
  return m ? `${m[1]}T00:00:00Z` : null;
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
  // Supports multiple possible payload shapes.
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

async function main() {
  const [logRaw, configRaw] = await Promise.all([
    fs.readFile(RECOMMENDATION_LOG, 'utf8'),
    fs.readFile(ODDS_CONFIG, 'utf8'),
  ]);
  const apiKey = process.env.ODDS_API_KEY || parseOddsApiKey(configRaw);
  if (!apiKey) {
    console.log('No ODDS API key configured; skipping passed-opportunity grading.');
    return;
  }

  const rows = parseTable(logRaw);
  const sitRows = rows.filter((r) => String(r.decision || '').trim().toLowerCase() === 'sit');
  const cache = (await readJson(CACHE_FILE)) || { updated_at: null, entries: {} };
  const entries = cache.entries || {};

  const sportKeys = [...new Set(
    sitRows
      .map((r) => SPORT_KEY_MAP[String(r.sport || '').trim().toUpperCase()])
      .filter(Boolean)
  )];

  const scoresBySport = {};
  for (const sportKey of sportKeys) {
    try {
      scoresBySport[sportKey] = await fetchScoresBySport(sportKey, apiKey);
    } catch (error) {
      console.log(`WARN: ${String(error.message || error)}`);
    }
  }

  for (const row of sitRows) {
    const recId = row.rec_id;
    if (!recId) continue;
    if (entries[recId]?.counterfactual_result && entries[recId]?.counterfactual_result !== 'ungraded') continue;

    const sportKey = SPORT_KEY_MAP[String(row.sport || '').trim().toUpperCase()];
    const events = scoresBySport[sportKey];
    if (!events || !Array.isArray(events)) continue;

    const selection = row.selection || '';
    const candidates = selectionCandidates(row.sport, selection);
    const matched = events.filter((event) => {
      const names = [event.home_team, event.away_team].map(normalizeName);
      return candidates.some((sel) => names.some((n) => n.includes(sel) || sel.includes(n)));
    });
    const event = pickBestEvent(matched, row.timestamp_ct);
    if (!event) continue;
    if (event.completed !== true) {
      entries[recId] = {
        ...(entries[recId] || {}),
        rec_id: recId,
        counterfactual_result: 'ungraded',
        event_id: event.id || null,
        event_label: `${event.away_team} @ ${event.home_team}`,
        graded_at: new Date().toISOString(),
      };
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
    const usOdds = Number(row.recommended_odds_us);
    const decOdds = toDecimalOdds(usOdds);
    let unitPl = null;
    if (result === 'win' && decOdds) unitPl = decOdds - 1;
    if (result === 'loss') unitPl = -1;
    if (result === 'push') unitPl = 0;

    entries[recId] = {
      rec_id: recId,
      counterfactual_result: result,
      counterfactual_pl_unit: unitPl,
      event_id: event.id || null,
      event_label: `${event.away_team} @ ${event.home_team}`,
      graded_at: new Date().toISOString(),
    };
  }

  const out = {
    updated_at: new Date().toISOString(),
    entries,
  };
  await fs.writeFile(CACHE_FILE, `${JSON.stringify(out, null, 2)}\n`, 'utf8');
  console.log(`Updated passed-opportunity grades cache: ${CACHE_FILE}`);
}

main().catch((error) => {
  console.error(`Passed-opportunity grading failed: ${error.message}`);
  process.exit(1);
});

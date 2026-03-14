import fs from 'node:fs';
import path from 'node:path';

export const CANDIDATE_MARKET_HEADERS = [
  'run_id',
  'scan_time_ct',
  'event_id',
  'sport',
  'league',
  'market_type',
  'selection',
  'book',
  'odds_american',
  'odds_decimal',
  'devig_implied_prob',
  'consensus_prob',
  'pre_conf_true_prob',
  'confidence_score',
  'post_conf_true_prob',
  'raw_edge_pct',
  'post_conf_edge_pct',
  'tier_threshold_pct',
  'price_edge_pass',
  'bet_permission_pass',
  'final_decision',
  'rejection_stage',
  'rejection_reason',
  'rec_id',
];

export const SUPPRESSED_CANDIDATE_HEADERS = [
  ...CANDIDATE_MARKET_HEADERS,
  'suppression_bucket',
  'gap_to_t3_pct',
  'confidence_penalty_pct',
  'consensus_penalty_pct',
];

export const SUPPRESSION_ENRICHMENT_HEADERS = [
  'rec_id',
  'scan_time_ct',
  'event_id',
  'event_label',
  'closing_odds',
  'closing_implied_prob',
  'closing_clv_pct',
  'result_if_played',
  'hypothetical_profit',
  'hypothetical_units',
  'graded_at_utc',
  'enrichment_status',
  'enrichment_note',
];

export function parseAsNumber(text) {
  if (text === null || text === undefined || text === '') return null;
  const clean = String(text).replace(/[^0-9.-]/g, '');
  if (!clean || clean === '-' || clean === '.' || clean === '-.') return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

export function parsePercent(text) {
  return parseAsNumber(text);
}

export function round2(value) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(2));
}

export function normalizeDecision(text) {
  return String(text || '').trim().toLowerCase();
}

export function normalizeReason(text) {
  return String(text || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/[^\w]/g, '');
}

export function splitReasonCodes(text) {
  const aliases = {
    edge_below_20_minimum: 'no_edge',
    edge_below_minimum_threshold: 'no_edge',
    edge_below_threshold: 'no_edge',
    data_confidence_below_required_threshold: 'low_confidence',
    odds_data_could_not_be_verified: 'stale_or_unverified_odds',
    daily_exposure_cap_reached: 'exposure_cap_reached',
    circuit_breaker_risk_mode_active: 'breaker_active',
  };
  return String(text || '')
    .split(/[|,;/]+/)
    .map((part) => normalizeReason(part))
    .map((reason) => aliases[reason] || reason)
    .filter(Boolean);
}

function escapeCsv(value) {
  const text = value === null || value === undefined ? '' : String(value);
  if (/[,"\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(',')];
  for (const row of rows) {
    lines.push(headers.map((header) => escapeCsv(row?.[header] ?? '')).join(','));
  }
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${lines.join('\n')}\n`, 'utf8');
}

export function readCsv(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  const lines = raw.split(/\r?\n/);
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).filter(Boolean).map((line) => {
    const values = parseCsvLine(line);
    const row = {};
    headers.forEach((header, idx) => {
      row[header] = values[idx] ?? '';
    });
    return row;
  });
}

function parseCsvLine(line) {
  const out = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  out.push(current);
  return out;
}

export function parseDateKey(timestampCt) {
  const match = String(timestampCt || '').match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

export function parseTimestampMs(input) {
  const value = String(input || '').trim();
  if (!value) return null;
  const direct = Date.parse(value);
  if (Number.isFinite(direct)) return direct;
  const dateOnly = value.match(/(\d{4}-\d{2}-\d{2})/);
  if (dateOnly) return Date.parse(`${dateOnly[1]}T00:00:00Z`);
  return null;
}

function mapLeague(rawSport) {
  const sport = String(rawSport || '').trim().toUpperCase();
  if (sport === 'CBB') return 'NCAAB';
  return sport;
}

function mapSportCategory(rawSport) {
  const league = mapLeague(rawSport);
  if (['NBA', 'NCAAB', 'CBB'].includes(league)) return 'basketball';
  if (league === 'NHL') return 'hockey';
  if (['MMA', 'UFC'].includes(league)) return 'mma';
  if (league.includes('SOCCER') || league === 'EPL') return 'soccer';
  return String(rawSport || '').trim().toLowerCase() || null;
}

function mapQualityValue(value, field) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return 0.5;
  const direct = Number(raw);
  if (Number.isFinite(direct)) return Math.max(0, Math.min(1, direct));
  const maps = {
    default: {
      high: 0.9,
      medium: 0.7,
      med: 0.7,
      low: 0.4,
      live: 0.85,
      tight: 0.75,
      wide: 0.4,
      volatile: 0.35,
      n_a: 0.5,
      na: 0.5,
      unknown: 0.5,
      backfill_unknown: 0.5,
      confirmed: 0.9,
      verified: 0.9,
      stale: 0.25,
      unverified: 0.25,
    },
    injury_quality: {
      high: 0.85,
      medium: 0.65,
      low: 0.35,
      n_a: 0.5,
      na: 0.5,
    },
    market_quality: {
      tight: 0.8,
      medium: 0.7,
      wide: 0.35,
      volatile: 0.3,
      low: 0.35,
      high: 0.85,
    },
  };
  const normalized = raw.replace(/[^a-z0-9]+/g, '_');
  return maps[field]?.[normalized] ?? maps.default[normalized] ?? 0.5;
}

export function computeConfidenceScore(row) {
  const direct = parseAsNumber(row.confidence_score ?? row.confidence_total);
  if (direct !== null) {
    if (direct > 1) return round2(direct / 100);
    return round2(Math.max(0, Math.min(1, direct)));
  }
  const oddsQuality = mapQualityValue(row.odds_quality, 'odds_quality');
  const injuryQuality = mapQualityValue(row.injury_quality, 'injury_quality');
  const marketQuality = mapQualityValue(row.market_quality, 'market_quality');
  return round2((0.4 * oddsQuality) + (0.3 * injuryQuality) + (0.3 * marketQuality));
}

function computePostConfidenceTrueProb(preConfTrueProb, consensusProb, confidenceScore, rejectionReason) {
  if (preConfTrueProb === null) return null;
  if (consensusProb === null) return preConfTrueProb;
  if (rejectionReason !== 'low_confidence') return preConfTrueProb;
  const confidence = confidenceScore ?? 0.5;
  const scale = Math.max(0, Math.min(1, confidence / 0.6));
  return round2(consensusProb + ((preConfTrueProb - consensusProb) * scale));
}

function computeTierThreshold(rawEdgePct) {
  if (rawEdgePct !== null && rawEdgePct >= 6) return 6;
  if (rawEdgePct !== null && rawEdgePct >= 4) return 4;
  return 2;
}

function computeRejectionStage({ decision, rejectionReason, rawEdgePct, postConfEdgePct, tierThresholdPct }) {
  if (decision === 'bet') return '';
  if (['stale_or_unverified_odds', 'integrity_fail', 'data_freshness_fail'].includes(rejectionReason)) return 'integrity_gate';
  if (['exposure_cap_reached', 'breaker_active', 'exposure_cap', 'drawdown_governor'].includes(rejectionReason)) return 'risk_gate';
  if (rejectionReason === 'low_confidence') return 'confidence_gate';
  if (['weak_consensus', 'market_confidence_too_low', 'market_quality_fail'].includes(rejectionReason)) return 'consensus_anchor';
  if (rawEdgePct === null || rawEdgePct <= 0) return 'no_raw_edge';
  if (postConfEdgePct !== null && postConfEdgePct < tierThresholdPct) return 'threshold_gate';
  return 'threshold_gate';
}

function buildRunId(timestampCt) {
  const text = String(timestampCt || '').trim();
  const match = text.match(/(\d{4}-\d{2}-\d{2}).*?(\d{1,2}:\d{2})/);
  if (!match) return parseDateKey(text) || 'unknown_run';
  return `${match[1]}-${match[2].replace(':', '')}`;
}

export function buildCandidateMarketRows(recommendationRows) {
  return (recommendationRows || [])
    .filter((row) => {
      const decision = normalizeDecision(row.decision);
      return decision === 'bet' || decision === 'sit';
    })
    .map((row) => {
      const impliedProb = parsePercent(row.implied_prob_fair);
      const preConfTrueProb = parsePercent(row.true_prob);
      const rawEdgePct = parsePercent(row.edge_pct);
      const rejectionReason = splitReasonCodes(row.rejection_reason)[0] || '';
      const confidenceScore = computeConfidenceScore(row);
      const postConfTrueProb = computePostConfidenceTrueProb(preConfTrueProb, impliedProb, confidenceScore, rejectionReason);
      const postConfEdgePct = preConfTrueProb !== null && postConfTrueProb !== null && impliedProb !== null
        ? round2(postConfTrueProb - impliedProb)
        : rawEdgePct;
      const tierThresholdPct = computeTierThreshold(rawEdgePct);
      const decision = normalizeDecision(row.decision);
      const rejectionStage = computeRejectionStage({
        decision,
        rejectionReason,
        rawEdgePct,
        postConfEdgePct,
        tierThresholdPct,
      });
      return {
        run_id: buildRunId(row.timestamp_ct),
        scan_time_ct: row.timestamp_ct || '',
        event_id: row.event_id || '',
        sport: mapSportCategory(row.sport),
        league: mapLeague(row.sport),
        market_type: row.market || '',
        selection: row.selection || '',
        book: row.source_book || '',
        odds_american: row.recommended_odds_us || '',
        odds_decimal: row.recommended_odds_dec || '',
        devig_implied_prob: impliedProb !== null ? round2(impliedProb) : '',
        consensus_prob: impliedProb !== null ? round2(impliedProb) : '',
        pre_conf_true_prob: preConfTrueProb !== null ? round2(preConfTrueProb) : '',
        confidence_score: confidenceScore !== null ? round2(confidenceScore) : '',
        post_conf_true_prob: postConfTrueProb !== null ? round2(postConfTrueProb) : '',
        raw_edge_pct: rawEdgePct !== null ? round2(rawEdgePct) : '',
        post_conf_edge_pct: postConfEdgePct !== null ? round2(postConfEdgePct) : '',
        tier_threshold_pct: tierThresholdPct,
        price_edge_pass: rawEdgePct !== null && rawEdgePct >= tierThresholdPct ? 'true' : 'false',
        bet_permission_pass: decision === 'bet' ? 'true' : 'false',
        final_decision: decision.toUpperCase(),
        rejection_stage: rejectionStage,
        rejection_reason: rejectionReason,
        rec_id: row.rec_id || '',
      };
    });
}

export function buildSuppressedCandidateRows(candidateRows) {
  return (candidateRows || [])
    .filter((row) => {
      const finalDecision = String(row.final_decision || '').toUpperCase();
      const pre = parseAsNumber(row.raw_edge_pct);
      const post = parseAsNumber(row.post_conf_edge_pct);
      return finalDecision === 'SIT' && ((pre !== null && pre >= 2.0) || (post !== null && post >= 1.5));
    })
    .map((row) => {
      const preConfTrueProb = parseAsNumber(row.pre_conf_true_prob);
      const postConfTrueProb = parseAsNumber(row.post_conf_true_prob);
      const consensusProb = parseAsNumber(row.consensus_prob);
      const postConfEdgePct = parseAsNumber(row.post_conf_edge_pct);
      let suppressionBucket = 'threshold_near_miss';
      const rejectionStage = row.rejection_stage;
      const rawEdgePct = parseAsNumber(row.raw_edge_pct);
      if (rawEdgePct !== null && rawEdgePct >= 2 && rejectionStage === 'confidence_gate') suppressionBucket = 'confidence_suppression';
      else if (rejectionStage === 'consensus_anchor') suppressionBucket = 'consensus_flattening';
      else if (postConfEdgePct !== null && postConfEdgePct >= 1.5 && postConfEdgePct < 2) suppressionBucket = 'threshold_near_miss';
      else suppressionBucket = 'suppressed_candidate';
      return {
        ...row,
        suppression_bucket: suppressionBucket,
        gap_to_t3_pct: postConfEdgePct !== null ? round2(2.0 - postConfEdgePct) : '',
        confidence_penalty_pct: preConfTrueProb !== null && postConfTrueProb !== null ? round2(preConfTrueProb - postConfTrueProb) : '',
        consensus_penalty_pct: preConfTrueProb !== null && consensusProb !== null ? round2(preConfTrueProb - consensusProb) : '',
      };
    });
}

export function buildSuppressionSummary(candidateRows, targetDate) {
  const rows = (candidateRows || []).filter((row) => !targetDate || String(row.scan_time_ct || '').includes(targetDate));
  const count = (predicate) => rows.filter(predicate).length;
  return {
    markets_scanned: rows.length,
    markets_with_raw_edge_over_0_5: count((row) => (parseAsNumber(row.raw_edge_pct) ?? -Infinity) > 0.5),
    markets_with_pre_conf_edge_over_2_0: count((row) => (parseAsNumber(row.raw_edge_pct) ?? -Infinity) >= 2.0),
    markets_rejected_by_confidence_gate: count((row) => row.rejection_stage === 'confidence_gate'),
    markets_rejected_by_threshold_gate: count((row) => row.rejection_stage === 'threshold_gate'),
    markets_rejected_by_risk_gate: count((row) => row.rejection_stage === 'risk_gate'),
    final_approved_bets: count((row) => String(row.final_decision || '').toUpperCase() === 'BET'),
  };
}

export function average(values) {
  const nums = values.map((value) => parseAsNumber(value)).filter((value) => value !== null);
  if (nums.length === 0) return null;
  return round2(nums.reduce((sum, value) => sum + value, 0) / nums.length);
}

export function safeRate(num, den) {
  if (!Number.isFinite(num) || !Number.isFinite(den) || den <= 0) return null;
  return round2((num / den) * 100);
}

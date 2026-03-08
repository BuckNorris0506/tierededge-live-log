import fs from 'node:fs';
import path from 'node:path';

const DEFAULT_SOURCE = '/Users/jaredbuckman/.openclaw/workspace/memory/betting-state.md';
const DEFAULT_OUT = path.resolve(process.cwd(), 'public', 'data.json');

const sourcePath = process.argv[2] || DEFAULT_SOURCE;
const outPath = process.argv[3] || DEFAULT_OUT;
const redactPending = String(process.env.REDACT_PENDING || 'false').toLowerCase() === 'true';

function fail(msg) {
  console.error(`ERROR: ${msg}`);
  process.exit(1);
}

function extractSection(markdown, title) {
  const header = `## ${title}`;
  const start = markdown.indexOf(header);
  if (start === -1) return '';

  const lineEnd = markdown.indexOf('\n', start);
  if (lineEnd === -1) return '';

  const rest = markdown.slice(lineEnd + 1);
  const nextHeaderOffset = rest.search(/\n## /);
  if (nextHeaderOffset === -1) return rest.trim();
  return rest.slice(0, nextHeaderOffset).trim();
}

function extractFirstSection(markdown, titles) {
  for (const title of titles) {
    const section = extractSection(markdown, title);
    if (section) return section;
  }
  return '';
}

function parseBulletMap(section) {
  const out = {};
  const lines = section.split('\n').map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    if (!line.startsWith('- ')) continue;
    const body = line.slice(2);
    const idx = body.indexOf(':');
    if (idx === -1) continue;
    const key = body.slice(0, idx).trim();
    const value = body.slice(idx + 1).trim();
    out[key] = value;
  }
  return out;
}

function parseTable(section) {
  const lines = section
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|'));

  if (lines.length < 2) return [];

  const headers = lines[0]
    .split('|')
    .map((s) => s.trim())
    .filter(Boolean);

  const rows = [];
  for (let i = 2; i < lines.length; i += 1) {
    const rowParts = lines[i]
      .split('|')
      .map((s) => s.trim())
      .filter(Boolean);
    if (rowParts.length !== headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) {
      row[headers[j]] = rowParts[j];
    }
    rows.push(row);
  }
  return rows;
}

function parsePending(section) {
  return section
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('- '))
    .map((line) => line.slice(2).trim());
}

function parseSchema(markdown) {
  const schemaMatch = markdown.match(/^#\s+TieredEdge Quant State \(Schema\s+([^\)]+)\)/m);
  return schemaMatch ? schemaMatch[1].trim() : 'unknown';
}

function parseLastUpdated(markdown) {
  const match = markdown.match(/^Last Updated:\s*(.+)$/m);
  return match ? match[1].trim() : 'unknown';
}

function parseAsNumber(text) {
  if (!text) return null;
  const clean = String(text).replace(/[^0-9.-]/g, '');
  if (!clean || clean === '-' || clean === '.' || clean === '-.') return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

function parsePercent(text) {
  const n = parseAsNumber(text);
  if (n === null) return null;
  return n;
}

function parseClvValue(text) {
  if (!text) return null;
  const raw = String(text).trim().toLowerCase();
  if (!raw || raw === '-' || raw === 'n/a') return null;
  return parseAsNumber(raw);
}

function parseRecommendationRows(markdown) {
  return parseTable(markdown);
}

function normalizeDecision(text) {
  return String(text || '').trim().toLowerCase();
}

function normalizeReason(text) {
  return String(text || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/[^\w]/g, '');
}

function splitReasonCodes(text) {
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

function parseDateFromLastUpdated(lastUpdated) {
  const match = String(lastUpdated || '').match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function round2(n) {
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

function safeRate(num, den) {
  if (!den) return null;
  return num / den;
}

function resolveRecommendationLogPath(markdown, sourcePath) {
  const pointer = parseBulletMap(extractSection(markdown, 'Recommendation Log Pointer'));
  const rawPath = String(pointer.Path || '').replace(/`/g, '').trim();
  if (!rawPath) return null;
  const sourceDir = path.dirname(sourcePath);
  const candidates = [
    path.resolve(sourceDir, rawPath),
    path.resolve(sourceDir, '..', rawPath),
    path.resolve(sourceDir, path.basename(rawPath)),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function normalizeKeyPart(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^\w\s.+-]/g, '');
}

function buildBetIdentity(row) {
  return [
    normalizeKeyPart(row.Sport || row.sport),
    normalizeKeyPart(row.Market || row.market),
    normalizeKeyPart(row.Bet || row.selection || row['Selection']),
    normalizeKeyPart(row.Book || row.book || row.source_book),
  ].join('|');
}

function dedupeStalePendingBetLog(betLog) {
  const hasGradedByIdentity = new Set();
  for (const row of betLog) {
    const result = normalizeDecision(row.Result || row.result);
    if (result && result !== 'pending') {
      hasGradedByIdentity.add(buildBetIdentity(row));
    }
  }
  return betLog.filter((row) => {
    const result = normalizeDecision(row.Result || row.result);
    if (result !== 'pending') return true;
    const id = buildBetIdentity(row);
    return !hasGradedByIdentity.has(id);
  });
}

function computePassedOpportunityTracker({ recommendationRows, targetDate }) {
  const sitRows = recommendationRows
    .filter((row) => normalizeDecision(row.decision) === 'sit')
    .filter((row) => {
      if (!targetDate) return true;
      return String(row.timestamp_ct || '').includes(targetDate);
    });

  const entries = sitRows.map((row) => {
    const sport = row.sport || 'Unknown';
    const selection = row.selection || row.market || 'Unknown selection';
    const edge = parsePercent(row.edge_pct);
    const edgeText = edge !== null ? `${edge}%` : 'N/A';
    const odds = row.recommended_odds_us || row.odds_us || 'N/A';
    const result = normalizeDecision(row.counterfactual_result || row.if_bet_result || row.result);
    const status = result || 'ungraded';
    const readable = status === 'ungraded'
      ? 'awaiting grading'
      : (status === 'loss' ? 'they lost' : (status === 'win' ? 'they won' : status));

    const sentence = `We passed on ${selection} (${sport}) at +EV ${edgeText} (${odds}). Outcome: ${readable}.`;

    return {
      timestamp_ct: row.timestamp_ct || null,
      sport,
      selection,
      edge_percent: edge,
      odds_us: odds,
      outcome_if_bet: status,
      narrative: sentence,
      rejection_reason: row.rejection_reason || null,
      counterfactual_pl: parseAsNumber(row.counterfactual_pl || row.if_bet_pl || row.counterfactual_p_l),
    };
  });

  const graded = entries.filter((e) => e.outcome_if_bet !== 'ungraded');
  const wins = graded.filter((e) => e.outcome_if_bet === 'win').length;
  const losses = graded.filter((e) => e.outcome_if_bet === 'loss').length;
  const pushes = graded.filter((e) => e.outcome_if_bet === 'push').length;

  return {
    total_passed_opportunities: entries.length,
    graded_count: graded.length,
    ungraded_count: Math.max(0, entries.length - graded.length),
    record_if_bet: graded.length > 0 ? `${wins}-${losses}${pushes > 0 ? `-${pushes}` : ''}` : null,
    entries,
  };
}

const SUPPORTED_SIT_REASON_CODES = [
  'no_edge',
  'weak_consensus',
  'limited_liquidity',
  'high_volatility',
  'market_confidence_too_low',
  'exposure_cap',
  'drawdown_governor',
  'large_spread_instability',
  'strategy_filter_reject',
  'low_confidence',
  'manual_override',
  'market_quality_fail',
  'stale_or_unverified_odds',
  'exposure_cap_reached',
  'breaker_active',
];

function computeEdgeDistributionTransparency({ recommendationRows, rejectedOpportunities, targetDate }) {
  const inDate = recommendationRows.filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const candidateRows =
    inDate.length > 0
      ? inDate
      : rejectedOpportunities.map((row) => ({
        edge_pct: row['Edge %'] || row.edge_pct,
        sport: row.Sport || row.sport || 'unknown',
        market: row.Market || row.market || 'unknown',
      }));

  const edges = candidateRows
    .map((row) => ({
      edge: parsePercent(row.edge_pct),
      sport: String(row.sport || 'unknown'),
      market: String(row.market || 'unknown'),
    }))
    .filter((row) => row.edge !== null);

  const buckets = {
    edge_0_1: 0,
    edge_1_2: 0,
    edge_2_3: 0,
    edge_3_4: 0,
    edge_4_5: 0,
    edge_5_plus: 0,
  };
  const bySport = {};
  const byMarketType = {};

  for (const row of edges) {
    const e = row.edge;
    if (e >= 0 && e < 1) buckets.edge_0_1 += 1;
    else if (e >= 1 && e < 2) buckets.edge_1_2 += 1;
    else if (e >= 2 && e < 3) buckets.edge_2_3 += 1;
    else if (e >= 3 && e < 4) buckets.edge_3_4 += 1;
    else if (e >= 4 && e < 5) buckets.edge_4_5 += 1;
    else if (e >= 5) buckets.edge_5_plus += 1;

    bySport[row.sport] = (bySport[row.sport] || 0) + 1;
    byMarketType[row.market] = (byMarketType[row.market] || 0) + 1;
  }

  return {
    buckets,
    total_edges_observed: edges.length,
    by_sport: bySport,
    by_market_type: byMarketType,
  };
}

function computeDecisionQuality({
  lastUpdatedCt,
  currentStatus,
  betLog,
  todaysBets,
  rejectedOpportunities,
  rejectionSummary,
  sitAccountability,
  recommendationRows,
}) {
  const targetDate = parseDateFromLastUpdated(lastUpdatedCt);
  const allowedSitReasons = new Set(SUPPORTED_SIT_REASON_CODES);

  const tierPlacedBets = todaysBets.filter((row) => /^T[123]$/i.test(String(row.Tier || '').trim()));

  const recRowsForDate = recommendationRows.filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const recPlacedRows = recRowsForDate.filter((row) => normalizeDecision(row.decision) === 'bet');
  const recSitRows = recRowsForDate.filter((row) => normalizeDecision(row.decision) === 'sit');

  const placedBetsCount = recPlacedRows.length > 0 ? recPlacedRows.length : tierPlacedBets.length;
  const rejectedFromSummary = parseAsNumber(rejectionSummary['Total Rejected']) || 0;
  const useSummaryRejectionMode = recSitRows.length === 0 && rejectedFromSummary > rejectedOpportunities.length;
  const rejectedPlaysCount =
    recSitRows.length > 0
      ? recSitRows.length
      : (useSummaryRejectionMode
        ? rejectedFromSummary
        : (rejectedOpportunities.length > 0 ? rejectedOpportunities.length : rejectedFromSummary));

  const clvTierBets = betLog.filter((row) => /^T[123]$/i.test(String(row.Tier || '').trim()));
  const clvValues = clvTierBets.map((row) => parseClvValue(row.CLV)).filter((n) => n !== null);
  const positiveClvBetsCount = clvValues.filter((n) => n > 0).length;
  const positiveClvRate = safeRate(positiveClvBetsCount, clvValues.length);
  const avgClv = clvValues.length > 0 ? clvValues.reduce((a, b) => a + b, 0) / clvValues.length : null;

  const edgeValuesPlaced = recPlacedRows
    .map((row) => parsePercent(row.edge_pct))
    .filter((n) => n !== null);
  const avgEdgePlaced =
    edgeValuesPlaced.length > 0
      ? edgeValuesPlaced.reduce((a, b) => a + b, 0) / edgeValuesPlaced.length
      : null;

  const validEdgePlacedCount =
    recPlacedRows.length > 0
      ? recPlacedRows.filter((row) => {
          const edge = parsePercent(row.edge_pct);
          return edge !== null ? edge >= 2.0 : true;
        }).length
      : placedBetsCount;

  const exposureCompliance = parsePercent(currentStatus['Exposure Compliance (7d)']);
  const riskRuleCompliantCount =
    exposureCompliance !== null && exposureCompliance < 100
      ? Math.floor((placedBetsCount * exposureCompliance) / 100)
      : placedBetsCount;

  const clvSupportCount = clvValues.length > 0 ? Math.min(placedBetsCount, positiveClvBetsCount) : placedBetsCount;
  const highQualityBetDecisions = Math.max(
    0,
    Math.min(placedBetsCount, validEdgePlacedCount, riskRuleCompliantCount, clvSupportCount)
  );

  let sitQualityRows = [];
  if (recSitRows.length > 0) {
    sitQualityRows = recSitRows.map((row) => ({
      reasons: splitReasonCodes(row.rejection_reason),
      edge: parsePercent(row.edge_pct),
    }));
  } else if (!useSummaryRejectionMode && rejectedOpportunities.length > 0) {
    sitQualityRows = rejectedOpportunities.map((row) => ({
      reasons: splitReasonCodes(row['Sit Reason'] || row.reason || row.rejection_reason),
      edge: parsePercent(row['Edge %'] || row.edge_pct),
    }));
  }

  let highQualitySitDecisions = 0;
  if (sitQualityRows.length > 0) {
    highQualitySitDecisions = sitQualityRows.filter((row) => {
      if (!row.reasons.length) return false;
      if (!row.reasons.every((r) => allowedSitReasons.has(r))) return false;
      if (row.reasons.includes('no_edge') && row.edge !== null) return row.edge < 2.0;
      return true;
    }).length;
  } else {
    const knownReasonCount = [
      'no_edge',
      'low_confidence',
      'stale_or_unverified_odds',
      'exposure_cap_reached',
      'breaker_active',
    ]
      .map((k) => parseAsNumber(rejectionSummary[k]) || 0)
      .reduce((a, b) => a + b, 0);
    highQualitySitDecisions = Math.min(rejectedPlaysCount, knownReasonCount);
  }

  const rejectedByReason = {};
  for (const row of sitQualityRows) {
    for (const reason of row.reasons) {
      rejectedByReason[reason] = (rejectedByReason[reason] || 0) + 1;
    }
  }
  if (Object.keys(rejectedByReason).length === 0) {
    for (const k of ['no_edge', 'low_confidence', 'stale_or_unverified_odds', 'exposure_cap_reached', 'breaker_active']) {
      const c = parseAsNumber(rejectionSummary[k]);
      if (c !== null) rejectedByReason[k] = c;
    }
  }

  let rejectedEvTotal = parseAsNumber(sitAccountability['Net EV Rejected']);
  let avoidedNegativeEv = parseAsNumber(sitAccountability['Avoided Negative EV']);
  if (rejectedEvTotal === null || avoidedNegativeEv === null) {
    const sitEdges = sitQualityRows.map((row) => row.edge).filter((n) => n !== null);
    if (rejectedEvTotal === null) {
      rejectedEvTotal = sitEdges.filter((e) => e > 0).reduce((a, b) => a + b / 100, 0);
    }
    if (avoidedNegativeEv === null) {
      avoidedNegativeEv = sitEdges.filter((e) => e < 0).reduce((a, b) => a + Math.abs(b / 100), 0);
    }
  }

  const totalDecisions = placedBetsCount + rejectedPlaysCount;
  const betQualityRate = safeRate(highQualityBetDecisions, placedBetsCount);
  const sitQualityRate = safeRate(highQualitySitDecisions, rejectedPlaysCount);
  const decisionQualityRate = safeRate(highQualityBetDecisions + highQualitySitDecisions, totalDecisions);

  const largestEdgeRejected = sitQualityRows.length > 0
    ? sitQualityRows.reduce((best, row) => {
      if (row.edge === null) return best;
      if (best === null || row.edge > best) return row.edge;
      return best;
    }, null)
    : null;

  return {
    placed_bets_count: placedBetsCount,
    rejected_plays_count: rejectedPlaysCount,
    high_quality_bet_decisions: highQualityBetDecisions,
    high_quality_sit_decisions: highQualitySitDecisions,
    positive_clv_bets_count: positiveClvBetsCount,
    positive_clv_rate: positiveClvRate !== null ? round2(positiveClvRate * 100) : null,
    avg_clv: round2(avgClv),
    avg_edge_placed: round2(avgEdgePlaced),
    rejected_ev_total: round2(rejectedEvTotal),
    avoided_negative_ev: round2(avoidedNegativeEv),
    bet_quality_rate: betQualityRate !== null ? round2(betQualityRate * 100) : null,
    sit_quality_rate: sitQualityRate !== null ? round2(sitQualityRate * 100) : null,
    sit_decision_quality_count: highQualitySitDecisions,
    decision_quality_rate: decisionQualityRate !== null ? round2(decisionQualityRate * 100) : null,
    total_decisions: totalDecisions,
    largest_edge_rejected: round2(largestEdgeRejected),
    rejected_by_reason: rejectedByReason,
    sit_reason_codes_supported: SUPPORTED_SIT_REASON_CODES,
  };
}

function computeDailyDecisionSummary({
  scannerStats,
  decisionQuality,
  rejectedOpportunities,
}) {
  const largestDetected = scannerStats['Largest Edge Detected'] || null;
  const largestRejectedStat = scannerStats['Largest Edge Rejected'] || null;
  let largestRejectedRow = null;
  for (const row of rejectedOpportunities) {
    const edge = parsePercent(row['Edge %'] || row.edge_pct);
    if (edge === null) continue;
    if (!largestRejectedRow || edge > largestRejectedRow.edge) {
      largestRejectedRow = {
        edge,
        sport: row.Sport || row.sport || 'unknown',
        market: row.Market || row.market || 'unknown',
        book: row.Book || row.book || 'unknown',
        reason: row['Sit Reason'] || row.reason || row.rejection_reason || 'unknown',
      };
    }
  }

  const topReasons = Object.entries(decisionQuality.rejected_by_reason || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([reason, count]) => `${reason} (${count})`);

  const bets = decisionQuality.placed_bets_count || 0;
  const sits = decisionQuality.rejected_plays_count || 0;
  const verdict = bets > 0
    ? `Selective action: ${bets} bet(s), ${sits} sit decision(s).`
    : `Markets were tight. ${sits} sit decision(s) preserved discipline.`;

  return {
    games_scanned: scannerStats['Games Scanned'] || null,
    edges_detected: scannerStats['Edges Detected'] || null,
    bets_placed: bets,
    sits,
    strongest_edge_found: largestDetected,
    strongest_edge_rejected: largestRejectedStat || (largestRejectedRow ? `${largestRejectedRow.edge}%` : null),
    largest_edge_rejected_today: largestRejectedRow ? `${largestRejectedRow.edge}%` : null,
    largest_edge_rejected_context: largestRejectedRow
      ? `${largestRejectedRow.sport} | ${largestRejectedRow.market} | ${largestRejectedRow.book}`
      : null,
    rejection_reason_for_largest_edge: largestRejectedRow ? largestRejectedRow.reason : null,
    top_rejection_reasons: topReasons,
    final_daily_verdict: verdict,
  };
}

function computeSitAccountabilitySummary({ sitAccountability, rejectedOpportunities }) {
  const avoidedLosses = parseAsNumber(sitAccountability['Avoided Losses (count)']);
  const missedWinners = parseAsNumber(sitAccountability['Missed Winners (count)']);
  const netIfFollowedAllSits = parseAsNumber(sitAccountability['Net P/L If Followed All Sits']);
  const netEvRejected = parseAsNumber(sitAccountability['Net EV Rejected']);

  let gradedFromTable = 0;
  let winsFromTable = 0;
  let lossesFromTable = 0;
  let pushesFromTable = 0;
  let counterfactualPlFromTable = 0;
  let hasCounterfactualRows = false;

  const resultKeys = ['If Bet Result', 'Counterfactual Result', 'Result'];
  const plKeys = ['Counterfactual P/L', 'Counterfactual PL', 'If Bet P/L', 'P/L If Bet'];

  for (const row of rejectedOpportunities) {
    const resultRaw = resultKeys.map((k) => row[k]).find(Boolean);
    const plRaw = plKeys.map((k) => row[k]).find(Boolean);
    const result = normalizeDecision(resultRaw);
    const pl = parseAsNumber(plRaw);

    if (result || pl !== null) {
      hasCounterfactualRows = true;
      gradedFromTable += 1;
    }

    if (result === 'win') winsFromTable += 1;
    if (result === 'loss') lossesFromTable += 1;
    if (result === 'push') pushesFromTable += 1;
    if (pl !== null) counterfactualPlFromTable += pl;
  }

  const hasManualLayer = avoidedLosses !== null || missedWinners !== null || netIfFollowedAllSits !== null;
  const wins = hasCounterfactualRows ? winsFromTable : (missedWinners ?? 0);
  const losses = hasCounterfactualRows ? lossesFromTable : (avoidedLosses ?? 0);
  const pushes = hasCounterfactualRows ? pushesFromTable : 0;
  const graded = hasCounterfactualRows ? gradedFromTable : ((wins || 0) + (losses || 0) + (pushes || 0));
  const netCounterfactualPl = hasCounterfactualRows ? counterfactualPlFromTable : netIfFollowedAllSits;

  const moneySavedBySitting = netCounterfactualPl !== null && netCounterfactualPl < 0 ? round2(Math.abs(netCounterfactualPl)) : 0;
  const missedProfitBySitting = netCounterfactualPl !== null && netCounterfactualPl > 0 ? round2(netCounterfactualPl) : 0;
  const sitDecisionWinRateIfBet = safeRate(wins, graded);

  return {
    source: hasCounterfactualRows ? 'rejected_opportunities_table' : (hasManualLayer ? 'sit_accountability_fields' : 'insufficient_data'),
    passed_bets_graded: graded,
    passed_bets_record_if_bet: graded > 0 ? `${wins}-${losses}${pushes > 0 ? `-${pushes}` : ''}` : null,
    passed_bets_wins_if_bet: wins,
    passed_bets_losses_if_bet: losses,
    passed_bets_pushes_if_bet: pushes,
    passed_bets_win_rate_if_bet: sitDecisionWinRateIfBet !== null ? round2(sitDecisionWinRateIfBet * 100) : null,
    money_saved_by_sitting: round2(moneySavedBySitting),
    missed_profit_by_sitting: round2(missedProfitBySitting),
    net_counterfactual_pl_if_bet: round2(netCounterfactualPl),
    net_ev_rejected: round2(netEvRejected),
  };
}

function buildPayload(markdown) {
  const lastUpdatedCt = parseLastUpdated(markdown);
  const targetDate = parseDateFromLastUpdated(lastUpdatedCt);
  const currentStatus = parseBulletMap(extractSection(markdown, 'Current Status'));
  const lifetimeStats = parseBulletMap(extractSection(markdown, 'Lifetime Stats'));
  const rejectionSummary = parseBulletMap(extractSection(markdown, 'Daily Rejection Summary'));
  const sitAccountability = parseBulletMap(extractSection(markdown, 'Sit Accountability'));
  const scannerStats = parseBulletMap(extractSection(markdown, 'Scanner Statistics'));
  const marketConfidence = parseBulletMap(extractSection(markdown, 'Market Confidence'));
  const canonicalDecisionEngine = parseBulletMap(extractSection(markdown, 'Canonical Decision Engine Module'));
  const drawdownGovernor = parseBulletMap(extractSection(markdown, 'Drawdown Governor'));
  const edgeDistribution = parseBulletMap(extractSection(markdown, 'Edge Distribution'));
  const reliabilityIndex = parseBulletMap(extractSection(markdown, 'Reliability Index'));
  const dailySummary = parseBulletMap(extractSection(markdown, 'Daily Summary'));
  const marketTypeReliabilityIndex = parseBulletMap(extractSection(markdown, 'Market Type Reliability Index'));
  const sitReasonCodeStandard = parseBulletMap(extractSection(markdown, 'Sit Reason Code Standard'));
  const ruleLedgerPointer = parseBulletMap(extractSection(markdown, 'Rule Ledger Pointer'));
  const expectationFraming = parseBulletMap(extractSection(markdown, 'Expectation Framing'));
  const executionQuality = parseBulletMap(extractSection(markdown, 'Execution Quality (Slippage)'));
  const weeklyRunningTotals = parseBulletMap(extractSection(markdown, 'Weekly Running Totals'));

  const todaysBetsRaw = parseTable(extractSection(markdown, "Today's Bets"));
  const betLogRaw = parseTable(extractSection(markdown, 'Bet Log (All Graded Bets)'));
  const betLog = dedupeStalePendingBetLog(betLogRaw);
  const rejectedOpportunities = parseTable(
    extractFirstSection(markdown, ['Rejected Opportunities (Today)', 'Rejected Opportunities'])
  );
  const ledger = parseTable(extractSection(markdown, 'Ledger'));
  const pendingBetsRaw = parsePending(extractSection(markdown, 'Pending Bets (awaiting result)'));
  const recLogPath = resolveRecommendationLogPath(markdown, sourcePath);
  const recommendationRows =
    recLogPath && fs.existsSync(recLogPath)
      ? parseRecommendationRows(fs.readFileSync(recLogPath, 'utf8'))
      : [];

  const todaysBets = redactPending
    ? todaysBetsRaw.filter((row) => String(row.Result || '').toUpperCase() !== 'PENDING')
    : todaysBetsRaw;
  const pendingBets = redactPending ? [] : pendingBetsRaw;

  const bankrollValue = parseAsNumber(currentStatus.Bankroll);
  const roiValue = parseAsNumber(lifetimeStats['Overall ROI']);
  const decisionQuality = computeDecisionQuality({
    lastUpdatedCt,
    currentStatus,
    betLog,
    todaysBets,
    rejectedOpportunities,
    rejectionSummary,
    sitAccountability,
    recommendationRows,
  });
  const edgeDistributionTransparency = computeEdgeDistributionTransparency({
    recommendationRows,
    rejectedOpportunities,
    targetDate,
  });
  const dailyDecisionSummary = computeDailyDecisionSummary({
    scannerStats,
    decisionQuality,
    rejectedOpportunities,
  });
  const sitAccountabilitySummary = computeSitAccountabilitySummary({
    sitAccountability,
    rejectedOpportunities,
  });
  const passedOpportunityTracker = computePassedOpportunityTracker({
    recommendationRows,
    targetDate,
  });

  return {
    generated_at_utc: new Date().toISOString(),
    source_file: sourcePath,
    schema: parseSchema(markdown),
    last_updated_ct: lastUpdatedCt,
    current_status: currentStatus,
    lifetime_stats: lifetimeStats,
    daily_rejection_summary: rejectionSummary,
    sit_accountability: sitAccountability,
    scanner_statistics: scannerStats,
    market_confidence: marketConfidence,
    canonical_decision_engine: canonicalDecisionEngine,
    drawdown_governor: drawdownGovernor,
    edge_distribution: edgeDistribution,
    reliability_index: reliabilityIndex,
    daily_summary: { ...dailySummary, ...dailyDecisionSummary },
    daily_decision_summary: dailyDecisionSummary,
    sit_accountability_summary: sitAccountabilitySummary,
    passed_opportunity_tracker: passedOpportunityTracker,
    edge_distribution_transparency: edgeDistributionTransparency,
    market_type_reliability_index: marketTypeReliabilityIndex,
    sit_reason_code_standard: sitReasonCodeStandard,
    rule_ledger_pointer: ruleLedgerPointer,
    expectation_framing: expectationFraming,
    rejected_opportunities: rejectedOpportunities,
    execution_quality: executionQuality,
    weekly_running_totals: weeklyRunningTotals,
    decision_quality: decisionQuality,
    pending_bets: pendingBets,
    todays_bets: todaysBets,
    bet_log: betLog,
    ledger,
    derived: {
      bankroll_numeric: bankrollValue,
      roi_percent_numeric: roiValue,
      today_recommended_count: todaysBets.length,
      pending_count: pendingBets.length,
      graded_bet_count: betLog.length,
      rejected_opportunities_count: rejectedOpportunities.length,
      decision_quality_rate: decisionQuality.decision_quality_rate,
      redacted_pending: redactPending,
    },
  };
}

if (!fs.existsSync(sourcePath)) {
  fail(`Source file not found: ${sourcePath}`);
}

const markdown = fs.readFileSync(sourcePath, 'utf8');
const payload = buildPayload(markdown);

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');

console.log(`Built public data: ${outPath}`);
console.log(`Schema: ${payload.schema} | Last Updated (CT): ${payload.last_updated_ct}`);
if (redactPending) {
  console.log('Pending plays were redacted (REDACT_PENDING=true).');
}

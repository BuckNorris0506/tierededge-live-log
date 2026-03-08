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
  const allowedSitReasons = new Set([
    'no_edge',
    'low_confidence',
    'stale_or_unverified_odds',
    'exposure_cap_reached',
    'breaker_active',
  ]);

  const tierPlacedBets = todaysBets.filter((row) => /^T[123]$/i.test(String(row.Tier || '').trim()));

  const recRowsForDate = recommendationRows.filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const recPlacedRows = recRowsForDate.filter((row) => normalizeDecision(row.decision) === 'bet');
  const recSitRows = recRowsForDate.filter((row) => normalizeDecision(row.decision) === 'sit');

  const placedBetsCount = recPlacedRows.length > 0 ? recPlacedRows.length : tierPlacedBets.length;
  const rejectedFromSummary = parseAsNumber(rejectionSummary['Total Rejected']) || 0;
  const rejectedPlaysCount =
    recSitRows.length > 0
      ? recSitRows.length
      : (rejectedOpportunities.length > 0 ? rejectedOpportunities.length : rejectedFromSummary);

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
      reason: normalizeReason(row.rejection_reason),
      edge: parsePercent(row.edge_pct),
    }));
  } else if (rejectedOpportunities.length > 0) {
    sitQualityRows = rejectedOpportunities.map((row) => ({
      reason: normalizeReason(row['Sit Reason'] || row.reason || row.rejection_reason),
      edge: parsePercent(row['Edge %'] || row.edge_pct),
    }));
  }

  let highQualitySitDecisions = 0;
  if (sitQualityRows.length > 0) {
    highQualitySitDecisions = sitQualityRows.filter((row) => {
      if (!allowedSitReasons.has(row.reason)) return false;
      if (row.reason === 'no_edge' && row.edge !== null) return row.edge < 2.0;
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
    if (!row.reason) continue;
    rejectedByReason[row.reason] = (rejectedByReason[row.reason] || 0) + 1;
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
    decision_quality_rate: decisionQualityRate !== null ? round2(decisionQualityRate * 100) : null,
    total_decisions: totalDecisions,
    rejected_by_reason: rejectedByReason,
  };
}

function buildPayload(markdown) {
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
  const expectationFraming = parseBulletMap(extractSection(markdown, 'Expectation Framing'));
  const executionQuality = parseBulletMap(extractSection(markdown, 'Execution Quality (Slippage)'));
  const weeklyRunningTotals = parseBulletMap(extractSection(markdown, 'Weekly Running Totals'));

  const todaysBetsRaw = parseTable(extractSection(markdown, "Today's Bets"));
  const betLog = parseTable(extractSection(markdown, 'Bet Log (All Graded Bets)'));
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
    lastUpdatedCt: parseLastUpdated(markdown),
    currentStatus,
    betLog,
    todaysBets,
    rejectedOpportunities,
    rejectionSummary,
    sitAccountability,
    recommendationRows,
  });

  return {
    generated_at_utc: new Date().toISOString(),
    source_file: sourcePath,
    schema: parseSchema(markdown),
    last_updated_ct: parseLastUpdated(markdown),
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
    daily_summary: dailySummary,
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

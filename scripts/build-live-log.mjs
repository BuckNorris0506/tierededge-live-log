import fs from 'node:fs';
import path from 'node:path';
import { readRuntimeStatusSnapshot } from './openclaw-runtime-utils.mjs';
import {
  buildCandidateMarketRows,
  buildSuppressedCandidateRows,
  buildSuppressionSummary,
  CANDIDATE_MARKET_HEADERS,
  SUPPRESSED_CANDIDATE_HEADERS,
  writeCsv,
  parseAsNumber as parseAuditNumber,
} from './suppression-audit-utils.mjs';
import {
  readNativeDecisionLedger,
  DEFAULT_NATIVE_ALL_LEDGER,
  DEFAULT_NATIVE_BETS_LEDGER,
  DEFAULT_NATIVE_PASS_LEDGER,
  DEFAULT_NATIVE_SUPPRESSED_LEDGER,
} from './native-decision-log-utils.mjs';
import {
  computeScanCoverageArtifacts,
  DEFAULT_SCAN_COVERAGE_POLICY,
} from './scan-coverage-utils.mjs';

const DEFAULT_SOURCE = '/Users/jaredbuckman/.openclaw/workspace/memory/betting-state.md';
const DEFAULT_OUT = path.resolve(process.cwd(), 'public', 'data.json');
const DEFAULT_PASSED_GRADES = '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json';
const DEFAULT_MARKET_CONTEXT_HOOKS = path.resolve(process.cwd(), 'config', 'market-context-hooks.json');
const DEFAULT_CONTRIBUTION_LEDGER = path.resolve(process.cwd(), 'data', 'bankroll-contributions.csv');
const DEFAULT_CONTRIBUTION_STATUS = path.resolve(process.cwd(), 'data', 'bankroll-contribution-status.json');
const DEFAULT_SCAN_POLICY = DEFAULT_SCAN_COVERAGE_POLICY;
const DEFAULT_CANDIDATE_MARKETS = path.resolve(process.cwd(), 'data', 'candidate-markets.csv');
const DEFAULT_SUPPRESSED_CANDIDATES = path.resolve(process.cwd(), 'data', 'suppressed-candidates.csv');
const DEFAULT_CANONICAL_STATE = path.resolve(process.cwd(), 'data', 'canonical-state.json');
const DEFAULT_RUN_ARTIFACTS = path.resolve(process.cwd(), 'data', 'run-artifacts.json');
const DEFAULT_BETS_LEDGER = path.resolve(process.cwd(), 'data', 'bets-ledger.json');
const DEFAULT_PASS_LEDGER = path.resolve(process.cwd(), 'data', 'passes-ledger.json');
const DEFAULT_SUPPRESSED_LEDGER = path.resolve(process.cwd(), 'data', 'suppressed-ledger.json');
const DEFAULT_GRADING_LEDGER = path.resolve(process.cwd(), 'data', 'grading-ledger.json');
const DEFAULT_CONTRIBUTIONS_LEDGER_JSON = path.resolve(process.cwd(), 'data', 'contributions-ledger.json');
const DATA_FRESHNESS_MAX_HOURS = 36;
const BANKROLL_CONTINUITY_MAX_DELTA_PCT = 0.1;
const BANKROLL_CONTINUITY_MAX_DELTA_ABS = 25;
const BANKROLL_FORMULA_MAX_DELTA_PCT = 0.01;
const BANKROLL_FORMULA_MAX_DELTA_ABS = 5;

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

function extractDatedSection(markdown, title, dateKey) {
  if (!dateKey) return '';
  const date = new Date(`${dateKey}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return '';
  const month = date.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
  const day = date.toLocaleString('en-US', { day: 'numeric', timeZone: 'UTC' });
  const expectedHeader = `## ${title} (${month} ${day})`;
  const start = markdown.indexOf(expectedHeader);
  if (start === -1) return '';

  const lineEnd = markdown.indexOf('\n', start);
  if (lineEnd === -1) return '';
  const rest = markdown.slice(lineEnd + 1);
  const nextHeaderOffset = rest.search(/\n## /);
  if (nextHeaderOffset === -1) return rest.trim();
  return rest.slice(0, nextHeaderOffset).trim();
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
    if (rowParts.length < headers.length) continue;
    // Keep backward compatibility with historical rows that may include
    // an extra trailing notes column beyond the canonical header set.
    const normalizedParts = rowParts.slice(0, headers.length);
    const row = {};
    for (let j = 0; j < headers.length; j += 1) {
      row[headers[j]] = normalizedParts[j];
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
      continue;
    }
    if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out;
}

function readContributionLedger() {
  const out = {
    path: DEFAULT_CONTRIBUTION_LEDGER,
    entries: [],
  };
  if (!fs.existsSync(DEFAULT_CONTRIBUTION_LEDGER)) return out;
  const raw = fs.readFileSync(DEFAULT_CONTRIBUTION_LEDGER, 'utf8');
  const lines = raw.split('\n').map((line) => line.trim()).filter(Boolean);
  if (lines.length <= 1) return out;
  const headers = parseCsvLine(lines[0]).map((h) => String(h || '').trim());
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvLine(lines[i]);
    if (parts.length < headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) {
      row[headers[j]] = String(parts[j] || '').trim();
    }
    let realizedValues = [];
    const basisMonthCount = parseAsNumber(row.basis_month_count ?? row.basis_months_used);
    const basisMonthsUsed = String(row.basis_months_used || '')
      .split(/[|,]/)
      .map((s) => s.trim())
      .filter(Boolean);
    try {
      const parsed = JSON.parse(row.realized_profit_values_used || '[]');
      if (Array.isArray(parsed)) realizedValues = parsed.map((n) => round2(parseAsNumber(n))).filter((n) => n !== null);
    } catch {
      realizedValues = String(row.realized_profit_values_used || '')
        .split(/[|;]/)
        .map((n) => round2(parseAsNumber(n)))
        .filter((n) => n !== null);
    }
    out.entries.push({
      contribution_date: row.contribution_date || null,
      effective_month: row.effective_month || null,
      contribution_amount: round2(parseAsNumber(row.contribution_amount)),
      basis_month_count: basisMonthCount,
      basis_months_used: basisMonthsUsed,
      realized_profit_values_used: realizedValues,
      rolling_average_realized_profit: round2(parseAsNumber(row.rolling_average_realized_profit ?? row.rolling_average_profit)),
      entry_source: row.entry_source || null,
      notes: row.notes || null,
    });
  }
  out.entries.sort((a, b) => {
    const left = parseTimestampMs(a.contribution_date || '') || 0;
    const right = parseTimestampMs(b.contribution_date || '') || 0;
    return left - right;
  });
  return out;
}

function readContributionAutomationStatus() {
  try {
    return JSON.parse(fs.readFileSync(DEFAULT_CONTRIBUTION_STATUS, 'utf8'));
  } catch {
    return {
      status: 'unknown',
      reason: 'status_file_missing',
      last_run_ct: null,
      effective_month: null,
      appended: false,
      next_expected_cycle: null,
    };
  }
}

function readRuntimeStatus() {
  return readRuntimeStatusSnapshot() || {
    jobs: {},
    latest_successful_hunt: null,
    latest_successful_grading: null,
    next_edge_scan_ct: '06:00',
    freshness_anchor: {
      source: 'state_last_updated',
      timestamp_ct: null,
      timestamp_ms: null,
    },
    state_sync: {
      blocking_sync_gap: false,
    },
    warnings: ['runtime_status_missing'],
  };
}

function computeEffectiveLastUpdatedCt(lastUpdatedCt, runtimeStatus) {
  const stateMs = parseTimestampMs(lastUpdatedCt);
  const runtimeAnchorMs = parseTimestampMs(runtimeStatus?.freshness_anchor?.timestamp_ct);
  if (
    runtimeStatus?.state_sync?.blocking_sync_gap !== true
    && Number.isFinite(runtimeAnchorMs)
    && (!Number.isFinite(stateMs) || runtimeAnchorMs > stateMs)
  ) {
    return runtimeStatus.freshness_anchor.timestamp_ct;
  }
  return lastUpdatedCt;
}

function filterPlaceholderBetRows(rows) {
  return (rows || []).filter((row) => {
    const bet = String(row?.Bet || row?.bet || '').trim().toUpperCase();
    const tier = String(row?.Tier || row?.tier || '').trim().toUpperCase();
    if (bet === 'NO PLAYS') return false;
    if (!bet && !tier) return false;
    return true;
  });
}

function writeJson(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function mapNativeDecisionRowsToCandidateRows(rows) {
  return (rows || []).map((row) => ({
    run_id: row.run_id || '',
    scan_time_ct: row.timestamp_ct || '',
    event_id: row.event_id || '',
    sport: row.sport || '',
    league: row.league || '',
    market_type: row.market_type || '',
    selection: row.selection || '',
    book: row.sportsbook || '',
    odds_american: row.odds_american ?? '',
    odds_decimal: row.odds_decimal ?? '',
    devig_implied_prob: row.devig_implied_prob ?? '',
    consensus_prob: row.consensus_prob ?? '',
    pre_conf_true_prob: row.pre_conf_true_prob ?? '',
    confidence_score: row.confidence_score ?? '',
    post_conf_true_prob: row.post_conf_true_prob ?? '',
    raw_edge_pct: row.raw_edge_pct ?? '',
    post_conf_edge_pct: row.post_conf_edge_pct ?? '',
    tier_threshold_pct: row.tier_threshold_pct ?? '',
    price_edge_pass: row.price_edge_pass === true,
    bet_permission_pass: row.bet_permission_pass === true,
    final_decision: row.final_decision || '',
    rejection_stage: row.rejection_stage || '',
    rejection_reason: row.rejection_reason || '',
    rec_id: row.rec_id || '',
    event_label: row.event_label || '',
    bet_class: row.bet_class || '',
    include_in_core_strategy_metrics: row.include_in_core_strategy_metrics,
    include_in_actual_bankroll: row.include_in_actual_bankroll,
  }));
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

function isObservationBandPassRow(row) {
  if (normalizeDecision(row?.decision) !== 'sit') return false;
  const edge = parsePercent(row?.edge_pct);
  return edge !== null && edge > 0 && edge < 2;
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

function readPassedGradesCache() {
  try {
    return JSON.parse(fs.readFileSync(DEFAULT_PASSED_GRADES, 'utf8'));
  } catch {
    return { entries: {} };
  }
}

function readMarketContextHooksConfig() {
  const defaults = {
    enabled: true,
    mode: 'advisory_only',
    stale_after_hours: 18,
    confidence_modifiers: {
      market_leader_confirmed: 0.04,
      lineup_or_injury_uncertain: -0.06,
      rest_or_travel_disadvantage: -0.03,
      stale_context_signal: -0.03,
      unverified_context_signal: -0.04,
    },
    required_for_application: {
      verification_status: ['verified', 'confirmed'],
      stale_flag_must_be_false: true,
    },
  };
  try {
    const parsed = JSON.parse(fs.readFileSync(DEFAULT_MARKET_CONTEXT_HOOKS, 'utf8'));
    return {
      ...defaults,
      ...parsed,
      confidence_modifiers: {
        ...defaults.confidence_modifiers,
        ...(parsed?.confidence_modifiers || {}),
      },
      required_for_application: {
        ...defaults.required_for_application,
        ...(parsed?.required_for_application || {}),
      },
    };
  } catch {
    return defaults;
  }
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

function parseMonthKey(text) {
  const match = String(text || '').match(/(\d{4})-(\d{2})/);
  return match ? `${match[1]}-${match[2]}` : null;
}

function normalizeStatusValueForTargetDate(value, targetDate) {
  const raw = String(value || '').trim();
  if (!raw || !targetDate) return raw || null;
  const date = new Date(`${targetDate}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return raw;
  const month = date.toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });
  const day = date.toLocaleString('en-US', { day: 'numeric', timeZone: 'UTC' });
  const monthDayPattern = new RegExp(`${month}\\s+${day}\\b`, 'i');
  const containsAnyMonthDay = /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}\b/i.test(raw);
  if (containsAnyMonthDay && !monthDayPattern.test(raw)) {
    return 'N/A (stale status field)';
  }
  return raw;
}

function monthKeyFromDateKey(dateKey) {
  const match = String(dateKey || '').match(/^(\d{4})-(\d{2})-\d{2}$/);
  return match ? `${match[1]}-${match[2]}` : null;
}

function monthToIndex(monthKey) {
  const match = String(monthKey || '').match(/^(\d{4})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  return (year * 12) + (month - 1);
}

function indexToMonth(index) {
  if (!Number.isFinite(index)) return null;
  const year = Math.floor(index / 12);
  const month = (index % 12) + 1;
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}`;
}

function listMonthsInclusive(startMonth, endMonth) {
  const startIdx = monthToIndex(startMonth);
  const endIdx = monthToIndex(endMonth);
  if (!Number.isFinite(startIdx) || !Number.isFinite(endIdx) || startIdx > endIdx) return [];
  const out = [];
  for (let i = startIdx; i <= endIdx; i += 1) out.push(indexToMonth(i));
  return out;
}

function previousMonth(monthKey) {
  const idx = monthToIndex(monthKey);
  if (!Number.isFinite(idx)) return null;
  return indexToMonth(idx - 1);
}

function formatDateKeyFromMs(tsMs) {
  if (!Number.isFinite(tsMs)) return null;
  return new Date(tsMs).toISOString().slice(0, 10);
}

function parseTimestampMs(input) {
  const value = String(input || '').trim();
  if (!value) return null;
  const parsed = Date.parse(value);
  if (Number.isFinite(parsed)) return parsed;
  const dateOnly = value.match(/(\d{4}-\d{2}-\d{2})/);
  if (dateOnly) {
    const fallback = Date.parse(`${dateOnly[1]}T00:00:00Z`);
    return Number.isFinite(fallback) ? fallback : null;
  }
  return null;
}

function computeBankrollContributionPolicy({
  betLog,
  ledger,
  contributionLedgerEntries,
  currentStatus,
  lastUpdatedCt,
}) {
  const realizedMonthlyProfitMap = {};
  let realizedLifetimeProfit = 0;
  let coreEdgeProfitLifetime = 0;
  let funSgpProfitLifetime = 0;
  for (const row of betLog) {
    const result = normalizeDecision(row.Result);
    if (!result || result === 'pending') continue;
    const pl = parseAsNumber(row['P/L']);
    if (pl === null) continue;
    realizedLifetimeProfit += pl;
    if (row.bet_class === 'EDGE_BET') coreEdgeProfitLifetime += pl;
    if (row.bet_class === 'FUN_SGP') funSgpProfitLifetime += pl;
    const monthKey = monthKeyFromDateKey(String(row.Date || '').trim());
    if (!monthKey) continue;
    realizedMonthlyProfitMap[monthKey] = (realizedMonthlyProfitMap[monthKey] || 0) + pl;
  }

  const ledgerEntries = Array.isArray(ledger) ? ledger : [];
  const externalTypes = new Set(['DEPOSIT', 'CONTRIBUTION', 'RELOAD']);
  let ledgerExternalContributions = 0;
  const ledgerExternalContributionsByMonth = {};
  let startingBankroll = null;
  for (const row of ledgerEntries) {
    const amount = parseAsNumber(row.Amount);
    if (amount === null) continue;
    if (startingBankroll === null) startingBankroll = amount;
    const type = String(row.Type || '').trim().toUpperCase();
    if (externalTypes.has(type)) {
      ledgerExternalContributions += amount;
      const month = parseMonthKey(row.Date);
      if (month) ledgerExternalContributionsByMonth[month] = (ledgerExternalContributionsByMonth[month] || 0) + amount;
    }
  }

  const policyContributionTotal = (contributionLedgerEntries || [])
    .map((entry) => parseAsNumber(entry.contribution_amount))
    .filter((n) => n !== null)
    .reduce((a, b) => a + b, 0);
  const totalExternalContributions =
    policyContributionTotal > 0 ? policyContributionTotal : ledgerExternalContributions;

  const currentBankroll = parseAsNumber(currentStatus?.Bankroll);
  if (startingBankroll === null && currentBankroll !== null) {
    startingBankroll = round2(currentBankroll - totalExternalContributions - realizedLifetimeProfit);
  }

  const anchorDate = parseDateFromLastUpdated(lastUpdatedCt);
  const anchorMonth = parseMonthKey(anchorDate);
  const completedMonth = anchorMonth ? previousMonth(anchorMonth) : null;

  const monthKeysFromBets = Object.keys(realizedMonthlyProfitMap).sort();
  const monthKeysFromLedger = ledgerEntries
    .map((row) => parseMonthKey(row.Date))
    .filter(Boolean)
    .sort();
  const firstKnownMonth = monthKeysFromLedger[0] || monthKeysFromBets[0] || completedMonth;
  const completedMonths = (firstKnownMonth && completedMonth)
    ? listMonthsInclusive(firstKnownMonth, completedMonth)
    : [];

  const contributionBasisMonthCount = Math.min(3, completedMonths.length);
  const contributionBasisMonthsUsed = contributionBasisMonthCount > 0
    ? completedMonths.slice(-contributionBasisMonthCount)
    : [];
  const realizedProfitValuesUsed = contributionBasisMonthsUsed.map((monthKey) =>
    round2(realizedMonthlyProfitMap[monthKey] || 0)
  );
  const rollingAverageRealizedProfit = realizedProfitValuesUsed.length > 0
    ? round2(realizedProfitValuesUsed.reduce((a, b) => a + b, 0) / realizedProfitValuesUsed.length)
    : 0;
  const nextEstimatedContribution = round2(Math.max(0, rollingAverageRealizedProfit || 0));

  const currentMonthRealizedProfit = anchorMonth ? round2(realizedMonthlyProfitMap[anchorMonth] || 0) : null;
  const realizedMonthlyProfitExContributions = currentMonthRealizedProfit;
  const contributionBasisProfit = realizedProfitValuesUsed.length > 0
    ? round2(realizedProfitValuesUsed[realizedProfitValuesUsed.length - 1])
    : null;

  const lastContribution = (contributionLedgerEntries || []).length > 0
    ? contributionLedgerEntries[contributionLedgerEntries.length - 1]
    : null;

  const bankrollGrowthFromContributions = round2(totalExternalContributions);
  const bankrollGrowthFromBetting = round2(realizedLifetimeProfit);
  const strategyEquity =
    startingBankroll !== null
      ? round2(startingBankroll + bankrollGrowthFromBetting)
      : null;
  const coreStrategyEquity =
    startingBankroll !== null
      ? round2(startingBankroll + coreEdgeProfitLifetime)
      : null;
  const actualBankroll =
    startingBankroll !== null
      ? round2(startingBankroll + bankrollGrowthFromBetting + bankrollGrowthFromContributions)
      : null;
  const bankrollFormulaDiff =
    currentBankroll !== null && actualBankroll !== null
      ? round2(currentBankroll - actualBankroll)
      : null;

  const allMonthKeys = new Set([
    ...Object.keys(realizedMonthlyProfitMap),
    ...Object.keys(ledgerExternalContributionsByMonth),
  ]);
  if (firstKnownMonth) allMonthKeys.add(firstKnownMonth);
  if (anchorMonth) allMonthKeys.add(anchorMonth);
  const sortedMonthKeys = [...allMonthKeys].sort();

  let runningRealized = 0;
  let runningContrib = 0;
  const strategyEquityByMonth = {};
  const actualBankrollByMonth = {};
  for (const month of sortedMonthKeys) {
    runningRealized += (realizedMonthlyProfitMap[month] || 0);
    runningContrib += (ledgerExternalContributionsByMonth[month] || 0);
    if (startingBankroll !== null) {
      strategyEquityByMonth[month] = round2(startingBankroll + runningRealized);
      actualBankrollByMonth[month] = round2(startingBankroll + runningRealized + runningContrib);
    }
  }

  let monthlyInterpretation = 'No monthly contribution scheduled based on non-positive rolling profit.';
  if (nextEstimatedContribution > 0) {
    monthlyInterpretation = 'Next monthly contribution is scheduled from rolling realized betting profit.';
  }
  if (currentMonthRealizedProfit !== null && nextEstimatedContribution > 0) {
    const absProfit = Math.abs(currentMonthRealizedProfit);
    const absContribution = Math.abs(nextEstimatedContribution);
    if (absProfit > absContribution) monthlyInterpretation = 'Bankroll growth this month came primarily from betting profit.';
    if (absContribution > absProfit) monthlyInterpretation = 'Bankroll growth this month came primarily from external contribution.';
  }

  return {
    contribution_ledger_path: DEFAULT_CONTRIBUTION_LEDGER,
    contribution_ledger_entries: contributionLedgerEntries || [],
    starting_bankroll: round2(startingBankroll),
    current_bankroll: round2(currentBankroll),
    reported_current_bankroll: round2(currentBankroll),
    actual_bankroll: actualBankroll,
    strategy_equity: strategyEquity,
    overall_strategy_equity: strategyEquity,
    core_strategy_equity: round2(coreStrategyEquity),
    total_external_contributions: round2(totalExternalContributions),
    total_external_contributions_from_ledger: round2(ledgerExternalContributions),
    total_policy_contributions_recorded: round2(policyContributionTotal),
    realized_betting_profit_lifetime: round2(realizedLifetimeProfit),
    overall_betting_profit_lifetime: round2(realizedLifetimeProfit),
    core_edge_profit_lifetime: round2(coreEdgeProfitLifetime),
    fun_sgp_profit_lifetime: round2(funSgpProfitLifetime),
    bankroll_growth_from_betting: bankrollGrowthFromBetting,
    bankroll_growth_from_contributions: bankrollGrowthFromContributions,
    bankroll_formula_difference: bankrollFormulaDiff,
    realized_monthly_profit: currentMonthRealizedProfit,
    realized_monthly_profit_ex_contributions: realizedMonthlyProfitExContributions,
    contribution_basis_profit: contributionBasisProfit,
    realized_monthly_profit_map: Object.fromEntries(
      Object.entries(realizedMonthlyProfitMap)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => [k, round2(v)])
    ),
    external_contributions_monthly_map: Object.fromEntries(
      Object.entries(ledgerExternalContributionsByMonth)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => [k, round2(v)])
    ),
    strategy_equity_monthly_map: strategyEquityByMonth,
    actual_bankroll_monthly_map: actualBankrollByMonth,
    rolling_average_realized_profit: rollingAverageRealizedProfit,
    next_estimated_contribution: nextEstimatedContribution,
    contribution_basis_month_count: contributionBasisMonthCount,
    contribution_basis_months_used: contributionBasisMonthsUsed,
    realized_profit_values_used: realizedProfitValuesUsed,
    last_contribution_amount: round2(parseAsNumber(lastContribution?.contribution_amount)),
    last_contribution_date: lastContribution?.contribution_date || null,
    last_contribution_effective_month: lastContribution?.effective_month || null,
    monthly_interpretation: monthlyInterpretation,
    notes: {
      actual_bankroll_includes_external_contributions: true,
      strategy_equity_excludes_external_contributions: true,
      units_remain_primary_strategy_metric: true,
    },
  };
}

function computeDerivedLifetimeStats({ betLog, startingBankroll }) {
  const settledRows = filterSettledRows(betLog);
  const winRows = settledRows.filter((row) => normalizeDecision(row.Result) === 'win');
  const lossRows = settledRows.filter((row) => ['loss', 'cashed out'].includes(normalizeDecision(row.Result)));
  const totalStake = settledRows.reduce((sum, row) => sum + (parseAsNumber(row.Stake) || 0), 0);
  const totalProfit = settledRows.reduce((sum, row) => sum + (parseAsNumber(row['P/L']) || 0), 0);
  const roi = totalStake > 0 ? round2((totalProfit / totalStake) * 100) : null;
  const clvValues = settledRows
    .filter((row) => row.include_in_core_strategy_metrics !== false)
    .map((row) => parseClvValue(row.CLV))
    .filter((n) => n !== null);
  const avgClv = clvValues.length > 0 ? round2(clvValues.reduce((a, b) => a + b, 0) / clvValues.length) : null;
  const winRate = settledRows.length > 0 ? round2((winRows.length / settledRows.length) * 100) : null;
  const endingBankroll = parseAsNumber(settledRows[settledRows.length - 1]?.Bankroll);

  return {
    'Total Bets': String(settledRows.length),
    'Win Rate': settledRows.length > 0 ? `${winRate}% (${winRows.length}-${lossRows.length})` : 'N/A',
    'Overall ROI': roi !== null ? formatPctSigned(roi) : 'N/A',
    'Average CLV': avgClv !== null ? formatPctSigned(avgClv) : 'N/A',
    total_bets_numeric: settledRows.length,
    wins_numeric: winRows.length,
    losses_numeric: lossRows.length,
    overall_roi_numeric: roi,
    average_clv_numeric: avgClv,
    realized_profit_numeric: round2(totalProfit),
    total_stake_numeric: round2(totalStake),
    starting_bankroll_numeric: round2(startingBankroll),
    ending_bankroll_numeric: round2(endingBankroll),
  };
}

function computeDerivedWeeklyRunningTotals({ betLog, anchorDateKey }) {
  const anchorMs = parseTimestampMs(anchorDateKey ? `${anchorDateKey}T00:00:00Z` : null);
  const rows = filterSettledRows(betLog).filter((row) => {
    if (!Number.isFinite(anchorMs)) return true;
    const rowMs = parseTimestampMs(row.Date ? `${row.Date}T00:00:00Z` : null);
    if (!Number.isFinite(rowMs)) return false;
    return rowMs >= anchorMs - (6 * 24 * 60 * 60 * 1000) && rowMs <= anchorMs;
  });
  const wins = rows.filter((row) => normalizeDecision(row.Result) === 'win').length;
  const losses = rows.filter((row) => ['loss', 'cashed out'].includes(normalizeDecision(row.Result))).length;
  const pending = (betLog || []).filter((row) => normalizeDecision(row.Result) === 'pending').length;
  const totalStake = rows.reduce((sum, row) => sum + (parseAsNumber(row.Stake) || 0), 0);
  const totalProfit = rows.reduce((sum, row) => sum + (parseAsNumber(row['P/L']) || 0), 0);
  const roi = totalStake > 0 ? round2((totalProfit / totalStake) * 100) : null;
  const clvValues = rows
    .filter((row) => row.include_in_core_strategy_metrics !== false)
    .map((row) => parseClvValue(row.CLV))
    .filter((n) => n !== null);
  const avgClv = clvValues.length > 0 ? round2(clvValues.reduce((a, b) => a + b, 0) / clvValues.length) : null;

  return {
    'Bets': `${rows.length} | W: ${wins} | L: ${losses} | Push: 0 | Void: 0 | Half-Win: 0 | Half-Loss: 0 | Pending: ${pending}`,
    'ROI': `${roi !== null ? formatPctSigned(roi) : 'N/A'} | Avg CLV: ${avgClv !== null ? formatPctSigned(avgClv) : 'N/A'}`,
    bets_numeric: rows.length,
    wins_numeric: wins,
    losses_numeric: losses,
    pending_numeric: pending,
    roi_numeric: roi,
    average_clv_numeric: avgClv,
    realized_profit_numeric: round2(totalProfit),
  };
}

function parseBooleanLike(value) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return null;
  if (['true', '1', 'yes', 'y', 'on', 'confirmed', 'verified', 'active'].includes(raw)) return true;
  if (['false', '0', 'no', 'n', 'off', 'unconfirmed', 'unverified', 'inactive'].includes(raw)) return false;
  return null;
}

function normalizeVerificationStatus(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return null;
  if (['verified', 'confirm', 'confirmed', 'ok'].includes(raw)) return 'verified';
  if (['unverified', 'unknown', 'pending', 'unclear'].includes(raw)) return 'unverified';
  if (['stale', 'expired'].includes(raw)) return 'stale';
  return raw;
}

function parseMarketContextFromRecommendationRow(row, staleAfterHours) {
  const get = (...keys) => {
    for (const key of keys) {
      if (row[key] !== undefined && row[key] !== null && String(row[key]).trim() !== '') return row[key];
    }
    return null;
  };

  const marketLeaderPrice = get('market_leader_price', 'Market Leader Price');
  const marketLeaderMovement = get('market_leader_movement', 'Market Leader Movement');
  const injuryFlag = parseBooleanLike(get('injury_confirmation_flag', 'Injury Confirmation Flag'));
  const lineupFlag = parseBooleanLike(get('lineup_confirmation_flag', 'Lineup Confirmation Flag'));
  const restFlag = parseBooleanLike(get('rest_disadvantage_flag', 'Rest Disadvantage Flag'));
  const travelFlag = parseBooleanLike(get('travel_disadvantage_flag', 'Travel Disadvantage Flag'));
  const source = get('market_context_source', 'Market Context Source');
  const contextTimestamp = get('context_timestamp', 'Context Timestamp');
  const verificationStatus = normalizeVerificationStatus(get('context_verification_status', 'Context Verification Status'));
  const staleFlagRaw = parseBooleanLike(get('context_stale_flag', 'Context Stale Flag'));

  const hasAnySignal = [
    marketLeaderPrice,
    marketLeaderMovement,
    injuryFlag,
    lineupFlag,
    restFlag,
    travelFlag,
    source,
    contextTimestamp,
    verificationStatus,
    staleFlagRaw,
  ].some((value) => value !== null);

  if (!hasAnySignal) return null;

  const contextTsMs = parseTimestampMs(contextTimestamp);
  const recTsMs = parseTimestampMs(row.timestamp_ct);
  const staleByTime =
    contextTsMs !== null && recTsMs !== null
      ? ((recTsMs - contextTsMs) / (1000 * 60 * 60)) > staleAfterHours
      : null;
  const staleFlag = staleFlagRaw !== null ? staleFlagRaw : staleByTime;

  return {
    rec_id: row.rec_id || null,
    sport: row.sport || null,
    market: row.market || null,
    selection: row.selection || null,
    market_leader_price: marketLeaderPrice,
    market_leader_movement: marketLeaderMovement,
    injury_confirmation_flag: injuryFlag,
    lineup_confirmation_flag: lineupFlag,
    rest_disadvantage_flag: restFlag,
    travel_disadvantage_flag: travelFlag,
    market_context_source: source,
    context_timestamp: contextTimestamp || null,
    context_verification_status: verificationStatus,
    context_stale_flag: staleFlag,
  };
}

function computeMarketContextAudit({ recommendationRows, targetDate, config }) {
  const rowsForDate = recommendationRows.filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const signals = rowsForDate
    .map((row) => parseMarketContextFromRecommendationRow(row, config.stale_after_hours))
    .filter(Boolean);

  if (signals.length === 0) {
    return {
      enabled: Boolean(config.enabled),
      mode: config.mode,
      total_signals_observed: 0,
      applied_signal_count: 0,
      stale_signals_count: 0,
      unverified_signals_count: 0,
      by_source: {},
      confidence_modifier_suggested_avg: null,
      decision_notes: [],
      sample_signals: [],
    };
  }

  const requiredStatuses = new Set(
    (config.required_for_application?.verification_status || [])
      .map((value) => normalizeVerificationStatus(value))
      .filter(Boolean)
  );

  let appliedSignalCount = 0;
  let staleSignalsCount = 0;
  let unverifiedSignalsCount = 0;
  let modifierTotal = 0;
  const bySource = {};
  let leaderConfirmedCount = 0;
  let uncertaintyCount = 0;

  for (const signal of signals) {
    const source = signal.market_context_source || 'unknown';
    bySource[source] = (bySource[source] || 0) + 1;

    const isStale = signal.context_stale_flag === true;
    const isVerified =
      requiredStatuses.size === 0
        ? true
        : requiredStatuses.has(signal.context_verification_status);

    if (isStale) staleSignalsCount += 1;
    if (!isVerified) unverifiedSignalsCount += 1;

    const shouldApply =
      Boolean(config.enabled)
      && isVerified
      && (
        config.required_for_application?.stale_flag_must_be_false === true
          ? signal.context_stale_flag !== true
          : true
      );

    if (!shouldApply) continue;
    appliedSignalCount += 1;

    let modifier = 0;
    if (signal.market_leader_movement) {
      modifier += config.confidence_modifiers.market_leader_confirmed || 0;
      leaderConfirmedCount += 1;
    }
    if (signal.injury_confirmation_flag === false || signal.lineup_confirmation_flag === false) {
      modifier += config.confidence_modifiers.lineup_or_injury_uncertain || 0;
      uncertaintyCount += 1;
    }
    if (signal.rest_disadvantage_flag === true || signal.travel_disadvantage_flag === true) {
      modifier += config.confidence_modifiers.rest_or_travel_disadvantage || 0;
      uncertaintyCount += 1;
    }
    if (signal.context_stale_flag === true) {
      modifier += config.confidence_modifiers.stale_context_signal || 0;
    }
    if (!isVerified) {
      modifier += config.confidence_modifiers.unverified_context_signal || 0;
    }
    modifierTotal += modifier;
  }

  const decisionNotes = [];
  if (leaderConfirmedCount > 0) decisionNotes.push('Market-leader movement confirmed on selected markets.');
  if (uncertaintyCount > 0) decisionNotes.push('Lineup/injury/rest-travel context lowered confidence on select markets.');
  if (staleSignalsCount > 0) decisionNotes.push('Context signal stale — ignored where verification requirements were not met.');
  if (unverifiedSignalsCount > 0) decisionNotes.push('Unverified context signals were logged but not applied.');

  return {
    enabled: Boolean(config.enabled),
    mode: config.mode,
    total_signals_observed: signals.length,
    applied_signal_count: appliedSignalCount,
    stale_signals_count: staleSignalsCount,
    unverified_signals_count: unverifiedSignalsCount,
    by_source: bySource,
    confidence_modifier_suggested_avg: appliedSignalCount > 0 ? round2(modifierTotal / appliedSignalCount) : null,
    decision_notes: decisionNotes,
    sample_signals: signals.slice(0, 5),
  };
}

function round2(n) {
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

function safeRate(num, den) {
  if (!den) return null;
  return num / den;
}

function toDecimalOddsFromBetRow(row) {
  const dec = parseAsNumber(row['Odds (Dec)'] || row.odds_dec);
  if (dec !== null && dec > 1) return dec;

  const us = parseAsNumber(row['Odds (US)'] || row.odds_us);
  if (us === null || us === 0) return null;
  if (us > 0) return 1 + (us / 100);
  return 1 + (100 / Math.abs(us));
}

function erfApprox(x) {
  // Abramowitz and Stegun 7.1.26 approximation.
  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * absX);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const poly = (((((a5 * t) + a4) * t + a3) * t + a2) * t + a1) * t;
  const y = 1 - (poly * Math.exp(-(absX * absX)));
  return sign * y;
}

function stdNormalCdf(z) {
  if (!Number.isFinite(z)) return null;
  return 0.5 * (1 + erfApprox(z / Math.SQRT2));
}

function classifySampleStatus(sampleSize, pValue) {
  if (!Number.isFinite(sampleSize) || sampleSize < 30) return 'small sample (statistically inconclusive)';
  if (pValue === null || pValue === undefined || !Number.isFinite(pValue)) return 'statistically inconclusive';
  if (sampleSize >= 100 && pValue < 0.05) return 'meaningful evidence';
  if (sampleSize >= 50 && pValue < 0.2) return 'emerging signal';
  return 'statistically inconclusive';
}

function computeBinomialSummaryFromRows(rows) {
  const outcomes = rows
    .map((row) => ({
      result: normalizeDecision(row.Result || row.result),
      decimal_odds: toDecimalOddsFromBetRow(row),
    }))
    .filter((row) => row.result === 'win' || row.result === 'loss');

  const n = outcomes.length;
  const wins = outcomes.filter((row) => row.result === 'win').length;
  const losses = outcomes.filter((row) => row.result === 'loss').length;
  const impliedProbs = outcomes
    .map((row) => (row.decimal_odds && row.decimal_odds > 1 ? (1 / row.decimal_odds) : null))
    .filter((p) => p !== null);

  // Approximation note: if per-bet break-even probabilities vary, we compress to p_bar.
  const breakevenWinRate =
    impliedProbs.length > 0
      ? impliedProbs.reduce((a, b) => a + b, 0) / impliedProbs.length
      : null;
  const observedWinRate = n > 0 ? wins / n : null;

  let pValue = null;
  let confidenceLevel = null;
  if (n > 0 && breakevenWinRate !== null) {
    const variance = n * breakevenWinRate * (1 - breakevenWinRate);
    if (variance > 0) {
      const z = (wins - (n * breakevenWinRate)) / Math.sqrt(variance);
      const cdf = stdNormalCdf(Math.abs(z));
      if (cdf !== null) {
        pValue = 2 * (1 - cdf);
        if (pValue < 0) pValue = 0;
        if (pValue > 1) pValue = 1;
        confidenceLevel = 1 - pValue;
      }
    }
  }

  const sampleStatus = classifySampleStatus(n, pValue);
  return {
    total_settled_bets: n,
    wins,
    losses,
    observed_win_rate: observedWinRate !== null ? round2(observedWinRate * 100) : null,
    breakeven_win_rate: breakevenWinRate !== null ? round2(breakevenWinRate * 100) : null,
    p_value: pValue !== null ? Number(pValue.toFixed(4)) : null,
    confidence_level: confidenceLevel !== null ? round2(confidenceLevel * 100) : null,
    sample_status: sampleStatus,
  };
}

function computeRollingBinomialWindows(settledBets) {
  const sorted = [...settledBets].sort((a, b) => {
    const aTs = parseBetRowTimestampMs(a) || 0;
    const bTs = parseBetRowTimestampMs(b) || 0;
    return aTs - bTs;
  });

  const windows = [100, 200, 400];
  const out = {};
  for (const size of windows) {
    const windowRows = sorted.slice(-size);
    out[`rolling_${size}`] = computeBinomialSummaryFromRows(windowRows);
  }
  return out;
}

function parseBetRowTimestampMs(row) {
  const datePart = String(row.Date || '').trim();
  const timePart = String(row['Timestamp (CT)'] || row.timestamp_ct || '').trim();
  if (!datePart) return null;

  const hhmm = timePart.match(/^(\d{1,2}):(\d{2})$/);
  if (hhmm) {
    const hours = String(Number(hhmm[1])).padStart(2, '0');
    const mins = hhmm[2];
    return parseTimestampMs(`${datePart}T${hours}:${mins}:00Z`);
  }

  const ampm = timePart.match(/^(\d{1,2}):(\d{2})\s*([AP]M)$/i);
  if (ampm) {
    let hours = Number(ampm[1]);
    const mins = ampm[2];
    const marker = ampm[3].toUpperCase();
    if (marker === 'PM' && hours < 12) hours += 12;
    if (marker === 'AM' && hours === 12) hours = 0;
    return parseTimestampMs(`${datePart}T${String(hours).padStart(2, '0')}:${mins}:00Z`);
  }

  return parseTimestampMs(`${datePart} ${timePart}`);
}

function parseEdgePercentFromRow(row, keys) {
  for (const key of keys) {
    const value = row[key];
    if (value === null || value === undefined || value === '') continue;
    const parsed = parsePercent(value);
    if (parsed !== null) return parsed;
  }
  return null;
}

function buildRecommendationBetRowsByIdentity(recommendationRows) {
  const byIdentity = {};
  recommendationRows
    .filter((row) => normalizeDecision(row.decision) === 'bet')
    .forEach((row, idx) => {
      const identity = buildBetIdentity({
        Sport: row.sport,
        Market: row.market,
        Bet: row.selection,
        Book: row.source_book,
      });
      if (!byIdentity[identity]) byIdentity[identity] = [];
      byIdentity[identity].push({
        idx,
        row,
        tsMs: parseTimestampMs(row.timestamp_ct),
      });
    });
  return byIdentity;
}

function computeQuantPerformance({ betLog, recommendationRows, currentStatus, ledger, runtimeStatus }) {
  const recByIdentity = buildRecommendationBetRowsByIdentity(recommendationRows);
  const consumedRecIdx = new Set();
  const quantBetRows = [];
  const referenceBankrollFromLedger = (ledger || [])
    .map((row) => parseAsNumber(row.Bankroll))
    .find((value) => value !== null && value > 0);
  const currentBankroll = parseAsNumber(currentStatus?.Bankroll);
  const unitBaselineBankroll = referenceBankrollFromLedger || currentBankroll;
  const unitSize = unitBaselineBankroll !== null ? round2(unitBaselineBankroll * 0.01) : null;

  const coreBetLog = (betLog || []).filter((row) => row.include_in_core_strategy_metrics !== false);

  for (const bet of coreBetLog) {
    const result = normalizeDecision(bet.Result);
    if (!result || result === 'pending') continue;

    const identity = buildBetIdentity(bet);
    const candidates = recByIdentity[identity] || [];
    const betTsMs = parseBetRowTimestampMs(bet);

    let chosen = null;
    let chosenDistance = Number.MAX_SAFE_INTEGER;
    for (const candidate of candidates) {
      if (consumedRecIdx.has(candidate.idx)) continue;
      const distance =
        Number.isFinite(candidate.tsMs) && Number.isFinite(betTsMs)
          ? Math.abs(candidate.tsMs - betTsMs)
          : Number.MAX_SAFE_INTEGER;
      if (!chosen || distance < chosenDistance) {
        chosen = candidate;
        chosenDistance = distance;
      }
    }
    if (chosen) consumedRecIdx.add(chosen.idx);

    const rec = chosen?.row || {};
    const edgeAtDetection = parseEdgePercentFromRow(rec, ['edge_at_detection', 'edge_pct']);
    const edgeAtPlacement = parseEdgePercentFromRow(rec, ['edge_at_placement', 'edge_pct']);
    let edgeAtClose = parseEdgePercentFromRow(rec, ['edge_at_close']);
    const clv = parseClvValue(bet.CLV);
    if (edgeAtClose === null && edgeAtPlacement !== null && clv !== null) {
      edgeAtClose = edgeAtPlacement - clv;
    }

    const stake = parseAsNumber(bet.Stake);
    const edgeForEv = edgeAtPlacement !== null ? edgeAtPlacement : edgeAtDetection;
    const expectedValue = stake !== null && edgeForEv !== null ? stake * (edgeForEv / 100) : null;
    const actualProfit = parseAsNumber(bet['P/L']);
    const stakeUnits = stake !== null && unitSize !== null && unitSize > 0 ? stake / unitSize : null;
    const profitUnits = actualProfit !== null && unitSize !== null && unitSize > 0 ? actualProfit / unitSize : null;
    const expectedValueUnits = expectedValue !== null && unitSize !== null && unitSize > 0 ? expectedValue / unitSize : null;

    const edgeRetention =
      edgeAtDetection !== null && edgeAtDetection !== 0 && edgeAtPlacement !== null
        ? edgeAtPlacement / edgeAtDetection
        : null;
    const closingEdgeRetention =
      edgeAtDetection !== null && edgeAtDetection !== 0 && edgeAtClose !== null
        ? edgeAtClose / edgeAtDetection
        : null;
    const marketEfficiencyImpact = closingEdgeRetention !== null ? 1 - closingEdgeRetention : null;

    quantBetRows.push({
      date: bet.Date || null,
      timestamp_ct: bet['Timestamp (CT)'] || null,
      sport: bet.Sport || null,
      market: bet.Market || null,
      bet: bet.Bet || null,
      book: bet.Book || null,
      rec_id: rec.rec_id || null,
      edge_at_detection: round2(edgeAtDetection),
      edge_at_placement: round2(edgeAtPlacement),
      edge_at_close: round2(edgeAtClose),
      stake_units: round2(stakeUnits),
      profit_units: round2(profitUnits),
      expected_value: round2(expectedValue),
      expected_value_units: round2(expectedValueUnits),
      actual_profit: round2(actualProfit),
      edge_retention: edgeRetention !== null ? round2(edgeRetention) : null,
      closing_edge_retention: closingEdgeRetention !== null ? round2(closingEdgeRetention) : null,
      market_efficiency_impact: marketEfficiencyImpact !== null ? round2(marketEfficiencyImpact) : null,
      data_quality: {
        has_detection: edgeAtDetection !== null,
        has_placement: edgeAtPlacement !== null,
        has_close: edgeAtClose !== null,
      },
    });
  }

  const evValues = quantBetRows.map((r) => r.expected_value).filter((n) => n !== null);
  const evUnitValues = quantBetRows.map((r) => r.expected_value_units).filter((n) => n !== null);
  const actualValues = quantBetRows.map((r) => r.actual_profit).filter((n) => n !== null);
  const actualUnitValues = quantBetRows.map((r) => r.profit_units).filter((n) => n !== null);
  const stakedUnitValues = quantBetRows.map((r) => r.stake_units).filter((n) => n !== null);
  const detectionEdges = quantBetRows.map((r) => r.edge_at_detection).filter((n) => n !== null);
  const placementEdges = quantBetRows.map((r) => r.edge_at_placement).filter((n) => n !== null);
  const closeEdges = quantBetRows.map((r) => r.edge_at_close).filter((n) => n !== null);

  const expectedProfit = evValues.length > 0 ? evValues.reduce((a, b) => a + b, 0) : null;
  const actualProfit = actualValues.length > 0 ? actualValues.reduce((a, b) => a + b, 0) : null;
  const variance =
    expectedProfit !== null && actualProfit !== null
      ? actualProfit - expectedProfit
      : null;
  const expectedProfitUnits = evUnitValues.length > 0 ? evUnitValues.reduce((a, b) => a + b, 0) : null;
  const actualProfitUnits = actualUnitValues.length > 0 ? actualUnitValues.reduce((a, b) => a + b, 0) : null;
  const varianceUnits =
    expectedProfitUnits !== null && actualProfitUnits !== null
      ? actualProfitUnits - expectedProfitUnits
      : null;
  const evRealizationRatio =
    expectedProfit !== null && expectedProfit !== 0 && actualProfit !== null
      ? actualProfit / expectedProfit
      : null;
  const totalStakedUnits = stakedUnitValues.length > 0 ? stakedUnitValues.reduce((a, b) => a + b, 0) : null;
  const totalUnits = actualProfitUnits;
  const averageUnitsPerBet =
    totalStakedUnits !== null && quantBetRows.length > 0
      ? totalStakedUnits / quantBetRows.length
      : null;
  const roiUnits =
    totalUnits !== null && totalStakedUnits !== null && totalStakedUnits !== 0
      ? (totalUnits / totalStakedUnits) * 100
      : null;

  const avgEdgeAtDetection =
    detectionEdges.length > 0 ? detectionEdges.reduce((a, b) => a + b, 0) / detectionEdges.length : null;
  const avgEdgeAtPlacement =
    placementEdges.length > 0 ? placementEdges.reduce((a, b) => a + b, 0) / placementEdges.length : null;
  const avgEdgeAtClose =
    closeEdges.length > 0 ? closeEdges.reduce((a, b) => a + b, 0) / closeEdges.length : null;

  const edgeRetention =
    avgEdgeAtDetection !== null && avgEdgeAtDetection !== 0 && avgEdgeAtPlacement !== null
      ? avgEdgeAtPlacement / avgEdgeAtDetection
      : null;
  const closingEdgeRetention =
    avgEdgeAtDetection !== null && avgEdgeAtDetection !== 0 && avgEdgeAtClose !== null
      ? avgEdgeAtClose / avgEdgeAtDetection
      : null;
  const marketEfficiencyImpact =
    closingEdgeRetention !== null ? 1 - closingEdgeRetention : null;

  let systemStatus = 'Unknown';
  if (evRealizationRatio !== null) {
    if (evRealizationRatio > 1.25) systemStatus = 'Running Hot';
    else if (evRealizationRatio < 0.75) systemStatus = 'Running Cold';
    else systemStatus = 'Stable';
  }

  const binomialLifetime = computeBinomialSummaryFromRows(coreBetLog);
  const binomialRolling = computeRollingBinomialWindows(coreBetLog);

  return {
    bet_class_scope: 'EDGE_BET',
    settled_bets_evaluated: quantBetRows.length,
    unit_size: unitSize,
    unit_baseline_bankroll: unitBaselineBankroll !== null ? round2(unitBaselineBankroll) : null,
    total_units: round2(totalUnits),
    total_staked_units: round2(totalStakedUnits),
    average_units_per_bet: round2(averageUnitsPerBet),
    roi_units: round2(roiUnits),
    expected_value: round2(expectedProfit),
    expected_value_units: round2(expectedProfitUnits),
    expected_profit: round2(expectedProfit),
    expected_profit_units: round2(expectedProfitUnits),
    actual_profit: round2(actualProfit),
    actual_profit_units: round2(actualProfitUnits),
    variance: round2(variance),
    variance_units: round2(varianceUnits),
    ev_realization_ratio: evRealizationRatio !== null ? round2(evRealizationRatio) : null,
    observed_win_rate: binomialLifetime.observed_win_rate,
    breakeven_win_rate: binomialLifetime.breakeven_win_rate,
    p_value: binomialLifetime.p_value,
    confidence_level: binomialLifetime.confidence_level,
    sample_status: binomialLifetime.sample_status,
    binomial_significance: {
      lifetime: binomialLifetime,
      rolling_windows: binomialRolling,
      method: 'normal_approximation_with_effective_breakeven_rate',
      assumptions: [
        'Per-bet break-even rates are derived from recorded odds when available.',
        'Mixed-odds sequences are compressed to an effective average break-even rate for p-value approximation.',
        'Pushes are excluded from win/loss significance testing.',
      ],
    },
    edge_at_detection: round2(avgEdgeAtDetection),
    edge_at_placement: round2(avgEdgeAtPlacement),
    edge_at_close: round2(avgEdgeAtClose),
    edge_retention: edgeRetention !== null ? round2(edgeRetention) : null,
    closing_edge_retention: closingEdgeRetention !== null ? round2(closingEdgeRetention) : null,
    market_efficiency_impact: marketEfficiencyImpact !== null ? round2(marketEfficiencyImpact) : null,
    system_status: systemStatus,
    next_edge_scan_ct: runtimeStatus?.next_edge_scan_ct || '06:00',
    per_bet: quantBetRows,
  };
}

function formatMoneySigned(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  const sign = value > 0 ? '+' : (value < 0 ? '-' : '');
  return `${sign}$${Math.abs(value).toFixed(2)}`;
}

function formatPctSigned(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  const sign = value > 0 ? '+' : '';
  return `${sign}${value.toFixed(2)}%`;
}

function formatRatio(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  return value.toFixed(2);
}

function formatUnits(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  const sign = value > 0 ? '+' : (value < 0 ? '-' : '');
  return `${sign}${Math.abs(value).toFixed(2)}u`;
}

function formatPValue(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  return value.toFixed(4);
}

function explainIntegrityReason(code) {
  const map = {
    stale_state: 'Latest validated canonical state is stale.',
    data_freshness_fail: 'Latest validated canonical state is stale.',
    missing_api_key: 'Odds API key is missing.',
    partial_api_data: 'Upstream odds data was partial or incomplete.',
    degraded_data: 'Upstream data could not be fully verified.',
    quota_or_rate_limit: 'Upstream odds request was rate-limited or quota-limited.',
    malformed_response: 'Upstream odds response was malformed.',
    duplicate_rec_id: 'Recommendation log integrity failed because duplicate IDs were found.',
    missing_scan_days: 'Scan history and recommendation log are out of sync.',
    bankroll_discontinuity: 'Bankroll continuity check failed.',
    bankroll_integrity_failure: 'Bankroll state does not reconcile cleanly.',
    state_sync_gap: 'A successful run completed, but canonical state did not sync deterministically.',
    state_sync_failure: 'Canonical state sync failed.',
    recommendation_integrity_failure: 'Recommendation/pass logging is incomplete for the latest run.',
    payload_rebuild_failure: 'Public payload is stale relative to canonical state.',
  };
  return map[code] || code;
}

function formatEveningGradingReport({
  generatedAtUtc,
  quantPerformance,
  currentStatus,
  decisionQuality,
  bankrollContributionPolicy,
  bettingResultsSplit,
}) {
  const now = new Date(generatedAtUtc || Date.now());
  const dateLabel = now.toISOString().slice(0, 10);
  const drawdown = parsePercent(currentStatus['Drawdown from Peak']);

  return [
    `EVENING GRADING - ${dateLabel}`,
    '',
    `Bets Settled: ${quantPerformance.settled_bets_evaluated ?? 0}`,
    '',
    'OVERALL BETTING RESULTS',
    `Count: ${bettingResultsSplit?.overall?.count ?? 0}`,
    `Profit/Loss: ${formatMoneySigned(bettingResultsSplit?.overall?.profit_loss)}`,
    `Profit Units: ${formatUnits(bettingResultsSplit?.overall?.profit_units)}`,
    `ROI: ${formatPctSigned(bettingResultsSplit?.overall?.roi)}`,
    `Win Rate: ${formatPctSigned(bettingResultsSplit?.overall?.win_rate)}`,
    '',
    'FUN SGP RESULTS',
    `Count: ${bettingResultsSplit?.fun_sgp?.count ?? 0}`,
    `Profit/Loss: ${formatMoneySigned(bettingResultsSplit?.fun_sgp?.profit_loss)}`,
    `Profit Units: ${formatUnits(bettingResultsSplit?.fun_sgp?.profit_units)}`,
    `ROI: ${formatPctSigned(bettingResultsSplit?.fun_sgp?.roi)}`,
    '',
    'CORE TIEREDGE STRATEGY',
    `Profit: ${formatUnits(quantPerformance.total_units)}`,
    `Total Staked: ${quantPerformance.total_staked_units !== null && quantPerformance.total_staked_units !== undefined ? `${quantPerformance.total_staked_units.toFixed(2)}u` : 'N/A'}`,
    `ROI (Units): ${formatPctSigned(quantPerformance.roi_units)}`,
    '',
    'EXPECTATION',
    `Expected Profit: ${formatMoneySigned(quantPerformance.expected_profit)}`,
    `Actual Profit: ${formatMoneySigned(quantPerformance.actual_profit)}`,
    `Variance: ${formatMoneySigned(quantPerformance.variance)}`,
    `EV Realization: ${formatRatio(quantPerformance.ev_realization_ratio)}`,
    '',
    'SIGNAL QUALITY',
    `Average CLV: ${formatPctSigned(decisionQuality?.avg_clv)}`,
    `Positive CLV Rate: ${formatPctSigned(decisionQuality?.positive_clv_rate)}`,
    `Binomial p-value: ${formatPValue(quantPerformance.p_value)}`,
    `Confidence Level: ${formatPctSigned(quantPerformance.confidence_level)}`,
    `Sample Status: ${quantPerformance.sample_status || 'N/A'}`,
    '',
    'EDGE QUALITY',
    `Avg Edge Detected: ${formatPctSigned(quantPerformance.edge_at_detection)}`,
    `Avg Edge at Placement: ${formatPctSigned(quantPerformance.edge_at_placement)}`,
    `Avg Edge at Close: ${formatPctSigned(quantPerformance.edge_at_close)}`,
    `Edge Retention: ${quantPerformance.edge_retention !== null ? `${(quantPerformance.edge_retention * 100).toFixed(1)}%` : 'N/A'}`,
    `Market Efficiency Impact: ${quantPerformance.market_efficiency_impact !== null ? `${(quantPerformance.market_efficiency_impact * 100).toFixed(1)}%` : 'N/A'}`,
    '',
    'BANKROLL CONTRIBUTION POLICY',
    `Realized Monthly Profit: ${formatMoneySigned(bankrollContributionPolicy?.realized_monthly_profit)}`,
    `Rolling Average Realized Profit: ${formatMoneySigned(bankrollContributionPolicy?.rolling_average_realized_profit)}`,
    `Next Estimated Contribution: ${formatMoneySigned(bankrollContributionPolicy?.next_estimated_contribution)}`,
    `Contribution Basis Months: ${(bankrollContributionPolicy?.contribution_basis_months_used || []).join(', ') || 'N/A'}`,
    `Total External Contributions: ${formatMoneySigned(bankrollContributionPolicy?.total_external_contributions)}`,
    `Interpretation: ${bankrollContributionPolicy?.monthly_interpretation || 'N/A'}`,
    '',
    'BANKROLL OVERVIEW',
    `Actual Bankroll: ${formatMoneySigned(bankrollContributionPolicy?.actual_bankroll)}`,
    `Overall Strategy Equity: ${formatMoneySigned(bankrollContributionPolicy?.overall_strategy_equity ?? bankrollContributionPolicy?.strategy_equity)}`,
    `Core Strategy Equity: ${formatMoneySigned(bankrollContributionPolicy?.core_strategy_equity)}`,
    `Total External Contributions: ${formatMoneySigned(bankrollContributionPolicy?.total_external_contributions)}`,
    `Realized Betting Profit: ${formatMoneySigned(bankrollContributionPolicy?.realized_betting_profit_lifetime)}`,
    'Note: Actual bankroll includes external contributions.',
    'Note: Overall strategy equity excludes external contributions but includes FUN_SGP.',
    'Note: Core strategy equity excludes external contributions and FUN_SGP.',
    '',
    `Bankroll: ${currentStatus.Bankroll || 'N/A'}`,
    `Peak: ${currentStatus['All-Time High'] || 'N/A'}`,
    `Drawdown: ${drawdown !== null ? `${drawdown}%` : 'N/A'}`,
    '',
    `System Status: ${quantPerformance.system_status || 'Unknown'}`,
    `Next Edge Scan: ${quantPerformance.next_edge_scan_ct || '06:00'} CT`,
    '',
  ].join('\n');
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
  let out = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^\w\s.+-]/g, '');
  if (out === 'ufc') out = 'mma';
  if (out.includes('mixed martial arts')) out = 'mma';
  return out;
}

function buildBetIdentity(row) {
  return [
    normalizeKeyPart(row.Sport || row.sport),
    normalizeKeyPart(row.Market || row.market),
    normalizeKeyPart(row.Bet || row.selection || row['Selection']),
    normalizeKeyPart(row.Book || row.book || row.source_book),
  ].join('|');
}

function classifyBetClass(row) {
  const explicitClass = String(row['Bet Class'] || row.bet_class || '').trim().toUpperCase();
  if (explicitClass) return explicitClass;

  const tier = String(row.Tier || row.tier || '').trim().toUpperCase();
  const market = String(row.Market || row.market || '').trim().toUpperCase();
  const bet = String(row.Bet || row.bet || row.selection || '').trim().toUpperCase();

  if (tier === 'FUN' || market === 'SGP' || market === 'PARLAY' || bet.includes('PARLAY')) return 'FUN_SGP';
  if (/^T[123]$/.test(tier)) return 'EDGE_BET';
  return 'MANUAL_OTHER';
}

function shouldIncludeInCoreStrategyMetrics(betClass) {
  return betClass === 'EDGE_BET';
}

function shouldIncludeInActualBankroll(betClass) {
  return ['EDGE_BET', 'FUN_SGP', 'MANUAL_OTHER'].includes(betClass);
}

function deriveParlayLegs(row, betClass) {
  if (betClass !== 'FUN_SGP') return [];
  const raw = String(row.Bet || row.bet || row.selection || '').trim();
  if (!raw) return [];
  return raw
    .split(/\s*\/\s*/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function normalizeBetRow(row, fallbackDate = null) {
  const betClass = classifyBetClass(row);
  const normalized = {
    ...row,
    Date: row.Date || fallbackDate || null,
    bet_class: betClass,
    bet_subtype: betClass === 'FUN_SGP' ? String(row.Market || row.market || 'SGP').trim().toUpperCase() : null,
    include_in_core_strategy_metrics: shouldIncludeInCoreStrategyMetrics(betClass),
    include_in_actual_bankroll: shouldIncludeInActualBankroll(betClass),
    parlay_legs: deriveParlayLegs(row, betClass),
  };
  return normalized;
}

function filterSettledRows(rows) {
  return (rows || []).filter((row) => {
    const result = normalizeDecision(row.Result || row.result);
    return result && result !== 'pending';
  });
}

function computeBetClassSummary(rows) {
  const settledRows = filterSettledRows(rows).filter((row) => row.include_in_actual_bankroll !== false);
  const stakeValues = settledRows.map((row) => parseAsNumber(row.Stake)).filter((n) => n !== null);
  const plValues = settledRows.map((row) => parseAsNumber(row['P/L'])).filter((n) => n !== null);
  const decimalOdds = settledRows.map((row) => toDecimalOddsFromBetRow(row)).filter((n) => n !== null);
  const wins = settledRows.filter((row) => normalizeDecision(row.Result) === 'win').length;
  const losses = settledRows.filter((row) => normalizeDecision(row.Result) === 'loss').length;
  const pushes = settledRows.filter((row) => normalizeDecision(row.Result) === 'push').length;
  const totalStake = stakeValues.reduce((a, b) => a + b, 0);
  const totalProfit = plValues.reduce((a, b) => a + b, 0);
  const unitBase = totalStake > 0 && settledRows.length > 0
    ? stakeValues.reduce((a, b) => a + b, 0) / settledRows.length
    : null;
  const totalStakedUnits =
    unitBase !== null && unitBase > 0
      ? totalStake / unitBase
      : null;
  const profitUnits =
    unitBase !== null && unitBase > 0
      ? totalProfit / unitBase
      : null;
  const roi =
    totalStake > 0
      ? (totalProfit / totalStake) * 100
      : null;
  const averageOdds =
    decimalOdds.length > 0
      ? decimalOdds.reduce((a, b) => a + b, 0) / decimalOdds.length
      : null;
  const winRate =
    settledRows.length > 0
      ? (wins / settledRows.length) * 100
      : null;

  return {
    count: settledRows.length,
    wins,
    losses,
    pushes,
    total_stake: round2(totalStake),
    total_staked_units: round2(totalStakedUnits),
    profit_loss: round2(totalProfit),
    profit_units: round2(profitUnits),
    roi: round2(roi),
    average_odds_decimal: round2(averageOdds),
    average_odds_american: averageOdds !== null ? null : null,
    win_rate: round2(winRate),
  };
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

function computePassedOpportunityTracker({ recommendationRows, gradesCache }) {
  const sitRows = recommendationRows
    .filter((row) => isObservationBandPassRow(row));

  const entries = sitRows.map((row) => {
    const recId = row.rec_id || null;
    const graded = recId ? (gradesCache.entries?.[recId] || null) : null;
    const sport = row.sport || 'Unknown';
    const selection = row.selection || row.market || 'Unknown selection';
    const edge = parsePercent(row.edge_pct);
    const edgeText = edge !== null ? `${edge}%` : 'N/A';
    const odds = row.recommended_odds_us || row.odds_us || 'N/A';
    const result = normalizeDecision(
      row.counterfactual_result
      || row.if_bet_result
      || row.result
      || graded?.counterfactual_result
    );
    const status = result || 'ungraded';
    const readable = status === 'ungraded'
      ? 'awaiting grading'
      : (status === 'loss' ? 'they lost' : (status === 'win' ? 'they won' : status));

    const context = graded?.event_label ? ` in ${graded.event_label}` : ` (${sport})`;
    const sentence = `We passed on ${selection}${context} at +EV ${edgeText} (${odds}). Outcome: ${readable}.`;

    return {
      timestamp_ct: row.timestamp_ct || null,
      sport,
      rec_id: recId,
      selection,
      edge_percent: edge,
      odds_us: odds,
      outcome_if_bet: status,
      narrative: sentence,
      rejection_reason: row.rejection_reason || null,
      failure_reason: graded?.failure_reason || null,
      grade_source: graded?.grade_source || null,
      counterfactual_pl: parseAsNumber(
        row.counterfactual_pl
        || row.if_bet_pl
        || row.counterfactual_p_l
        || graded?.counterfactual_pl
        || graded?.counterfactual_pl_unit
      ),
      hypothetical_units: parseAsNumber(
        row.hypothetical_units
        || graded?.hypothetical_units
        || graded?.counterfactual_pl_unit
      ),
      event_label: graded?.event_label || null,
      settlement_date: graded?.settlement_date || null,
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

function computeModelSuppressionTrace({ candidateMarketRows, suppressedCandidateRows, targetDate }) {
  const rows = (candidateMarketRows || []).filter((row) => {
    if (!targetDate) return true;
    return String(row.scan_time_ct || '').includes(targetDate);
  });

  const traceRows = rows.map((row) => ({
    rec_id: row.rec_id || null,
    timestamp_ct: row.scan_time_ct || null,
    sport: row.league || row.sport || null,
    market: row.market_type || null,
    selection: row.selection || null,
    source_book: row.book || null,
    raw_market_price_us: row.odds_american || null,
    raw_market_price_dec: row.odds_decimal || null,
    de_vig_implied_probability: parseAuditNumber(row.devig_implied_prob),
    consensus_baseline_probability: parseAuditNumber(row.consensus_prob),
    pre_conf_true_probability: parseAuditNumber(row.pre_conf_true_prob),
    confidence_total: parseAuditNumber(row.confidence_score),
    post_conf_true_probability: parseAuditNumber(row.post_conf_true_prob),
    raw_edge_percent: parseAuditNumber(row.raw_edge_pct),
    final_edge_percent: parseAuditNumber(row.post_conf_edge_pct),
    threshold_gap_to_t3: (() => {
      const post = parseAuditNumber(row.post_conf_edge_pct);
      return post !== null ? round2(Math.max(0, 2 - post)) : null;
    })(),
    decision: normalizeDecision(row.final_decision) || null,
    rejection_reason: row.rejection_reason || null,
    suppression_stage: row.rejection_stage || null,
  }));

  const suspiciousRows = (suppressedCandidateRows || []).filter((row) => {
    if (targetDate && !String(row.scan_time_ct || '').includes(targetDate)) return false;
    return (parseAuditNumber(row.raw_edge_pct) ?? -Infinity) >= 2;
  });

  const summary = {
    total_candidates: traceRows.length,
    bets_count: traceRows.filter((row) => row.decision === 'bet').length,
    sits_count: traceRows.filter((row) => row.decision === 'sit').length,
    near_miss_count: traceRows.filter((row) => row.final_edge_percent !== null && row.final_edge_percent >= 0.5 && row.final_edge_percent < 2).length,
    sit_above_t3_count: suspiciousRows.length,
    sit_above_t3_by_reason: suspiciousRows.reduce((acc, row) => {
      const key = row.rejection_reason || 'unknown';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
  };

  const diagnosticNotes = [];
  if (summary.total_candidates >= 20 && summary.near_miss_count === 0 && summary.bets_count === 0) {
    diagnosticNotes.push('High-volume scan with zero bets and zero near-misses. Review whether the model is over-anchored or the slate was genuinely efficient.');
  }
  if (summary.sit_above_t3_count > 0) {
    diagnosticNotes.push('At least one candidate cleared the raw T3 edge threshold and still ended as SIT. Inspect confidence gating before trusting the board-level SIT conclusion.');
  }

  return {
    target_date: targetDate,
    summary,
    diagnostic_notes: diagnosticNotes,
    sample_rows: traceRows.slice(0, 50),
    suspicious_rows: suspiciousRows.slice(0, 50),
  };
}

function buildOperatorEdgeBoard({ candidateMarketRows, suppressedCandidateRows, targetDate }) {
  const scopedRows = (candidateMarketRows || []).filter((row) => {
    if (!targetDate) return true;
    return String(row.scan_time_ct || '').includes(targetDate);
  });

  const toBoardRow = (row) => ({
    timestamp_ct: row.scan_time_ct || null,
    sport: row.league || row.sport || null,
    event_id: row.event_id || null,
    market: row.market_type || null,
    selection: row.selection || null,
    book: row.book || null,
    odds_american: row.odds_american || null,
    raw_edge_pct: parseAuditNumber(row.raw_edge_pct),
    post_conf_edge_pct: parseAuditNumber(row.post_conf_edge_pct),
    confidence_score: parseAuditNumber(row.confidence_score),
    rejection_stage: row.rejection_stage || null,
    rejection_reason: row.rejection_reason || null,
    final_decision: row.final_decision || null,
  });

  const actionableBets = scopedRows
    .filter((row) => normalizeDecision(row.final_decision) === 'bet')
    .sort((a, b) => (parseAuditNumber(b.post_conf_edge_pct) ?? -Infinity) - (parseAuditNumber(a.post_conf_edge_pct) ?? -Infinity))
    .map(toBoardRow)
    .slice(0, 25);

  const passBand = scopedRows
    .filter((row) => {
      if (normalizeDecision(row.final_decision) !== 'sit') return false;
      const edge = parseAuditNumber(row.post_conf_edge_pct);
      return edge !== null && edge > 0 && edge < 2;
    })
    .sort((a, b) => (parseAuditNumber(b.post_conf_edge_pct) ?? -Infinity) - (parseAuditNumber(a.post_conf_edge_pct) ?? -Infinity))
    .map((row) => ({
      ...toBoardRow(row),
      gap_to_t3_pct: round2(Math.max(0, 2 - (parseAuditNumber(row.post_conf_edge_pct) ?? 0))),
    }))
    .slice(0, 50);

  const suppressed = (suppressedCandidateRows || [])
    .filter((row) => {
      if (targetDate && !String(row.scan_time_ct || '').includes(targetDate)) return false;
      return true;
    })
    .sort((a, b) => (parseAuditNumber(b.raw_edge_pct) ?? -Infinity) - (parseAuditNumber(a.raw_edge_pct) ?? -Infinity))
    .map((row) => ({
      ...toBoardRow(row),
      pre_conf_edge_pct: parseAuditNumber(row.raw_edge_pct),
      confidence_penalty_pct: parseAuditNumber(row.confidence_penalty_pct),
      consensus_penalty_pct: parseAuditNumber(row.consensus_penalty_pct),
      gap_to_t3_pct: parseAuditNumber(row.gap_to_t3_pct),
    }))
    .slice(0, 50);

  return {
    target_date: targetDate,
    actionable_bets: actionableBets,
    pass_band: passBand,
    suppressed_candidates: suppressed,
  };
}

function formatSuppressionSummaryLines(summary) {
  if (!summary) return [];
  return [
    'Suppression Summary:',
    `- markets_scanned=${summary.markets_scanned ?? 0}`,
    `- raw_edge_over_0_5=${summary.markets_with_raw_edge_over_0_5 ?? 0}`,
    `- pre_conf_edge_over_2_0=${summary.markets_with_pre_conf_edge_over_2_0 ?? 0}`,
    `- rejected_confidence=${summary.markets_rejected_by_confidence_gate ?? 0}`,
    `- rejected_threshold=${summary.markets_rejected_by_threshold_gate ?? 0}`,
    `- rejected_risk=${summary.markets_rejected_by_risk_gate ?? 0}`,
    `- final_approved_bets=${summary.final_approved_bets ?? 0}`,
  ];
}

function resolveSuppressionTargetDate(candidateMarketRows, preferredDate) {
  if (preferredDate && (candidateMarketRows || []).some((row) => String(row.scan_time_ct || '').includes(preferredDate))) {
    return preferredDate;
  }
  const latest = [...(candidateMarketRows || [])]
    .map((row) => String(row.scan_time_ct || ''))
    .filter(Boolean)
    .sort()
    .pop();
  const match = latest?.match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : preferredDate;
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

function computeRejectionReasonRanges({ recommendationRows, targetDate }) {
  const bucketDates = {
    today: new Set(),
    last_7: new Set(),
    all_time: new Set(),
  };
  const out = {
    today: { total_rejections: 0, by_reason: {}, top_rejection_reasons: [] },
    last_7: { total_rejections: 0, by_reason: {}, top_rejection_reasons: [] },
    all_time: { total_rejections: 0, by_reason: {}, top_rejection_reasons: [] },
  };

  const targetMs = parseTimestampMs(targetDate ? `${targetDate}T00:00:00Z` : null);
  const sevenDayStartMs = Number.isFinite(targetMs) ? targetMs - (6 * 24 * 60 * 60 * 1000) : null;

  const sitRows = recommendationRows.filter((row) => normalizeDecision(row.decision) === 'sit');
  for (const row of sitRows) {
    const rowMs = parseTimestampMs(row.timestamp_ct);
    const rowDate = formatDateKeyFromMs(rowMs);
    if (rowDate) bucketDates.all_time.add(rowDate);

    const reasons = splitReasonCodes(row.rejection_reason);
    const addReason = (bucketName, reason) => {
      out[bucketName].by_reason[reason] = (out[bucketName].by_reason[reason] || 0) + 1;
    };

    for (const reason of reasons) {
      addReason('all_time', reason);
    }
    out.all_time.total_rejections += 1;

    if (targetDate && String(row.timestamp_ct || '').includes(targetDate)) {
      if (rowDate) bucketDates.today.add(rowDate);
      out.today.total_rejections += 1;
      for (const reason of reasons) addReason('today', reason);
    }

    if (Number.isFinite(rowMs) && Number.isFinite(sevenDayStartMs) && rowMs >= sevenDayStartMs && rowMs <= (targetMs + 24 * 60 * 60 * 1000 - 1)) {
      if (rowDate) bucketDates.last_7.add(rowDate);
      out.last_7.total_rejections += 1;
      for (const reason of reasons) addReason('last_7', reason);
    }
  }

  for (const key of ['today', 'last_7', 'all_time']) {
    out[key].top_rejection_reasons = Object.entries(out[key].by_reason)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([reason, count]) => `${reason} (${count})`);
    out[key].days_covered = bucketDates[key].size;
  }

  return out;
}

function computeDataFreshness({ recommendationRows, gradesCache, generatedAtUtc, runtimeStatus, lastUpdatedCt, effectiveLastUpdatedCt }) {
  const lastRecRow = recommendationRows.length > 0 ? recommendationRows[recommendationRows.length - 1] : null;
  const candidateAnchors = [
    {
      source: 'runtime_freshness_anchor',
      time: runtimeStatus?.freshness_anchor?.timestamp_ct || null,
    },
    {
      source: 'state_last_updated',
      time: effectiveLastUpdatedCt || lastUpdatedCt || null,
    },
    {
      source: 'recommendation_log_last_row',
      time: lastRecRow?.timestamp_ct || null,
    },
  ]
    .map((anchor) => ({
      ...anchor,
      ts: parseTimestampMs(anchor.time),
    }))
    .filter((anchor) => Number.isFinite(anchor.ts))
    .sort((a, b) => b.ts - a.ts);
  const freshnessAnchor = candidateAnchors[0] || {
    source: 'unknown',
    time: 'unknown',
    ts: null,
  };
  return {
    recommendation_log_last_row_time: lastRecRow?.timestamp_ct || 'unknown',
    grading_cache_last_update: gradesCache?.updated_at || 'unknown',
    payload_build_time_utc: generatedAtUtc || new Date().toISOString(),
    state_last_updated_time: lastUpdatedCt || 'unknown',
    effective_last_updated_time: effectiveLastUpdatedCt || lastUpdatedCt || 'unknown',
    freshness_anchor_source: freshnessAnchor.source,
    freshness_anchor_time: freshnessAnchor.time,
  };
}

function mapEdgeToTier(edgePercent) {
  if (!Number.isFinite(edgePercent)) return null;
  if (edgePercent >= 6) return 'T1';
  if (edgePercent >= 4) return 'T2';
  if (edgePercent >= 2) return 'T3';
  return null;
}

function computeBankrollContinuityCheck({ betLog, ledger, currentStatus, lastUpdatedCt }) {
  const currentBankroll = parseAsNumber(currentStatus?.Bankroll);
  if (currentBankroll === null) {
    return {
      pass: false,
      reason: 'missing_current_bankroll',
      current_bankroll: null,
      expected_bankroll: null,
      delta: null,
      delta_pct: null,
      anchor_bankroll: null,
      anchor_date: null,
      post_anchor_external_contributions: 0,
      last_updated_day: parseDateFromLastUpdated(lastUpdatedCt),
    };
  }

  const settledRows = (betLog || []).filter((row) => {
    const result = normalizeDecision(row.Result || row.result);
    return result && result !== 'pending';
  });
  const anchorRow = settledRows.length > 0 ? settledRows[settledRows.length - 1] : null;
  const anchorBankroll = parseAsNumber(anchorRow?.Bankroll);
  const anchorDate = String(anchorRow?.Date || '').trim() || null;
  if (anchorBankroll === null || !anchorDate) {
    return {
      pass: true,
      reason: 'insufficient_anchor_data',
      current_bankroll: round2(currentBankroll),
      expected_bankroll: null,
      delta: null,
      delta_pct: null,
      anchor_bankroll: null,
      anchor_date: null,
      post_anchor_external_contributions: 0,
      last_updated_day: parseDateFromLastUpdated(lastUpdatedCt),
    };
  }

  const postAnchorExternalContributions = (ledger || []).reduce((sum, row) => {
    const type = String(row.Type || '').trim().toUpperCase();
    if (!['DEPOSIT', 'CONTRIBUTION', 'RELOAD'].includes(type)) return sum;
    const date = String(row.Date || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || date <= anchorDate) return sum;
    const amount = parseAsNumber(row.Amount);
    return amount === null ? sum : sum + amount;
  }, 0);

  const expectedBankroll = round2(anchorBankroll + postAnchorExternalContributions);
  const delta = round2(currentBankroll - expectedBankroll);
  const deltaPct =
    expectedBankroll !== null && expectedBankroll !== 0
      ? round2(Math.abs(delta) / expectedBankroll)
      : null;
  const pass =
    expectedBankroll !== null
    && Math.abs(delta) <= Math.max(BANKROLL_CONTINUITY_MAX_DELTA_ABS, expectedBankroll * BANKROLL_CONTINUITY_MAX_DELTA_PCT);

  return {
    pass,
    reason: pass ? 'pass' : 'bankroll_discontinuity',
    current_bankroll: round2(currentBankroll),
    expected_bankroll: expectedBankroll,
    delta,
    delta_pct: deltaPct,
    anchor_bankroll: round2(anchorBankroll),
    anchor_date: anchorDate,
    post_anchor_external_contributions: round2(postAnchorExternalContributions),
    last_updated_day: parseDateFromLastUpdated(lastUpdatedCt),
  };
}

function classifyIntegrityOutcome({ reasons, blockedByFreshness, blockedByBankroll, blockedByStateSync, blockedByRecommendationIntegrity, blockedByApiIntegrity, payloadRebuildStale, hasBets }) {
  if (reasons.includes('missing_api_key')) return 'missing_api_key';
  if (reasons.includes('partial_api_data')) return 'partial_api_data';
  if (reasons.includes('quota_or_rate_limit') || reasons.includes('malformed_response') || reasons.includes('degraded_data') || blockedByApiIntegrity) return 'degraded_data';
  if (reasons.includes('stale_state') || blockedByFreshness) return 'stale_state';
  if (reasons.includes('state_sync_failure') || blockedByStateSync) return 'state_sync_failure';
  if (reasons.includes('recommendation_integrity_failure') || blockedByRecommendationIntegrity) return 'state_sync_failure';
  if (reasons.includes('payload_rebuild_failure') || payloadRebuildStale) return 'payload_rebuild_failure';
  if (reasons.includes('bankroll_integrity_failure') || blockedByBankroll) return 'bankroll_integrity_failure';
  if (hasBets) return 'bet_ready';
  return 'true_no_edge_sit';
}

function computeIntegrityGate({
  recommendationRows,
  betLog,
  ledger,
  currentStatus,
  lastUpdatedCt,
  effectiveLastUpdatedCt,
  dataFreshness,
  generatedAtUtc,
  runtimeStatus,
  bankrollContributionPolicy,
  canonicalGeneratedAtUtc,
}) {
  const recIdSet = new Set();
  const duplicateRecIds = [];
  const recDays = new Set();
  for (const row of recommendationRows) {
    const recId = String(row.rec_id || '').trim();
    if (recId) {
      if (recIdSet.has(recId)) duplicateRecIds.push(recId);
      recIdSet.add(recId);
    }
    const day = parseDateFromLastUpdated(row.timestamp_ct);
    if (day) recDays.add(day);
  }

  const scanDays = new Set((runtimeStatus?.successful_hunt_days || []).filter(Boolean));
  const lastUpdatedDay = parseDateFromLastUpdated(lastUpdatedCt);
  if (scanDays.size === 0 && lastUpdatedDay) scanDays.add(lastUpdatedDay);

  const firstRecDay = [...recDays].sort()[0] || null;
  const relevantScanDays = firstRecDay
    ? [...scanDays].filter((day) => day >= firstRecDay)
    : [...scanDays];
  const ignoredPreLedgerScanDays = firstRecDay
    ? [...scanDays].filter((day) => day < firstRecDay).sort()
    : [];
  const runtimeNoAppendDays = new Set((runtimeStatus?.no_append_hunt_days || []).filter(Boolean));
  const missingScanDays = relevantScanDays
    .filter((day) => !recDays.has(day))
    .filter((day) => !runtimeNoAppendDays.has(day))
    .sort();
  const freshnessAnchorMs = parseTimestampMs(dataFreshness.freshness_anchor_time);
  const generatedMs = parseTimestampMs(generatedAtUtc);
  const freshnessHours =
    Number.isFinite(freshnessAnchorMs) && Number.isFinite(generatedMs)
      ? (generatedMs - freshnessAnchorMs) / (1000 * 60 * 60)
      : null;
  const freshnessPass = Number.isFinite(freshnessHours) ? freshnessHours <= DATA_FRESHNESS_MAX_HOURS : false;
  const bankrollContinuity = computeBankrollContinuityCheck({
    betLog,
    ledger,
    currentStatus,
    lastUpdatedCt: effectiveLastUpdatedCt || lastUpdatedCt,
  });
  const stateSyncGap = runtimeStatus?.state_sync?.blocking_sync_gap === true;
  const oddsApiKeyPresent = runtimeStatus?.odds_api_config?.key_present === true;
  const latestHunt = runtimeStatus?.latest_successful_hunt || null;
  const latestHuntDataCodes = [...new Set(latestHunt?.data_failure_codes || [])];
  const apiIntegrityReasons = [];
  if (!oddsApiKeyPresent) apiIntegrityReasons.push('missing_api_key');
  for (const code of latestHuntDataCodes) apiIntegrityReasons.push(code);
  const apiIntegrityPass = apiIntegrityReasons.length === 0;

  const ledgerComplete = duplicateRecIds.length === 0 && missingScanDays.length === 0;
  const bankrollFormulaDiff = parseAsNumber(bankrollContributionPolicy?.bankroll_formula_difference);
  const bankrollFormulaPass = bankrollFormulaDiff === null || Math.abs(bankrollFormulaDiff) <= Math.max(BANKROLL_FORMULA_MAX_DELTA_ABS, Math.abs(parseAsNumber(bankrollContributionPolicy?.actual_bankroll) || 0) * BANKROLL_FORMULA_MAX_DELTA_PCT);
  const bankrollIntegrityPass = bankrollContinuity.pass && bankrollFormulaPass;

  const latestHuntDate = latestHunt?.date_key || null;
  const latestHuntType = String(latestHunt?.message_type || '').toUpperCase();
  const targetRowsForLatestHunt = latestHuntDate
    ? recommendationRows.filter((row) => String(row.timestamp_ct || '').includes(latestHuntDate))
    : [];
  const latestHuntHasBetState = latestHuntType === 'BET'
    ? targetRowsForLatestHunt.some((row) => normalizeDecision(row.decision) === 'bet')
    : true;
  const latestHuntHasObservationState = latestHuntType === 'SIT'
    ? true
    : true;
  const recommendationIntegrityPass = ledgerComplete && latestHuntHasBetState && latestHuntHasObservationState;

  const canonicalMs = parseTimestampMs(canonicalGeneratedAtUtc);
  const payloadBuildMs = parseTimestampMs(generatedAtUtc);
  const payloadRebuildPass =
    Number.isFinite(canonicalMs) && Number.isFinite(payloadBuildMs)
      ? payloadBuildMs >= canonicalMs
      : true;

  const pass = freshnessPass && apiIntegrityPass && recommendationIntegrityPass && bankrollIntegrityPass && !stateSyncGap && payloadRebuildPass;

  const reasons = [];
  if (!freshnessPass) reasons.push('stale_state');
  for (const code of apiIntegrityReasons) {
    if (!reasons.includes(code)) reasons.push(code);
  }
  if (duplicateRecIds.length > 0) reasons.push('duplicate_rec_id');
  if (missingScanDays.length > 0) reasons.push('missing_scan_days');
  if (!recommendationIntegrityPass) reasons.push('recommendation_integrity_failure');
  if (!bankrollIntegrityPass) reasons.push('bankroll_integrity_failure');
  if (!bankrollContinuity.pass) reasons.push('bankroll_discontinuity');
  if (stateSyncGap) reasons.push('state_sync_failure');
  if (!payloadRebuildPass) reasons.push('payload_rebuild_failure');

  return {
    pass,
    checks: {
      data_freshness: freshnessPass ? 'pass' : 'fail',
      api_integrity: apiIntegrityPass ? 'pass' : 'fail',
      ledger_integrity: recommendationIntegrityPass ? 'pass' : 'fail',
      bankroll_continuity: bankrollIntegrityPass ? 'pass' : 'fail',
      state_sync: stateSyncGap ? 'fail' : 'pass',
      payload_rebuild: payloadRebuildPass ? 'pass' : 'fail',
      decision_engine_status: pass ? 'pass' : 'blocked',
      duplicate_rec_id_count: duplicateRecIds.length,
      missing_scan_days_count: missingScanDays.length,
    },
    diagnostics: {
      freshness_hours_since_anchor: round2(freshnessHours),
      freshness_anchor_source: dataFreshness.freshness_anchor_source,
      freshness_anchor_time: dataFreshness.freshness_anchor_time,
      first_recommendation_day: firstRecDay,
      ignored_pre_ledger_scan_days: ignoredPreLedgerScanDays,
      runtime_no_append_days: [...runtimeNoAppendDays].sort(),
      missing_scan_days: missingScanDays,
      duplicate_rec_ids: duplicateRecIds,
      latest_hunt_data_failure_codes: latestHuntDataCodes,
      odds_api_key_present: oddsApiKeyPresent,
      bankroll_formula_difference: bankrollFormulaDiff,
      payload_rebuild_canonical_generated_at_utc: canonicalGeneratedAtUtc || null,
      payload_rebuild_generated_at_utc: generatedAtUtc || null,
      latest_hunt_date: latestHuntDate,
      latest_hunt_type: latestHuntType || null,
      latest_hunt_has_bet_state: latestHuntHasBetState,
      bankroll_continuity: bankrollContinuity,
      runtime_status: runtimeStatus,
    },
    reasons,
  };
}

function computeTodayDecisionConsole({
  recommendationRows,
  targetDate,
  integrityGate,
  runtimeStatus,
}) {
  const recRowsForDate = recommendationRows.filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const sourceRows = recRowsForDate;

  const bets = sourceRows
    .filter((row) => normalizeDecision(row.decision) === 'bet')
    .map((row) => {
      const edge = parsePercent(row.edge_pct);
      return {
        rec_id: row.rec_id || null,
        selection: row.selection || row.bet || row.Bet || 'Unknown',
        market: row.market || row.Market || 'Unknown',
        edge_percent: edge,
        tier: mapEdgeToTier(edge),
        stake: row.kelly_stake || row.Stake || 'N/A',
        reason: 'edge above threshold, confidence gate passed',
      };
    });

  const sits = sourceRows
    .filter((row) => normalizeDecision(row.decision) === 'sit')
    .map((row) => ({
      rec_id: row.rec_id || null,
      label: `${row.sport || 'Market'} ${row.market || ''}`.trim(),
      reason: row.rejection_reason || 'no_edge',
    }));

  if (bets.length === 0 && sits.length === 0) {
    const latestHunt = runtimeStatus?.latest_successful_hunt;
    if (latestHunt?.date_key === targetDate && latestHunt?.message_type === 'SIT') {
      sits.push({
        rec_id: null,
        label: "Today's slate",
        reason: latestHunt.plain_reason || 'No qualifying edges found in the latest verified scan.',
      });
    }
  }

  const verdict = bets.length > 0
    ? `BET ${bets.length} opportunity(ies).`
    : 'NO BETS TODAY';
  const noBetsReason = bets.length === 0
    ? (sits.length > 0 ? 'No edges above threshold.' : 'No recommendations for current scan window.')
    : null;

  return {
    blocked: integrityGate.pass !== true,
    verdict,
    no_bets_reason: noBetsReason,
    bets,
    sits,
    next_action: integrityGate.pass === true
      ? (bets.length > 0 ? 'Review bet list and execute within exposure limits.' : 'SIT and wait for next scan.')
      : 'Resolve system health failures before acting on recommendations.',
  };
}

function buildCanonicalDecisionPayload({
  generatedAtUtc,
  integrityGate,
  todayDecisionConsole,
  executionState,
  accountabilitySummary,
  suppressionSummary,
}) {
  const blocked = integrityGate.pass !== true;
  const hasBets = (todayDecisionConsole.bets || []).length > 0;
  const messageType = blocked ? 'BLOCKED' : (hasBets ? 'BET' : 'SIT');
  const runClassification = classifyIntegrityOutcome({
    reasons: integrityGate.reasons || [],
    blockedByFreshness: integrityGate.checks?.data_freshness !== 'pass',
    blockedByBankroll: integrityGate.checks?.bankroll_continuity !== 'pass',
    blockedByStateSync: integrityGate.checks?.state_sync !== 'pass',
    blockedByRecommendationIntegrity: integrityGate.checks?.ledger_integrity !== 'pass',
    blockedByApiIntegrity: integrityGate.checks?.api_integrity !== 'pass',
    payloadRebuildStale: integrityGate.checks?.payload_rebuild !== 'pass',
    hasBets,
  });
  const thresholdReminder = 'Tier thresholds: T1 >= 6%, T2 >= 4%, T3 >= 2%. No qualifying edge -> SIT.';
  const why =
    messageType === 'BLOCKED'
      ? `Integrity gate failed: ${(integrityGate.reasons || []).map(explainIntegrityReason).join(' ')}`
      : (messageType === 'BET'
        ? 'Edge above threshold with confidence/risk gates passed.'
        : (todayDecisionConsole.no_bets_reason || 'No edges above threshold.'));

  return {
    schema: 'decision_payload_v1',
    generated_at_utc: generatedAtUtc,
    message_type: messageType,
    verdict: messageType,
    run_classification: runClassification,
    what_to_do_now: todayDecisionConsole.next_action,
    why,
    threshold_reminder: thresholdReminder,
    system_health: {
      data_freshness: integrityGate.checks.data_freshness,
      api_integrity: integrityGate.checks.api_integrity,
      ledger_integrity: integrityGate.checks.ledger_integrity,
      bankroll_continuity: integrityGate.checks.bankroll_continuity,
      state_sync: integrityGate.checks.state_sync,
      payload_rebuild: integrityGate.checks.payload_rebuild,
      decision_engine_status: integrityGate.checks.decision_engine_status,
      pass: integrityGate.pass,
    },
    execution_state: executionState,
    accountability_summary: accountabilitySummary,
    suppression_summary: suppressionSummary || null,
    bets: blocked ? [] : (todayDecisionConsole.bets || []),
    sits: blocked ? [] : (todayDecisionConsole.sits || []),
    blocked: blocked
      ? {
          reason_codes: integrityGate.reasons,
          reason_text: (integrityGate.reasons || []).map(explainIntegrityReason),
          diagnostics: integrityGate.diagnostics,
          action_to_avoid: 'Do not place bets from this scan.',
          recovery_required: 'Restore freshness + ledger integrity before decisions resume.',
        }
      : null,
  };
}

function healthPassLabel(pass) {
  return pass ? 'PASS' : 'FAIL';
}

function formatPercentValue(value) {
  if (value === null || value === undefined || value === '') return '—';
  return `${value}%`;
}

function formatTerminalDecisionMessage(payload) {
  const lines = [];
  const health = payload.system_health || {};
  const blocked = payload.message_type === 'BLOCKED';

  lines.push('TIEREDGE DECISION');
  lines.push(`Verdict: ${payload.verdict}`);
  lines.push(`Run classification: ${payload.run_classification || 'unknown'}`);
  lines.push(`What to do now: ${payload.what_to_do_now || '—'}`);
  lines.push(`Why: ${payload.why || '—'}`);
  lines.push(`System health: ${healthPassLabel(health.pass === true)}`);
  lines.push(
    `Checks: data=${health.data_freshness || 'fail'}, api=${health.api_integrity || 'fail'}, ledger=${health.ledger_integrity || 'fail'}, bankroll=${health.bankroll_continuity || 'fail'}, sync=${health.state_sync || 'fail'}, payload=${health.payload_rebuild || 'fail'}, engine=${health.decision_engine_status || 'blocked'}`
  );

  if (blocked) {
    const block = payload.blocked || {};
    lines.push(`Block reason: ${(block.reason_text || block.reason_codes || []).join(' ') || 'integrity_failure'}`);
    lines.push(`Avoid: ${block.action_to_avoid || 'Do not place bets from this scan.'}`);
    lines.push(`Recovery: ${block.recovery_required || 'Restore system health before decisions resume.'}`);
  } else if (payload.message_type === 'BET') {
    lines.push('Bets:');
    for (const bet of payload.bets || []) {
      lines.push(
        `- ${bet.selection || 'Unknown'} | ${bet.market || 'Unknown'} | edge ${formatPercentValue(bet.edge_percent)} | ${bet.tier || '—'} | stake ${bet.stake || '—'} | ${bet.reason || '—'}`
      );
    }
  } else {
    lines.push('SIT');
    for (const sit of payload.sits || []) {
      lines.push(`- ${sit.label || 'Market'}: ${sit.reason || 'No qualifying edge.'}`);
    }
    lines.push(`Threshold reminder: ${payload.threshold_reminder || 'No trustworthy edge -> SIT.'}`);
  }

  const execution = payload.execution_state || {};
  lines.push(
    `Execution: bankroll=${execution.bankroll || '—'} | open_exposure=${execution.open_exposure || '—'} | daily_exposure=${execution.daily_exposure_used || '—'} | breaker=${execution.circuit_breaker || '—'}`
  );

  const acc = payload.accountability_summary || {};
  lines.push(
    `Accountability: pending=${acc.pending_bets_count ?? '—'} | positive_clv_rate=${formatPercentValue(acc.positive_clv_rate)} | avg_clv=${acc.avg_clv ?? '—'} | recent_results=${acc.recent_results || '—'}`
  );
  lines.push(...formatSuppressionSummaryLines(payload.suppression_summary));

  return `${lines.join('\n')}\n`;
}

function formatWhatsAppDecisionMessage(payload) {
  const health = payload.system_health || {};
  const blocked = payload.message_type === 'BLOCKED';
  const lines = [];

  lines.push(`*TIEREDGE ${payload.verdict}*`);
  lines.push(`What now: ${payload.what_to_do_now || '—'}`);
  lines.push(`Classification: ${payload.run_classification || 'unknown'}`);
  lines.push(`Why: ${payload.why || '—'}`);
  lines.push(
    `Health: ${healthPassLabel(health.pass === true)} (data ${health.data_freshness || 'fail'}, api ${health.api_integrity || 'fail'}, ledger ${health.ledger_integrity || 'fail'}, bankroll ${health.bankroll_continuity || 'fail'}, sync ${health.state_sync || 'fail'}, payload ${health.payload_rebuild || 'fail'}, engine ${health.decision_engine_status || 'blocked'})`
  );

  if (blocked) {
    const block = payload.blocked || {};
    lines.push(`BLOCKED reason: ${(block.reason_text || block.reason_codes || []).join(' ') || 'integrity_failure'}`);
    lines.push(`Do NOT: ${block.action_to_avoid || 'Place bets from this scan.'}`);
    lines.push(`Recover: ${block.recovery_required || 'Restore system health before decisions resume.'}`);
  } else if (payload.message_type === 'BET') {
    lines.push('*BETS*');
    for (const bet of payload.bets || []) {
      lines.push(
        `- ${bet.selection || 'Unknown'} ${bet.market || ''} | edge ${formatPercentValue(bet.edge_percent)} | ${bet.tier || '—'} | stake ${bet.stake || '—'}`
      );
      lines.push(`  reason: ${bet.reason || '—'}`);
    }
  } else {
    lines.push('*SIT*');
    for (const sit of payload.sits || []) {
      lines.push(`- ${sit.label || 'Market'}: ${sit.reason || 'No qualifying edge.'}`);
    }
    lines.push(`Threshold: ${payload.threshold_reminder || 'No trustworthy edge -> SIT.'}`);
  }

  const execution = payload.execution_state || {};
  lines.push(
    `Execution: bankroll ${execution.bankroll || '—'}, exposure ${execution.open_exposure || '—'}, daily ${execution.daily_exposure_used || '—'}, breaker ${execution.circuit_breaker || '—'}`
  );

  const acc = payload.accountability_summary || {};
  lines.push(
    `Accountability: pending ${acc.pending_bets_count ?? '—'}, +CLV ${formatPercentValue(acc.positive_clv_rate)}, avg CLV ${acc.avg_clv ?? '—'}, results ${acc.recent_results || '—'}`
  );
  lines.push(...formatSuppressionSummaryLines(payload.suppression_summary));

  return `${lines.join('\n')}\n`;
}

function buildDashboardDecisionModel(payload) {
  const blocked = payload.message_type === 'BLOCKED';
  return {
    today: {
      verdict: payload.verdict,
      blocked,
      why: payload.why,
      what_to_do_now: payload.what_to_do_now,
      threshold_reminder: payload.threshold_reminder || null,
      bets: blocked ? [] : (payload.bets || []),
      sits: blocked ? [] : (payload.sits || []),
      blocked_info: blocked ? payload.blocked : null,
    },
    system_health: payload.system_health || {},
    execution: payload.execution_state || {},
    accountability: payload.accountability_summary || {},
    suppression_summary: payload.suppression_summary || {},
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
  const coreBetLog = (betLog || []).filter((row) => row.include_in_core_strategy_metrics !== false);

  const tierPlacedBets = todaysBets.filter((row) => /^T[123]$/i.test(String(row.Tier || '').trim()));

  const recRowsForDate = recommendationRows.filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const recPlacedRows = recRowsForDate.filter((row) => normalizeDecision(row.decision) === 'bet');
  const recSitRows = recRowsForDate.filter((row) => normalizeDecision(row.decision) === 'sit');

  const placedBetsCount =
    recRowsForDate.length > 0
      ? recPlacedRows.length
      : tierPlacedBets.length;
  const rejectedFromSummary = parseAsNumber(rejectionSummary['Total Rejected']) || 0;
  const useSummaryRejectionMode = recSitRows.length === 0 && rejectedFromSummary > rejectedOpportunities.length;
  const rejectedPlaysCount =
    recSitRows.length > 0
      ? recSitRows.length
      : (useSummaryRejectionMode
        ? rejectedFromSummary
        : (rejectedOpportunities.length > 0 ? rejectedOpportunities.length : rejectedFromSummary));

  const clvTierBets = coreBetLog.filter((row) => /^T[123]$/i.test(String(row.Tier || '').trim()));
  const clvValues = clvTierBets.map((row) => parseClvValue(row.CLV)).filter((n) => n !== null);
  const positiveClvBetsCount = clvValues.filter((n) => n > 0).length;
  const positiveClvRate = safeRate(positiveClvBetsCount, clvValues.length);
  const clvWinRate = positiveClvRate;
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
    total_clv_bets_evaluated: clvValues.length,
    clv_win_rate: clvWinRate !== null ? round2(clvWinRate * 100) : null,
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

function computeBettingResultsSplit({ betLog }) {
  const overallRows = (betLog || []).filter((row) => row.include_in_actual_bankroll !== false);
  const coreRows = overallRows.filter((row) => row.bet_class === 'EDGE_BET');
  const funRows = overallRows.filter((row) => row.bet_class === 'FUN_SGP');

  return {
    overall: {
      bet_class_scope: 'ALL_REAL_BETS',
      ...computeBetClassSummary(overallRows),
    },
    core_strategy: {
      bet_class_scope: 'EDGE_BET',
      ...computeBetClassSummary(coreRows),
    },
    fun_sgp: {
      bet_class_scope: 'FUN_SGP',
      ...computeBetClassSummary(funRows),
    },
  };
}

function computeWeeklyPerformanceReview({
  decisionQuality,
  executionQuality,
  quantPerformance,
  sitAccountabilitySummary,
  currentStatus,
  bankrollContributionPolicy,
  bettingResultsSplit,
}) {
  const profitFromBets = parseAsNumber(quantPerformance?.actual_profit);
  const profitIfAllSitsBet = parseAsNumber(sitAccountabilitySummary?.net_counterfactual_pl_if_bet);
  const decisionEdge =
    profitFromBets !== null && profitIfAllSitsBet !== null
      ? profitFromBets - profitIfAllSitsBet
      : null;
  const clvWinRate = parseAsNumber(decisionQuality?.clv_win_rate);
  let clvInterpretation = null;
  if (clvWinRate !== null) {
    if (clvWinRate > 60) clvInterpretation = 'very_strong_signal';
    else if (clvWinRate >= 55) clvInterpretation = 'strong_signal';
    else if (clvWinRate >= 52) clvInterpretation = 'possible_edge';
    else clvInterpretation = 'random_or_inconclusive';
  }

  return {
    clv_metrics: {
      average_clv: decisionQuality?.avg_clv ?? null,
      positive_clv_rate: decisionQuality?.positive_clv_rate ?? null,
      clv_win_rate: decisionQuality?.clv_win_rate ?? null,
      clv_win_rate_interpretation: clvInterpretation,
      total_bets_evaluated: decisionQuality?.total_clv_bets_evaluated ?? null,
      execution_slippage: executionQuality?.['Avg Slippage (last 25 bets)'] || null,
    },
    decision_quality: {
      profit_from_bets: round2(profitFromBets),
      profit_if_all_sits_bet: round2(profitIfAllSitsBet),
      decision_edge: round2(decisionEdge),
      sit_discipline_rate: currentStatus?.['Sit Discipline Rate (7d)'] || null,
    },
    overall_betting_results: {
      ...bettingResultsSplit?.overall,
      label: 'Overall results include FUN_SGP bets.',
    },
    core_strategy_results: {
      ...bettingResultsSplit?.core_strategy,
      label: 'Core strategy metrics exclude FUN_SGP bets.',
    },
    fun_sgp_results: {
      ...bettingResultsSplit?.fun_sgp,
      label: 'FUN_SGP results are included in bankroll truth, but excluded from core strategy validation.',
    },
    bankroll_contribution_policy: {
      actual_bankroll: bankrollContributionPolicy?.actual_bankroll ?? null,
      strategy_equity: bankrollContributionPolicy?.strategy_equity ?? null,
      overall_strategy_equity: bankrollContributionPolicy?.overall_strategy_equity ?? bankrollContributionPolicy?.strategy_equity ?? null,
      core_strategy_equity: bankrollContributionPolicy?.core_strategy_equity ?? null,
      realized_betting_profit_lifetime: bankrollContributionPolicy?.realized_betting_profit_lifetime ?? null,
      core_edge_profit_lifetime: bankrollContributionPolicy?.core_edge_profit_lifetime ?? null,
      fun_sgp_profit_lifetime: bankrollContributionPolicy?.fun_sgp_profit_lifetime ?? null,
      bankroll_growth_from_betting: bankrollContributionPolicy?.bankroll_growth_from_betting ?? null,
      bankroll_growth_from_contributions: bankrollContributionPolicy?.bankroll_growth_from_contributions ?? null,
      realized_monthly_profit: bankrollContributionPolicy?.realized_monthly_profit ?? null,
      contribution_basis_month_count: bankrollContributionPolicy?.contribution_basis_month_count ?? null,
      contribution_basis_months_used: bankrollContributionPolicy?.contribution_basis_months_used || [],
      next_estimated_contribution: bankrollContributionPolicy?.next_estimated_contribution ?? null,
      total_contributions_to_date: bankrollContributionPolicy?.total_external_contributions ?? null,
      interpretation: bankrollContributionPolicy?.monthly_interpretation || null,
      contribution_adjusted_summary: 'Strategy equity isolates betting performance by excluding external contributions.',
    },
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

function computeDerivedDailyRejectionSummary({ recommendationRows, targetDate }) {
  const inDate = (recommendationRows || []).filter((row) => {
    if (!targetDate) return true;
    return String(row.timestamp_ct || '').includes(targetDate);
  });
  const sitRows = inDate.filter((row) => normalizeDecision(row.decision) === 'sit');
  const counts = {
    no_edge: 0,
    low_confidence: 0,
    stale_or_unverified_odds: 0,
    exposure_cap_reached: 0,
    breaker_active: 0,
  };
  for (const row of sitRows) {
    for (const reason of splitReasonCodes(row.rejection_reason)) {
      if (counts[reason] !== undefined) counts[reason] += 1;
    }
  }
  return {
    'Total Markets Checked': inDate.length > 0 ? String(inDate.length) : 'N/A',
    'Total Rejected': sitRows.length > 0 ? String(sitRows.length) : 'N/A',
    ...Object.fromEntries(Object.entries(counts).map(([k, v]) => [k, sitRows.length > 0 ? String(v) : 'N/A'])),
    'Plays Recommended': inDate.length > 0 ? String(inDate.filter((row) => normalizeDecision(row.decision) === 'bet').length) : 'N/A',
    summary_source: sitRows.length > 0 || inDate.length > 0 ? 'recommendation_log' : 'unavailable_for_target_date',
    summary_target_date: targetDate || null,
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

function computeSitAccountabilitySummaryFromPassedTracker(passedOpportunityTracker) {
  const entries = (passedOpportunityTracker?.entries || [])
    .filter((entry) => normalizeDecision(entry.outcome_if_bet) !== 'ungraded');
  const graded = entries.length;
  const wins = entries.filter((entry) => normalizeDecision(entry.outcome_if_bet) === 'win').length;
  const losses = entries.filter((entry) => normalizeDecision(entry.outcome_if_bet) === 'loss').length;
  const pushes = entries.filter((entry) => normalizeDecision(entry.outcome_if_bet) === 'push').length;
  const netCounterfactualPl = entries.reduce((acc, entry) => acc + (parseAsNumber(entry.counterfactual_pl) || 0), 0);
  const sitDecisionWinRateIfBet = safeRate(wins, graded);
  const moneySavedBySitting = netCounterfactualPl < 0 ? Math.abs(netCounterfactualPl) : 0;
  const missedProfitBySitting = netCounterfactualPl > 0 ? netCounterfactualPl : 0;

  return {
    source: 'passed_opportunity_tracker',
    passed_bets_graded: graded,
    passed_bets_record_if_bet: graded > 0 ? `${wins}-${losses}${pushes > 0 ? `-${pushes}` : ''}` : null,
    passed_bets_wins_if_bet: wins,
    passed_bets_losses_if_bet: losses,
    passed_bets_pushes_if_bet: pushes,
    passed_bets_win_rate_if_bet: sitDecisionWinRateIfBet !== null ? round2(sitDecisionWinRateIfBet * 100) : null,
    money_saved_by_sitting: round2(moneySavedBySitting),
    missed_profit_by_sitting: round2(missedProfitBySitting),
    net_counterfactual_pl_if_bet: round2(netCounterfactualPl),
    net_ev_rejected: null,
  };
}

function buildCanonicalRunArtifacts({
  runtimeStatus,
  recommendationRows,
  betLog,
  currentStatus,
  stateLastUpdatedCt,
  effectiveLastUpdatedCt,
  generatedAtUtc,
}) {
  const stateLastUpdatedMs = parseTimestampMs(stateLastUpdatedCt);
  const effectiveStateMs = parseTimestampMs(effectiveLastUpdatedCt);
  const bankrollSnapshot = round2(parseAsNumber(currentStatus?.Bankroll));
  const recommendationDays = new Set(
    (recommendationRows || [])
      .map((row) => parseDateFromLastUpdated(row.timestamp_ct))
      .filter(Boolean)
  );
  const settledBetDays = new Set(
    (betLog || [])
      .filter((row) => normalizeDecision(row.Result) && normalizeDecision(row.Result) !== 'pending')
      .map((row) => String(row.Date || '').trim())
      .filter(Boolean)
  );

  const jobs = [
    { key: 'morning_edge_hunt', ledgerType: 'hunt' },
    { key: 'friday_sgp', ledgerType: 'hunt' },
    { key: 'evening_grading', ledgerType: 'grading' },
    { key: 'weekly_review', ledgerType: 'review' },
  ];

  const artifacts = [];
  for (const jobDef of jobs) {
    const successfulRuns = runtimeStatus?.jobs?.[jobDef.key]?.successful_runs || [];
    for (const run of successfulRuns) {
      const targetDate = run.date_key || parseDateFromLastUpdated(run.run_at_ct);
      const hasRecommendationState = targetDate ? recommendationDays.has(targetDate) : false;
      const hasSettlementState = targetDate ? settledBetDays.has(targetDate) : false;
      const stateSyncRequired = jobDef.ledgerType === 'review' ? false : run.requires_state_sync === true;
      const stateSyncCompleted = stateSyncRequired
        ? (jobDef.ledgerType === 'grading' ? hasSettlementState || (Number.isFinite(stateLastUpdatedMs) && Number.isFinite(run.run_at_ms) && stateLastUpdatedMs >= run.run_at_ms) : hasRecommendationState)
        : true;

      artifacts.push({
        run_id: run.session_id || `${jobDef.key}:${run.run_at_ms || targetDate || 'unknown'}`,
        run_type: jobDef.ledgerType,
        job_name: jobDef.key,
        timestamp_ct: run.run_at_ct || null,
        timestamp_ms: run.run_at_ms || null,
        target_date: targetDate || null,
        source_data_status: run.status || null,
        bankroll_snapshot: targetDate === parseDateFromLastUpdated(effectiveLastUpdatedCt) ? bankrollSnapshot : null,
        hunt_result_classification: run.message_type || null,
        actionable_bets_exist: run.has_actionable_bets === true,
        state_sync_required: stateSyncRequired,
        state_sync_completed: stateSyncCompleted,
        canonical_state_write_completed: stateSyncCompleted,
        payload_rebuild_completed: true,
        payload_rebuild_generated_at_utc: generatedAtUtc,
        success_criteria_met: stateSyncCompleted,
        failure_reason: stateSyncCompleted ? null : 'canonical_state_sync_missing',
        notes: run.plain_reason || null,
      });
    }
  }

  artifacts.sort((a, b) => (a.timestamp_ms || 0) - (b.timestamp_ms || 0));
  return {
    generated_at_utc: generatedAtUtc,
    state_last_updated_ct: stateLastUpdatedCt,
    effective_last_updated_ct: effectiveLastUpdatedCt,
    latest_run: [...artifacts].reverse().find((artifact) => artifact.run_type !== 'review') || artifacts[artifacts.length - 1] || null,
    latest_hunt: [...artifacts].reverse().find((artifact) => artifact.run_type === 'hunt') || null,
    artifacts,
  };
}

function buildCanonicalLedgers({
  betLog,
  recommendationRows,
  candidateMarketRows,
  suppressedCandidateRows,
  contributionLedgerEntries,
}) {
  const betsLedger = (betLog || []).map((row, index) => ({
    ledger_index: index + 1,
    ...row,
  }));
  const passLedger = (recommendationRows || [])
    .filter((row) => normalizeDecision(row.decision) === 'sit')
    .map((row, index) => {
      const edgePct = parsePercent(row.edge_pct);
      const clamped = edgePct !== null && edgePct > 0 && edgePct < 2;
      return {
        ledger_index: index + 1,
        ...row,
        edge_pct_numeric: edgePct,
        in_zero_to_two_band: clamped,
      };
    })
    .filter((row) => row.in_zero_to_two_band === true);
  const suppressedLedger = (suppressedCandidateRows || []).map((row, index) => ({
    ledger_index: index + 1,
    ...row,
  }));
  const gradingLedger = (betLog || [])
    .filter((row) => normalizeDecision(row.Result) && normalizeDecision(row.Result) !== 'pending')
    .map((row, index) => ({
      ledger_index: index + 1,
      date: row.Date || null,
      timestamp_ct: row['Timestamp (CT)'] || null,
      bet: row.Bet || null,
      result: row.Result || null,
      profit_loss: row['P/L'] || null,
      bankroll: row.Bankroll || null,
      bet_class: row.bet_class || null,
      stake: row.Stake || null,
      clv: row.CLV || null,
    }));
  const contributionsLedger = (contributionLedgerEntries || []).map((entry, index) => ({
    ledger_index: index + 1,
    ...entry,
  }));
  const candidateLedger = (candidateMarketRows || []).map((row, index) => ({
    ledger_index: index + 1,
    ...row,
  }));

  return {
    bets: betsLedger,
    passes_zero_to_two: passLedger,
    suppressed_candidates: suppressedLedger,
    contributions: contributionsLedger,
    grading_results: gradingLedger,
    candidate_markets: candidateLedger,
  };
}

function buildPayload(markdown) {
  const lastUpdatedCt = parseLastUpdated(markdown);
  const runtimeStatus = readRuntimeStatus();
  const effectiveLastUpdatedCt = computeEffectiveLastUpdatedCt(lastUpdatedCt, runtimeStatus);
  const targetDate = parseDateFromLastUpdated(effectiveLastUpdatedCt);
  const marketContextHooksConfig = readMarketContextHooksConfig();
  const currentStatus = parseBulletMap(extractSection(markdown, 'Current Status'));
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
  const ignoredMarkdownSummaries = {
    lifetime_stats: parseBulletMap(extractSection(markdown, 'Lifetime Stats')),
    daily_rejection_summary: parseBulletMap(extractSection(markdown, 'Daily Rejection Summary')),
    weekly_running_totals: parseBulletMap(extractSection(markdown, 'Weekly Running Totals')),
  };

  const todaysBetsSection = extractDatedSection(markdown, "Today's Bets", targetDate);
  const todaysBetsRaw = filterPlaceholderBetRows(parseTable(todaysBetsSection));
  const betLogRaw = parseTable(extractSection(markdown, 'Bet Log (All Graded Bets)'));
  const betLog = dedupeStalePendingBetLog(betLogRaw).map((row) => normalizeBetRow(row));
  const rejectedOpportunities = parseTable(
    extractFirstSection(markdown, ['Rejected Opportunities (Today)', 'Rejected Opportunities'])
  );
  const ledger = parseTable(extractSection(markdown, 'Ledger'));
  const contributionLedger = readContributionLedger();
  const contributionAutomationStatus = readContributionAutomationStatus();
  const pendingBetsRaw = parsePending(extractSection(markdown, 'Pending Bets (awaiting result)'));
  const recLogPath = resolveRecommendationLogPath(markdown, sourcePath);
  const recommendationRows =
    recLogPath && fs.existsSync(recLogPath)
      ? parseRecommendationRows(fs.readFileSync(recLogPath, 'utf8'))
      : [];
  const nativeDecisionRows = readNativeDecisionLedger(DEFAULT_NATIVE_ALL_LEDGER);
  const nativeCandidateRows = mapNativeDecisionRowsToCandidateRows(nativeDecisionRows);
  const candidateMarketRows = nativeCandidateRows.length > 0
    ? nativeCandidateRows
    : buildCandidateMarketRows(recommendationRows);
  const suppressedCandidateRows = nativeCandidateRows.length > 0
    ? mapNativeDecisionRowsToCandidateRows(readNativeDecisionLedger(DEFAULT_NATIVE_SUPPRESSED_LEDGER))
    : buildSuppressedCandidateRows(candidateMarketRows);
  const suppressionTargetDate = targetDate;
  const passedGradesCache = readPassedGradesCache();

  const todaysBetsNormalized = todaysBetsRaw.map((row) => normalizeBetRow(row, targetDate));
  const todaysBets = redactPending
    ? todaysBetsNormalized.filter((row) => String(row.Result || '').toUpperCase() !== 'PENDING')
    : todaysBetsNormalized;
  const pendingBets = redactPending ? [] : pendingBetsRaw;

  const bankrollValue = parseAsNumber(currentStatus.Bankroll);
  const derivedDailyRejectionSummary = computeDerivedDailyRejectionSummary({
    recommendationRows,
    targetDate,
  });
  const decisionQuality = computeDecisionQuality({
    lastUpdatedCt: effectiveLastUpdatedCt,
    currentStatus,
    betLog,
    todaysBets,
    rejectedOpportunities,
    rejectionSummary: {},
    sitAccountability,
    recommendationRows,
  });
  const edgeDistributionTransparency = computeEdgeDistributionTransparency({
    recommendationRows,
    rejectedOpportunities,
    targetDate,
  });
  const rejectionReasonRanges = computeRejectionReasonRanges({
    recommendationRows,
    targetDate,
  });
  const dailyDecisionSummary = computeDailyDecisionSummary({
    scannerStats,
    decisionQuality,
    rejectedOpportunities,
  });
  const marketContextAudit = computeMarketContextAudit({
    recommendationRows,
    targetDate,
    config: marketContextHooksConfig,
  });
  if ((marketContextAudit.decision_notes || []).length > 0) {
    dailyDecisionSummary.market_context_notes = marketContextAudit.decision_notes;
  }
  const passedOpportunityTracker = computePassedOpportunityTracker({
    recommendationRows,
    gradesCache: passedGradesCache,
  });
  const modelSuppressionTrace = computeModelSuppressionTrace({
    candidateMarketRows,
    suppressedCandidateRows,
    targetDate: suppressionTargetDate,
  });
  const operatorEdgeBoard = buildOperatorEdgeBoard({
    candidateMarketRows,
    suppressedCandidateRows,
    targetDate: suppressionTargetDate,
  });
  const dailySuppressionSummary = buildSuppressionSummary(candidateMarketRows, suppressionTargetDate);
  const quantPerformance = computeQuantPerformance({
    betLog,
    recommendationRows,
    currentStatus,
    ledger,
    runtimeStatus,
  });
  const bankrollContributionPolicy = computeBankrollContributionPolicy({
    betLog,
    ledger,
    contributionLedgerEntries: contributionLedger.entries,
    currentStatus,
    lastUpdatedCt: effectiveLastUpdatedCt,
  });
  const derivedLifetimeStats = computeDerivedLifetimeStats({
    betLog,
    startingBankroll: bankrollContributionPolicy.starting_bankroll,
  });
  const derivedWeeklyRunningTotals = computeDerivedWeeklyRunningTotals({
    betLog,
    anchorDateKey: targetDate,
  });
  const sitAccountabilitySummary =
    (passedOpportunityTracker?.graded_count || 0) > 0
      ? computeSitAccountabilitySummaryFromPassedTracker(passedOpportunityTracker)
      : computeSitAccountabilitySummary({
          sitAccountability,
          rejectedOpportunities,
        });
  const bettingResultsSplit = computeBettingResultsSplit({ betLog });
  const weeklyPerformanceReview = computeWeeklyPerformanceReview({
    decisionQuality,
    executionQuality,
    quantPerformance,
    sitAccountabilitySummary,
    currentStatus,
    bankrollContributionPolicy,
    bettingResultsSplit,
  });
  const generatedAtUtc = new Date().toISOString();
  const scanCoverageArtifacts = computeScanCoverageArtifacts({
    policy: JSON.parse(fs.readFileSync(DEFAULT_SCAN_POLICY, 'utf8')),
    asOfDate: new Date(generatedAtUtc),
  });
  const dataFreshness = computeDataFreshness({
    recommendationRows,
    gradesCache: passedGradesCache,
    generatedAtUtc,
    runtimeStatus,
    lastUpdatedCt,
    effectiveLastUpdatedCt,
  });
  const integrityGate = computeIntegrityGate({
    recommendationRows,
    betLog,
    ledger,
    currentStatus,
    lastUpdatedCt,
    effectiveLastUpdatedCt,
    dataFreshness,
    generatedAtUtc,
    runtimeStatus,
    bankrollContributionPolicy,
    canonicalGeneratedAtUtc: generatedAtUtc,
  });
  const todayDecisionConsole = computeTodayDecisionConsole({
    recommendationRows,
    targetDate,
    todaysBets,
    integrityGate,
    runtimeStatus,
  });
  const openExposure = normalizeStatusValueForTargetDate(currentStatus['Daily Exposure Used'], targetDate) || null;
  const dailyVerdict = dailyDecisionSummary.final_daily_verdict || null;
  const executionState = {
    bankroll: currentStatus.Bankroll || null,
    open_exposure: openExposure,
    daily_exposure_used: openExposure,
    circuit_breaker: currentStatus['Circuit Breaker'] || null,
  };
  const accountabilitySummary = {
    pending_bets_count: pendingBets.filter((item) => String(item).toLowerCase() !== 'none').length,
    positive_clv_rate: decisionQuality.positive_clv_rate,
    avg_clv: decisionQuality.avg_clv,
    recent_results: derivedLifetimeStats['Win Rate'] || null,
    sit_accountability: sitAccountabilitySummary,
  };
  const canonicalDecisionPayload = buildCanonicalDecisionPayload({
    generatedAtUtc,
    integrityGate,
    todayDecisionConsole,
    executionState,
    accountabilitySummary,
    suppressionSummary: dailySuppressionSummary,
  });
  const decisionTerminalText = formatTerminalDecisionMessage(canonicalDecisionPayload);
  const decisionWhatsAppText = formatWhatsAppDecisionMessage(canonicalDecisionPayload);
  const eveningGradingReportText = formatEveningGradingReport({
    generatedAtUtc,
    quantPerformance,
    currentStatus,
    decisionQuality,
    bankrollContributionPolicy,
    bettingResultsSplit,
  });
  const decisionDashboardModel = buildDashboardDecisionModel(canonicalDecisionPayload);
  const canonicalRunArtifacts = buildCanonicalRunArtifacts({
    runtimeStatus,
    recommendationRows,
    betLog,
    currentStatus,
    stateLastUpdatedCt: lastUpdatedCt,
    effectiveLastUpdatedCt,
    generatedAtUtc,
  });
  const canonicalLedgers = buildCanonicalLedgers({
    betLog,
    recommendationRows,
    candidateMarketRows,
    suppressedCandidateRows,
    contributionLedgerEntries: contributionLedger.entries,
  });

  return {
    generated_at_utc: generatedAtUtc,
    source_file: sourcePath,
    schema: parseSchema(markdown),
    last_updated_ct: effectiveLastUpdatedCt,
    state_last_updated_ct: lastUpdatedCt,
    runtime_status: runtimeStatus,
    current_status: currentStatus,
    lifetime_stats: derivedLifetimeStats,
    lifetime_stats_derived: derivedLifetimeStats,
    daily_rejection_summary: derivedDailyRejectionSummary,
    sit_accountability: sitAccountability,
    scanner_statistics: scannerStats,
    market_confidence: marketConfidence,
    canonical_decision_engine: canonicalDecisionEngine,
    drawdown_governor: drawdownGovernor,
    edge_distribution: edgeDistribution,
    reliability_index: reliabilityIndex,
    daily_summary: { ...dailySummary, ...dailyDecisionSummary },
    daily_decision_summary: dailyDecisionSummary,
    rejection_reason_ranges: rejectionReasonRanges,
    market_context: marketContextAudit,
    market_context_hooks_config: {
      mode: marketContextHooksConfig.mode,
      stale_after_hours: marketContextHooksConfig.stale_after_hours,
      confidence_modifiers: marketContextHooksConfig.confidence_modifiers,
      required_for_application: marketContextHooksConfig.required_for_application,
    },
    sit_accountability_summary: sitAccountabilitySummary,
    passed_opportunity_tracker: passedOpportunityTracker,
    model_suppression_trace: modelSuppressionTrace,
    operator_edge_board: operatorEdgeBoard,
    suppression_summary: dailySuppressionSummary,
    scan_coverage_policy: scanCoverageArtifacts.scan_priority_design,
    request_budget_model: scanCoverageArtifacts.request_budget_model,
    cache_reuse_policy: scanCoverageArtifacts.cache_reuse_policy,
    scan_coverage_artifacts: scanCoverageArtifacts,
    suppression_artifacts: {
      candidate_markets_path: DEFAULT_CANDIDATE_MARKETS,
      suppressed_candidates_path: DEFAULT_SUPPRESSED_CANDIDATES,
      candidate_market_count: candidateMarketRows.length,
      suppressed_candidate_count: suppressedCandidateRows.length,
      target_date: suppressionTargetDate,
      target_date_row_count: candidateMarketRows.filter((row) => !suppressionTargetDate || String(row.scan_time_ct || '').includes(suppressionTargetDate)).length,
      trace_origin: nativeDecisionRows.length > 0 ? 'native_decision_time_emission' : 'reconstructed_from_recommendation_log',
    },
    edge_distribution_transparency: edgeDistributionTransparency,
    market_type_reliability_index: marketTypeReliabilityIndex,
    sit_reason_code_standard: sitReasonCodeStandard,
    rule_ledger_pointer: ruleLedgerPointer,
    expectation_framing: expectationFraming,
    rejected_opportunities: rejectedOpportunities,
    execution_quality: executionQuality,
    weekly_running_totals: derivedWeeklyRunningTotals,
    weekly_performance_review: weeklyPerformanceReview,
    betting_results_split: bettingResultsSplit,
    overall_betting_results: bettingResultsSplit.overall,
    core_strategy_results: bettingResultsSplit.core_strategy,
    fun_sgp_results: bettingResultsSplit.fun_sgp,
    bankroll_contribution_policy: bankrollContributionPolicy,
    bankroll_contribution_ledger: contributionLedger.entries,
    bankroll_contribution_ledger_path: contributionLedger.path,
    bankroll_contribution_automation: contributionAutomationStatus,
    quant_performance: quantPerformance,
    unit_size: quantPerformance.unit_size,
    stake_units: quantPerformance.per_bet.map((row) => row.stake_units),
    profit_units: quantPerformance.per_bet.map((row) => row.profit_units),
    total_units: quantPerformance.total_units,
    total_staked_units: quantPerformance.total_staked_units,
    average_units_per_bet: quantPerformance.average_units_per_bet,
    roi_units: quantPerformance.roi_units,
    expected_value: quantPerformance.expected_value,
    expected_value_units: quantPerformance.expected_value_units,
    expected_profit: quantPerformance.expected_profit,
    expected_profit_units: quantPerformance.expected_profit_units,
    actual_profit: quantPerformance.actual_profit,
    actual_profit_units: quantPerformance.actual_profit_units,
    variance: quantPerformance.variance,
    variance_units: quantPerformance.variance_units,
    ev_realization_ratio: quantPerformance.ev_realization_ratio,
    observed_win_rate: quantPerformance.observed_win_rate,
    breakeven_win_rate: quantPerformance.breakeven_win_rate,
    p_value: quantPerformance.p_value,
    confidence_level: quantPerformance.confidence_level,
    sample_status: quantPerformance.sample_status,
    edge_at_detection: quantPerformance.edge_at_detection,
    edge_at_placement: quantPerformance.edge_at_placement,
    edge_at_close: quantPerformance.edge_at_close,
    edge_retention: quantPerformance.edge_retention,
    closing_edge_retention: quantPerformance.closing_edge_retention,
    market_efficiency_impact: quantPerformance.market_efficiency_impact,
    data_freshness: dataFreshness,
    integrity_gate: integrityGate,
    decision_payload_v1: canonicalDecisionPayload,
    decision_renderers: {
      terminal_text: decisionTerminalText,
      whatsapp_text: decisionWhatsAppText,
      evening_grading_report_text: eveningGradingReportText,
      dashboard_model: decisionDashboardModel,
    },
    decision_console: {
      // Backward-compatible alias; canonical dashboard mapping is decision_renderers.dashboard_model.
      ...decisionDashboardModel,
    },
    open_exposure: openExposure,
    daily_verdict: dailyVerdict,
    starting_bankroll: bankrollContributionPolicy.starting_bankroll,
    actual_bankroll: bankrollContributionPolicy.actual_bankroll,
    strategy_equity: bankrollContributionPolicy.strategy_equity,
    overall_strategy_equity: bankrollContributionPolicy.overall_strategy_equity,
    core_strategy_equity: bankrollContributionPolicy.core_strategy_equity,
    total_external_contributions: bankrollContributionPolicy.total_external_contributions,
    realized_betting_profit_lifetime: bankrollContributionPolicy.realized_betting_profit_lifetime,
    core_edge_profit_lifetime: bankrollContributionPolicy.core_edge_profit_lifetime,
    fun_sgp_profit_lifetime: bankrollContributionPolicy.fun_sgp_profit_lifetime,
    bankroll_growth_from_betting: bankrollContributionPolicy.bankroll_growth_from_betting,
    bankroll_growth_from_contributions: bankrollContributionPolicy.bankroll_growth_from_contributions,
    realized_monthly_profit: bankrollContributionPolicy.realized_monthly_profit,
    rolling_average_realized_profit: bankrollContributionPolicy.rolling_average_realized_profit,
    next_estimated_contribution: bankrollContributionPolicy.next_estimated_contribution,
    contribution_basis_month_count: bankrollContributionPolicy.contribution_basis_month_count,
    last_contribution_amount: bankrollContributionPolicy.last_contribution_amount,
    last_contribution_date: bankrollContributionPolicy.last_contribution_date,
    bankroll_trend_support: {
      actual_bankroll_monthly_map: bankrollContributionPolicy.actual_bankroll_monthly_map || {},
      strategy_equity_monthly_map: bankrollContributionPolicy.strategy_equity_monthly_map || {},
    },
    canonical_truth: {
      canonical_state_path: DEFAULT_CANONICAL_STATE,
      run_artifacts_path: DEFAULT_RUN_ARTIFACTS,
      bets_ledger_path: DEFAULT_BETS_LEDGER,
      passes_ledger_path: DEFAULT_PASS_LEDGER,
      suppressed_ledger_path: DEFAULT_SUPPRESSED_LEDGER,
      grading_ledger_path: DEFAULT_GRADING_LEDGER,
      contributions_ledger_path: DEFAULT_CONTRIBUTIONS_LEDGER_JSON,
      markdown_summary_sections_are_noncanonical: true,
    },
    markdown_reference_sections: {
      sit_accountability: sitAccountability,
      scanner_statistics: scannerStats,
      market_confidence: marketConfidence,
      canonical_decision_engine: canonicalDecisionEngine,
      drawdown_governor: drawdownGovernor,
      edge_distribution: edgeDistribution,
      reliability_index: reliabilityIndex,
      daily_summary: dailySummary,
      market_type_reliability_index: marketTypeReliabilityIndex,
      sit_reason_code_standard: sitReasonCodeStandard,
      rule_ledger_pointer: ruleLedgerPointer,
      expectation_framing: expectationFraming,
      execution_quality_reference: executionQuality,
      ignored_summary_sections: ignoredMarkdownSummaries,
    },
    normalized: {
      // Canonical aliases to reduce key-shape drift while preserving legacy fields.
      open_exposure: openExposure,
      daily_verdict: dailyVerdict,
      starting_bankroll: bankrollContributionPolicy.starting_bankroll,
      actual_bankroll: bankrollContributionPolicy.actual_bankroll,
      strategy_equity: bankrollContributionPolicy.strategy_equity,
      overall_strategy_equity: bankrollContributionPolicy.overall_strategy_equity,
      core_strategy_equity: bankrollContributionPolicy.core_strategy_equity,
      total_external_contributions: bankrollContributionPolicy.total_external_contributions,
      realized_betting_profit_lifetime: bankrollContributionPolicy.realized_betting_profit_lifetime,
      core_edge_profit_lifetime: bankrollContributionPolicy.core_edge_profit_lifetime,
      fun_sgp_profit_lifetime: bankrollContributionPolicy.fun_sgp_profit_lifetime,
      bankroll_growth_from_betting: bankrollContributionPolicy.bankroll_growth_from_betting,
      bankroll_growth_from_contributions: bankrollContributionPolicy.bankroll_growth_from_contributions,
      realized_monthly_profit: bankrollContributionPolicy.realized_monthly_profit,
      rolling_average_realized_profit: bankrollContributionPolicy.rolling_average_realized_profit,
      next_estimated_contribution: bankrollContributionPolicy.next_estimated_contribution,
      contribution_basis_month_count: bankrollContributionPolicy.contribution_basis_month_count,
      last_contribution_amount: bankrollContributionPolicy.last_contribution_amount,
      last_contribution_date: bankrollContributionPolicy.last_contribution_date,
    },
    decision_quality: decisionQuality,
    pending_bets: pendingBets,
    todays_bets: todaysBets,
    bet_log: betLog,
    ledger,
    derived: {
      bankroll_numeric: bankrollValue,
      roi_percent_numeric: derivedLifetimeStats.overall_roi_numeric,
      today_recommended_count: todaysBets.length,
      pending_count: pendingBets.length,
      graded_bet_count: betLog.length,
      rejected_opportunities_count: rejectedOpportunities.length,
      decision_quality_rate: decisionQuality.decision_quality_rate,
      expected_profit: quantPerformance.expected_profit,
      expected_profit_units: quantPerformance.expected_profit_units,
      actual_profit: quantPerformance.actual_profit,
      actual_profit_units: quantPerformance.actual_profit_units,
      variance: quantPerformance.variance,
      variance_units: quantPerformance.variance_units,
      ev_realization_ratio: quantPerformance.ev_realization_ratio,
      total_units: quantPerformance.total_units,
      total_staked_units: quantPerformance.total_staked_units,
      roi_units: quantPerformance.roi_units,
      p_value: quantPerformance.p_value,
      confidence_level: quantPerformance.confidence_level,
      sample_status: quantPerformance.sample_status,
      redacted_pending: redactPending,
      starting_bankroll: bankrollContributionPolicy.starting_bankroll,
      actual_bankroll: bankrollContributionPolicy.actual_bankroll,
      strategy_equity: bankrollContributionPolicy.strategy_equity,
      total_external_contributions: bankrollContributionPolicy.total_external_contributions,
      realized_betting_profit_lifetime: bankrollContributionPolicy.realized_betting_profit_lifetime,
      bankroll_growth_from_betting: bankrollContributionPolicy.bankroll_growth_from_betting,
      bankroll_growth_from_contributions: bankrollContributionPolicy.bankroll_growth_from_contributions,
      realized_monthly_profit: bankrollContributionPolicy.realized_monthly_profit,
      rolling_average_realized_profit: bankrollContributionPolicy.rolling_average_realized_profit,
      next_estimated_contribution: bankrollContributionPolicy.next_estimated_contribution,
      contribution_basis_month_count: bankrollContributionPolicy.contribution_basis_month_count,
      last_contribution_amount: bankrollContributionPolicy.last_contribution_amount,
      last_contribution_date: bankrollContributionPolicy.last_contribution_date,
    },
    __canonical: {
      state_template: {
        generated_at_utc: generatedAtUtc,
        source_ingest: {
          betting_state_path: sourcePath,
          recommendation_log_path: recLogPath,
          runtime_status_snapshot_path: path.resolve(process.cwd(), 'data', 'openclaw-runtime-status.json'),
          contribution_ledger_path: DEFAULT_CONTRIBUTION_LEDGER,
          source_last_updated_ct: lastUpdatedCt,
          effective_last_updated_ct: effectiveLastUpdatedCt,
          target_date: targetDate,
        },
        canonical_rules: {
          actual_bets_source: 'bet_log_table',
          passes_source: nativeDecisionRows.length > 0 ? 'native_decision_0_to_2_pass_ledger' : 'recommendation_log_0_to_2_band',
          suppressed_candidates_source: nativeDecisionRows.length > 0 ? 'native_suppressed_candidates_ledger' : 'recommendation_log_reconstruction',
          bankroll_source: 'current_status_plus_ledger_reconciliation',
          contributions_source: 'bankroll_contributions_csv_with_ledger_fallback',
          grading_source: 'bet_log_settled_rows',
          markdown_summary_sections_noncanonical: Object.keys(ignoredMarkdownSummaries),
        },
        latest_run_artifact: canonicalRunArtifacts.latest_run,
        run_artifacts: canonicalRunArtifacts.artifacts,
        canonical_ledgers: {
          bets: { path: DEFAULT_BETS_LEDGER, row_count: canonicalLedgers.bets.length, rows: canonicalLedgers.bets },
          passes_zero_to_two: { path: DEFAULT_PASS_LEDGER, row_count: canonicalLedgers.passes_zero_to_two.length, rows: canonicalLedgers.passes_zero_to_two },
          suppressed_candidates: { path: DEFAULT_SUPPRESSED_LEDGER, row_count: canonicalLedgers.suppressed_candidates.length, rows: canonicalLedgers.suppressed_candidates },
          contributions: { path: DEFAULT_CONTRIBUTIONS_LEDGER_JSON, row_count: canonicalLedgers.contributions.length, rows: canonicalLedgers.contributions },
          grading_results: { path: DEFAULT_GRADING_LEDGER, row_count: canonicalLedgers.grading_results.length, rows: canonicalLedgers.grading_results },
          candidate_markets: { path: DEFAULT_CANDIDATE_MARKETS, row_count: canonicalLedgers.candidate_markets.length, rows: canonicalLedgers.candidate_markets },
          native_decision_observations: { path: DEFAULT_NATIVE_ALL_LEDGER, row_count: nativeDecisionRows.length },
          native_bets_ledger: { path: DEFAULT_NATIVE_BETS_LEDGER, row_count: readNativeDecisionLedger(DEFAULT_NATIVE_BETS_LEDGER).length },
          native_pass_ledger: { path: DEFAULT_NATIVE_PASS_LEDGER, row_count: readNativeDecisionLedger(DEFAULT_NATIVE_PASS_LEDGER).length },
          native_suppressed_ledger: { path: DEFAULT_NATIVE_SUPPRESSED_LEDGER, row_count: readNativeDecisionLedger(DEFAULT_NATIVE_SUPPRESSED_LEDGER).length },
        },
        canonical_state_success: {
          run_artifact_exists: Boolean(canonicalRunArtifacts.latest_run),
          canonical_state_write_required: true,
          canonical_ledgers_ready: true,
          downstream_payload_rebuild_required: true,
        },
      },
      candidate_market_rows: candidateMarketRows,
      suppressed_candidate_rows: suppressedCandidateRows,
      run_artifacts: canonicalRunArtifacts,
      ledgers: canonicalLedgers,
    },
  };
}

if (!fs.existsSync(sourcePath)) {
  fail(`Source file not found: ${sourcePath}`);
}

const markdown = fs.readFileSync(sourcePath, 'utf8');
const payload = buildPayload(markdown);
const canonicalTemplate = payload.__canonical?.state_template;
const candidateMarketRows = payload.__canonical?.candidate_market_rows || [];
const suppressedCandidateRows = payload.__canonical?.suppressed_candidate_rows || [];
delete payload.__canonical;

const canonicalState = {
  ...canonicalTemplate,
  public_payload: payload,
};

writeJson(DEFAULT_CANONICAL_STATE, canonicalState);
writeJson(DEFAULT_RUN_ARTIFACTS, {
  generated_at_utc: canonicalState.generated_at_utc,
  latest_run_artifact: canonicalState.latest_run_artifact,
  artifacts: canonicalState.run_artifacts,
});
writeJson(DEFAULT_BETS_LEDGER, canonicalState.canonical_ledgers?.bets || {});
writeJson(DEFAULT_PASS_LEDGER, canonicalState.canonical_ledgers?.passes_zero_to_two || {});
writeJson(DEFAULT_SUPPRESSED_LEDGER, canonicalState.canonical_ledgers?.suppressed_candidates || {});
writeJson(DEFAULT_GRADING_LEDGER, canonicalState.canonical_ledgers?.grading_results || {});
writeJson(DEFAULT_CONTRIBUTIONS_LEDGER_JSON, canonicalState.canonical_ledgers?.contributions || {});

const canonicalSnapshot = JSON.parse(fs.readFileSync(DEFAULT_CANONICAL_STATE, 'utf8'));
const publicPayload = canonicalSnapshot.public_payload;

writeCsv(DEFAULT_CANDIDATE_MARKETS, CANDIDATE_MARKET_HEADERS, canonicalSnapshot.canonical_ledgers?.candidate_markets?.rows || candidateMarketRows);
writeCsv(DEFAULT_SUPPRESSED_CANDIDATES, SUPPRESSED_CANDIDATE_HEADERS, canonicalSnapshot.canonical_ledgers?.suppressed_candidates?.rows || suppressedCandidateRows);

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(publicPayload, null, 2)}\n`, 'utf8');

const outDir = path.dirname(outPath);
if (publicPayload?.decision_renderers?.terminal_text) {
  fs.writeFileSync(path.join(outDir, 'decision-terminal.txt'), publicPayload.decision_renderers.terminal_text, 'utf8');
}
if (publicPayload?.decision_renderers?.whatsapp_text) {
  fs.writeFileSync(path.join(outDir, 'decision-whatsapp.txt'), publicPayload.decision_renderers.whatsapp_text, 'utf8');
}
if (publicPayload?.decision_renderers?.evening_grading_report_text) {
  fs.writeFileSync(path.join(outDir, 'evening-grading-report.txt'), publicPayload.decision_renderers.evening_grading_report_text, 'utf8');
}

console.log(`Built public data: ${outPath}`);
console.log(`Built canonical state: ${DEFAULT_CANONICAL_STATE}`);
console.log(`Schema: ${publicPayload.schema} | Last Updated (CT): ${publicPayload.last_updated_ct}`);
if (redactPending) {
  console.log('Pending plays were redacted (REDACT_PENDING=true).');
}

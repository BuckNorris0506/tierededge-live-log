import fs from 'node:fs';
import path from 'node:path';
import { OPENCLAW_PATHS } from './openclaw-runtime-utils.mjs';

const SCRIPT_DIR = path.dirname(new URL(import.meta.url).pathname);
const REPO_ROOT = path.resolve(SCRIPT_DIR, '..');

export const DEFAULT_SCAN_COVERAGE_POLICY = path.resolve(REPO_ROOT, 'config', 'scan-coverage-policy.json');

function readJsonSafe(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function readTextSafe(filePath, fallback = '') {
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch {
    return fallback;
  }
}

function parseCommaList(input) {
  return String(input || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseOddsApiConfigMarkdown(markdown) {
  const activeSportsBlock = String(markdown || '').match(/^SPORTS_ACTIVE=\s*([\s\S]*?)^\s*# Parameters/m)?.[1] || '';
  const activeSports = activeSportsBlock
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/,$/, ''))
    .filter(Boolean);

  return {
    active_sports: activeSports,
    bookmakers: parseCommaList(String(markdown || '').match(/^BOOKMAKERS=(.+)$/m)?.[1] || ''),
    markets: parseCommaList(String(markdown || '').match(/^MARKETS=(.+)$/m)?.[1] || ''),
    regions: String(markdown || '').match(/^REGIONS=(.+)$/m)?.[1]?.trim() || null,
  };
}

function monthActive(key, month) {
  const seasonal = {
    basketball_nba: [1, 2, 3, 4, 5, 10, 11, 12],
    basketball_ncaab: [1, 2, 3, 11, 12],
    icehockey_nhl: [1, 2, 3, 4, 10, 11, 12],
    baseball_mlb: [3, 4, 5, 6, 7, 8, 9, 10],
    mma_mixed_martial_arts: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    soccer_epl: [1, 2, 3, 4, 5, 8, 9, 10, 11, 12],
    americanfootball_nfl: [1, 9, 10, 11, 12],
    americanfootball_ncaaf: [1, 8, 9, 10, 11, 12],
    soccer_uefa_champs_league: [2, 3, 4, 9, 10, 11, 12],
    soccer_mls: [2, 3, 4, 5, 6, 7, 8, 9, 10],
    tennis_atp: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    tennis_wta: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    boxing_boxing: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    golf_pga: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    motorsport_nascar: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    darts_pdc: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    esports_lol: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    esports_cs2: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    esports_valorant: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    esports_dota2: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
  };
  const months = seasonal[key];
  return Array.isArray(months) ? months.includes(month) : true;
}

function unique(list) {
  return [...new Set((list || []).filter(Boolean))];
}

function countActiveSports(keys, month) {
  return unique(keys).filter((key) => monthActive(key, month)).length;
}

function round2(value) {
  return Number.isFinite(value) ? Math.round(value * 100) / 100 : null;
}

export function loadScanCoveragePolicy(policyPath = DEFAULT_SCAN_COVERAGE_POLICY) {
  return readJsonSafe(policyPath, null);
}

export function computeScanCoverageArtifacts({
  policy = loadScanCoveragePolicy(),
  asOfDate = new Date(),
  oddsApiConfigPath = OPENCLAW_PATHS.oddsApiConfig,
} = {}) {
  const configMarkdown = readTextSafe(oddsApiConfigPath, '');
  const oddsConfig = parseOddsApiConfigMarkdown(configMarkdown);
  const month = Number(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Chicago',
    month: 'numeric',
  }).format(asOfDate));

  const tierAKeys = policy?.priority_tiers?.tier_a?.sports || [];
  const tierBKeys = policy?.priority_tiers?.tier_b?.sports || [];
  const tierCKeys = policy?.priority_tiers?.tier_c?.sports || [];

  const activeTierA = countActiveSports(tierAKeys, month);
  const activeTierB = countActiveSports(tierBKeys, month);
  const activeTierC = countActiveSports(tierCKeys, month);
  const activeConfiguredSports = countActiveSports(oddsConfig.active_sports, month);

  const sportsIndexCost = policy?.request_cost_model?.sports_index_request_cost ?? 1;
  const sportOddsCost = policy?.request_cost_model?.sport_odds_request_cost ?? 1;

  const legacyMorningBroadRun = sportsIndexCost + (activeConfiguredSports * sportOddsCost);
  const optimizedMorningBaseline = sportsIndexCost + (activeTierA * sportOddsCost);
  const optimizedMorningExpanded = optimizedMorningBaseline + (activeTierB * sportOddsCost);
  const optimizedMorningMax = optimizedMorningExpanded + (activeTierC * sportOddsCost);

  const fridaySgpBaseline = 1;
  const fridaySgpExpanded = Math.min(2, activeTierA);
  const gradingBaseline = 0;
  const gradingWithCacheMiss = 3;

  const dailyOptimizedTypical = optimizedMorningBaseline + gradingBaseline;
  const dailyOptimizedExpanded = optimizedMorningExpanded + gradingWithCacheMiss;
  const weeklyOptimizedTypical = (optimizedMorningBaseline * 7) + fridaySgpBaseline + gradingWithCacheMiss;
  const weeklyOptimizedExpanded = (optimizedMorningExpanded * 7) + fridaySgpExpanded + (gradingWithCacheMiss * 3);
  const weeklyLegacyBroad = legacyMorningBroadRun * 7;

  const configuredDailyBudget = Number(policy?.configured_request_budget_daily || 0);
  const configuredWeeklyBudget = Number(policy?.configured_request_budget_weekly || 0);

  return {
    schema: policy?.schema || 'tierededge_scan_coverage_v1',
    primary_feed: policy?.primary_feed || 'The Odds API',
    current_month: month,
    configured_budget: {
      daily_requests: configuredDailyBudget,
      weekly_requests: configuredWeeklyBudget,
      working_daily_soft_cap: Number(policy?.working_daily_soft_cap || 0),
      low_quota_guardrail_threshold: Number(policy?.low_quota_guardrail_threshold || 0),
    },
    current_config_snapshot: {
      active_sports_count: activeConfiguredSports,
      bookmakers: oddsConfig.bookmakers,
      markets: oddsConfig.markets,
      regions: oddsConfig.regions,
    },
    request_budget_model: {
      legacy_broad_scan_estimate: {
        morning_edge_hunt_per_run: legacyMorningBroadRun,
        daily_total: legacyMorningBroadRun,
        weekly_total: weeklyLegacyBroad,
      },
      optimized_policy_estimate: {
        daily_typical: dailyOptimizedTypical,
        daily_expanded: dailyOptimizedExpanded,
        weekly_typical: weeklyOptimizedTypical,
        weekly_expanded: weeklyOptimizedExpanded,
      },
      jobs: {
        morning_edge_hunt: {
          current_estimated_requests_per_run: legacyMorningBroadRun,
          optimized_typical_requests_per_run: optimizedMorningBaseline,
          optimized_expanded_requests_per_run: optimizedMorningExpanded,
          notes: 'One /sports call plus one odds call per sport scanned. Books and main markets are bundled in the same sport request.'
        },
        evening_grading: {
          current_estimated_requests_per_run: 0,
          optimized_typical_requests_per_run: 0,
          optimized_expanded_requests_per_run: 0,
          notes: 'Settlement uses logged bet state, not live odds requests.'
        },
        friday_sgp: {
          current_estimated_requests_per_run: 1,
          optimized_typical_requests_per_run: fridaySgpBaseline,
          optimized_expanded_requests_per_run: fridaySgpExpanded,
          notes: 'Today-only SGP should stay focused on one same-game slate, not a broad multi-sport scan.'
        },
        passed_opportunity_grading: {
          current_estimated_requests_per_run: gradingWithCacheMiss,
          optimized_typical_requests_per_run: 0,
          optimized_expanded_requests_per_run: gradingWithCacheMiss,
          notes: 'Scores lookups are now cacheable; most rebuilds should reuse completed score responses.'
        },
        enrichment_and_audits: {
          current_estimated_requests_per_run: 0,
          optimized_typical_requests_per_run: 0,
          optimized_expanded_requests_per_run: 0,
          notes: 'Suppression/monthly audits are local-derivation jobs only.'
        },
      },
      wasted_requests_today_if_unbounded: Math.max(0, legacyMorningBroadRun - optimizedMorningBaseline),
      daily_headroom_after_typical_usage: configuredDailyBudget > 0 ? Math.max(0, configuredDailyBudget - dailyOptimizedTypical) : null,
      weekly_headroom_after_typical_usage: configuredWeeklyBudget > 0 ? Math.max(0, configuredWeeklyBudget - weeklyOptimizedTypical) : null,
    },
    scan_priority_design: {
      tier_a: {
        label: policy?.priority_tiers?.tier_a?.label || 'Highest priority',
        active_sports_this_month: unique(tierAKeys).filter((key) => monthActive(key, month)),
        books: policy?.priority_tiers?.tier_a?.default_books || [],
        comparison_books: policy?.priority_tiers?.tier_a?.comparison_books || [],
        markets: policy?.priority_tiers?.tier_a?.markets || [],
      },
      tier_b: {
        label: policy?.priority_tiers?.tier_b?.label || 'Medium priority',
        active_sports_this_month: unique(tierBKeys).filter((key) => monthActive(key, month)),
        books: policy?.priority_tiers?.tier_b?.default_books || [],
        comparison_books: policy?.priority_tiers?.tier_b?.comparison_books || [],
        markets: policy?.priority_tiers?.tier_b?.markets || [],
      },
      tier_c: {
        label: policy?.priority_tiers?.tier_c?.label || 'Low priority',
        active_sports_this_month: unique(tierCKeys).filter((key) => monthActive(key, month)),
        books: policy?.priority_tiers?.tier_c?.default_books || [],
        comparison_books: policy?.priority_tiers?.tier_c?.comparison_books || [],
        markets: policy?.priority_tiers?.tier_c?.markets || [],
      },
      expansion_rules: {
        candidate_density_floor: Number(policy?.expansion_rules?.candidate_density_floor || 0),
        near_miss_floor: Number(policy?.expansion_rules?.near_miss_floor || 0),
        expand_to_tier_b_if_sparse: Boolean(policy?.expansion_rules?.expand_to_tier_b_if_sparse),
        expand_to_tier_c_only_with_surplus: Boolean(policy?.expansion_rules?.expand_to_tier_c_only_with_surplus),
        tier_c_minimum_headroom: Number(policy?.expansion_rules?.tier_c_minimum_headroom || 0),
        comparison_books_expand_only_after_core_scan: Boolean(policy?.expansion_rules?.comparison_books_expand_only_after_core_scan),
        props_enabled_default: Boolean(policy?.expansion_rules?.props_enabled_default),
        alt_lines_enabled_default: Boolean(policy?.expansion_rules?.alt_lines_enabled_default),
      },
    },
    cache_reuse_policy: {
      tier_a_cache_minutes: Number(policy?.priority_tiers?.tier_a?.cache_minutes || 0),
      tier_b_cache_minutes: Number(policy?.priority_tiers?.tier_b?.cache_minutes || 0),
      tier_c_cache_minutes: Number(policy?.priority_tiers?.tier_c?.cache_minutes || 0),
      scores_cache_minutes: Number(policy?.grading_reuse_policy?.scores_cache_minutes || 0),
      reuse_completed_scores: Boolean(policy?.grading_reuse_policy?.reuse_completed_scores),
      notes: [
        'Tier A markets refresh fastest because those books move fastest and matter most.',
        'Tier B/Tier C can reuse responses longer without hurting operator value.',
        'Completed-score lookups should be reused aggressively; they do not need a 10-minute refresh cadence.'
      ],
    },
    operator_value_notes: {
      wasted_requests_now: [
        'Broad all-sports morning scans spend requests on low-value sports that rarely produce operator-actionable edges.',
        'Repeated score fetches during rebuild cycles waste requests unless cached.',
        'Scanning props and alt lines by default would add noise before the main markets are exhausted.'
      ],
      high_value_expansions: [
        'Expand Tier A to additional comparison books inside the same sport request when coverage is sparse.',
        'Expand to Tier B only after the core board is thin or near-miss density is too low.',
        'Keep props and alt lines off by default until native decision-time coverage is stronger.'
      ],
    },
  };
}

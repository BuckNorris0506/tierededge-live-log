import fs from 'node:fs';
import path from 'node:path';

export const OPENCLAW_PATHS = {
  root: '/Users/jaredbuckman/.openclaw',
  jobs: '/Users/jaredbuckman/.openclaw/cron/jobs.json',
  runsDir: '/Users/jaredbuckman/.openclaw/cron/runs',
  bettingState: '/Users/jaredbuckman/.openclaw/workspace/memory/betting-state.md',
  recommendationLog: '/Users/jaredbuckman/.openclaw/workspace/memory/recommendation-log.md',
  passedOpportunityGrades: '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json',
  oddsApiConfig: '/Users/jaredbuckman/.openclaw/workspace/memory/odds-api-config.md',
};

export const DEFAULT_RUNTIME_STATUS_SNAPSHOT = path.resolve(process.cwd(), 'data', 'openclaw-runtime-status.json');

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

function statMsSafe(filePath) {
  try {
    return fs.statSync(filePath).mtimeMs;
  } catch {
    return null;
  }
}

export function parseTimestampMs(input) {
  const value = String(input || '').trim();
  if (!value) return null;

  const direct = Date.parse(value);
  if (Number.isFinite(direct)) return direct;

  const ctMatch = value.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{1,2}:\d{2})(?:\s*([AP]M))?\s*CT$/i);
  if (ctMatch) {
    let hours;
    let minutes;
    if (ctMatch[3]) {
      const [h, m] = ctMatch[2].split(':');
      hours = Number(h);
      minutes = Number(m);
      const marker = ctMatch[3].toUpperCase();
      if (marker === 'PM' && hours < 12) hours += 12;
      if (marker === 'AM' && hours === 12) hours = 0;
    } else {
      const [h, m] = ctMatch[2].split(':');
      hours = Number(h);
      minutes = Number(m);
    }
    const local = new Date(Number(ctMatch[1].slice(0, 4)), Number(ctMatch[1].slice(5, 7)) - 1, Number(ctMatch[1].slice(8, 10)), hours, minutes, 0, 0);
    return Number.isFinite(local.getTime()) ? local.getTime() : null;
  }

  const dateOnly = value.match(/(\d{4}-\d{2}-\d{2})/);
  if (dateOnly) {
    const parsed = Date.parse(`${dateOnly[1]}T00:00:00Z`);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function formatCtTimestamp(inputMs) {
  if (!Number.isFinite(inputMs)) return null;
  const date = new Date(inputMs);
  const datePart = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Chicago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  const timePart = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Chicago',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date);
  return `${datePart} ${timePart} CT`;
}

function extractDateKey(input) {
  const match = String(input || '').match(/(\d{4}-\d{2}-\d{2})/);
  return match ? match[1] : null;
}

function parseBettingStateLastUpdated(markdown) {
  const match = String(markdown || '').match(/^Last Updated:\s*(.+)$/m);
  return match ? match[1].trim() : null;
}

function parseOddsApiConfig(markdown) {
  const envName =
    String(markdown || '').match(/^API_KEY_ENV=(.+)$/m)?.[1]?.trim()
    || String(markdown || '').match(/^\s*api_key_env:\s*(.+)$/mi)?.[1]?.trim()
    || 'ODDS_API_KEY';
  const envValue = process.env[envName] || null;
  const rawApiKey = String(markdown || '').match(/^API_KEY=(.+)$/m)?.[1]?.trim() || null;
  const apiKey = (typeof envValue === 'string' && envValue.trim()) ? envValue.trim() : rawApiKey;
  const baseUrl = String(markdown || '').match(/^BASE_URL=(.+)$/m)?.[1]?.trim() || null;
  const freeTierScan = String(markdown || '').match(/One full scan at (\d{1,2}:\d{2})\s*AM CT/i)?.[1] || null;
  const normalizedFreeTierScan = freeTierScan
    ? `${String(Number(freeTierScan.split(':')[0])).padStart(2, '0')}:${freeTierScan.split(':')[1]}`
    : null;
  return {
    key_present: Boolean(apiKey),
    key_suffix: apiKey ? apiKey.slice(-4) : null,
    api_key_env: envName,
    api_key_source: apiKey === rawApiKey ? 'config_file' : 'environment',
    base_url: baseUrl,
    source_status: apiKey ? 'present' : 'missing_api_key',
    free_tier_scan_ct: normalizedFreeTierScan,
    source_path: OPENCLAW_PATHS.oddsApiConfig,
  };
}

function detectHuntDataFailureSignals(text) {
  const normalized = String(text || '').replace(/\s+/g, ' ').trim();
  const upper = normalized.toUpperCase();
  const codes = [];
  if (!normalized) return codes;
  if (/MISSING API KEY|NO ODDS API KEY|ODDS API KEY MISSING|API KEY NOT CONFIGURED/.test(upper)) codes.push('missing_api_key');
  if (/RATE LIMIT|RATELIMIT|QUOTA|429/.test(upper)) codes.push('quota_or_rate_limit');
  if (/PARTIAL DATA|PARTIAL RESPONSE|INCOMPLETE DATA|INCOMPLETE RESPONSE|MISSING BOOKS|INSUFFICIENT BOOKS/.test(upper)) codes.push('partial_api_data');
  if (/MALFORMED RESPONSE|INVALID JSON|PARSE ERROR|SCHEMA ERROR|VALIDATION FAILED/.test(upper)) codes.push('malformed_response');
  if (/STALE RESPONSE|STALE DATA|STALE OR UNVERIFIED ODDS|CANNOT_VERIFY_ODDS|ODDS COULD NOT BE VERIFIED/.test(upper)) codes.push('stale_response');
  return [...new Set(codes)];
}

function extractCronTimeCt(expr) {
  const parts = String(expr || '').trim().split(/\s+/);
  if (parts.length < 2) return null;
  const minute = Number(parts[0]);
  const hour = Number(parts[1]);
  if (!Number.isFinite(minute) || !Number.isFinite(hour)) return null;
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
}

function readRunEvents(runFile) {
  const text = readTextSafe(runFile, '');
  return text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .filter((event) => event.action === 'finished');
}

function classifyHuntSummary(summary) {
  const text = String(summary || '').replace(/\s+/g, ' ').trim();
  const upper = text.toUpperCase();
  const dataFailureCodes = detectHuntDataFailureSignals(text);
  const playsMatch = text.match(/VERDICT:\s*(\d+)\s+plays found/i);
  const explicitSit = /VERDICT:\s*SIT/i.test(text) || /DECISION:\s*SIT/i.test(text);
  const explicitBlocked = /Status:\s*BLOCKED/i.test(text) || /Integrity gate failed/i.test(text);
  const cannotVerify = /CANNOT_VERIFY_ODDS/i.test(upper);
  const noPlays =
    /NO PLAYS/i.test(upper)
    || /0 plays found/i.test(text)
    || /Full pass\./i.test(text)
    || /No qualifying edges found/i.test(text)
    || /No \+EV edges? meet tier thresholds/i.test(text)
    || /RECOMMENDED PLAYS:\s*\*?No /i.test(text)
    || /RECOMMENDED PLAYS:\s*None/i.test(text);
  const hasActionableBets =
    (playsMatch && Number(playsMatch[1]) > 0)
    || (/RECOMMENDED PLAYS:/i.test(text) && !noPlays && !cannotVerify);

  if (explicitBlocked || cannotVerify) {
    return {
      message_type: 'BLOCKED',
      has_actionable_bets: false,
      requires_state_sync: false,
      data_failure_codes: dataFailureCodes,
      data_status: dataFailureCodes.length > 0 ? 'degraded_data' : 'blocked',
      plain_reason: cannotVerify
        ? 'Odds could not be verified for the latest scheduled scan.'
        : 'Integrity gate failed during the latest scheduled scan.',
    };
  }
  if (hasActionableBets) {
    return {
      message_type: 'BET',
      has_actionable_bets: true,
      requires_state_sync: true,
      data_failure_codes: dataFailureCodes,
      data_status: dataFailureCodes.length > 0 ? 'degraded_data' : 'verified',
      plain_reason: 'The latest scheduled scan reported at least one actionable bet.',
    };
  }
  if (explicitSit || noPlays) {
    return {
      message_type: 'SIT',
      has_actionable_bets: false,
      requires_state_sync: false,
      data_failure_codes: dataFailureCodes,
      data_status: dataFailureCodes.length > 0 ? 'degraded_data' : 'verified',
      plain_reason: 'The latest scheduled scan found no qualifying edges.',
    };
  }
  return {
    message_type: 'UNKNOWN',
    has_actionable_bets: false,
    requires_state_sync: false,
    data_failure_codes: dataFailureCodes,
    data_status: dataFailureCodes.length > 0 ? 'degraded_data' : 'unknown',
    plain_reason: 'The latest scheduled scan could not be classified reliably.',
  };
}

function classifyGradingSummary(summary) {
  const text = String(summary || '').replace(/\s+/g, ' ').trim();
  const noStateChange =
    /no pending bets/i.test(text)
    || /no action required/i.test(text)
    || /0 bets graded/i.test(text)
    || /no state updates required/i.test(text);
  return {
    requires_state_sync: !noStateChange,
    plain_reason: noStateChange
      ? 'Latest grading run reported no state changes.'
      : 'Latest grading run implies bankroll or result updates.',
  };
}

function summarizeRun(event, type) {
  if (!event) return null;
  const runAtMs = Number.isFinite(event.runAtMs) ? event.runAtMs : (Number.isFinite(event.ts) ? event.ts : null);
  const classifier = type === 'hunt' ? classifyHuntSummary(event.summary) : classifyGradingSummary(event.summary);
  return {
    status: event.status || null,
    delivery_status: event.deliveryStatus || null,
    error: event.error || null,
    run_at_ms: runAtMs,
    run_at_ct: formatCtTimestamp(runAtMs),
    date_key: extractDateKey(formatCtTimestamp(runAtMs)),
    summary: event.summary || null,
    session_id: event.sessionId || null,
    ...classifier,
  };
}

function buildJobStatus(job, type) {
  if (!job) return null;
  const runFile = path.join(OPENCLAW_PATHS.runsDir, `${job.id}.jsonl`);
  const events = readRunEvents(runFile);
  const latestFinished = summarizeRun(events[events.length - 1], type);
  const latestSuccessful = summarizeRun([...events].reverse().find((event) => event.status === 'ok'), type);
  const successfulRuns = events
    .filter((event) => event.status === 'ok')
    .map((event) => summarizeRun(event, type))
    .filter(Boolean);
  return {
    id: job.id,
    name: job.name,
    enabled: job.enabled === true,
    schedule_expr: job.schedule?.expr || null,
    schedule_tz: job.schedule?.tz || null,
    schedule_time_ct: extractCronTimeCt(job.schedule?.expr),
    payload_message: job.payload?.message || null,
    latest_finished: latestFinished,
    latest_successful: latestSuccessful,
    successful_runs: successfulRuns,
  };
}

function computeConfigWarnings(jobStatuses, oddsApiConfig) {
  const warnings = [];
  const monthlyReload = jobStatuses.monthly_reload;
  if (monthlyReload?.enabled) warnings.push('monthly_reload_enabled');
  const morningHunt = jobStatuses.morning_edge_hunt;
  if (morningHunt?.payload_message && /8:00 AM CT/i.test(morningHunt.payload_message)) warnings.push('morning_hunt_prompt_schedule_drift');
  if (oddsApiConfig?.free_tier_scan_ct && morningHunt?.schedule_time_ct && oddsApiConfig.free_tier_scan_ct !== morningHunt.schedule_time_ct) {
    warnings.push('odds_config_scan_policy_drift');
  }
  if (oddsApiConfig?.key_present !== true) warnings.push('odds_api_key_missing');
  const fridaySgp = jobStatuses.friday_sgp;
  if (fridaySgp?.payload_message && !/today only/i.test(fridaySgp.payload_message)) warnings.push('friday_sgp_prompt_missing_today_guard');
  return warnings;
}

export function buildRuntimeStatus() {
  const jobs = readJsonSafe(OPENCLAW_PATHS.jobs, { jobs: [] })?.jobs || [];
  const byName = Object.fromEntries(jobs.map((job) => [job.name, job]));
  const bettingStateMarkdown = readTextSafe(OPENCLAW_PATHS.bettingState, '');
  const oddsApiConfig = parseOddsApiConfig(readTextSafe(OPENCLAW_PATHS.oddsApiConfig, ''));
  const stateLastUpdatedCt = parseBettingStateLastUpdated(bettingStateMarkdown);
  const stateLastUpdatedMs = parseTimestampMs(stateLastUpdatedCt);

  const jobStatuses = {
    morning_edge_hunt: buildJobStatus(byName['morning-edge-hunt'], 'hunt'),
    evening_grading: buildJobStatus(byName['evening-grading'], 'grading'),
    friday_sgp: buildJobStatus(byName['friday-sgp'], 'hunt'),
    weekly_review: buildJobStatus(byName['weekly-review'], 'grading'),
    monthly_reload: buildJobStatus(byName['monthly-reload'], 'grading'),
  };

  const latestHunt = jobStatuses.morning_edge_hunt?.latest_successful || null;
  const latestGrading = jobStatuses.evening_grading?.latest_successful || null;

  const huntAfterState = Number.isFinite(latestHunt?.run_at_ms) && Number.isFinite(stateLastUpdatedMs)
    ? latestHunt.run_at_ms > stateLastUpdatedMs
    : false;
  const gradingAfterState = Number.isFinite(latestGrading?.run_at_ms) && Number.isFinite(stateLastUpdatedMs)
    ? latestGrading.run_at_ms > stateLastUpdatedMs
    : false;

  const blockingSyncGap =
    (huntAfterState && latestHunt?.requires_state_sync === true)
    || (gradingAfterState && latestGrading?.requires_state_sync === true);

  let freshnessAnchor = {
    source: 'state_last_updated',
    timestamp_ct: stateLastUpdatedCt,
    timestamp_ms: stateLastUpdatedMs,
  };

  if (!blockingSyncGap && Number.isFinite(latestHunt?.run_at_ms) && (!Number.isFinite(stateLastUpdatedMs) || latestHunt.run_at_ms > stateLastUpdatedMs)) {
    freshnessAnchor = {
      source: 'latest_successful_hunt',
      timestamp_ct: latestHunt.run_at_ct,
      timestamp_ms: latestHunt.run_at_ms,
    };
  }

  const stateFiles = {
    betting_state: {
      path: OPENCLAW_PATHS.bettingState,
      last_updated_ct: stateLastUpdatedCt,
      file_mtime_ct: formatCtTimestamp(statMsSafe(OPENCLAW_PATHS.bettingState)),
    },
    recommendation_log: {
      path: OPENCLAW_PATHS.recommendationLog,
      file_mtime_ct: formatCtTimestamp(statMsSafe(OPENCLAW_PATHS.recommendationLog)),
    },
    passed_opportunity_grades: {
      path: OPENCLAW_PATHS.passedOpportunityGrades,
      file_mtime_ct: formatCtTimestamp(statMsSafe(OPENCLAW_PATHS.passedOpportunityGrades)),
    },
  };

  const warnings = computeConfigWarnings(jobStatuses, oddsApiConfig);
  if (blockingSyncGap) warnings.push('state_sync_gap');

  return {
    generated_at_utc: new Date().toISOString(),
    jobs: jobStatuses,
    latest_successful_hunt: latestHunt,
    latest_successful_grading: latestGrading,
    successful_hunt_days: [...new Set((jobStatuses.morning_edge_hunt?.successful_runs || [])
      .map((run) => run.date_key)
      .filter(Boolean))],
    no_append_hunt_days: [...new Set((jobStatuses.morning_edge_hunt?.successful_runs || [])
      .filter((run) => run.requires_state_sync === false && ['SIT', 'BLOCKED'].includes(String(run.message_type || '').toUpperCase()))
      .map((run) => run.date_key)
      .filter(Boolean))],
    next_edge_scan_ct: jobStatuses.morning_edge_hunt?.schedule_time_ct || null,
    odds_api_config: oddsApiConfig,
    freshness_anchor: freshnessAnchor,
    state_files: stateFiles,
    state_sync: {
      blocking_sync_gap: blockingSyncGap,
      hunt_after_state: huntAfterState,
      grading_after_state: gradingAfterState,
      latest_hunt_requires_state_sync: latestHunt?.requires_state_sync === true,
      latest_grading_requires_state_sync: latestGrading?.requires_state_sync === true,
    },
    warnings,
  };
}

export function readRuntimeStatusSnapshot(snapshotPath = DEFAULT_RUNTIME_STATUS_SNAPSHOT) {
  return readJsonSafe(snapshotPath, null);
}

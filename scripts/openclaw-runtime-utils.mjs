import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { CORE_PATHS } from './core-ledger-utils.mjs';

const SCRIPT_DIR = path.dirname(new URL(import.meta.url).pathname);
const REPO_ROOT = path.resolve(SCRIPT_DIR, '..');

export const OPENCLAW_PATHS = {
  root: '/Users/jaredbuckman/.openclaw',
  jobs: '/Users/jaredbuckman/.openclaw/cron/jobs.json',
  runsDir: '/Users/jaredbuckman/.openclaw/cron/runs',
  sessionsDir: '/Users/jaredbuckman/.openclaw/agents/main/sessions',
  sessionsIndex: '/Users/jaredbuckman/.openclaw/agents/main/sessions/sessions.json',
  bettingState: '/Users/jaredbuckman/.openclaw/workspace/memory/betting-state.md',
  recommendationLog: '/Users/jaredbuckman/.openclaw/workspace/memory/recommendation-log.md',
  passedOpportunityGrades: '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json',
  oddsApiConfig: '/Users/jaredbuckman/.openclaw/workspace/memory/odds-api-config.md',
  repoRoot: REPO_ROOT,
};

export const DEFAULT_RUNTIME_STATUS_SNAPSHOT = path.resolve(REPO_ROOT, 'data', 'openclaw-runtime-status.json');

function readJsonSafe(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function readJsonlSafe(filePath, fallback = []) {
  try {
    return fs.readFileSync(filePath, 'utf8')
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
      .filter(Boolean);
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

function readCanonicalHuntRun() {
  return readJsonSafe(CORE_PATHS.canonicalHuntRun, null);
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

function readSecureOddsKeyStatus(envName = 'ODDS_API_KEY') {
  try {
    const stdout = execFileSync('env', [
      '-u',
      envName,
      'node',
      '/Users/jaredbuckman/.openclaw/workspace/tierededge_runtime/odds-key-cli.mjs',
      'status',
    ], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const parsed = JSON.parse(String(stdout || '{}'));
    return {
      configured: parsed.configured === true,
      provider: parsed.provider || null,
      format_valid: parsed.format_valid === true,
    };
  } catch {
    return {
      configured: false,
      provider: null,
      format_valid: false,
    };
  }
}

function parseOddsApiConfig(markdown) {
  const envName =
    String(markdown || '').match(/^API_KEY_ENV=(.+)$/m)?.[1]?.trim()
    || String(markdown || '').match(/^\s*api_key_env:\s*(.+)$/mi)?.[1]?.trim()
    || 'ODDS_API_KEY';
  const envValue = process.env[envName] || null;
  const rawApiKey = String(markdown || '').match(/^API_KEY=(.+)$/m)?.[1]?.trim() || null;
  const envKey = (typeof envValue === 'string' && envValue.trim()) ? envValue.trim() : null;
  const secureStatus = readSecureOddsKeyStatus(envName);
  const apiKey = envKey || rawApiKey;
  const keyPresent = Boolean(envKey || rawApiKey || secureStatus.configured);
  const baseUrl = String(markdown || '').match(/^BASE_URL=(.+)$/m)?.[1]?.trim() || null;
  const freeTierScan = String(markdown || '').match(/One full scan at (\d{1,2}:\d{2})\s*AM CT/i)?.[1] || null;
  const normalizedFreeTierScan = freeTierScan
    ? `${String(Number(freeTierScan.split(':')[0])).padStart(2, '0')}:${freeTierScan.split(':')[1]}`
    : null;
  let apiKeySource = 'missing';
  if (envKey) {
    apiKeySource = 'environment';
  } else if (rawApiKey) {
    apiKeySource = 'config_file';
  } else if (secureStatus.configured) {
    apiKeySource = secureStatus.provider || 'secure_store';
  } else if (envName) {
    apiKeySource = 'environment_reference';
  }
  return {
    key_present: keyPresent,
    key_suffix: apiKey ? apiKey.slice(-4) : null,
    api_key_env: envName,
    api_key_source: apiKeySource,
    secure_key_present: secureStatus.configured,
    secure_key_provider: secureStatus.provider,
    base_url: baseUrl,
    source_status: keyPresent ? 'present' : 'missing_api_key',
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
  if (/INVALID_KEY|AUTHENTICATION FAILURE|UNAUTHORIZED|401/.test(upper)) codes.push('auth_failure');
  if (/RATE LIMIT|RATELIMIT|QUOTA|429/.test(upper)) codes.push('quota_or_rate_limit');
  if (/PARTIAL DATA|PARTIAL RESPONSE|INCOMPLETE DATA|INCOMPLETE RESPONSE|MISSING BOOKS|INSUFFICIENT BOOKS/.test(upper)) codes.push('partial_api_data');
  if (/MALFORMED RESPONSE|INVALID JSON|PARSE ERROR|SCHEMA ERROR|VALIDATION FAILED/.test(upper)) codes.push('malformed_response');
  if (/STALE RESPONSE|STALE DATA|STALE OR UNVERIFIED ODDS|CANNOT_VERIFY_ODDS|ODDS COULD NOT BE VERIFIED/.test(upper)) codes.push('stale_response');
  if (/GATEWAY TIMEOUT|ENGINE_OVERLOADED|UNHANDLED STOP REASON|RPC PROBE FAILED|SERVICE UNAVAILABLE|TIMED OUT/.test(upper)) codes.push('runtime_gateway_failure');
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
  const looksIncomplete =
    !/VERDICT:/i.test(text)
    && !/DECISION:/i.test(text)
    && (
      /NOW ANALYZING/i.test(upper)
      || /LET ME PROCESS/i.test(upper)
      || /I HAVE LIVE ODDS DATA/i.test(upper)
      || /I['’]VE RECEIVED FRESH ODDS DATA/i.test(upper)
    );
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
  if (looksIncomplete) {
    return {
      message_type: 'BLOCKED',
      has_actionable_bets: false,
      requires_state_sync: false,
      data_failure_codes: [...new Set([...dataFailureCodes, 'runtime_gateway_failure'])],
      data_status: 'degraded_data',
      plain_reason: 'Latest scheduled scan returned an incomplete summary after fetching odds.',
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
  const nativeAppendFailed =
    /native ledger append failed/i.test(text)
    || /native append failed/i.test(text);
  return {
    requires_state_sync: !noStateChange,
    native_append_failed: nativeAppendFailed,
    plain_reason: nativeAppendFailed
      ? 'Latest grading summary reported markdown updates after a native-ledger append failure.'
      : noStateChange
      ? 'Latest grading run reported no state changes.'
      : 'Latest grading run implies bankroll or result updates.',
  };
}

function readSessionTranscript(sessionId) {
  if (!sessionId) return null;
  const sessionFile = path.join(OPENCLAW_PATHS.sessionsDir, `${sessionId}.jsonl`);
  const transcript = readTextSafe(sessionFile, '');
  if (!transcript) return null;

  let latestAssistantText = null;
  let latestToolAggregated = null;
  const lines = transcript
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    try {
      const entry = JSON.parse(line);
      if (entry?.type !== 'message') continue;
      const message = entry.message || {};
      const content = Array.isArray(message.content) ? message.content : [];
      if (message.role === 'assistant') {
        const textParts = content
          .filter((item) => item?.type === 'text' && item.text)
          .map((item) => item.text);
        if (textParts.length) latestAssistantText = textParts.join('\n');
      }
      if (message.role === 'toolResult' && message.details?.aggregated) {
        latestToolAggregated = message.details.aggregated;
      }
    } catch {
      continue;
    }
  }

  const resolvedSummary = latestAssistantText || latestToolAggregated || null;
  return {
    session_file: sessionFile,
    transcript,
    resolved_summary: resolvedSummary,
  };
}

function summarizeRun(event, type) {
  if (!event) return null;
  const runAtMs = Number.isFinite(event.runAtMs) ? event.runAtMs : (Number.isFinite(event.ts) ? event.ts : null);
  const sessionData = readSessionTranscript(event.sessionId);
  const effectiveSummary = sessionData?.resolved_summary || event.summary || null;
  const classifier = type === 'hunt' ? classifyHuntSummary(effectiveSummary) : classifyGradingSummary(effectiveSummary);
  return {
    status: event.status || null,
    delivery_status: event.deliveryStatus || null,
    error: event.error || null,
    run_at_ms: runAtMs,
    run_at_ct: formatCtTimestamp(runAtMs),
    date_key: extractDateKey(formatCtTimestamp(runAtMs)),
    summary: effectiveSummary,
    raw_summary: event.summary || null,
    session_id: event.sessionId || null,
    session_file: sessionData?.session_file || null,
    ...classifier,
  };
}

function readSessionsIndex() {
  return readJsonSafe(OPENCLAW_PATHS.sessionsIndex, {});
}

function detectSessionAttemptSignals(text) {
  const lines = String(text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  const relevantFragments = [];
  for (const line of lines) {
    try {
      const entry = JSON.parse(line);
      if (entry?.type !== 'message') continue;
      const message = entry.message || {};
      if (message.role === 'assistant') {
        if (entry.errorMessage) relevantFragments.push(entry.errorMessage);
        const content = Array.isArray(message.content) ? message.content : [];
        for (const item of content) {
          if (item?.type === 'text' && item.text) relevantFragments.push(item.text);
        }
      } else if (message.role === 'toolResult') {
        const toolCallId = String(message.toolCallId || '');
        if (/^read:/.test(toolCallId)) continue;
        const content = Array.isArray(message.content) ? message.content : [];
        for (const item of content) {
          if (item?.type === 'text' && item.text) relevantFragments.push(item.text);
        }
        if (message.details?.aggregated) relevantFragments.push(message.details.aggregated);
      }
    } catch {
      continue;
    }
  }

  const normalized = relevantFragments.join('\n').replace(/\s+/g, ' ').trim();
  const upper = normalized.toUpperCase();
  const dataFailureCodes = detectHuntDataFailureSignals(normalized);
  const hasLiveOddsPayload =
    /"SPORT_KEY":"BASKETBALL_NBA"/.test(upper)
    || /"SPORT_KEY":"BASKETBALL_NCAAB"/.test(upper)
    || /"SPORT_KEY":"ICEHOCKEY_NHL"/.test(upper)
    || /BOOKMAKERS/.test(upper)
    || /ALL TIER A MARKETS/.test(upper);
  const hasQuotaFailure = /OUT_OF_USAGE_CREDITS|0 REQUESTS REMAINING|RATE LIMIT EXCEEDED/.test(upper);
  const hasAuthFailure = /INVALID_KEY|AUTHENTICATION FAILURE|UNAUTHORIZED|401/.test(upper);
  const hasRuntimeFailure = /ENGINE_OVERLOADED|GATEWAY TIMEOUT|UNHANDLED STOP REASON|TIMED OUT/.test(upper);
  return {
    data_failure_codes: [...new Set(dataFailureCodes)],
    has_live_odds_payload: hasLiveOddsPayload,
    has_quota_failure: hasQuotaFailure,
    has_auth_failure: hasAuthFailure,
    has_runtime_failure: hasRuntimeFailure,
  };
}

function buildLatestCronAttempt(job) {
  if (!job?.id) return null;
  const sessionsIndex = readSessionsIndex();
  const prefix = `agent:main:cron:${job.id}:run:`;
  const candidates = Object.entries(sessionsIndex)
    .filter(([key, value]) => key.startsWith(prefix) && value?.sessionId)
    .map(([key, value]) => ({
      key,
      session_id: value.sessionId,
      updated_at_ms: Number(value.updatedAt) || null,
      label: value.label || null,
      session_file: value.sessionFile || path.join(OPENCLAW_PATHS.sessionsDir, `${value.sessionId}.jsonl`),
    }))
    .filter((entry) => Number.isFinite(entry.updated_at_ms))
    .sort((a, b) => a.updated_at_ms - b.updated_at_ms);

  const latest = candidates[candidates.length - 1];
  if (!latest) return null;

  const transcript = readTextSafe(latest.session_file, '');
  const signals = detectSessionAttemptSignals(transcript);
  const lockPath = `${latest.session_file}.lock`;
  const lockExists = fs.existsSync(lockPath);
  const attemptCodes = [...new Set([
    ...(signals.data_failure_codes || []),
    ...((signals.has_runtime_failure || lockExists) ? ['runtime_gateway_failure'] : []),
  ])];

  let messageType = 'UNKNOWN';
  let plainReason = 'Latest cron attempt could not be classified reliably.';
  if (signals.has_runtime_failure || lockExists) {
    messageType = 'BLOCKED';
    plainReason = signals.has_live_odds_payload
      ? 'Latest cron attempt fetched live odds but failed at runtime before completion.'
      : 'Latest cron attempt failed at the OpenClaw runtime/gateway layer before completion.';
  } else if (signals.has_auth_failure) {
    messageType = 'BLOCKED';
    plainReason = 'Latest cron attempt failed authentication against The Odds API.';
  } else if (signals.has_quota_failure) {
    messageType = 'BLOCKED';
    plainReason = 'Latest cron attempt exhausted or was rate-limited by The Odds API.';
  } else if (signals.has_live_odds_payload) {
    messageType = 'IN_PROGRESS';
    plainReason = 'Latest cron attempt has live odds payload but no finished artifact yet.';
  }

  return {
    session_id: latest.session_id,
    updated_at_ms: latest.updated_at_ms,
    updated_at_ct: formatCtTimestamp(latest.updated_at_ms),
    session_file: latest.session_file,
    lock_exists: lockExists,
    message_type: messageType,
    plain_reason: plainReason,
    has_live_odds_payload: signals.has_live_odds_payload,
    data_failure_codes: attemptCodes,
  };
}

function buildJobStatus(job, type, invalidSessionIds = new Set()) {
  if (!job) return null;
  const runFile = path.join(OPENCLAW_PATHS.runsDir, `${job.id}.jsonl`);
  const events = readRunEvents(runFile);
  const validEvents = events.filter((event) => !invalidSessionIds.has(String(event.sessionId || '').trim()));
  const latestFinishedOverall = summarizeRun(events[events.length - 1], type);
  const latestFinished = summarizeRun(validEvents[validEvents.length - 1], type);
  const latestSuccessful = summarizeRun([...validEvents].reverse().find((event) => event.status === 'ok'), type);
  const successfulRuns = events
    .filter((event) => event.status === 'ok')
    .filter((event) => !invalidSessionIds.has(String(event.sessionId || '').trim()))
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
    latest_finished_overall: latestFinishedOverall,
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
  const invalidSessionIds = new Set(
    readJsonlSafe(path.resolve(REPO_ROOT, 'data', 'hunt-audit-log.jsonl'), [])
      .filter((row) => String(row.invalid_status || '').toLowerCase().includes('invalid'))
      .flatMap((row) => [row.session_id, row.session_path])
      .map((value) => String(value || '').trim())
      .filter(Boolean)
  );
  const stateLastUpdatedCt = parseBettingStateLastUpdated(bettingStateMarkdown);
  const stateLastUpdatedMs = parseTimestampMs(stateLastUpdatedCt);

  const jobStatuses = {
    morning_edge_hunt: buildJobStatus(byName['morning-edge-hunt'], 'hunt', invalidSessionIds),
    evening_grading: buildJobStatus(byName['evening-grading'], 'grading', invalidSessionIds),
    friday_sgp: buildJobStatus(byName['friday-sgp'], 'hunt', invalidSessionIds),
    weekly_review: buildJobStatus(byName['weekly-review'], 'grading', invalidSessionIds),
    monthly_reload: buildJobStatus(byName['monthly-reload'], 'grading', invalidSessionIds),
  };

  const latestHuntAttempt = buildLatestCronAttempt(byName['morning-edge-hunt']);
  const latestCanonicalHuntRun = readCanonicalHuntRun();
  const validLatestHuntAttempt = invalidSessionIds.has(String(latestHuntAttempt?.session_id || '').trim())
    || invalidSessionIds.has(String(latestHuntAttempt?.session_file || '').trim())
    ? null
    : latestHuntAttempt;
  const latestFinishedHuntOverall = jobStatuses.morning_edge_hunt?.latest_finished_overall || null;
  const latestFinishedHunt = jobStatuses.morning_edge_hunt?.latest_finished || null;
  const latestHunt = jobStatuses.morning_edge_hunt?.latest_successful || null;
  const latestGrading = jobStatuses.evening_grading?.latest_successful || null;
  const latestFinishedOverallInvalidated =
    Boolean(latestFinishedHuntOverall?.session_id)
    && invalidSessionIds.has(String(latestFinishedHuntOverall.session_id || '').trim());
  const attemptMatchesFinishedSession =
    Boolean(validLatestHuntAttempt?.session_id)
    && validLatestHuntAttempt?.session_id === latestFinishedHunt?.session_id;
  const useAttemptAsCurrentHunt =
    !attemptMatchesFinishedSession
    && Number.isFinite(validLatestHuntAttempt?.updated_at_ms)
    && (
      !Number.isFinite(latestFinishedHunt?.run_at_ms)
      || validLatestHuntAttempt.updated_at_ms > latestFinishedHunt.run_at_ms
    );
  const latestCurrentHunt = useAttemptAsCurrentHunt
    ? {
        status: validLatestHuntAttempt?.lock_exists ? 'running_or_stuck' : 'runtime_error',
        delivery_status: null,
        error: validLatestHuntAttempt?.plain_reason || null,
        run_at_ms: validLatestHuntAttempt?.updated_at_ms || null,
        run_at_ct: validLatestHuntAttempt?.updated_at_ct || null,
        date_key: extractDateKey(validLatestHuntAttempt?.updated_at_ct),
        summary: null,
        session_id: validLatestHuntAttempt?.session_id || null,
        message_type: validLatestHuntAttempt?.message_type || 'UNKNOWN',
        has_actionable_bets: false,
        requires_state_sync: false,
        data_failure_codes: validLatestHuntAttempt?.data_failure_codes || [],
        data_status: (validLatestHuntAttempt?.data_failure_codes || []).length > 0 ? 'degraded_data' : 'unknown',
        plain_reason: validLatestHuntAttempt?.plain_reason || null,
      }
    : latestFinishedOverallInvalidated
      ? {
          status: 'invalidated',
          delivery_status: latestFinishedHuntOverall?.delivery_status || null,
          error: 'Latest finished hunt was audit-invalidated.',
          run_at_ms: latestFinishedHuntOverall?.run_at_ms || null,
          run_at_ct: latestFinishedHuntOverall?.run_at_ct || null,
          date_key: latestFinishedHuntOverall?.date_key || null,
          summary: latestFinishedHuntOverall?.summary || null,
          raw_summary: latestFinishedHuntOverall?.raw_summary || null,
          session_id: latestFinishedHuntOverall?.session_id || null,
          session_file: latestFinishedHuntOverall?.session_file || null,
          message_type: 'INVALIDATED',
          has_actionable_bets: false,
          requires_state_sync: false,
          data_failure_codes: [],
          data_status: 'invalidated',
          plain_reason: 'Latest finished hunt was audit-invalidated and excluded from canonical recommendation truth.',
        }
    : (latestFinishedHunt || latestHunt || null);
  const normalizedLatestCurrentHunt =
    attemptMatchesFinishedSession && latestFinishedHunt
      ? latestFinishedHunt
      : latestCurrentHunt;
  const canonicalRunAtMs = parseTimestampMs(latestCanonicalHuntRun?.generated_at_utc || latestCanonicalHuntRun?.run_at_ct);
  const latestCurrentRunAtMs = normalizedLatestCurrentHunt?.run_at_ms;
  const preferCanonicalHuntRun =
    latestCanonicalHuntRun?.status === 'ok'
    && Number.isFinite(canonicalRunAtMs)
    && (!Number.isFinite(latestCurrentRunAtMs) || canonicalRunAtMs >= latestCurrentRunAtMs);
  const effectiveLatestCurrentHunt = preferCanonicalHuntRun
    ? {
        status: latestCanonicalHuntRun.status,
        delivery_status: 'repo_direct',
        error: null,
        run_at_ms: canonicalRunAtMs,
        run_at_ct: latestCanonicalHuntRun.run_at_ct || formatCtTimestamp(canonicalRunAtMs),
        date_key: extractDateKey(latestCanonicalHuntRun.run_at_ct || formatCtTimestamp(canonicalRunAtMs)),
        summary: latestCanonicalHuntRun.summary || null,
        raw_summary: latestCanonicalHuntRun.summary || null,
        session_id: null,
        session_file: null,
        message_type: latestCanonicalHuntRun.message_type || 'UNKNOWN',
        has_actionable_bets: latestCanonicalHuntRun.has_actionable_bets === true,
        requires_state_sync: latestCanonicalHuntRun.requires_state_sync === true,
        data_failure_codes: [],
        data_status: 'verified',
        plain_reason: latestCanonicalHuntRun.plain_reason || 'Latest repo-owned canonical hunt completed successfully.',
        source: 'canonical_repo_hunt',
        native_rows_appended: latestCanonicalHuntRun.native_rows_appended ?? null,
      }
    : normalizedLatestCurrentHunt;

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
  if (useAttemptAsCurrentHunt) warnings.push('newer_hunt_attempt_without_finished_artifact');
  if (latestFinishedOverallInvalidated) warnings.push('latest_hunt_invalidated');
  if (latestGrading?.native_append_failed) warnings.push('latest_grading_native_append_failed');

  return {
    generated_at_utc: new Date().toISOString(),
    jobs: jobStatuses,
    latest_hunt_attempt: validLatestHuntAttempt,
    latest_hunt_current: effectiveLatestCurrentHunt,
    latest_successful_hunt: latestHunt,
    latest_canonical_hunt_run: latestCanonicalHuntRun,
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

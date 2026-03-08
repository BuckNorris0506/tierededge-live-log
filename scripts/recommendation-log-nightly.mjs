#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import {
  appendRecommendationRows,
  DEFAULT_RECOMMENDATION_LOG,
  parseDateKey,
  readRecommendationLog,
  validateRecommendationLog,
} from './recommendation-log-utils.mjs';

const DEFAULT_BETTING_STATE = '/Users/jaredbuckman/.openclaw/workspace/memory/betting-state.md';
const DEFAULT_OUT = path.resolve(process.cwd(), 'public', 'integrity', 'recommendation-log-integrity.json');

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

function parseTable(section) {
  const lines = section
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|'));
  if (lines.length < 2) return [];
  const headers = lines[0].split('|').map((s) => s.trim()).filter(Boolean);
  const rows = [];
  for (let i = 2; i < lines.length; i += 1) {
    const parts = lines[i].split('|').map((s) => s.trim()).filter(Boolean);
    if (parts.length !== headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = parts[j];
    rows.push(row);
  }
  return rows;
}

function parseLastUpdatedDate(markdown) {
  const m = markdown.match(/^Last Updated:\s*(\d{4}-\d{2}-\d{2})/m);
  return m ? m[1] : null;
}

function normalizeReason(input) {
  return String(input || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s_-]/g, '')
    .replace(/\s+/g, '_');
}

function inferScanDays({ recommendationRows, bettingStateRaw }) {
  const recDays = new Set(recommendationRows.map((row) => parseDateKey(row.timestamp_ct)).filter(Boolean));
  const scanDays = new Set([...recDays]);

  const betLogRows = parseTable(extractSection(bettingStateRaw, 'Bet Log (All Graded Bets)'));
  for (const row of betLogRows) {
    const day = String(row.Date || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(day)) scanDays.add(day);
  }

  const lastUpdatedDay = parseLastUpdatedDate(bettingStateRaw);
  if (lastUpdatedDay) scanDays.add(lastUpdatedDay);
  return { recDays, scanDays, lastUpdatedDay };
}

function buildBackfillRowsFromState({ bettingStateRaw, day, existingRecCountForDay }) {
  // Conservative backfill: only generate SIT rows from explicit rejected opportunities
  // when recommendation rows for that day are absent.
  if (existingRecCountForDay > 0) return [];
  const rows = parseTable(extractSection(bettingStateRaw, 'Rejected Opportunities (Today)'));
  if (rows.length === 0) return [];

  return rows.map((row, idx) => {
    const time = String(row['Timestamp (CT)'] || row.Timestamp || '00:00').trim();
    const edge = String(row['Edge %'] || '').trim();
    const reason = normalizeReason(row['Sit Reason'] || row.reason || 'backfill_unknown_reason');
    const recId = `backfill-${day.replace(/-/g, '')}-${String(idx + 1).padStart(3, '0')}`;
    return {
      rec_id: recId,
      timestamp_ct: `${day} ${time}`,
      sport: row.Sport || 'unknown',
      market: row.Market || 'unknown',
      selection: row.Market || row.Event || 'unknown',
      source_book: row.Book || 'unknown',
      recommended_odds_us: row['Odds (US)'] || '',
      recommended_odds_dec: '',
      true_prob: row['True Prob'] || '',
      implied_prob_fair: row['Implied Prob (de-vig)'] || '',
      edge_pct: edge,
      kelly_stake: '$0.00',
      decision: 'SIT',
      rejection_reason: reason,
      odds_quality: 'backfill_unknown',
      injury_quality: 'backfill_unknown',
      market_quality: 'backfill_unknown',
      confidence_total: 'backfill_unknown',
    };
  });
}

async function main() {
  const startedAt = new Date().toISOString();
  const [logInfo, bettingStateRaw] = await Promise.all([
    readRecommendationLog(DEFAULT_RECOMMENDATION_LOG),
    fs.readFile(DEFAULT_BETTING_STATE, 'utf8'),
  ]);

  const validation = await validateRecommendationLog(DEFAULT_RECOMMENDATION_LOG);
  if (validation.duplicate_rec_ids.length > 0) {
    throw new Error(`duplicate_rec_id:${validation.duplicate_rec_ids.join(',')}`);
  }

  const { recDays, scanDays, lastUpdatedDay } = inferScanDays({
    recommendationRows: logInfo.rows,
    bettingStateRaw,
  });

  const missingDays = [...scanDays].filter((day) => !recDays.has(day)).sort();
  const backfilledDays = [];
  const warnings = [];

  for (const day of missingDays) {
    const recCountForDay = logInfo.rows.filter((row) => parseDateKey(row.timestamp_ct) === day).length;
    const backfillRows = buildBackfillRowsFromState({
      bettingStateRaw,
      day,
      existingRecCountForDay: recCountForDay,
    });
    if (backfillRows.length > 0) {
      const result = await appendRecommendationRows(backfillRows, DEFAULT_RECOMMENDATION_LOG);
      backfilledDays.push({ day, rows_appended: result.appended });
    } else {
      warnings.push({
        day,
        code: 'missing_day_unrecoverable',
        message: 'Scan day detected but no reconstructable rejected-opportunity rows found.',
      });
    }
  }

  const postValidation = await validateRecommendationLog(DEFAULT_RECOMMENDATION_LOG);
  const out = {
    started_at_utc: startedAt,
    completed_at_utc: new Date().toISOString(),
    source_files: {
      recommendation_log: DEFAULT_RECOMMENDATION_LOG,
      betting_state: DEFAULT_BETTING_STATE,
    },
    checks: {
      required_columns_present: postValidation.missing_columns.length === 0,
      missing_required_columns: postValidation.missing_columns,
      rec_id_unique: postValidation.duplicate_rec_ids.length === 0,
      duplicate_rec_ids: postValidation.duplicate_rec_ids,
      timestamps_valid: postValidation.bad_timestamp_rows.length === 0,
      bad_timestamp_rows: postValidation.bad_timestamp_rows,
      scan_days_detected: [...scanDays].sort(),
      rec_days_present: [...recDays].sort(),
      missing_scan_days: missingDays,
      last_updated_day: lastUpdatedDay,
    },
    backfill: {
      days_backfilled: backfilledDays,
      warnings,
    },
    status: postValidation.duplicate_rec_ids.length > 0 ? 'failed' : 'ok',
  };

  await fs.mkdir(path.dirname(DEFAULT_OUT), { recursive: true });
  await fs.writeFile(DEFAULT_OUT, `${JSON.stringify(out, null, 2)}\n`, 'utf8');
  console.log(`Nightly recommendation integrity report written: ${DEFAULT_OUT}`);
  if (warnings.length > 0) {
    for (const warning of warnings) {
      console.warn(`WARN ${warning.day}: ${warning.message}`);
    }
  }
}

main().catch((error) => {
  console.error(`Nightly recommendation integrity failed: ${error.message}`);
  process.exit(1);
});

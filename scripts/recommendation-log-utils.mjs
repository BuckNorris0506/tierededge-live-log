import fs from 'node:fs/promises';

export const DEFAULT_RECOMMENDATION_LOG = '/Users/jaredbuckman/.openclaw/workspace/memory/recommendation-log.md';

export const REQUIRED_RECOMMENDATION_COLUMNS = [
  'rec_id',
  'timestamp_ct',
  'sport',
  'market',
  'selection',
  'source_book',
  'recommended_odds_us',
  'recommended_odds_dec',
  'true_prob',
  'implied_prob_fair',
  'edge_pct',
  'kelly_stake',
  'decision',
  'rejection_reason',
  'odds_quality',
  'injury_quality',
  'market_quality',
  'confidence_total',
];

function parseTable(markdown) {
  const lines = markdown
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|'));
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = lines[0].split('|').map((s) => s.trim()).filter(Boolean);
  const rows = [];
  for (let i = 2; i < lines.length; i += 1) {
    const parts = lines[i].split('|').map((s) => s.trim()).filter(Boolean);
    if (parts.length < headers.length) continue;
    // Tolerate legacy rows with extra trailing notes columns.
    const normalizedParts = parts.slice(0, headers.length);
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = normalizedParts[j];
    rows.push(row);
  }
  return { headers, rows };
}

function escapeCell(value) {
  return String(value ?? '').replace(/\|/g, '/').trim();
}

export function parseDateKey(timestampCt) {
  const m = String(timestampCt || '').match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

export function isTimestampFormatValid(timestampCt) {
  const text = String(timestampCt || '').trim();
  if (!text) return false;
  const hasDate = /\d{4}-\d{2}-\d{2}/.test(text);
  const parsed = Date.parse(text);
  return hasDate && Number.isFinite(parsed);
}

export async function readRecommendationLog(logPath = DEFAULT_RECOMMENDATION_LOG) {
  const raw = await fs.readFile(logPath, 'utf8');
  const { headers, rows } = parseTable(raw);
  return { raw, headers, rows };
}

export async function validateRecommendationLog(logPath = DEFAULT_RECOMMENDATION_LOG) {
  const { headers, rows } = await readRecommendationLog(logPath);
  const missingColumns = REQUIRED_RECOMMENDATION_COLUMNS.filter((column) => !headers.includes(column));
  const recIdSet = new Set();
  const duplicateRecIds = [];
  const badTimestampRows = [];

  for (const row of rows) {
    const recId = String(row.rec_id || '').trim();
    if (recId) {
      if (recIdSet.has(recId)) duplicateRecIds.push(recId);
      recIdSet.add(recId);
    }
    if (!isTimestampFormatValid(row.timestamp_ct)) badTimestampRows.push(recId || '(missing rec_id)');
  }

  return {
    headers,
    row_count: rows.length,
    missing_columns: missingColumns,
    duplicate_rec_ids: [...new Set(duplicateRecIds)],
    bad_timestamp_rows: badTimestampRows,
  };
}

export async function appendRecommendationRows(rowsToAppend, logPath = DEFAULT_RECOMMENDATION_LOG) {
  const { raw, headers, rows } = await readRecommendationLog(logPath);
  const missingColumns = REQUIRED_RECOMMENDATION_COLUMNS.filter((column) => !headers.includes(column));
  if (missingColumns.length > 0) {
    throw new Error(`missing_required_columns:${missingColumns.join(',')}`);
  }

  const existingRecIds = new Set(rows.map((row) => String(row.rec_id || '').trim()).filter(Boolean));
  const prepared = [...rowsToAppend]
    .filter(Boolean)
    .map((row) => {
      const recId = String(row.rec_id || '').trim();
      if (!recId) throw new Error('row_missing_rec_id');
      if (existingRecIds.has(recId)) throw new Error(`duplicate_rec_id:${recId}`);
      if (!isTimestampFormatValid(row.timestamp_ct)) throw new Error(`bad_timestamp:${recId}`);
      existingRecIds.add(recId);
      return row;
    })
    .sort((a, b) => Date.parse(String(a.timestamp_ct)) - Date.parse(String(b.timestamp_ct)));

  if (prepared.length === 0) return { appended: 0 };

  const lastExistingTs = rows.length > 0 ? Date.parse(String(rows[rows.length - 1].timestamp_ct || '')) : null;
  if (Number.isFinite(lastExistingTs)) {
    for (const row of prepared) {
      const ts = Date.parse(String(row.timestamp_ct || ''));
      // Append-only ledger should remain chronological; reject out-of-order inserts.
      if (Number.isFinite(ts) && ts < lastExistingTs) {
        throw new Error(`out_of_order_append:${row.rec_id}`);
      }
    }
  }

  const appendedLines = prepared.map((row) => `| ${headers.map((h) => escapeCell(row[h])).join(' | ')} |`);
  const nextRaw = `${raw.trimEnd()}\n${appendedLines.join('\n')}\n`;
  await fs.writeFile(logPath, nextRaw, 'utf8');
  return { appended: prepared.length };
}

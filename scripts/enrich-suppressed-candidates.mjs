import fs from 'node:fs';
import path from 'node:path';
import {
  readCsv,
  writeCsv,
  SUPPRESSION_ENRICHMENT_HEADERS,
  parseAsNumber,
} from './suppression-audit-utils.mjs';

const DEFAULT_SUPPRESSED = path.resolve(process.cwd(), 'data', 'suppressed-candidates.csv');
const DEFAULT_OUT = path.resolve(process.cwd(), 'data', 'suppression-audit-enrichment.csv');
const DEFAULT_GRADES = '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json';
const DEFAULT_PUBLIC_DATA = path.resolve(process.cwd(), 'public', 'data.json');

function readJsonSafe(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

const suppressedRows = readCsv(process.argv[2] || DEFAULT_SUPPRESSED);
const gradesCache = readJsonSafe(DEFAULT_GRADES, { entries: {} });
const payload = readJsonSafe(DEFAULT_PUBLIC_DATA, {});
const unitSize = parseAsNumber(payload?.quant_performance?.unit_size);

const rows = suppressedRows.map((row) => {
  const recId = row.rec_id || '';
  const grade = gradesCache.entries?.[recId] || null;
  const hypotheticalUnits = parseAsNumber(grade?.counterfactual_pl_unit);
  const hypotheticalProfit = hypotheticalUnits !== null && unitSize !== null ? Number((hypotheticalUnits * unitSize).toFixed(2)) : null;

  let enrichmentStatus = 'missing_grade';
  let enrichmentNote = 'No post-game enrichment available yet.';
  if (grade) {
    enrichmentStatus = 'graded_from_counterfactual_cache';
    enrichmentNote = 'Closing odds/CLV unavailable in source cache; result and hypothetical units derived from passed-opportunity grading cache.';
  }

  return {
    rec_id: recId,
    scan_time_ct: row.scan_time_ct || '',
    event_id: grade?.event_id || row.event_id || '',
    event_label: grade?.event_label || '',
    closing_odds: '',
    closing_implied_prob: '',
    closing_clv_pct: '',
    result_if_played: grade?.counterfactual_result || '',
    hypothetical_profit: hypotheticalProfit ?? '',
    hypothetical_units: hypotheticalUnits ?? '',
    graded_at_utc: grade?.graded_at || '',
    enrichment_status: enrichmentStatus,
    enrichment_note: enrichmentNote,
  };
});

writeCsv(process.argv[3] || DEFAULT_OUT, SUPPRESSION_ENRICHMENT_HEADERS, rows);
console.log(`Built suppression enrichment: ${process.argv[3] || DEFAULT_OUT}`);

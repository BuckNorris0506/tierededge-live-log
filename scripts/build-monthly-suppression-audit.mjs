import fs from 'node:fs';
import path from 'node:path';
import {
  readCsv,
  parseAsNumber,
  parseTimestampMs,
  average,
  safeRate,
} from './suppression-audit-utils.mjs';

const DEFAULT_CANDIDATES = path.resolve(process.cwd(), 'data', 'candidate-markets.csv');
const DEFAULT_SUPPRESSED = path.resolve(process.cwd(), 'data', 'suppressed-candidates.csv');
const DEFAULT_ENRICHMENT = path.resolve(process.cwd(), 'data', 'suppression-audit-enrichment.csv');
const DEFAULT_PUBLIC_OUT = path.resolve(process.cwd(), 'public', 'monthly-suppression-audit.json');
const DEFAULT_ROOT_OUT = path.resolve(process.cwd(), 'monthly-suppression-audit.json');
const WINDOW_MS = 30 * 24 * 60 * 60 * 1000;

function readMergedRows() {
  const candidates = readCsv(process.argv[2] || DEFAULT_CANDIDATES);
  const suppressed = readCsv(process.argv[3] || DEFAULT_SUPPRESSED);
  const enrichment = readCsv(process.argv[4] || DEFAULT_ENRICHMENT);
  const enrichmentByRecId = Object.fromEntries(enrichment.map((row) => [row.rec_id, row]));
  return { candidates, suppressed: suppressed.map((row) => ({ ...row, ...(enrichmentByRecId[row.rec_id] || {}) })) };
}

function inWindow(row, cutoffMs) {
  const ts = parseTimestampMs(row.scan_time_ct);
  return Number.isFinite(ts) && ts >= cutoffMs;
}

function bucketFor(row) {
  const pre = parseAsNumber(row.raw_edge_pct);
  const post = parseAsNumber(row.post_conf_edge_pct);
  const confidencePenalty = parseAsNumber(row.confidence_penalty_pct) || 0;
  if (pre !== null && pre >= 2 && row.rejection_stage === 'confidence_gate') return 'bucket_a_confidence_suppression';
  if (post !== null && post >= 1.5 && post < 2 && row.rejection_stage === 'threshold_gate') return 'bucket_b_threshold_near_miss';
  if ((pre !== null && post !== null && (pre - post) >= 0.5) && (row.rejection_stage === 'consensus_anchor' || confidencePenalty > 0)) return 'bucket_c_consensus_flattening';
  return 'bucket_d_true_no_edge';
}

function summarizeBucket(rows, totalScanned) {
  const graded = rows.filter((row) => String(row.result_if_played || '').trim());
  const wins = graded.filter((row) => String(row.result_if_played).toLowerCase() === 'win').length;
  const clvValues = rows.map((row) => parseAsNumber(row.closing_clv_pct)).filter((value) => value !== null);
  const positiveClvRate = clvValues.length > 0 ? safeRate(clvValues.filter((value) => value > 0).length, clvValues.length) : null;
  const hypotheticalUnits = rows.map((row) => parseAsNumber(row.hypothetical_units)).filter((value) => value !== null);
  const oneUnitStakeCount = hypotheticalUnits.length;
  return {
    count: rows.length,
    percent_of_total_scanned_markets: safeRate(rows.length, totalScanned),
    average_raw_edge_pct: average(rows.map((row) => row.raw_edge_pct)),
    average_post_conf_edge_pct: average(rows.map((row) => row.post_conf_edge_pct)),
    average_confidence_penalty_pct: average(rows.map((row) => row.confidence_penalty_pct)),
    average_consensus_penalty_pct: average(rows.map((row) => row.consensus_penalty_pct)),
    average_closing_clv_pct: average(rows.map((row) => row.closing_clv_pct)),
    positive_clv_rate: positiveClvRate,
    hypothetical_units: hypotheticalUnits.length > 0 ? Number(hypotheticalUnits.reduce((sum, value) => sum + value, 0).toFixed(2)) : null,
    hypothetical_roi: oneUnitStakeCount > 0 ? Number(((hypotheticalUnits.reduce((sum, value) => sum + value, 0) / oneUnitStakeCount) * 100).toFixed(2)) : null,
    win_rate_if_played: graded.length > 0 ? safeRate(wins, graded.length) : null,
  };
}

const { candidates, suppressed } = readMergedRows();
const nowMs = Date.now();
const cutoffMs = nowMs - WINDOW_MS;
const candidateWindow = candidates.filter((row) => inWindow(row, cutoffMs));
const suppressedWindow = suppressed.filter((row) => inWindow(row, cutoffMs));
const totalScanned = candidateWindow.length;

const buckets = {
  bucket_a_confidence_suppression: [],
  bucket_b_threshold_near_miss: [],
  bucket_c_consensus_flattening: [],
  bucket_d_true_no_edge: [],
};
for (const row of suppressedWindow) buckets[bucketFor(row)].push(row);

const audit = {
  generated_at_utc: new Date().toISOString(),
  audit_window_days: 30,
  total_scanned_markets: totalScanned,
  total_suppressed_candidates: suppressedWindow.length,
  assumptions: [
    'Confidence-gated markets with raw T3-or-better edge are classified into Bucket A because the engine stores a hard gate rather than a separate post-confidence probability field.',
    'Hypothetical ROI is computed on a one-unit counterfactual basis when grading cache data exists.',
    'Closing odds and CLV remain null when the source grading cache does not store them.',
  ],
  buckets: Object.fromEntries(
    Object.entries(buckets).map(([key, rows]) => [key, {
      label: key,
      ...summarizeBucket(rows, totalScanned),
      sample_rows: rows.slice(0, 10).map((row) => ({
        rec_id: row.rec_id,
        scan_time_ct: row.scan_time_ct,
        selection: row.selection,
        book: row.book,
        raw_edge_pct: parseAsNumber(row.raw_edge_pct),
        post_conf_edge_pct: parseAsNumber(row.post_conf_edge_pct),
        rejection_stage: row.rejection_stage,
        rejection_reason: row.rejection_reason,
        result_if_played: row.result_if_played || null,
        hypothetical_units: parseAsNumber(row.hypothetical_units),
      })),
    }])
  ),
};

for (const outPath of [process.argv[5] || DEFAULT_PUBLIC_OUT, DEFAULT_ROOT_OUT]) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(audit, null, 2)}\n`, 'utf8');
}
console.log(`Built monthly suppression audit: ${process.argv[5] || DEFAULT_PUBLIC_OUT}`);

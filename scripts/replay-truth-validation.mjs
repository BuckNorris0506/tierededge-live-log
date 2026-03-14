#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { readRecommendationLog } from './recommendation-log-utils.mjs';
import { readRuntimeStatusSnapshot } from './openclaw-runtime-utils.mjs';

const DEFAULT_PAYLOAD = path.resolve(process.cwd(), 'public', 'data.json');

function parsePercent(value) {
  const n = Number(String(value || '').replace(/[^0-9.-]/g, ''));
  return Number.isFinite(n) ? n : null;
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
  return String(text || '')
    .split(/[|,;/]+/)
    .map((part) => normalizeReason(part))
    .filter(Boolean);
}

function computeTrace(rows, targetDate) {
  const traceRows = rows
    .filter((row) => !targetDate || String(row.timestamp_ct || '').includes(targetDate))
    .map((row) => {
      const edge = parsePercent(row.edge_pct);
      const rejectionReason = splitReasonCodes(row.rejection_reason)[0] || null;
      let suppressionStage = 'accepted';
      if (normalizeDecision(row.decision) === 'bet') suppressionStage = 'accepted';
      else if (edge !== null && edge < 2) suppressionStage = 'threshold_shortfall';
      else if (rejectionReason === 'low_confidence') suppressionStage = 'confidence_gate';
      else suppressionStage = rejectionReason || 'other_gate';

      return {
        rec_id: row.rec_id || null,
        timestamp_ct: row.timestamp_ct || null,
        sport: row.sport || null,
        market: row.market || null,
        selection: row.selection || null,
        source_book: row.source_book || null,
        raw_market_price_us: row.recommended_odds_us || null,
        de_vig_implied_probability: parsePercent(row.implied_prob_fair),
        true_probability_estimate: parsePercent(row.true_prob),
        final_edge_percent: edge,
        confidence_total: row.confidence_total || null,
        decision: normalizeDecision(row.decision),
        rejection_reason: rejectionReason,
        suppression_stage: suppressionStage,
      };
    });

  const suspiciousRows = traceRows.filter((row) =>
    row.decision === 'sit'
    && row.final_edge_percent !== null
    && row.final_edge_percent >= 2
  );

  return {
    target_date: targetDate,
    total_candidates: traceRows.length,
    bets_count: traceRows.filter((row) => row.decision === 'bet').length,
    sits_count: traceRows.filter((row) => row.decision === 'sit').length,
    near_miss_count: traceRows.filter((row) => row.final_edge_percent !== null && row.final_edge_percent >= 0.5 && row.final_edge_percent < 2).length,
    suspicious_rows: suspiciousRows,
    sample_rows: traceRows.slice(0, 25),
  };
}

async function main() {
  const dateArg = process.argv[2] || null;
  const runtimeStatus = readRuntimeStatusSnapshot();
  const payload = fs.existsSync(DEFAULT_PAYLOAD)
    ? JSON.parse(fs.readFileSync(DEFAULT_PAYLOAD, 'utf8'))
    : null;
  const targetDate = dateArg || runtimeStatus?.latest_successful_hunt?.date_key || payload?.last_updated_ct?.match(/(\d{4}-\d{2}-\d{2})/)?.[1] || null;
  const log = await readRecommendationLog();
  const trace = computeTrace(log.rows, targetDate);

  const output = {
    runtime_freshness_anchor: runtimeStatus?.freshness_anchor || null,
    state_sync: runtimeStatus?.state_sync || null,
    target_date: targetDate,
    integrity_gate: payload?.integrity_gate || null,
    decision_payload: payload?.decision_payload_v1 || null,
    model_suppression_trace: trace,
  };

  console.log(JSON.stringify(output, null, 2));
}

main().catch((error) => {
  console.error(`Replay validation failed: ${error.message}`);
  process.exit(1);
});

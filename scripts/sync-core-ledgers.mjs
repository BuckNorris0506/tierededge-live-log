#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { CORE_PATHS, readJson, readJsonl, writeJsonl, parseNumber, round2, extractDateKey } from './core-ledger-utils.mjs';

const RECOMMENDATION_LOG = '/Users/jaredbuckman/.openclaw/workspace/memory/recommendation-log.md';
const PASSED_GRADES = '/Users/jaredbuckman/.openclaw/workspace/memory/passed-opportunity-grades.json';
const BETTING_STATE = '/Users/jaredbuckman/.openclaw/workspace/memory/betting-state.md';

function parseTable(section) {
  const lines = String(section || '')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|'));
  if (lines.length < 2) return [];
  const headers = lines[0].split('|').map((s) => s.trim()).filter(Boolean);
  const rows = [];
  for (let i = 2; i < lines.length; i += 1) {
    const parts = lines[i].split('|').map((s) => s.trim()).filter(Boolean);
    if (parts.length < headers.length) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) row[headers[j]] = parts[j];
    rows.push(row);
  }
  return rows;
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

function parseRecommendationLog() {
  if (!fs.existsSync(RECOMMENDATION_LOG)) return [];
  const markdown = fs.readFileSync(RECOMMENDATION_LOG, 'utf8');
  const rows = parseTable(markdown);
  return rows.map((row) => {
    if (/\-GRADE$/i.test(String(row.rec_id || '').trim())) return null;
    const decision = String(row.decision || '').trim().toUpperCase();
    if (!['BET', 'SIT'].includes(decision)) return null;
    const edgePct = parseNumber(row.edge_pct);
    const decisionKind = decision === 'BET'
      ? 'BET'
      : (edgePct !== null && edgePct > 0 && edgePct < 2
          ? 'PASS'
          : (edgePct !== null && edgePct >= 2 ? 'SUPPRESSED' : 'OTHER'));
    const base = {
      entry_id: `decision::${row.rec_id}`,
      rec_id: row.rec_id,
      run_id: extractDateKey(row.timestamp_ct) ? `${extractDateKey(row.timestamp_ct)}::recommendation_log` : 'recommendation_log',
      timestamp_ct: row.timestamp_ct,
      target_date: extractDateKey(row.timestamp_ct),
      sport: row.sport,
      league: null,
      event_id: null,
      event_label: null,
      market_type: row.market,
      selection: row.selection,
      sportsbook: row.source_book,
      odds_american: row.recommended_odds_us,
      odds_decimal: parseNumber(row.recommended_odds_dec),
      devig_implied_prob: parseNumber(row.implied_prob_fair),
      consensus_prob: parseNumber(row.implied_prob_fair),
      pre_conf_true_prob: parseNumber(row.true_prob),
      confidence_score: null,
      post_conf_true_prob: parseNumber(row.true_prob),
      raw_edge_pct: edgePct,
      post_conf_edge_pct: edgePct,
      tier_threshold_pct: edgePct !== null && edgePct >= 6 ? 6 : edgePct !== null && edgePct >= 4 ? 4 : 2,
      price_edge_pass: decision === 'BET' || (edgePct !== null && edgePct >= 2),
      bet_permission_pass: decision === 'BET',
      final_decision: decision === 'BET' ? 'BET' : 'SIT',
      rejection_stage: decision === 'BET' ? '' : (edgePct !== null && edgePct > 0 && edgePct < 2 ? 'threshold_gate' : (String(row.rejection_reason || '').trim().toLowerCase() === 'low_confidence' ? 'confidence_gate' : 'no_raw_edge')),
      rejection_reason: decision === 'BET' ? '' : String(row.rejection_reason || '').trim().toLowerCase(),
      bet_class: decision === 'BET' ? 'EDGE_BET' : 'EDGE_BET',
      include_in_core_strategy_metrics: true,
      include_in_actual_bankroll: decision === 'BET',
      decision_kind: decisionKind,
      source: 'recommendation_log',
      kelly_stake: row.kelly_stake || '$0.00',
    };
    return base;
  }).filter((row) => row && (row.decision_kind === 'BET' || row.decision_kind === 'PASS' || row.decision_kind === 'SUPPRESSED'));
}

function parsePassedGrades() {
  const grades = readJson(PASSED_GRADES, { entries: {} });
  return Object.values(grades.entries || {});
}

function parseContributionsFromMarkdown() {
  if (!fs.existsSync(BETTING_STATE)) return [];
  const markdown = fs.readFileSync(BETTING_STATE, 'utf8');
  const ledgerSection = extractSection(markdown, 'Ledger');
  const rows = parseTable(ledgerSection);
  return rows.map((row, index) => ({
    entry_id: `bankroll::${row.Date || 'unknown'}::${row.Key || index}`,
    entry_type: String(row.Key || '').trim().toLowerCase() === 'initial' ? 'STARTING_BANKROLL' : 'CONTRIBUTION',
    date: row.Date,
    amount: round2(parseNumber(row.Amount)),
    key: row.Key || null,
    note: row.Note || null,
    source: 'betting_state_ledger',
  })).filter((row) => row.amount !== null);
}

function buildDecisionLedger() {
  const existingRows = readJsonl(CORE_PATHS.decisionLedger);
  const recommendationRows = parseRecommendationLog();
  const entries = [];
  const seen = new Set();

  for (const row of existingRows) {
    if (/\-GRADE$/i.test(String(row.rec_id || '').trim())) continue;
    const key = row.entry_id || `${row.run_id}::${row.rec_id}`;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    entries.push(row);
  }

  for (const row of recommendationRows) {
    if (seen.has(row.entry_id)) continue;
    seen.add(row.entry_id);
    entries.push(row);
  }

  entries.sort((a, b) => String(a.timestamp_ct || '').localeCompare(String(b.timestamp_ct || '')) || String(a.entry_id).localeCompare(String(b.entry_id)));
  return entries;
}

function buildGradingLedger() {
  const existingRows = readJsonl(CORE_PATHS.gradingLedger);
  const passGrades = parsePassedGrades();
  const rows = [];
  const seen = new Set();

  for (const row of existingRows) {
    if (!row.grading_id || seen.has(row.grading_id)) continue;
    seen.add(row.grading_id);
    rows.push(row);
  }

  for (const row of passGrades) {
    const gradingId = `pass::${row.rec_id}`;
    if (seen.has(gradingId)) continue;
    seen.add(gradingId);
    rows.push({
      grading_id: gradingId,
      grading_type: 'PASS',
      ref_id: `decision::${row.rec_id}`,
      date: row.settlement_date || null,
      timestamp_ct: row.graded_at || null,
      selection: row.event_label || row.rec_id,
      result: row.counterfactual_result || row.outcome_if_bet || 'ungraded',
      profit_loss: round2(parseNumber(row.counterfactual_pl ?? row.counterfactual_pl_unit)),
      stake: null,
      bankroll_after: null,
      clv: null,
      bet_class: 'EDGE_BET',
      source: row.grade_source || 'passed_opportunity_grades',
      failure_reason: row.failure_reason || null,
    });
  }

  rows.sort((a, b) => String(a.date || a.timestamp_ct || '').localeCompare(String(b.date || b.timestamp_ct || '')) || String(a.grading_id).localeCompare(String(b.grading_id)));
  return rows;
}

function buildBankrollLedger() {
  const existingRows = readJsonl(CORE_PATHS.bankrollLedger);
  const importedRows = existingRows.length === 0 ? parseContributionsFromMarkdown() : [];
  const rows = [];
  const seen = new Set();

  for (const row of existingRows) {
    const key = row.entry_id || row.id;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    rows.push(row);
  }

  for (const row of importedRows) {
    const key = row.entry_id || row.id;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    rows.push(row);
  }

  rows.sort((a, b) => String(a.date || a.contribution_date || '').localeCompare(String(b.date || b.contribution_date || '')) || String(a.entry_id || a.id).localeCompare(String(b.entry_id || b.id)));
  return rows;
}

function main() {
  const decisions = buildDecisionLedger();
  const grading = buildGradingLedger();
  const bankroll = buildBankrollLedger();
  writeJsonl(CORE_PATHS.decisionLedger, decisions);
  writeJsonl(CORE_PATHS.gradingLedger, grading);
  writeJsonl(CORE_PATHS.bankrollLedger, bankroll);
  console.log(`Synced core ledgers: decisions=${decisions.length} grading=${grading.length} bankroll=${bankroll.length}`);
}

main();

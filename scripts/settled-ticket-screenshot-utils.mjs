import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import {
  DATA_DIR,
  CORE_PATHS,
  appendJsonl,
  parseNumber,
  readJson,
  readJsonl,
  round2,
  writeJson,
} from './core-ledger-utils.mjs';
import { extractImagePathsFromMessageText, runSwiftOcr } from './execution-screenshot-utils.mjs';
import { readExecutionLog } from './execution-layer-utils.mjs';
import { enrichGradingRowWithClv } from './grading-market-truth-utils.mjs';

export const WHATSAPP_SETTLEMENT_INBOX_DIR = path.join(DATA_DIR, 'whatsapp-settlement-inbox');
export const WHATSAPP_SETTLEMENT_PENDING_STORE_PATH = path.join(DATA_DIR, 'whatsapp-settlement-pending.json');
const DEFAULT_PENDING_TTL_HOURS = 4;
const DEFAULT_WHATSAPP_SENDER_KEY = 'whatsapp:self';

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function normalize(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[@|]/g, ' ')
    .replace(/\bml\b/g, ' ml ')
    .replace(/[^a-z0-9+\-.\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function compact(value) {
  return normalize(value).replace(/\s+/g, '');
}

function safeSenderKey(value) {
  return String(value || DEFAULT_WHATSAPP_SENDER_KEY)
    .toLowerCase()
    .replace(/[^a-z0-9:+._-]/g, '-')
    .slice(0, 120);
}

function detectSportsbook(lines) {
  const joined = normalize(lines.join(' '));
  if (joined.includes('draftkings sportsbook') || joined.includes('draftkings')) return 'DraftKings';
  if (joined.includes('fanduel')) return 'FanDuel';
  if (joined.includes('betmgm')) return 'BetMGM';
  if (joined.includes('caesars')) return 'Caesars';
  return null;
}

function parseHeader(headerLine) {
  const cleaned = String(headerLine || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return { selection: null, odds: null, warnings: ['missing_selection'] };
  const oddsMatch = cleaned.match(/([+-]\d{2,4})\s*$/);
  const odds = oddsMatch ? oddsMatch[1] : null;
  const selection = cleaned.replace(/\s*[+-]\d{2,4}\s*$/, '').trim();
  const warnings = [];
  if (!selection) warnings.push('missing_selection');
  if (!odds) warnings.push('missing_odds');
  return { selection: selection || null, odds, warnings };
}

function parseStakePayout(line, lines) {
  const lineText = String(line || '');
  const joined = [lineText, ...lines].join(' ');
  const stake = lineText.match(/wager:\s*\$([0-9]+(?:\.[0-9]{2})?)/i)?.[1] || null;
  const toPay = joined.match(/(?:to pay|paid|return|payout):\s*\$([0-9]+(?:\.[0-9]{2})?)/i)?.[1] || null;
  const cashout = joined.match(/cash out\s*\$([0-9]+(?:\.[0-9]{2})?)/i)?.[1] || null;
  return { stake, payout: toPay || cashout || null };
}

function detectSettlementStatus(lines) {
  const joined = normalize(lines.slice(-4).join(' '));
  if (joined.includes('partial cash out') || joined.includes('partial cashout')) return 'partial_cashout';
  if (joined.includes('cash out')) return 'cashed_out';
  if (/\bwon\b/.test(joined) || joined.includes('settled won')) return 'win';
  if (/\blost\b/.test(joined) || joined.includes('settled lost')) return 'loss';
  if (/\bvoid\b/.test(joined)) return 'void';
  if (/\bpush\b/.test(joined)) return 'push';
  if (/\bpending\b/.test(joined) || /\bopen\b/.test(joined)) return 'pending';
  return 'unknown';
}

function parseTicketTimestamp(lines) {
  const monthPattern = /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i;
  for (const line of lines.slice().reverse()) {
    if (!monthPattern.test(line)) continue;
    const match = line.match(/((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))/i);
    if (match) return match[1];
  }
  return null;
}

function parseEvent(lines) {
  const monthPattern = /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i;
  const teamLike = [];
  for (const line of lines) {
    const trimmed = String(line || '').trim();
    if (!trimmed) continue;
    if (/^wager:/i.test(trimmed)) continue;
    if (monthPattern.test(trimmed)) continue;
    if (/^(open|pending|won|lost|void|push)$/i.test(trimmed)) continue;
    if (/cash out/i.test(trimmed)) continue;
    if (/draftkings|fanduel|betmgm|caesars/i.test(trimmed)) continue;
    if (/[+-]\d{2,4}\s*$/.test(trimmed)) continue;
    if (/^(moneyline|spread|total|sgp)$/i.test(trimmed)) continue;
    if (trimmed.length < 3) continue;
    teamLike.push(trimmed);
  }
  const eventCandidates = [];
  for (let i = 0; i < teamLike.length - 1; i += 1) {
    eventCandidates.push(`${teamLike[i]} @ ${teamLike[i + 1]}`);
  }
  return eventCandidates[0] || null;
}

function inferMarketType(block, marketLine) {
  const joined = normalize(block.join(' '));
  const market = normalize(marketLine);
  if (joined.includes('moneyline') || market.includes('moneyline')) return 'Moneyline';
  if (joined.includes('spread') || market.includes('spread')) return 'Spread';
  if (joined.includes('total') || market.includes('total')) return 'Total';
  if (joined.includes('sgp')) return 'SGP';
  return marketLine || null;
}

function splitBlocks(lines) {
  const wagerIndices = [];
  lines.forEach((line, index) => {
    if (/wager:\s*\$/i.test(line)) wagerIndices.push(index);
  });
  if (wagerIndices.length === 0) {
    return [{ lines, warnings: ['unsupported_layout'] }];
  }
  return wagerIndices.map((wagerIndex, idx) => {
    const start = Math.max(0, wagerIndex - 4);
    const end = idx + 1 < wagerIndices.length ? Math.max(start + 1, wagerIndices[idx + 1] - 4) : lines.length;
    return { lines: lines.slice(start, end), warnings: [] };
  });
}

function scoreCompleteness(extracted, warnings) {
  let score = extracted.base_confidence || 0.4;
  if (extracted.selection) score += 0.15;
  if (extracted.odds) score += 0.1;
  if (extracted.stake) score += 0.1;
  if (extracted.event) score += 0.15;
  if (extracted.payout) score += 0.1;
  if (extracted.settlement_status && extracted.settlement_status !== 'unknown') score += 0.15;
  if (warnings.includes('missing_event_identity')) score -= 0.2;
  if (warnings.includes('missing_stake')) score -= 0.2;
  if (warnings.includes('missing_settlement_status')) score -= 0.2;
  if (warnings.includes('ambiguous_multiple_bets')) score -= 0.15;
  return Math.max(0, Math.min(1, round2(score)));
}

function parseSingleSettledBlock({ screenshotFilename, sportsbook, block, averageConfidence }) {
  const lines = block.lines.map((line) => String(line || '').replace(/\s+/g, ' ').trim()).filter(Boolean);
  const warnings = [...block.warnings];
  const wagerIndex = lines.findIndex((line) => /wager:\s*\$/i.test(line));
  const headerLine = wagerIndex > 0 ? lines[wagerIndex - 2] || lines[wagerIndex - 1] : lines[0];
  const marketLine = wagerIndex > 0 ? lines[wagerIndex - 1] : lines[1];
  const header = parseHeader(headerLine);
  warnings.push(...header.warnings);
  const stakeLine = lines.find((line) => /wager:\s*\$/i.test(line)) || '';
  const { stake, payout } = parseStakePayout(stakeLine, lines);
  if (!stake) warnings.push('missing_stake');
  const event = parseEvent(lines);
  if (!event) warnings.push('missing_event_identity');
  const settlementStatus = detectSettlementStatus(lines);
  if (settlementStatus === 'unknown') warnings.push('missing_settlement_status');
  const ticketTimestamp = parseTicketTimestamp(lines);
  const marketType = inferMarketType(lines, marketLine);
  const confidence = scoreCompleteness({
    selection: header.selection,
    odds: header.odds,
    stake,
    payout,
    event,
    settlement_status: settlementStatus,
    base_confidence: averageConfidence,
  }, warnings);
  if (confidence < 0.55) warnings.push('low_parse_confidence');

  return {
    screenshot_filename: path.basename(screenshotFilename),
    sportsbook,
    event,
    market_type: marketType,
    selection: header.selection,
    odds: header.odds,
    stake,
    payout,
    ticket_timestamp: ticketTimestamp,
    settlement_status: settlementStatus,
    extraction_confidence: confidence,
    parse_warnings: unique(warnings),
    raw_lines: lines,
  };
}

function parseSettledDocument(document) {
  if (document.error) {
    return {
      image_path: document.image_path,
      screenshot_filename: path.basename(document.image_path),
      sportsbook: null,
      average_confidence: 0,
      parsed_bets: [{
        screenshot_filename: path.basename(document.image_path),
        sportsbook: null,
        event: null,
        market_type: null,
        selection: null,
        odds: null,
        stake: null,
        payout: null,
        ticket_timestamp: null,
        settlement_status: 'unknown',
        extraction_confidence: 0,
        parse_warnings: ['unreadable_screenshot', document.error],
        raw_lines: [],
      }],
    };
  }

  const lines = String(document.full_text || '').split('\n').map((line) => line.trim()).filter(Boolean);
  const sportsbook = detectSportsbook(lines);
  const blocks = splitBlocks(lines);
  const multiple = blocks.length > 1;
  const parsedBets = blocks.map((block) => {
    const bet = parseSingleSettledBlock({
      screenshotFilename: document.image_path,
      sportsbook,
      block,
      averageConfidence: Number(document.average_confidence || 0),
    });
    if (multiple) {
      bet.parse_warnings = unique([...bet.parse_warnings, 'ambiguous_multiple_bets']);
      bet.extraction_confidence = Math.max(0, round2(bet.extraction_confidence - 0.1));
    }
    return bet;
  });

  return {
    image_path: document.image_path,
    screenshot_filename: path.basename(document.image_path),
    sportsbook,
    average_confidence: Number(document.average_confidence || 0),
    parsed_bets: parsedBets,
  };
}

function selectionScore(a, b) {
  const na = normalize(a);
  const nb = normalize(b);
  if (!na || !nb) return 0;
  if (na === nb) return 50;
  if (na.includes(nb) || nb.includes(na)) return 35;
  const tokensA = new Set(na.split(' '));
  const tokensB = new Set(nb.split(' '));
  let overlap = 0;
  for (const token of tokensA) {
    if (token && tokensB.has(token)) overlap += 1;
  }
  return Math.min(30, overlap * 8);
}

function eventScore(extractedEvent, candidateEvent, candidateSelection) {
  const event = normalize(extractedEvent);
  const candidate = normalize(candidateEvent || candidateSelection);
  if (!event || !candidate) return 0;
  let overlap = 0;
  for (const token of event.split(' ')) {
    if (token && candidate.includes(token)) overlap += 1;
  }
  return Math.min(25, overlap * 5);
}

function sportsbookScore(extractedBook, candidateBook) {
  if (!extractedBook || !candidateBook) return 0;
  return normalize(extractedBook) === normalize(candidateBook) ? 12 : 0;
}

function oddsScore(a, b) {
  const na = parseNumber(a);
  const nb = parseNumber(b);
  if (!Number.isFinite(na) || !Number.isFinite(nb)) return 0;
  const diff = Math.abs(na - nb);
  if (diff === 0) return 15;
  if (diff <= 5) return 10;
  if (diff <= 10) return 6;
  return 0;
}

function stakeScore(a, b) {
  const na = parseNumber(a);
  const nb = parseNumber(b);
  if (!Number.isFinite(na) || !Number.isFinite(nb)) return 0;
  const diff = Math.abs(na - nb);
  if (diff === 0) return 10;
  if (diff <= 1) return 6;
  if (diff <= 3) return 2;
  return 0;
}

function timestampScore(a, b) {
  const ams = Date.parse(String(a || '').replace(' CT', ''));
  const bms = Date.parse(String(b || '').replace(' CT', ''));
  if (!Number.isFinite(ams) || !Number.isFinite(bms)) return 0;
  const diffMinutes = Math.abs(ams - bms) / 60000;
  if (diffMinutes <= 15) return 10;
  if (diffMinutes <= 90) return 6;
  if (diffMinutes <= 480) return 2;
  return 0;
}

function loadRecommendationUniverse() {
  return readJsonl(CORE_PATHS.decisionLedger)
    .filter((row) => row.final_decision === 'BET' || row.decision_kind === 'BET')
    .map((row) => ({
      rec_id: row.rec_id || null,
      run_id: row.run_id || null,
      selection: row.selection,
      event: row.event_label || null,
      sportsbook: row.sportsbook || null,
      odds: row.odds_american || null,
      timestamp: row.timestamp_ct || null,
      source: 'recommendation',
    }));
}

function loadExecutionUniverse() {
  return readExecutionLog().map((row) => ({
    execution_id: row.execution_id,
    rec_id: row.rec_id || null,
    run_id: row.run_id || null,
    selection: row.selection || null,
    event: row.event || null,
    sportsbook: row.actual_sportsbook || row.recommended_sportsbook || null,
    odds: row.actual_odds || row.recommended_odds || null,
    stake: row.actual_stake || row.recommended_stake || null,
    timestamp: row.bet_slip_timestamp || row.recommendation_timestamp || null,
    row,
  }));
}

function classifyMatch(scored, matchedType) {
  if (!scored.length) {
    return { match_status: 'unmatched_manual_bet', confidence_level: 'low', matched: null, warnings: ['no_reliable_match'] };
  }
  const sorted = scored.slice().sort((a, b) => b.score - a.score);
  const top = sorted[0];
  const second = sorted[1];
  if (top.score < 45) {
    return { match_status: 'unmatched_manual_bet', confidence_level: 'low', matched: null, warnings: ['no_reliable_match'] };
  }
  if (second && top.score - second.score <= 5) {
    return { match_status: 'ambiguous_match', confidence_level: 'low', matched: top.candidate, warnings: ['ambiguous_match'] };
  }
  if (top.score >= 75) {
    return {
      match_status: matchedType,
      confidence_level: 'high',
      matched: top.candidate,
      warnings: [],
    };
  }
  return {
    match_status: 'matched_with_low_confidence',
    confidence_level: 'medium',
    matched: top.candidate,
    warnings: ['low_match_confidence'],
  };
}

function matchSettledBet(parsedBet, executions, recommendations) {
  const executionScores = executions.map((candidate) => ({
    candidate,
    score:
      selectionScore(parsedBet.selection, candidate.selection)
      + eventScore(parsedBet.event, candidate.event, candidate.selection)
      + sportsbookScore(parsedBet.sportsbook, candidate.sportsbook)
      + oddsScore(parsedBet.odds, candidate.odds)
      + stakeScore(parsedBet.stake, candidate.stake)
      + timestampScore(parsedBet.ticket_timestamp, candidate.timestamp),
  }));
  const executionMatch = classifyMatch(executionScores, 'matched_to_execution');
  if (executionMatch.matched) return executionMatch;

  const recScores = recommendations.map((candidate) => ({
    candidate,
    score:
      selectionScore(parsedBet.selection, candidate.selection)
      + eventScore(parsedBet.event, candidate.event, candidate.selection)
      + sportsbookScore(parsedBet.sportsbook, candidate.sportsbook)
      + oddsScore(parsedBet.odds, candidate.odds)
      + timestampScore(parsedBet.ticket_timestamp, candidate.timestamp),
  }));
  return classifyMatch(recScores, 'matched_to_recommendation_only');
}

function existingGradingMatch(parsedBet, matchedExecution) {
  const grades = readJsonl(CORE_PATHS.gradingLedger);
  const selection = normalize(parsedBet.selection);
  const refId = matchedExecution?.execution_id || matchedExecution?.rec_id || null;
  return grades.find((row) => {
    const selectionMatch = normalize(row.selection) === selection;
    const refMatch = refId && String(row.ref_id || '') === String(refId);
    return selectionMatch || refMatch;
  }) || null;
}

function mapSettlementResult(status) {
  const normalized = String(status || '').toLowerCase();
  if (normalized === 'win') return 'WIN';
  if (normalized === 'loss') return 'LOSS';
  if (normalized === 'void') return 'VOID';
  if (normalized === 'push') return 'PUSH';
  if (normalized === 'cashed_out') return 'CASHED_OUT';
  if (normalized === 'partial_cashout') return 'PARTIAL_CASHOUT';
  if (normalized === 'pending') return 'PENDING';
  return 'UNKNOWN';
}

function computeProfitLoss(status, stake, payout) {
  const stakeNum = parseNumber(stake);
  const payoutNum = parseNumber(payout);
  const normalized = String(status || '').toLowerCase();
  if (!Number.isFinite(stakeNum)) return null;
  if (normalized === 'loss') return round2(-stakeNum);
  if (normalized === 'push' || normalized === 'void') return 0;
  if ((normalized === 'win' || normalized === 'cashed_out' || normalized === 'partial_cashout') && Number.isFinite(payoutNum)) {
    return round2(payoutNum - stakeNum);
  }
  return null;
}

function buildReconciliationFlags(parsedBet, matched, existingGrade, warnings) {
  const flags = [];
  const status = String(parsedBet.settlement_status || '').toLowerCase();
  const existingResult = normalize(existingGrade?.result || '');
  if (status === 'cashed_out') flags.push('cashout_detected');
  if (status === 'partial_cashout') flags.push('partial_settlement');
  if ((warnings || []).includes('ambiguous_match') || (warnings || []).includes('low_match_confidence')) flags.push('manual_reconciliation_required');
  if (existingGrade) {
    const mapped = normalize(mapSettlementResult(status));
    if (mapped && existingResult && mapped !== existingResult) flags.push('grading_mismatch');
  }
  if (!matched) flags.push('manual_reconciliation_required');
  return unique(flags);
}

function buildPreviewItem(parsedBet, matchResult) {
  const matched = matchResult.matched;
  const matchedExecution = matchResult.match_status === 'matched_to_execution' || matched?.execution_id ? matched : null;
  const matchedRecommendation = !matchedExecution && matched ? matched : null;
  const existingGrade = existingGradingMatch(parsedBet, matchedExecution);
  const warnings = unique([...(parsedBet.parse_warnings || []), ...(matchResult.warnings || [])]);
  const reconciliationFlags = buildReconciliationFlags(parsedBet, matched, existingGrade, warnings);
  const stake = parseNumber(parsedBet.stake);
  const payout = parseNumber(parsedBet.payout);
  const proposedRow = {
    grading_id: `reconciliation::${matchedExecution?.execution_id || matchedRecommendation?.rec_id || compact(parsedBet.selection || 'unknown')}::${Date.now()}`,
    grading_type: 'RECONCILIATION',
    ref_id: matchedExecution?.execution_id || matchedRecommendation?.rec_id || `${parsedBet.screenshot_filename}::${compact(parsedBet.selection || 'unknown')}`,
    execution_log_id: matchedExecution?.execution_id || null,
    rec_id: matchedExecution?.rec_id || matchedRecommendation?.rec_id || null,
    sportsbook: parsedBet.sportsbook,
    event: parsedBet.event,
    market: parsedBet.market_type,
    selection: parsedBet.selection,
    actual_odds: parsedBet.odds || null,
    actual_stake: stake,
    settlement_status: parsedBet.settlement_status,
    settlement_payout: payout,
    settlement_source: 'whatsapp_screenshot',
    screenshot_filename: parsedBet.screenshot_filename,
    extraction_confidence: parsedBet.extraction_confidence,
    reconciliation_flag: reconciliationFlags[0] || null,
    reconciliation_flags: reconciliationFlags,
    ingestion_timestamp: new Date().toISOString(),
    notes: warnings,
    warnings,
    result: mapSettlementResult(parsedBet.settlement_status),
    profit_loss: computeProfitLoss(parsedBet.settlement_status, parsedBet.stake, parsedBet.payout),
    stake,
    source: 'whatsapp_screenshot',
    ticket_timestamp: parsedBet.ticket_timestamp || null,
    existing_grading_match: existingGrade ? {
      grading_id: existingGrade.grading_id,
      result: existingGrade.result,
      source: existingGrade.source,
    } : null,
  };
  return {
    preview_id: proposedRow.ref_id,
    screenshot_filename: parsedBet.screenshot_filename,
    extracted_fields: parsedBet,
    match_status: matchResult.match_status,
    confidence_level: matchResult.confidence_level,
    matched_execution_id: matchedExecution?.execution_id || null,
    matched_rec_id: matchedExecution?.rec_id || matchedRecommendation?.rec_id || null,
    warnings,
    proposed_grading_row: proposedRow,
  };
}

function renderPreviewText(preview) {
  const lines = [];
  lines.push(`SETTLED TICKET PREVIEW (${preview.items.length})`);
  lines.push(`Status: ${preview.status}`);
  for (const [index, item] of preview.items.entries()) {
    lines.push('');
    lines.push(`${index + 1}. ${item.extracted_fields.selection || 'UNKNOWN'} | ${item.extracted_fields.event || 'UNKNOWN EVENT'}`);
    lines.push(`   Book: ${item.extracted_fields.sportsbook || 'UNKNOWN'} | Market: ${item.extracted_fields.market_type || 'UNKNOWN'}`);
    lines.push(`   Odds: ${item.extracted_fields.odds || 'MISSING'} | Stake: ${item.extracted_fields.stake || 'MISSING'} | Payout: ${item.extracted_fields.payout || 'MISSING'}`);
    lines.push(`   Settlement: ${item.extracted_fields.settlement_status || 'unknown'}`);
    lines.push(`   Match: ${item.match_status} | execution_id: ${item.matched_execution_id || 'NONE'} | rec_id: ${item.matched_rec_id || 'NONE'}`);
    if (item.warnings.length) lines.push(`   Warnings: ${item.warnings.join(', ')}`);
  }
  return lines.join('\n');
}

export function buildSettledTicketPreview(imagePaths, previewPath) {
  const executions = loadExecutionUniverse();
  const recommendations = loadRecommendationUniverse();
  const documents = runSwiftOcr(imagePaths);
  const screenshotPreviews = documents.map(parseSettledDocument);
  const items = screenshotPreviews.flatMap((doc) => doc.parsed_bets.map((parsedBet) => buildPreviewItem(parsedBet, matchSettledBet(parsedBet, executions, recommendations))));
  const status = items.some((item) => item.warnings.includes('unreadable_screenshot'))
    ? 'requires_review'
    : items.some((item) => item.confidence_level === 'low' || item.warnings.includes('missing_settlement_status'))
      ? 'requires_confirmation'
      : 'ready_for_confirmation';
  const preview = {
    generated_at_utc: new Date().toISOString(),
    status,
    source_images: imagePaths.map((imagePath) => path.resolve(imagePath)),
    screenshot_previews: screenshotPreviews,
    items,
    verification_actions: ['CONFIRM_ALL', 'CONFIRM_SELECTED', 'REJECT'],
    preview_text: renderPreviewText({ status, items }),
  };
  writeJson(previewPath, preview);
  return preview;
}

function buildWhatsappPreviewText(pendingEntry) {
  const lines = [];
  lines.push(`SETTLED TICKET PREVIEW (${pendingEntry.preview.items.length})`);
  lines.push(`Pending ID: ${pendingEntry.pending_id}`);
  lines.push(`Expires: ${pendingEntry.expires_at_utc}`);
  for (const [index, item] of pendingEntry.preview.items.entries()) {
    lines.push('');
    lines.push(`${index + 1}. ${item.extracted_fields.selection || 'UNKNOWN'} | ${item.extracted_fields.event || 'UNKNOWN EVENT'}`);
    lines.push(`Book: ${item.extracted_fields.sportsbook || 'UNKNOWN'} | Settlement: ${item.extracted_fields.settlement_status || 'unknown'}`);
    lines.push(`Odds: ${item.extracted_fields.odds || 'MISSING'} | Stake: ${item.extracted_fields.stake || 'MISSING'} | Payout: ${item.extracted_fields.payout || 'MISSING'}`);
    lines.push(`Match: ${item.match_status}${item.matched_execution_id ? ` | execution: ${item.matched_execution_id}` : ''}${item.matched_rec_id ? ` | rec_id: ${item.matched_rec_id}` : ''}`);
    if (item.warnings.length) lines.push(`Warnings: ${item.warnings.join(', ')}`);
  }
  lines.push('');
  lines.push('Reply with one of:');
  lines.push('- CONFIRM ALL');
  lines.push('- CONFIRM 1,3');
  lines.push('- REJECT');
  lines.push('- CANCEL');
  lines.push('- HELP');
  return lines.join('\n');
}

function loadPendingStore(pendingStorePath = WHATSAPP_SETTLEMENT_PENDING_STORE_PATH) {
  const store = readJson(pendingStorePath, null);
  if (store && typeof store === 'object' && !Array.isArray(store)) {
    return { generated_at_utc: store.generated_at_utc || new Date().toISOString(), pending: Array.isArray(store.pending) ? store.pending : [] };
  }
  return { generated_at_utc: new Date().toISOString(), pending: [] };
}

function savePendingStore(store, pendingStorePath = WHATSAPP_SETTLEMENT_PENDING_STORE_PATH) {
  writeJson(pendingStorePath, {
    generated_at_utc: new Date().toISOString(),
    pending: Array.isArray(store?.pending) ? store.pending : [],
  });
}

function compactStore(store) {
  const now = Date.now();
  const pending = (store?.pending || []).filter((entry) => {
    const expiresAt = Date.parse(entry?.expires_at_utc || '');
    return !Number.isFinite(expiresAt) || expiresAt > now;
  });
  return { generated_at_utc: new Date().toISOString(), pending };
}

function stageInboxImages(imagePaths, { senderKey, pendingId }) {
  ensureDir(WHATSAPP_SETTLEMENT_INBOX_DIR);
  return imagePaths.map((imagePath, index) => {
    const absolute = path.resolve(imagePath);
    const ext = path.extname(absolute) || '.img';
    const baseName = `${safeSenderKey(senderKey)}__${pendingId}__${String(index + 1).padStart(2, '0')}${ext}`;
    const dest = path.join(WHATSAPP_SETTLEMENT_INBOX_DIR, baseName);
    fs.copyFileSync(absolute, dest);
    const sidecarCandidates = [
      `${absolute}.ocr.txt`,
      `${absolute}.txt`,
      path.join(path.dirname(absolute), `${path.basename(absolute, path.extname(absolute))}.ocr.txt`),
      path.join(path.dirname(absolute), `${path.basename(absolute, path.extname(absolute))}.txt`),
    ];
    for (const candidate of sidecarCandidates) {
      if (!fs.existsSync(candidate)) continue;
      const sidecarName = path.basename(candidate).replace(path.basename(absolute, path.extname(absolute)), path.basename(dest, path.extname(dest)));
      const sidecarDest = path.join(WHATSAPP_SETTLEMENT_INBOX_DIR, sidecarName);
      fs.copyFileSync(candidate, sidecarDest);
    }
    return dest;
  });
}

function selectedItems(preview, confirmValue) {
  if (!confirmValue || confirmValue.toUpperCase() === 'REJECT') return [];
  if (confirmValue.toUpperCase() === 'ALL' || confirmValue.toUpperCase() === 'CONFIRM_ALL') return preview.items;
  const requested = new Set(confirmValue.split(',').map((part) => part.trim()).filter(Boolean));
  return preview.items.filter((item, index) => requested.has(String(index + 1)) || requested.has(item.preview_id));
}

export function confirmSettledTicketPreview({ previewPath, confirm }) {
  const preview = readJson(previewPath, null);
  if (!preview) throw new Error(`missing_preview:${previewPath}`);
  const chosen = selectedItems(preview, confirm);
  if (!chosen.length) return { preview, appended: [], skipped: preview.items.length };
  const appended = [];
  for (const item of chosen) {
    const row = { ...item.proposed_grading_row };
    if (!row.settlement_status || row.settlement_status === 'unknown') throw new Error(`missing_settlement_status:${item.preview_id}`);
    if (!row.actual_stake && row.actual_stake !== 0) throw new Error(`missing_stake:${item.preview_id}`);
    if (!row.event) throw new Error(`missing_event_identity:${item.preview_id}`);
    const enrichedRow = enrichGradingRowWithClv(row);
    appendJsonl(CORE_PATHS.gradingLedger, enrichedRow, (entry) => String(entry.grading_id || ''));
    appended.push(enrichedRow);
  }
  return { preview, appended, skipped: preview.items.length - appended.length };
}

export function createWhatsappSettlementPreview({
  senderKey = DEFAULT_WHATSAPP_SENDER_KEY,
  imagePaths = [],
  pendingStorePath = WHATSAPP_SETTLEMENT_PENDING_STORE_PATH,
  ttlHours = DEFAULT_PENDING_TTL_HOURS,
}) {
  const store = compactStore(loadPendingStore(pendingStorePath));
  const pendingId = `wasettle-${Date.now()}-${randomUUID().slice(0, 8)}`;
  const stagedImages = stageInboxImages(imagePaths, { senderKey, pendingId });
  const previewPath = path.join(DATA_DIR, 'pending-whatsapp-settlements', `${pendingId}.json`);
  ensureDir(path.dirname(previewPath));
  const preview = buildSettledTicketPreview(stagedImages, previewPath);
  const expiresAt = new Date(Date.now() + (ttlHours * 60 * 60 * 1000)).toISOString();
  const pendingEntry = {
    pending_id: pendingId,
    channel: 'whatsapp_settlement',
    sender_key: safeSenderKey(senderKey),
    created_at_utc: new Date().toISOString(),
    expires_at_utc: expiresAt,
    preview_path: previewPath,
    staged_images: stagedImages,
    status: 'pending_confirmation',
    preview,
    preview_text: buildWhatsappPreviewText({ pending_id: pendingId, expires_at_utc: expiresAt, preview }),
  };
  store.pending = (store.pending || []).filter((entry) => !(entry.channel === 'whatsapp_settlement' && entry.sender_key === pendingEntry.sender_key));
  store.pending.push(pendingEntry);
  savePendingStore(store, pendingStorePath);
  return pendingEntry;
}

function loadPendingEntry({ senderKey = DEFAULT_WHATSAPP_SENDER_KEY, pendingStorePath = WHATSAPP_SETTLEMENT_PENDING_STORE_PATH }) {
  const store = compactStore(readJson(pendingStorePath, null) || loadPendingStore(pendingStorePath));
  savePendingStore(store, pendingStorePath);
  const entry = (store.pending || []).find((candidate) => candidate.channel === 'whatsapp_settlement' && candidate.sender_key === safeSenderKey(senderKey));
  return { store, entry: entry || null };
}

function resolveConfirmSelection(command) {
  const normalized = String(command || '').trim();
  if (/^CONFIRM\s+ALL$/i.test(normalized)) return 'CONFIRM_ALL';
  const numbered = normalized.match(/^CONFIRM\s+(.+)$/i);
  if (numbered) return numbered[1].trim();
  if (/^REJECT$/i.test(normalized) || /^CANCEL$/i.test(normalized)) return 'REJECT';
  return null;
}

export function handleWhatsappSettlementCommand({
  senderKey = DEFAULT_WHATSAPP_SENDER_KEY,
  command,
  pendingStorePath = WHATSAPP_SETTLEMENT_PENDING_STORE_PATH,
}) {
  const normalized = String(command || '').trim();
  const { store, entry } = loadPendingEntry({ senderKey, pendingStorePath });
  if (!entry) {
    return { status: 'no_pending_preview', whatsapp_text: 'No pending settled-ticket preview found. Send a settled sportsbook screenshot first.' };
  }
  if (/^HELP$/i.test(normalized)) {
    return { status: 'help', whatsapp_text: buildWhatsappPreviewText(entry) };
  }
  if (/^(CANCEL|REJECT)$/i.test(normalized)) {
    const nextStore = compactStore(store);
    nextStore.pending = (nextStore.pending || []).filter((candidate) => candidate.pending_id !== entry.pending_id);
    savePendingStore(nextStore, pendingStorePath);
    return { status: 'cancelled', whatsapp_text: `Cancelled settled-ticket ingestion ${entry.pending_id}. Nothing was appended.` };
  }
  const confirmSelection = resolveConfirmSelection(normalized);
  if (!confirmSelection) {
    return { status: 'unknown_command', whatsapp_text: 'Unrecognized settled-ticket command. Reply with HELP for valid options.' };
  }
  const confirmation = confirmSettledTicketPreview({ previewPath: entry.preview_path, confirm: confirmSelection });
  const nextStore = compactStore(store);
  nextStore.pending = (nextStore.pending || []).filter((candidate) => candidate.pending_id !== entry.pending_id);
  savePendingStore(nextStore, pendingStorePath);
  const lines = [];
  lines.push(`Settled-ticket log updated: ${confirmation.appended.length} appended, ${confirmation.skipped} skipped.`);
  confirmation.appended.forEach((row, index) => {
    lines.push(`${index + 1}. ${row.selection || row.event} | ${row.settlement_status} | payout=${row.settlement_payout ?? 'N/A'} | flags=${(row.reconciliation_flags || []).join(',') || 'none'}`);
  });
  if (!confirmation.appended.length) lines.push('Nothing appended.');
  return { status: 'confirmed', confirmation, whatsapp_text: lines.join('\n') };
}

export { extractImagePathsFromMessageText };

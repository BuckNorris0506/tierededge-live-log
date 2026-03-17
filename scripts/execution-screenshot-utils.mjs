import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { REPO_ROOT, DATA_DIR, CORE_PATHS, readJson, readJsonl, writeJson, parseNumber, round2 } from './core-ledger-utils.mjs';
import { appendExecutionLogRow, readExecutionLog } from './execution-layer-utils.mjs';

export const SCREENSHOT_PREVIEW_PATH = path.join(DATA_DIR, 'execution-screenshot-preview.json');
export const WHATSAPP_SCREENSHOT_INBOX_DIR = path.join(DATA_DIR, 'whatsapp-screenshot-inbox');
export const WHATSAPP_PENDING_STORE_PATH = path.join(DATA_DIR, 'whatsapp-execution-pending.json');
const OCR_SCRIPT_PATH = path.join(REPO_ROOT, 'scripts', 'screenshot-ocr.swift');
const OCR_RUNTIME_DIR = path.join(DATA_DIR, 'ocr-runtime');

const DEFAULT_PENDING_TTL_HOURS = 4;
const DEFAULT_WHATSAPP_SENDER_KEY = 'whatsapp:self';

function loadSidecarOcr(imagePath) {
  const sidecarCandidates = [
    `${imagePath}.ocr.txt`,
    `${imagePath}.txt`,
    path.join(path.dirname(imagePath), `${path.basename(imagePath, path.extname(imagePath))}.ocr.txt`),
    path.join(path.dirname(imagePath), `${path.basename(imagePath, path.extname(imagePath))}.txt`),
  ];
  for (const candidate of sidecarCandidates) {
    if (!fs.existsSync(candidate)) continue;
    const text = fs.readFileSync(candidate, 'utf8');
    const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);
    return {
      image_path: imagePath,
      lines: lines.map((line) => ({ text: line, confidence: 1 })),
      full_text: text,
      average_confidence: 1,
      ocr_source: 'sidecar_text',
    };
  }
  return null;
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

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function titleCaseToken(token) {
  return token ? token.charAt(0).toUpperCase() + token.slice(1).toLowerCase() : token;
}

function prettifySelection(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function detectSportsbook(lines) {
  const joined = normalize(lines.join(' '));
  if (joined.includes('draftkings sportsbook') || joined.includes('draftkings')) return 'DraftKings';
  if (joined.includes('fanduel')) return 'FanDuel';
  if (joined.includes('betmgm')) return 'BetMGM';
  if (joined.includes('caesars')) return 'Caesars';
  return null;
}

function detectStatus(lines) {
  for (const line of lines) {
    const normalized = normalize(line);
    if (normalized === 'open') return 'open';
    if (normalized === 'pending') return 'pending';
    if (normalized === 'won') return 'settled';
    if (normalized === 'lost') return 'settled';
    if (normalized.includes('cash out')) return 'cashed_out';
  }
  return 'unknown';
}

function parseHeader(headerLine) {
  const cleaned = String(headerLine || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return { selection: null, odds: null, warnings: ['missing_selection'] };
  const oddsMatch = cleaned.match(/([+-]\d{2,4})\s*$/);
  const odds = oddsMatch ? oddsMatch[1] : null;
  const selection = cleaned
    .replace(/^sgp\s+\d+\s+picks?\s*/i, '')
    .replace(/\s*[+-]\d{2,4}\s*$/, '')
    .trim();
  const warnings = [];
  if (!selection) warnings.push('missing_selection');
  if (!odds) warnings.push('missing_odds');
  return { selection: selection || null, odds, warnings };
}

function parseStakeLine(line) {
  const wager = String(line || '').match(/wager:\s*\$([0-9]+(?:\.[0-9]{2})?)/i)?.[1] || null;
  const toPay = String(line || '').match(/(?:to pay|paid):\s*\$([0-9]+(?:\.[0-9]{2})?)/i)?.[1] || null;
  return { wager, toPay };
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
    if (/^(open|pending|won|lost)$/i.test(trimmed)) continue;
    if (/cash out/i.test(trimmed)) continue;
    if (/^hide picks/i.test(trimmed)) continue;
    if (/draftkings/i.test(trimmed)) continue;
    if (/^\+?\d+%/.test(trimmed)) continue;
    if (/bet & get/i.test(trimmed)) continue;
    if (/[+-]\d{2,4}\s*$/.test(trimmed)) continue;
    if (/^(moneyline|spread|total|sgp|open|pending|won|lost)$/i.test(trimmed)) continue;
    if (trimmed.length < 3) continue;
    teamLike.push(trimmed);
  }
  const eventCandidates = [];
  for (let i = 0; i < teamLike.length - 1; i += 1) {
    if (teamLike[i + 1].length < 3) continue;
    eventCandidates.push(`${teamLike[i]} @ ${teamLike[i + 1]}`);
  }
  return eventCandidates[0] || null;
}

function inferMarketType(block, marketLine) {
  const joined = normalize(block.join(' '));
  const market = normalize(marketLine);
  if (joined.includes('sgp') || joined.includes(' picks')) return 'SGP';
  if (market.includes('moneyline')) return 'Moneyline';
  if (market.includes('spread')) return 'Spread';
  if (market.includes('total')) return 'Total';
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

function classifyBetType(block, marketType, matchedRecommendation) {
  const joined = normalize(block.join(' '));
  if (joined.includes('sgp') || String(marketType || '').toLowerCase() === 'sgp') return 'FUN_SGP';
  if (matchedRecommendation?.bet_class) return matchedRecommendation.bet_class;
  return 'MANUAL_OTHER';
}

function scoreCompleteness(extracted, warnings) {
  let score = extracted.base_confidence || 0.4;
  if (extracted.selection) score += 0.15;
  if (extracted.odds) score += 0.15;
  if (extracted.stake) score += 0.15;
  if (extracted.event) score += 0.15;
  if (extracted.ticket_timestamp) score += 0.05;
  if (warnings.includes('missing_event_identity')) score -= 0.2;
  if (warnings.includes('missing_odds')) score -= 0.2;
  if (warnings.includes('missing_stake')) score -= 0.2;
  if (warnings.includes('ambiguous_multiple_bets')) score -= 0.2;
  return Math.max(0, Math.min(1, round2(score)));
}

function parseSingleBetBlock({ screenshotFilename, sportsbook, block, averageConfidence }) {
  const lines = block.lines.map((line) => String(line || '').replace(/\s+/g, ' ').trim()).filter(Boolean);
  const warnings = [...block.warnings];
  const wagerIndex = lines.findIndex((line) => /wager:\s*\$/i.test(line));
  const headerLine = wagerIndex > 0 ? lines[wagerIndex - 2] || lines[wagerIndex - 1] : lines[0];
  const marketLine = wagerIndex > 0 ? lines[wagerIndex - 1] : lines[1];
  const header = parseHeader(headerLine);
  warnings.push(...header.warnings);
  const stakeLine = lines.find((line) => /wager:\s*\$/i.test(line)) || '';
  const { wager, toPay } = parseStakeLine(stakeLine);
  if (!wager) warnings.push('missing_stake');
  const event = parseEvent(lines);
  if (!event) warnings.push('missing_event_identity');
  const ticketTimestamp = parseTicketTimestamp(lines);
  const status = detectStatus(lines);
  const marketType = inferMarketType(lines, marketLine);
  const betType = marketType === 'SGP' ? 'FUN_SGP' : 'EDGE_BET';
  const confidence = scoreCompleteness({
    selection: header.selection,
    odds: header.odds,
    stake: wager,
    event,
    ticket_timestamp: ticketTimestamp,
    base_confidence: averageConfidence,
  }, warnings);

  if (confidence < 0.55) warnings.push('low_parse_confidence');

  return {
    screenshot_filename: path.basename(screenshotFilename),
    sportsbook,
    event,
    market_type: marketType,
    selection: header.selection ? prettifySelection(header.selection) : null,
    odds: header.odds,
    stake: wager,
    to_win_or_payout: toPay,
    bet_type: betType,
    ticket_timestamp: ticketTimestamp,
    status,
    extraction_confidence: confidence,
    parse_warnings: unique(warnings),
    raw_lines: lines,
  };
}

function parseScreenshotDocument(document) {
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
        to_win_or_payout: null,
        bet_type: 'MANUAL_OTHER',
        ticket_timestamp: null,
        status: 'unknown',
        extraction_confidence: 0,
        parse_warnings: ['unreadable_screenshot', document.error],
        raw_lines: [],
      }],
    };
  }
  const lines = String(document.full_text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  const sportsbook = detectSportsbook(lines);
  const blocks = splitBlocks(lines);
  const multiple = blocks.length > 1;
  const parsedBets = blocks.map((block) => {
    const bet = parseSingleBetBlock({
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

function parseRuntimeRecommendations(summary, runId, targetDate) {
  const text = String(summary || '');
  const rows = [];
  const pattern = /- \[ \] (.+?)\s+([+-]\d{2,4}) \| ([^\n]+)\n\s+Timestamp \(CT\): ([^\n]+)\n\s+True Prob: ([0-9.]+)% \| Implied Prob \(de-vig\): ([0-9.]+)% \| Edge: \+?([0-9.]+)%\n\s+Kelly Stake: \$([0-9.]+)/g;
  let match;
  let index = 1;
  while ((match = pattern.exec(text)) !== null) {
    const selection = match[1].trim();
    const odds = match[2];
    const books = match[3].split('/').map((item) => item.trim()).filter(Boolean);
    rows.push({
      rec_id: null,
      recommendation_key: `${targetDate || 'runtime'}::${index}`,
      run_id: runId,
      selection,
      sportsbook: books[0] || null,
      sportsbook_options: books,
      odds_american: odds,
      timestamp_ct: match[4].trim(),
      post_conf_true_prob: Number(match[5]),
      devig_implied_prob: Number(match[6]),
      post_conf_edge_pct: Number(match[7]),
      kelly_stake: Number(match[8]),
      bet_class: 'EDGE_BET',
      source: 'runtime_summary',
    });
    index += 1;
  }
  return rows;
}

function loadRecommendationUniverse() {
  const runtimeStatus = readJson(CORE_PATHS.runtimeStatus, {});
  const decisionRows = readJsonl(CORE_PATHS.decisionLedger)
    .filter((row) => row.final_decision === 'BET' || row.decision_kind === 'BET')
    .map((row) => ({
      rec_id: row.rec_id || null,
      recommendation_key: row.entry_id,
      run_id: row.run_id,
      sport: row.sport || null,
      league: row.league || row.sport || null,
      selection: row.selection,
      event_label: row.event_label,
      normalized_event: row.event_label || null,
      sportsbook: row.sportsbook,
      sportsbook_options: [row.sportsbook].filter(Boolean),
      odds_american: row.odds_american,
      timestamp_ct: row.timestamp_ct,
      post_conf_true_prob: row.post_conf_true_prob,
      devig_implied_prob: row.devig_implied_prob,
      post_conf_edge_pct: row.post_conf_edge_pct,
      kelly_stake: parseNumber(row.kelly_stake),
      bet_class: row.bet_class || 'EDGE_BET',
      source: row.source || 'decision_ledger',
    }));

  const runtimeRows = parseRuntimeRecommendations(
    runtimeStatus?.latest_hunt_current?.summary,
    runtimeStatus?.latest_hunt_current?.session_id || runtimeStatus?.latest_hunt_current?.run_id || null,
    runtimeStatus?.latest_hunt_current?.date_key || null
  );

  return [...decisionRows, ...runtimeRows];
}

function sportsbookMatch(extractedBook, candidate) {
  const books = [candidate.sportsbook, ...(candidate.sportsbook_options || [])].filter(Boolean).map(normalize);
  return extractedBook && books.includes(normalize(extractedBook));
}

function selectionScore(extractedSelection, candidateSelection) {
  const a = normalize(extractedSelection);
  const b = normalize(candidateSelection);
  if (!a || !b) return 0;
  if (a === b) return 50;
  if (a.includes(b) || b.includes(a)) return 35;
  const tokensA = new Set(a.split(' '));
  const tokensB = new Set(b.split(' '));
  let overlap = 0;
  for (const token of tokensA) {
    if (token && tokensB.has(token)) overlap += 1;
  }
  return Math.min(30, overlap * 8);
}

function eventScore(extractedEvent, candidateSelection) {
  const event = normalize(extractedEvent);
  const selection = normalize(candidateSelection);
  if (!event || !selection) return 0;
  const eventTokens = event.split(' ').filter(Boolean);
  let overlap = 0;
  for (const token of eventTokens) {
    if (selection.includes(token)) overlap += 1;
  }
  return Math.min(20, overlap * 5);
}

function timestampScore(extractedTimestamp, candidateTimestamp) {
  const a = Date.parse(String(extractedTimestamp || '').replace(' CT', ''));
  const b = Date.parse(String(candidateTimestamp || '').replace(' CT', ''));
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  const diffMinutes = Math.abs(a - b) / 60000;
  if (diffMinutes <= 30) return 10;
  if (diffMinutes <= 120) return 6;
  if (diffMinutes <= 480) return 3;
  return 0;
}

function oddsScore(extractedOdds, candidateOdds) {
  const a = parseNumber(extractedOdds);
  const b = parseNumber(candidateOdds);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  const diff = Math.abs(a - b);
  if (diff === 0) return 20;
  if (diff <= 5) return 16;
  if (diff <= 10) return 10;
  if (diff <= 20) return 4;
  return 0;
}

function classifyMatch(scored) {
  if (!scored.length) {
    return { match_status: 'unmatched_manual_bet', confidence_level: 'low', matched_recommendation: null, warnings: ['no_recommendation_match'] };
  }
  const sorted = scored.slice().sort((a, b) => b.score - a.score);
  const top = sorted[0];
  const second = sorted[1];
  if (top.score < 45) {
    return { match_status: 'unmatched_manual_bet', confidence_level: 'low', matched_recommendation: null, warnings: ['no_recommendation_match'] };
  }
  if (second && top.score - second.score <= 5) {
    return { match_status: 'ambiguous_match', confidence_level: 'low', matched_recommendation: top.candidate, warnings: ['ambiguous_match'] };
  }
  if (top.score >= 75) {
    return { match_status: top.candidate.rec_id ? 'matched_to_recommendation' : 'matched_with_low_confidence', confidence_level: top.candidate.rec_id ? 'high' : 'medium', matched_recommendation: top.candidate, warnings: [] };
  }
  return { match_status: 'matched_with_low_confidence', confidence_level: 'medium', matched_recommendation: top.candidate, warnings: ['low_match_confidence'] };
}

function matchParsedBetToRecommendation(parsedBet, recommendations) {
  const scored = recommendations.map((candidate) => {
    let score = 0;
    score += selectionScore(parsedBet.selection, candidate.selection);
    score += eventScore(parsedBet.event, candidate.selection);
    score += sportsbookMatch(parsedBet.sportsbook, candidate) ? 12 : 0;
    score += oddsScore(parsedBet.odds, candidate.odds_american);
    score += timestampScore(parsedBet.ticket_timestamp, candidate.timestamp_ct);
    return { candidate, score };
  });
  return classifyMatch(scored);
}

function buildProposedExecutionRow(parsedBet, matchResult) {
  const matched = matchResult.matched_recommendation;
  const actualStake = parsedBet.stake || '';
  const recommendedStake = matched?.kelly_stake ?? '';
  const recommendedOdds = matched?.odds_american ?? '';
  const actualOdds = parsedBet.odds || '';
  const recommendedNum = parseNumber(recommendedOdds);
  const actualNum = parseNumber(actualOdds);
  const drift = Number.isFinite(recommendedNum) && Number.isFinite(actualNum)
    ? `${actualNum - recommendedNum} cents`
    : 'N/A';
  const notes = unique([
    ...parsedBet.parse_warnings,
    ...matchResult.warnings,
  ]);
  const manualOverride = matchResult.match_status !== 'matched_to_recommendation';
  const overrideReason = manualOverride
    ? (matchResult.match_status === 'unmatched_manual_bet'
      ? 'unmatched_manual_bet'
      : matchResult.match_status)
    : null;
  const overrideJustification = manualOverride
    ? `Confirmed via screenshot ingestion. Match status=${matchResult.match_status}; warnings=${notes.join(',') || 'none'}.`
    : null;

  return {
    rec_id: matched?.rec_id || null,
    run_id: matched?.run_id || null,
    sport: matched?.sport || null,
    league: matched?.league || null,
    normalized_event: matched?.normalized_event || parsedBet.event || null,
    match_status: matchResult.match_status,
    sportsbook: parsedBet.sportsbook,
    event: parsedBet.event,
    market: parsedBet.market_type,
    selection: parsedBet.selection,
    recommended_odds: recommendedOdds || null,
    actual_odds: actualOdds || null,
    recommended_stake: recommendedStake || null,
    actual_stake: actualStake || null,
    line_price_drift: drift,
    screenshot_filename: parsedBet.screenshot_filename,
    extraction_confidence: parsedBet.extraction_confidence,
    manual_override_flag: manualOverride,
    override_reason: overrideReason,
    override_justification: overrideJustification,
    ingestion_timestamp: new Date().toISOString(),
    notes,
    warnings: notes,
    execution_approval_result: manualOverride ? 'REJECT_EXECUTION' : 'APPROVED_TO_BET',
    recommended_sportsbook: matched?.sportsbook || parsedBet.sportsbook,
    actual_sportsbook: parsedBet.sportsbook,
    recommendation_timestamp: matched?.timestamp_ct || null,
    bet_slip_timestamp: parsedBet.ticket_timestamp || null,
    actual_status: parsedBet.status,
    bet_type: parsedBet.bet_type,
    to_win_or_payout: parsedBet.to_win_or_payout,
  };
}

function buildPreviewItem(parsedBet, matchResult) {
  const proposedRow = buildProposedExecutionRow(parsedBet, matchResult);
  return {
    preview_id: proposedRow.rec_id || `${parsedBet.screenshot_filename}::${compact(parsedBet.selection || 'unknown')}`,
    screenshot_filename: parsedBet.screenshot_filename,
    extracted_fields: parsedBet,
    matched_rec_id: matchResult.matched_recommendation?.rec_id || null,
    matched_context: matchResult.matched_recommendation ? {
      recommendation_key: matchResult.matched_recommendation.recommendation_key,
      run_id: matchResult.matched_recommendation.run_id,
      sport: matchResult.matched_recommendation.sport || null,
      league: matchResult.matched_recommendation.league || null,
      selection: matchResult.matched_recommendation.selection,
      event_label: matchResult.matched_recommendation.event_label || null,
      normalized_event: matchResult.matched_recommendation.normalized_event || null,
      sportsbook: matchResult.matched_recommendation.sportsbook,
      odds_american: matchResult.matched_recommendation.odds_american,
      timestamp_ct: matchResult.matched_recommendation.timestamp_ct,
    } : null,
    match_status: matchResult.match_status,
    confidence_level: matchResult.confidence_level,
    warnings: unique([...parsedBet.parse_warnings, ...matchResult.warnings]),
    proposed_execution_log_row: proposedRow,
  };
}

function renderPreviewText(preview) {
  const lines = [];
  lines.push(`SCREENSHOT EXECUTION PREVIEW (${preview.items.length} bets)`);
  lines.push(`Status: ${preview.status}`);
  lines.push(`Generated: ${preview.generated_at_utc}`);
  for (const [index, item] of preview.items.entries()) {
    lines.push('');
    lines.push(`${index + 1}. ${item.extracted_fields.selection || 'UNKNOWN BET'} | ${item.extracted_fields.event || 'UNKNOWN EVENT'}`);
    lines.push(`   Screenshot: ${item.screenshot_filename}`);
    lines.push(`   Sportsbook: ${item.extracted_fields.sportsbook || 'UNKNOWN'}`);
    lines.push(`   Market: ${item.extracted_fields.market_type || 'UNKNOWN'} | Odds: ${item.extracted_fields.odds || 'MISSING'} | Stake: ${item.extracted_fields.stake || 'MISSING'}`);
    lines.push(`   Match: ${item.match_status} | Confidence: ${item.confidence_level}`);
    lines.push(`   rec_id: ${item.matched_rec_id || 'NONE'}`);
    lines.push(`   Proposed row: actual_odds=${item.proposed_execution_log_row.actual_odds || 'MISSING'} actual_stake=${item.proposed_execution_log_row.actual_stake || 'MISSING'} approval=${item.proposed_execution_log_row.execution_approval_result}`);
    if (item.warnings.length) {
      lines.push(`   Warnings: ${item.warnings.join(', ')}`);
    }
  }
  return lines.join('\n');
}

export function parseArgs(argv) {
  const result = { images: [], confirm: null, previewPath: SCREENSHOT_PREVIEW_PATH };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--image' || arg === '--images') {
      while (argv[i + 1] && !argv[i + 1].startsWith('--')) {
        result.images.push(argv[i + 1]);
        i += 1;
      }
    } else if (arg === '--preview') {
      result.previewPath = argv[i + 1];
      i += 1;
    } else if (arg === '--confirm') {
      result.confirm = argv[i + 1];
      i += 1;
    } else {
      result.images.push(arg);
    }
  }
  return result;
}

export function extractImagePathsFromMessageText(text) {
  const matches = [...String(text || '').matchAll(/\[media attached:\s*([^\]\n]+?)\s+\(image\/[^\)\n]+\)\]/gi)];
  return unique(matches.map((match) => String(match[1] || '').trim()));
}

function safeSenderKey(value) {
  return String(value || DEFAULT_WHATSAPP_SENDER_KEY)
    .toLowerCase()
    .replace(/[^a-z0-9:+._-]/g, '-')
    .slice(0, 120);
}

function loadPendingStore(pendingStorePath = WHATSAPP_PENDING_STORE_PATH) {
  const store = readJson(pendingStorePath, null);
  if (store && typeof store === 'object' && !Array.isArray(store)) {
    return {
      generated_at_utc: store.generated_at_utc || new Date().toISOString(),
      pending: Array.isArray(store.pending) ? store.pending : [],
    };
  }
  return {
    generated_at_utc: new Date().toISOString(),
    pending: [],
  };
}

function savePendingStore(store, pendingStorePath = WHATSAPP_PENDING_STORE_PATH) {
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
  return {
    generated_at_utc: new Date().toISOString(),
    pending,
  };
}

function stageInboxImages(imagePaths, { senderKey, pendingId }) {
  ensureDir(WHATSAPP_SCREENSHOT_INBOX_DIR);
  return imagePaths.map((imagePath, index) => {
    const absolute = path.resolve(imagePath);
    const ext = path.extname(absolute) || '.img';
    const baseName = `${safeSenderKey(senderKey)}__${pendingId}__${String(index + 1).padStart(2, '0')}${ext}`;
    const dest = path.join(WHATSAPP_SCREENSHOT_INBOX_DIR, baseName);
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
      const sidecarDest = path.join(WHATSAPP_SCREENSHOT_INBOX_DIR, sidecarName);
      fs.copyFileSync(candidate, sidecarDest);
    }
    return dest;
  });
}

function buildWhatsappPreviewText(pendingEntry) {
  const lines = [];
  lines.push(`BET SCREENSHOT PREVIEW (${pendingEntry.preview.items.length})`);
  lines.push(`Pending ID: ${pendingEntry.pending_id}`);
  lines.push(`Expires: ${pendingEntry.expires_at_utc}`);
  for (const [index, item] of pendingEntry.preview.items.entries()) {
    lines.push('');
    lines.push(`${index + 1}. ${item.extracted_fields.selection || 'UNKNOWN'} | ${item.extracted_fields.event || 'UNKNOWN EVENT'}`);
    lines.push(`Book: ${item.extracted_fields.sportsbook || 'UNKNOWN'} | Market: ${item.extracted_fields.market_type || 'UNKNOWN'}`);
    lines.push(`Odds: ${item.extracted_fields.odds || 'MISSING'} | Stake: ${item.extracted_fields.stake || 'MISSING'}`);
    lines.push(`Match: ${item.match_status}${item.matched_rec_id ? ` | rec_id: ${item.matched_rec_id}` : ''}`);
    if (item.warnings.length) {
      lines.push(`Warnings: ${item.warnings.join(', ')}`);
    }
  }
  lines.push('');
  lines.push('Reply with one of:');
  lines.push('- CONFIRM ALL');
  lines.push('- CONFIRM 1,3');
  lines.push('- REJECT');
  lines.push('- CANCEL');
  lines.push('- HELP');
  lines.push('- EDIT 1 ODDS +136');
  lines.push('- EDIT 1 STAKE 20.50');
  return lines.join('\n');
}

function parseWhatsappEditCommand(command) {
  const match = String(command || '').trim().match(/^EDIT\s+(\d+)\s+(ODDS|STAKE)\s+(.+)$/i);
  if (!match) return null;
  return {
    index: Number(match[1]),
    field: match[2].toUpperCase(),
    value: match[3].trim(),
  };
}

function applyWhatsappEdit(pendingEntry, edit) {
  const item = pendingEntry.preview.items[edit.index - 1];
  if (!item) {
    return { ok: false, message: `EDIT failed: item ${edit.index} not found.` };
  }
  if (edit.field === 'ODDS') {
    item.extracted_fields.odds = edit.value;
    item.proposed_execution_log_row.actual_odds = edit.value;
  } else if (edit.field === 'STAKE') {
    item.extracted_fields.stake = edit.value;
    item.proposed_execution_log_row.actual_stake = edit.value;
  } else {
    return { ok: false, message: `EDIT failed: unsupported field ${edit.field}.` };
  }
  item.warnings = unique([...(item.warnings || []), `edited_${edit.field.toLowerCase()}`]);
  item.proposed_execution_log_row.warnings = unique([...(item.proposed_execution_log_row.warnings || []), `edited_${edit.field.toLowerCase()}`]);
  item.proposed_execution_log_row.notes = unique([...(item.proposed_execution_log_row.notes || []), `edited_${edit.field.toLowerCase()}`]);
  return { ok: true, message: `Updated item ${edit.index} ${edit.field} to ${edit.value}.` };
}

function resolveConfirmSelection(command) {
  const normalized = String(command || '').trim();
  if (/^CONFIRM\s+ALL$/i.test(normalized)) return 'CONFIRM_ALL';
  const numbered = normalized.match(/^CONFIRM\s+(.+)$/i);
  if (numbered) return numbered[1].trim();
  if (/^REJECT$/i.test(normalized) || /^CANCEL$/i.test(normalized)) return 'REJECT';
  return null;
}

export function runSwiftOcr(imagePaths) {
  const absolutePaths = imagePaths.map((imagePath) => path.resolve(imagePath));
  const sidecarDocs = [];
  const pendingPaths = [];
  for (const absolutePath of absolutePaths) {
    const sidecar = loadSidecarOcr(absolutePath);
    if (sidecar) {
      sidecarDocs.push(sidecar);
    } else {
      pendingPaths.push(absolutePath);
    }
  }

  if (!pendingPaths.length) return sidecarDocs;

  ensureDir(OCR_RUNTIME_DIR);
  const moduleCachePath = path.join(OCR_RUNTIME_DIR, 'swift-module-cache');
  ensureDir(moduleCachePath);

  const result = spawnSync('/usr/bin/swift', [OCR_SCRIPT_PATH, ...pendingPaths], {
    cwd: REPO_ROOT,
    encoding: 'utf8',
    env: {
      ...process.env,
      SWIFT_MODULE_CACHE_PATH: moduleCachePath,
      CLANG_MODULE_CACHE_PATH: moduleCachePath,
      TMPDIR: OCR_RUNTIME_DIR,
    },
  });
  if (result.status !== 0) {
    return [
      ...sidecarDocs,
      ...pendingPaths.map((imagePath) => ({
        image_path: imagePath,
        lines: [],
        full_text: '',
        average_confidence: 0,
        error: 'ocr_backend_unavailable',
      })),
    ];
  }
  return [...sidecarDocs, ...JSON.parse(result.stdout)];
}

export function buildScreenshotExecutionPreview(imagePaths, previewPath = SCREENSHOT_PREVIEW_PATH) {
  const recommendations = loadRecommendationUniverse();
  const documents = runSwiftOcr(imagePaths);
  const screenshotPreviews = documents.map(parseScreenshotDocument);
  const items = screenshotPreviews.flatMap((screenshot) => screenshot.parsed_bets.map((parsedBet) => {
    const matchResult = matchParsedBetToRecommendation(parsedBet, recommendations);
    return buildPreviewItem(parsedBet, matchResult);
  }));

  const status = items.some((item) => item.warnings.includes('unreadable_screenshot'))
    ? 'requires_review'
    : items.some((item) => item.confidence_level === 'low')
      ? 'requires_confirmation'
      : 'ready_for_confirmation';

  const preview = {
    generated_at_utc: new Date().toISOString(),
    status,
    source_images: imagePaths.map((imagePath) => path.resolve(imagePath)),
    screenshot_previews: screenshotPreviews,
    items,
    verification_actions: ['CONFIRM_ALL', 'CONFIRM_SELECTED', 'REJECT', 'EDIT_FIELDS'],
    preview_text: renderPreviewText({ generated_at_utc: new Date().toISOString(), status, items }),
  };

  writeJson(previewPath, preview);
  return preview;
}

function selectedItems(preview, confirmValue) {
  if (!confirmValue || confirmValue.toUpperCase() === 'REJECT') return [];
  if (confirmValue.toUpperCase() === 'ALL' || confirmValue.toUpperCase() === 'CONFIRM_ALL') {
    return preview.items;
  }
  const requested = new Set(confirmValue.split(',').map((part) => part.trim()).filter(Boolean));
  return preview.items.filter((item, index) => requested.has(String(index + 1)) || requested.has(item.preview_id));
}

export function confirmScreenshotExecutionPreview({ previewPath = SCREENSHOT_PREVIEW_PATH, confirm }) {
  const preview = readJson(previewPath, null);
  if (!preview) {
    throw new Error(`missing_preview:${previewPath}`);
  }
  const chosen = selectedItems(preview, confirm);
  if (!chosen.length) {
    return { preview, appended: [], skipped: preview.items.length };
  }

  const appended = [];
  for (const item of chosen) {
    const row = { ...item.proposed_execution_log_row };
    row.execution_id = row.execution_id || `execution::screenshot::${item.preview_id}::${Date.now()}`;
    row.rec_id = row.rec_id || `unmatched::${item.preview_id}`;
    row.run_id = row.run_id || 'manual-screenshot-ingestion';
    row.sport = row.sport || item.matched_context?.sport || '';
    row.league = row.league || item.matched_context?.league || '';
    row.normalized_event = row.normalized_event || item.matched_context?.normalized_event || row.event || '';
    row.event = row.event || item.extracted_fields.event || '';
    row.market = row.market || item.extracted_fields.market_type || '';
    row.recommendation_timestamp = row.recommendation_timestamp || item.matched_context?.timestamp_ct || row.bet_slip_timestamp || '';
    row.recommended_sportsbook = row.recommended_sportsbook || item.matched_context?.sportsbook || row.actual_sportsbook || '';
    row.recommended_odds = row.recommended_odds || null;
    row.recommended_stake = row.recommended_stake || null;
    row.actual_sportsbook = row.actual_sportsbook || item.extracted_fields.sportsbook || '';
    row.actual_odds = row.actual_odds || item.extracted_fields.odds || '';
    row.actual_stake = row.actual_stake || item.extracted_fields.stake || '';
    row.bet_slip_timestamp = row.bet_slip_timestamp || item.extracted_fields.ticket_timestamp || '';
    row.selection = row.selection || item.extracted_fields.selection || '';
    row.confirmation_source = row.confirmation_source || 'whatsapp';
    row.match_status = row.match_status || item.match_status || 'unmatched_manual_bet';
    row.screenshot_filename = row.screenshot_filename || item.screenshot_filename || '';
    row.extraction_confidence = row.extraction_confidence ?? item.extracted_fields.extraction_confidence ?? null;
    row.ingestion_timestamp = row.ingestion_timestamp || new Date().toISOString();
    if (!row.actual_odds) throw new Error(`missing_odds:${item.preview_id}`);
    if (!row.actual_stake) throw new Error(`missing_stake:${item.preview_id}`);
    if (!row.event) throw new Error(`missing_event_identity:${item.preview_id}`);
    appendExecutionLogRow(row);
    appended.push(row);
  }

  return {
    preview,
    appended,
    skipped: preview.items.length - appended.length,
    execution_log_count: readExecutionLog().length,
  };
}

export function createWhatsappExecutionPreview({
  senderKey = DEFAULT_WHATSAPP_SENDER_KEY,
  imagePaths = [],
  pendingStorePath = WHATSAPP_PENDING_STORE_PATH,
  ttlHours = DEFAULT_PENDING_TTL_HOURS,
}) {
  const store = compactStore(loadPendingStore(pendingStorePath));
  const pendingId = `waexec-${Date.now()}-${randomUUID().slice(0, 8)}`;
  const stagedImages = stageInboxImages(imagePaths, { senderKey, pendingId });
  const previewPath = path.join(DATA_DIR, 'pending-whatsapp-previews', `${pendingId}.json`);
  ensureDir(path.dirname(previewPath));
  const preview = buildScreenshotExecutionPreview(stagedImages, previewPath);
  const createdAt = new Date().toISOString();
  const expiresAt = new Date(Date.now() + (ttlHours * 60 * 60 * 1000)).toISOString();

  const pendingEntry = {
    pending_id: pendingId,
    channel: 'whatsapp',
    sender_key: safeSenderKey(senderKey),
    created_at_utc: createdAt,
    expires_at_utc: expiresAt,
    preview_path: previewPath,
    staged_images: stagedImages,
    status: 'pending_confirmation',
    preview,
    preview_text: buildWhatsappPreviewText({
      pending_id: pendingId,
      expires_at_utc: expiresAt,
      preview,
    }),
  };

  store.pending = (store.pending || []).filter((entry) => !(entry.channel === 'whatsapp' && entry.sender_key === pendingEntry.sender_key));
  store.pending.push(pendingEntry);
  writeJson(previewPath, preview);
  savePendingStore(store, pendingStorePath);
  return pendingEntry;
}

function loadPendingEntry({ senderKey = DEFAULT_WHATSAPP_SENDER_KEY, pendingStorePath = WHATSAPP_PENDING_STORE_PATH }) {
  const store = compactStore(readJson(pendingStorePath, null) || loadPendingStore(pendingStorePath));
  savePendingStore(store, pendingStorePath);
  const entry = (store.pending || []).find((candidate) => candidate.channel === 'whatsapp' && candidate.sender_key === safeSenderKey(senderKey));
  return { store, entry: entry || null };
}

function finalizePendingEntry(store, updatedEntry, pendingStorePath = WHATSAPP_PENDING_STORE_PATH) {
  const next = compactStore(store);
  if (!updatedEntry) {
    savePendingStore(next, pendingStorePath);
    return;
  }
  next.pending = (next.pending || []).map((entry) => entry.pending_id === updatedEntry.pending_id ? updatedEntry : entry);
  savePendingStore(next, pendingStorePath);
}

export function handleWhatsappExecutionCommand({
  senderKey = DEFAULT_WHATSAPP_SENDER_KEY,
  command,
  pendingStorePath = WHATSAPP_PENDING_STORE_PATH,
}) {
  const normalized = String(command || '').trim();
  const { store, entry } = loadPendingEntry({ senderKey, pendingStorePath });

  if (!entry) {
    return {
      status: 'no_pending_preview',
      whatsapp_text: 'No pending screenshot preview found. Send a sportsbook screenshot first.',
    };
  }

  if (/^HELP$/i.test(normalized)) {
    return {
      status: 'help',
      whatsapp_text: buildWhatsappPreviewText(entry),
    };
  }

  if (/^(CANCEL|REJECT)$/i.test(normalized)) {
    const nextStore = compactStore(store);
    nextStore.pending = (nextStore.pending || []).filter((candidate) => candidate.pending_id !== entry.pending_id);
    savePendingStore(nextStore, pendingStorePath);
    return {
      status: 'cancelled',
      whatsapp_text: `Cancelled screenshot ingestion ${entry.pending_id}. Nothing was appended.`,
    };
  }

  const edit = parseWhatsappEditCommand(normalized);
  if (edit) {
    const result = applyWhatsappEdit(entry, edit);
    finalizePendingEntry(store, entry, pendingStorePath);
    return {
      status: result.ok ? 'edited' : 'edit_failed',
      whatsapp_text: `${result.message}\n\n${buildWhatsappPreviewText(entry)}`,
    };
  }

  const confirmSelection = resolveConfirmSelection(normalized);
  if (!confirmSelection) {
    return {
      status: 'unknown_command',
      whatsapp_text: 'Unrecognized screenshot command. Reply with HELP for valid options.',
    };
  }

  const confirmation = confirmScreenshotExecutionPreview({
    previewPath: entry.preview_path,
    confirm: confirmSelection,
  });

  const nextStore = compactStore(store);
  nextStore.pending = (nextStore.pending || []).filter((candidate) => candidate.pending_id !== entry.pending_id);
  savePendingStore(nextStore, pendingStorePath);

  const lines = [];
  lines.push(`Execution log updated: ${confirmation.appended.length} appended, ${confirmation.skipped} skipped.`);
  confirmation.appended.forEach((row, index) => {
    lines.push(`${index + 1}. ${row.selection || row.event} | ${row.actual_sportsbook || 'UNKNOWN'} | ${row.actual_odds || 'MISSING'} | $${row.actual_stake}`);
  });
  if (!confirmation.appended.length) {
    lines.push('Nothing appended.');
  }
  return {
    status: 'confirmed',
    confirmation,
    whatsapp_text: lines.join('\n'),
  };
}

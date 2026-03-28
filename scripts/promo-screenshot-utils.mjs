import path from 'node:path';
import { DATA_DIR, round2, writeJson } from './core-ledger-utils.mjs';
import { runSwiftOcr } from './execution-screenshot-utils.mjs';

export const PROMO_SCREENSHOT_PREVIEW_PATH = path.join(DATA_DIR, 'promo-screenshot-preview.json');

function normalize(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9%+$\-.\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function unique(values) {
  return [...new Set((values || []).filter(Boolean))];
}

function detectSportsbook(text) {
  const normalized = normalize(text);
  if (normalized.includes('draftkings') || /\bdk\b/.test(normalized)) return 'DraftKings';
  if (normalized.includes('fanduel') || /\bfd\b/.test(normalized)) return 'FanDuel';
  if (normalized.includes('betmgm') || /\bmgm\b/.test(normalized)) return 'BetMGM';
  if (normalized.includes('caesars') || /\bczr\b/.test(normalized)) return 'Caesars';
  if (normalized.includes('bet365')) return 'bet365';
  return null;
}

function detectPromoType(text) {
  const normalized = normalize(text);
  if (normalized.includes('no sweat')) return 'NO SWEAT TOKEN';
  if (normalized.includes('early win')) return 'EARLY WIN TOKEN';
  if (normalized.includes('profit boost') || /\bboost\b/.test(normalized)) return 'PROFIT BOOST';
  return null;
}

function detectBoostPercent(text) {
  const match = String(text || '').match(/(\d+(?:\.\d+)?)\s*(?:%|percent\b)/i);
  if (!match) return null;
  const numeric = Number(match[1]);
  return Number.isFinite(numeric) ? round2(numeric) : null;
}

function detectScope(text) {
  const normalized = normalize(text);
  if (/\bgeneral\b/.test(normalized)) return 'GENERAL';
  if (/\bmlb\b|baseball/.test(normalized)) return 'MLB';
  if (/\bnba\b/.test(normalized)) return 'NBA';
  if (/\bnhl\b|hockey/.test(normalized)) return 'NHL';
  if (/\bnfl\b|football/.test(normalized)) return 'NFL';
  if (/\bncaab\b|college basketball|college bball|college hoops|\bcbb\b/.test(normalized)) return 'NCAAB';
  if (/\bwnba\b/.test(normalized)) return 'WNBA';
  if (/\bsgp\+?\b|same game parlay/.test(normalized)) return 'SGP';
  if (/\bmoneyline\b|\bml\b/.test(normalized)) return 'MONEYLINE';
  return null;
}

function detectMaxWager(text) {
  const match = String(text || '').match(/(?:max(?:imum)?\s+(?:wager|bet)|up to)\s*:?\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)/i);
  if (!match) return null;
  const numeric = Number(match[1]);
  return Number.isFinite(numeric) ? round2(numeric) : null;
}

function detectMinTotalOdds(text) {
  const explicit = String(text || '').match(/(?:min(?:imum)?\s+total\s+odds|min(?:imum)?\s+odds|odds\s+of)\s*:?\s*([+-]\d{2,4})/i);
  if (explicit) return explicit[1];
  return null;
}

function detectBetTypes(text) {
  const normalized = normalize(text);
  const labels = [];
  if (/\b3\+?\s*leg\b/.test(normalized)) labels.push('3+ leg');
  if (/\bsgp\+?\b|same game parlay/.test(normalized)) labels.push('SGP');
  if (/\bstraight\b/.test(normalized)) labels.push('Straight');
  if (/\bparlay\b/.test(normalized) && !labels.includes('SGP')) labels.push('Parlay');
  if (/\bmoneyline\b|\bml\b/.test(normalized)) labels.push('Moneyline');
  return labels.length ? labels.join(', ') : null;
}

function detectExpiresRaw(lines) {
  for (const line of lines) {
    const explicit = String(line || '').match(/(?:expires?|expiration)\s*:?\s*(.+)$/i);
    if (explicit?.[1]) return explicit[1].trim();
  }
  return 'Not specified';
}

function parsePromoDocument(document) {
  const lines = String(document?.full_text || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  const joined = lines.join('\n');
  const promoType = detectPromoType(joined);
  const sportsbook = detectSportsbook(joined);
  const boostPercent = promoType === 'PROFIT BOOST' ? detectBoostPercent(joined) : null;
  const scope = detectScope(joined);
  const maxWager = detectMaxWager(joined);
  const minTotalOdds = detectMinTotalOdds(joined);
  const betTypes = detectBetTypes(joined);
  const expiresRaw = detectExpiresRaw(lines);

  const warnings = [];
  if (!promoType) warnings.push('missing_reward_type');
  if (!sportsbook) warnings.push('missing_sportsbook');
  if (promoType === 'PROFIT BOOST' && boostPercent === null) warnings.push('missing_boost_percent');
  if (!scope) warnings.push('missing_scope');

  const confidence = Math.max(
    0,
    Math.min(
      1,
      round2(
        (Number(document?.average_confidence || 0) * 0.5)
        + (promoType ? 0.15 : 0)
        + (sportsbook ? 0.15 : 0)
        + (scope ? 0.1 : 0)
        + (boostPercent !== null ? 0.1 : 0)
      ),
    ),
  );

  return {
    preview_id: `${path.basename(document?.image_path || 'promo')}::${promoType || 'unknown'}`,
    screenshot_filename: path.basename(document?.image_path || 'unknown'),
    extracted_fields: {
      promo_type: promoType,
      sportsbook,
      boost_percent: boostPercent,
      scope,
      max_wager: maxWager,
      bet_types: betTypes,
      min_total_odds: minTotalOdds,
      expires_raw: expiresRaw,
      status: 'ACTIVE',
      extraction_confidence: confidence,
      parse_warnings: unique(warnings),
      raw_lines: lines,
    },
    warnings: unique(warnings),
    confidence_level: confidence >= 0.75 ? 'high' : confidence >= 0.55 ? 'medium' : 'low',
  };
}

export function buildPromoScreenshotPreview(imagePaths, previewPath = PROMO_SCREENSHOT_PREVIEW_PATH) {
  const documents = runSwiftOcr(imagePaths);
  const items = documents.map(parsePromoDocument);
  const preview = {
    generated_at_utc: new Date().toISOString(),
    status: items.some((item) => item.confidence_level === 'low') ? 'requires_confirmation' : 'ready_for_confirmation',
    source_images: imagePaths.map((imagePath) => path.resolve(imagePath)),
    items,
  };
  writeJson(previewPath, preview);
  return preview;
}

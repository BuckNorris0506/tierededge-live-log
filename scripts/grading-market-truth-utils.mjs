import { parseNumber, readJsonl, round2, writeJsonl, CORE_PATHS } from './core-ledger-utils.mjs';

const FINAL_STATUSES = new Set(['win', 'loss', 'void', 'push', 'cashed_out', 'partial_cashout']);

function normalize(value) {
  return String(value || '').trim().toLowerCase();
}

function americanToImpliedProb(oddsValue) {
  const odds = parseNumber(oddsValue);
  if (!Number.isFinite(odds) || odds === 0) return null;
  if (odds > 0) return round2(100 / (odds + 100));
  return round2(Math.abs(odds) / (Math.abs(odds) + 100));
}

function parseLegacyClv(clv) {
  if (clv === null || clv === undefined) return null;
  const text = String(clv).trim();
  if (!text || text === '-' || /^n\/a$/i.test(text)) return null;
  const numeric = parseNumber(text);
  return Number.isFinite(numeric) ? numeric : null;
}

export function enrichGradingRowWithClv(row) {
  const next = { ...row };
  const status = normalize(next.settlement_status || next.result);
  const isSettledBet = (String(next.grading_type || '').toUpperCase() === 'BET' || String(next.grading_type || '').toUpperCase() === 'RECONCILIATION') && FINAL_STATUSES.has(status);
  if (!isSettledBet) return next;

  const existingClosingOdds = parseNumber(next.closing_odds);
  const legacyClv = parseLegacyClv(next.clv);

  if (Number.isFinite(existingClosingOdds)) {
    next.closing_implied_prob = next.closing_implied_prob ?? americanToImpliedProb(existingClosingOdds);
    next.clv_price_delta = next.clv_price_delta ?? round2((parseNumber(next.actual_odds) || parseNumber(next.odds_american) || 0) - existingClosingOdds);
    next.clv_prob_delta = next.clv_prob_delta ?? (() => {
      const takenProb = americanToImpliedProb(next.actual_odds || next.odds_american);
      return takenProb !== null && next.closing_implied_prob !== null ? round2(next.closing_implied_prob - takenProb) : null;
    })();
    next.clv_status = next.clv_status || 'exact_close_found';
    next.clv_source = next.clv_source || 'closing_odds_field';
    next.clv_warning = next.clv_warning || null;
    return next;
  }

  if (legacyClv !== null) {
    next.clv_price_delta = next.clv_price_delta ?? legacyClv;
    next.clv_prob_delta = next.clv_prob_delta ?? null;
    next.clv_status = next.clv_status || 'proxy_close_found';
    next.clv_source = next.clv_source || 'legacy_clv_field';
    next.clv_warning = next.clv_warning || 'Legacy CLV string available, but exact closing odds were not stored.';
    next.closing_odds = next.closing_odds ?? null;
    next.closing_implied_prob = next.closing_implied_prob ?? null;
    return next;
  }

  next.closing_odds = next.closing_odds ?? null;
  next.closing_implied_prob = next.closing_implied_prob ?? null;
  next.clv_price_delta = next.clv_price_delta ?? null;
  next.clv_prob_delta = next.clv_prob_delta ?? null;
  next.clv_status = next.clv_status || 'missing_clv_source';
  next.clv_source = next.clv_source || 'none';
  next.clv_warning = next.clv_warning || 'No closing market source attached to this settled bet.';
  return next;
}

export function backfillGradingClvFields() {
  const rows = readJsonl(CORE_PATHS.gradingLedger);
  const enriched = rows.map((row) => enrichGradingRowWithClv(row));
  writeJsonl(CORE_PATHS.gradingLedger, enriched);
  const settled = enriched.filter((row) => ['BET', 'RECONCILIATION'].includes(String(row.grading_type || '').toUpperCase()) && FINAL_STATUSES.has(normalize(row.settlement_status || row.result)));
  const withClv = settled.filter((row) => String(row.clv_status || '') !== 'missing_clv_source').length;
  return {
    settled_rows: settled.length,
    with_clv_anchor: withClv,
    missing_clv_anchor: settled.length - withClv,
  };
}

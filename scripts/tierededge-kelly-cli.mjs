#!/usr/bin/env node
import process from 'node:process';

const TIER_MULTIPLIERS = {
  T1: 0.75,
  T2: 0.4,
  T3: 0.2,
  FUN: 0,
};

function toNumber(value, name) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    throw new Error(`Invalid ${name}: ${value}`);
  }
  return num;
}

function americanToDecimal(americanOdds) {
  const odds = toNumber(americanOdds, 'american_odds');
  if (odds === 0) throw new Error('American odds cannot be 0.');
  return odds > 0 ? 1 + (odds / 100) : 1 + (100 / Math.abs(odds));
}

function roundToHalf(value) {
  return Math.round(value * 2) / 2;
}

function formatMoney(value) {
  return `$${value.toFixed(2)}`;
}

export function computeKellyBreakdown({
  bankroll,
  american_odds,
  true_prob,
  implied_prob_fair = null,
  tier = 'T3',
  breaker_active = false,
}) {
  const bankrollNum = toNumber(bankroll, 'bankroll');
  const p = toNumber(true_prob, 'true_prob');
  const impliedFair = implied_prob_fair == null ? null : toNumber(implied_prob_fair, 'implied_prob_fair');
  if (p <= 0 || p >= 1) throw new Error(`true_prob must be between 0 and 1 exclusive. Got ${p}`);
  if (impliedFair != null && (impliedFair <= 0 || impliedFair >= 1)) {
    throw new Error(`implied_prob_fair must be between 0 and 1 exclusive. Got ${impliedFair}`);
  }

  const tierKey = String(tier || 'T3').toUpperCase();
  const multiplier = TIER_MULTIPLIERS[tierKey];
  if (multiplier == null) throw new Error(`Unsupported tier: ${tier}`);

  const decimal_odds = americanToDecimal(american_odds);
  const b = decimal_odds - 1;
  const offered_implied_prob_raw = 1 / decimal_odds;
  const q = 1 - p;
  const raw_kelly_fraction = ((b * p) - q) / b;
  const fractional_kelly_fraction = raw_kelly_fraction > 0 ? raw_kelly_fraction * multiplier : 0;
  const pre_breaker_stake = bankrollNum * fractional_kelly_fraction;
  const breaker_multiplier = breaker_active ? 0.5 : 1;
  const post_breaker_stake = pre_breaker_stake * breaker_multiplier;
  const rounded_stake = roundToHalf(post_breaker_stake);
  const final_stake = rounded_stake < 0.5 ? 0 : rounded_stake;
  const final_decision_hint = final_stake > 0 ? 'EXECUTABLE' : 'SUB_MIN_STAKE';

  return {
    bankroll: bankrollNum,
    american_odds: toNumber(american_odds, 'american_odds'),
    decimal_odds,
    net_odds_b: b,
    implied_prob_offered_raw: offered_implied_prob_raw,
    implied_prob_fair: impliedFair,
    true_prob: p,
    raw_edge_pct_vs_fair: impliedFair == null ? null : (p - impliedFair) * 100,
    raw_edge_pct_vs_offered: (p - offered_implied_prob_raw) * 100,
    raw_kelly_fraction,
    tier: tierKey,
    fractional_kelly_multiplier: multiplier,
    fractional_kelly_fraction,
    breaker_active: Boolean(breaker_active),
    breaker_multiplier,
    pre_breaker_stake,
    rounded_to_half_stake: rounded_stake,
    min_executable_stake: 0.5,
    final_stake,
    final_decision_hint,
    formatted: {
      bankroll: formatMoney(bankrollNum),
      pre_breaker_stake: formatMoney(pre_breaker_stake),
      rounded_to_half_stake: formatMoney(rounded_stake),
      final_stake: formatMoney(final_stake),
    },
  };
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2).replace(/-/g, '_');
    const next = argv[i + 1];
    if (next == null || next.startsWith('--')) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function main() {
  try {
    const args = parseArgs(process.argv);
    if (args.examples) {
      const bankroll = 1328.88;
      const examples = [
        { label: 'Timberwolves -3.5', american_odds: -115, true_prob: 0.535, implied_prob_fair: 0.511, tier: 'T3' },
        { label: 'George Mason -4.5', american_odds: -110, true_prob: 0.525, implied_prob_fair: 0.5, tier: 'T3' },
        { label: 'Yale -5.5', american_odds: -105, true_prob: 0.545, implied_prob_fair: 0.519, tier: 'T3' },
        { label: 'Bruins ML', american_odds: 140, true_prob: 0.42, implied_prob_fair: 0.399, tier: 'T3' },
      ];
      const results = examples.map((example) => ({ label: example.label, ...computeKellyBreakdown({ bankroll, ...example }) }));
      process.stdout.write(`${JSON.stringify(results, null, 2)}\n`);
      return;
    }

    const result = computeKellyBreakdown({
      bankroll: args.bankroll,
      american_odds: args.american_odds,
      true_prob: args.true_prob,
      implied_prob_fair: args.implied_prob_fair,
      tier: args.tier,
      breaker_active: String(args.breaker_active || 'false').toLowerCase() === 'true',
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } catch (error) {
    console.error(`ERROR: ${error.message}`);
    process.exit(1);
  }
}

if (process.argv[1] && new URL(`file://${process.argv[1]}`).href === import.meta.url) {
  main();
}

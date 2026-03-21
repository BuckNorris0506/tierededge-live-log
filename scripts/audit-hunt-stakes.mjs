#!/usr/bin/env node
import fs from 'node:fs/promises';
import process from 'node:process';
import { computeKellyBreakdown } from './tierededge-kelly-cli.mjs';

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

function parseNumber(value, name) {
  const cleaned = String(value ?? '').replace(/[^0-9.+-]/g, '');
  const num = Number(cleaned);
  if (!Number.isFinite(num)) {
    throw new Error(`Invalid ${name}: ${value}`);
  }
  return num;
}

function deriveTier(edgePct) {
  if (edgePct >= 6) return 'T1';
  if (edgePct >= 4) return 'T2';
  if (edgePct >= 2) return 'T3';
  return 'NONE';
}

async function readRows(input) {
  if (!input) throw new Error('Missing --input JSON or @file path.');
  if (input.startsWith('@')) {
    return JSON.parse(await fs.readFile(input.slice(1), 'utf8'));
  }
  return JSON.parse(input);
}

async function main() {
  const args = parseArgs(process.argv);
  const rows = await readRows(args.input);
  const bankrollOverride = args.bankroll ? parseNumber(args.bankroll, 'bankroll') : null;
  const payload = [];

  for (const row of rows) {
    const bankroll = bankrollOverride ?? parseNumber(row.bankroll_snapshot ?? row.bankroll, 'bankroll');
    const trueProb = parseNumber(row.post_conf_true_prob ?? row.true_prob, 'true_prob');
    const impliedFair = parseNumber(row.devig_implied_prob ?? row.implied_prob_fair, 'implied_prob_fair');
    const edgePct = (trueProb - impliedFair) * 100;
    const derivedTier = deriveTier(edgePct);
    const reportedTier = String(row.bet_class || row.tier || '').toUpperCase();
    const breakdown = derivedTier === 'NONE'
      ? null
      : computeKellyBreakdown({
          bankroll,
          american_odds: row.odds_american,
          true_prob: trueProb,
          implied_prob_fair: impliedFair,
          tier: derivedTier,
        });

    payload.push({
      selection: row.selection,
      bankroll,
      reported_tier: reportedTier || null,
      derived_tier: derivedTier,
      odds_american: row.odds_american,
      decimal_odds: breakdown?.decimal_odds ?? null,
      true_prob: trueProb,
      implied_prob_fair: impliedFair,
      edge_pct: Number(edgePct.toFixed(2)),
      raw_kelly_fraction: breakdown ? Number(breakdown.raw_kelly_fraction.toFixed(6)) : null,
      tier_multiplier: breakdown?.fractional_kelly_multiplier ?? null,
      pre_round_stake: breakdown ? Number(breakdown.pre_breaker_stake.toFixed(6)) : null,
      rounded_final_stake: breakdown?.final_stake ?? 0,
      reported_kelly_stake: row.kelly_stake != null ? parseNumber(row.kelly_stake, 'kelly_stake') : null,
      tier_matches: derivedTier === reportedTier,
      stake_matches: row.kelly_stake == null || !breakdown
        ? null
        : Math.abs(parseNumber(row.kelly_stake, 'kelly_stake') - breakdown.final_stake) <= 0.001,
    });
  }

  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
}

main().catch((error) => {
  console.error(`audit-hunt-stakes failed: ${error.message}`);
  process.exit(1);
});

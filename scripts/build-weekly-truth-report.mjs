#!/usr/bin/env node
import { writeWeeklyTruthReport } from './behavioral-accountability-utils.mjs';

const { report } = writeWeeklyTruthReport();
console.log(JSON.stringify({
  generated_at_utc: report.generated_at_utc,
  settled_bet_count: report.settled_bet_count,
  override_totals: report.override_totals,
  top_bleeding_categories: report.top_bleeding_categories,
}, null, 2));

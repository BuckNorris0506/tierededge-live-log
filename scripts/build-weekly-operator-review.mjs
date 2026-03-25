#!/usr/bin/env node
import { writeWeeklyOperatorReview } from './behavioral-accountability-utils.mjs';

const { report } = writeWeeklyOperatorReview();
console.log(JSON.stringify({
  generated_at_utc: report.generated_at_utc,
  window: report.window,
  valid_runs: report.sections?.run_truth?.valid_runs ?? 0,
  invalidated_runs: report.sections?.run_truth?.invalidated_runs ?? 0,
  total_bets: report.sections?.hunt_totals?.total_bets ?? 0,
  total_sits: report.sections?.hunt_totals?.total_sits ?? 0,
}, null, 2));

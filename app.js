async function loadData() {
  if (window.__LIVE_DATA__) return window.__LIVE_DATA__;
  const res = await fetch(`/tierededge-live-log/data.json?t=${Date.now()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error('Failed to load data.json');
  const data = await res.json();
  return data;
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

const MISSING = 'Insufficient data';

function formatValue(value) {
  if (value === null || value === undefined || value === '') return MISSING;
  if (typeof value === 'number' && !Number.isFinite(value)) return 'Insufficient data';
  if (typeof value === 'string' && ['n/a', 'na', '-'].includes(value.trim().toLowerCase())) return 'Insufficient data';
  if (Array.isArray(value)) return value.length ? value.join(', ') : MISSING;
  if (typeof value === 'object') return MISSING;
  return String(value);
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const parsed = Number(String(value).replace(/[^0-9.-]/g, ''));
  return Number.isFinite(parsed) ? parsed : null;
}

function hideSection(id, hidden = true) {
  const node = document.getElementById(id);
  if (!node) return;
  node.hidden = hidden;
}

function parsePercent(value) {
  const n = Number(String(value || '').replace(/[^0-9.-]/g, ''));
  return Number.isFinite(n) ? n : null;
}

function formatLoggedAt(dateValue, timeValue) {
  const datePart = String(dateValue || '').trim();
  const timePart = String(timeValue || '').trim();
  if (!datePart && !timePart) return MISSING;

  if (!datePart) return timePart ? `${timePart} CT` : MISSING;

  const date = new Date(`${datePart}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return timePart ? `${timePart} CT` : datePart;

  let outputTime = timePart;
  const hhmm = timePart.match(/^(\d{1,2}):(\d{2})$/);
  if (hhmm) {
    const hours = Number(hhmm[1]);
    const minutes = hhmm[2];
    const ampm = hours >= 12 ? 'PM' : 'AM';
    const twelveHour = ((hours + 11) % 12) + 1;
    outputTime = `${twelveHour}:${minutes} ${ampm}`;
  }

  const month = date.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' });
  const day = date.toLocaleString('en-US', { day: 'numeric', timeZone: 'UTC' });
  return `${month} ${day}, ${outputTime}${outputTime ? ' CT' : ''}`;
}

function prettyReason(code) {
  const map = {
    no_edge: 'No edge',
    low_confidence: 'Low confidence',
    stale_or_unverified_odds: 'Odds not verified',
    exposure_cap_reached: 'Exposure cap reached',
    breaker_active: 'Circuit breaker active',
    bankroll_discontinuity: 'Bankroll continuity failed',
  };
  const normalized = String(code || '').trim().toLowerCase();
  if (map[normalized]) return map[normalized];
  return normalized.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()) || MISSING;
}

function renderMeta(data) {
  const meta = document.getElementById('meta');
  if (!meta) return;
  meta.innerHTML = '';
  meta.appendChild(el('div', '', `Updated (CT): ${data.last_updated_ct || 'unknown'}`));
  meta.appendChild(el('div', '', `Generated (UTC): ${data.generated_at_utc || 'unknown'}`));
  meta.appendChild(el('div', 'quiet', `Schema: ${data.schema || 'unknown'}`));
}

function renderRows(listId, rows) {
  const list = document.getElementById(listId);
  if (!list) return;
  list.innerHTML = '';
  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderTable(tableId, rows, columns, emptyText) {
  const table = document.getElementById(tableId);
  if (!table) return;
  table.innerHTML = '';

  if (!rows || rows.length === 0) {
    const tbody = el('tbody');
    const tr = el('tr');
    const td = el('td', '', emptyText || 'No data');
    td.colSpan = (columns || []).length || 1;
    tr.appendChild(td);
    tbody.appendChild(tr);
    table.appendChild(tbody);
    return;
  }

  const headers = columns || Object.keys(rows[0]);
  const thead = el('thead');
  const trh = el('tr');
  for (const header of headers) {
    const th = el('th', '', header);
    if (isNumericColumn(header)) th.classList.add('num-col');
    trh.appendChild(th);
  }
  thead.appendChild(trh);

  const tbody = el('tbody');
  for (const row of rows) {
    const tr = el('tr');
    for (const header of headers) {
      const td = el('td', '', formatValue(row[header]));
      if (isNumericColumn(header)) td.classList.add('num-col');
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }

  table.appendChild(thead);
  table.appendChild(tbody);
}

function isNumericColumn(header) {
  const key = String(header || '').toLowerCase();
  return [
    'odds',
    'stake',
    'profit/loss',
    'p/l',
    'clv',
    'roi',
    'edge',
    'prob',
    'delta',
    'rate',
    'hours',
    'count',
    'freshness',
  ].some((token) => key.includes(token));
}

function makeCard(title, value, small) {
  const card = el('article', `card${small ? ' card-small' : ''}`);
  card.appendChild(el('h3', '', title));
  card.appendChild(el('p', '', formatValue(value)));
  return card;
}

function renderSummaryCards(data) {
  const top = document.getElementById('summary-cards');
  const support = document.getElementById('support-cards');
  if (!top || !support) return;

  const status = data.current_status || {};
  const settled = data.analytics_summary?.settled_performance?.overall || {};
  const clv = data.analytics_summary?.clv_analytics || {};
  const executionQuality = data.analytics_summary?.execution_quality || {};
  const edgeValidation = data.edge_validation?.summary || {};
  const openRisk = data.open_risk_summary || {};
  const accountability = data.behavioral_accountability || {};

  top.innerHTML = '';
  top.appendChild(makeCard('Bankroll', status.Bankroll));
  top.appendChild(makeCard('Open Risk', openRisk.total_stake_at_risk || MISSING));
  top.appendChild(makeCard('Settled P/L', settled.realized_profit || MISSING));
  top.appendChild(makeCard('CLV Coverage', clv.coverage_pct_label || MISSING));

  support.innerHTML = '';
  support.appendChild(makeCard('Settled Bets', settled.settled_bet_count ?? MISSING, true));
  support.appendChild(makeCard('EV Coverage', edgeValidation.ev_coverage_pct_label || MISSING, true));
  support.appendChild(makeCard('Snapshot Coverage', executionQuality.snapshot_coverage_pct_label || MISSING, true));
  support.appendChild(makeCard('Overrides (Month)', accountability.overrides?.monthly_override_count ?? MISSING, true));
  support.appendChild(makeCard('Post-Mortem', accountability.post_mortem?.current_status || MISSING, true));
}

function buildBetRows(data) {
  const lastUpdatedDay = String(data.last_updated_ct || '').match(/(\d{4}-\d{2}-\d{2})/)?.[1] || MISSING;
  const todayRows = (data.todays_bets || []).map((row) => ({
    Date: lastUpdatedDay,
    'Logged At': formatLoggedAt(lastUpdatedDay, row['Timestamp (CT)']),
    'Bet Class': row.bet_class,
    Sport: row.Sport,
    Market: row.Market,
    Bet: row.Bet,
    'Odds (US)': row['Odds (US)'],
    'Odds (Dec)': row['Odds (Dec)'],
    Book: row.Book,
    Stake: row.Stake,
    Tier: row.Tier,
    Result: row.Result,
    'P/L': row['P/L'],
    CLV: row.CLV,
    'CLV Status': row['CLV Status'],
    'Closing Odds': row['Closing Odds'],
  }));

  if (todayRows.length > 0) {
    return {
      title: `Today's Bets (${todayRows.length})`,
      rows: todayRows,
      emptyText: 'No bets logged for today.',
    };
  }

  const recent = (data.bet_log || []).slice(0, 50).map((row) => ({
    Date: row.Date,
    'Logged At': formatLoggedAt(row.Date, row['Timestamp (CT)']),
    'Bet Class': row.bet_class,
    Sport: row.Sport,
    Market: row.Market,
    Bet: row.Bet,
    'Odds (US)': row['Odds (US)'],
    'Odds (Dec)': row['Odds (Dec)'],
    Book: row.Book,
    Stake: row.Stake,
    Tier: row.Tier,
    Result: row.Result,
    'P/L': row['P/L'],
    CLV: row.CLV,
    'CLV Status': row['CLV Status'],
    'Closing Odds': row['Closing Odds'],
  }));

  return {
    title: 'Bet Log (Recent 50)',
    rows: recent,
    emptyText: 'No graded bets logged yet.',
  };
}

function renderBets(data) {
  const config = buildBetRows(data);
  const title = document.getElementById('bets-title');
  if (title) title.textContent = config.title;
  renderTable(
    'bets-table',
    config.rows,
    ['Date', 'Logged At', 'Bet Class', 'Sport', 'Market', 'Bet', 'Odds (US)', 'Odds (Dec)', 'Book', 'Stake', 'Tier', 'Result', 'P/L', 'CLV', 'CLV Status', 'Closing Odds'],
    config.emptyText
  );
}

function renderPending(data) {
  const pending = (data.pending_bets || [])
    .filter((value) => value)
    .map((value, idx) => {
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if (!trimmed || trimmed.toLowerCase() === 'none') return null;
        return { '#': idx + 1, Bet: trimmed, Status: 'PENDING', Notes: MISSING };
      }
      return {
        '#': idx + 1,
        Bet: value.selection || value.event || MISSING,
        Status: value.status || 'PENDING',
        Book: value.sportsbook || MISSING,
        Stake: value.actual_stake !== null && value.actual_stake !== undefined ? `$${value.actual_stake}` : MISSING,
        Logged: value.bet_slip_timestamp || MISSING,
        Notes: value.manual_override_flag ? `manual override${value.notes ? ` (${value.notes})` : ''}` : (value.notes || MISSING),
      };
    })
    .filter(Boolean);

  renderTable('pending-table', pending, ['#', 'Bet', 'Status', 'Book', 'Stake', 'Logged', 'Notes'], 'No pending bets.');
}

function renderDailyRejectionSummary(data) {
  const summary = data.daily_rejection_summary || {};
  const rows = [
    ['Total markets checked', summary['Total Markets Checked']],
    ['Total rejected', summary['Total Rejected']],
    ['No edge', summary.no_edge],
    ['Low confidence', summary.low_confidence],
    ['Odds not verified', summary.stale_or_unverified_odds],
    ['Exposure cap reached', summary.exposure_cap_reached],
    ['Circuit breaker active', summary.breaker_active],
  ];
  renderRows('rejection-summary-list', rows);
}

function renderExecutionQuality(data) {
  const eq = data.analytics_summary?.execution_quality || {};
  const rows = [
    ['Status', eq.status || 'Insufficient data'],
    ['Execution rows', eq.execution_row_count ?? MISSING],
    ['Sample status', eq.sample_size_status || MISSING],
    ['Matched to recommendation', eq.matched_to_recommendation_rate_label || MISSING],
    ['Snapshot coverage', eq.snapshot_coverage_pct_label || MISSING],
    ['Same-book quote coverage', eq.same_book_quote_coverage_pct_label || MISSING],
    ['Average price drift', eq.average_price_drift_cents_label || MISSING],
    ['Average absolute price drift', eq.average_absolute_price_drift_cents_label || MISSING],
    ['CLV coverage', eq.clv_coverage_pct_label || MISSING],
    ['Sample note', eq.sample_note || MISSING],
  ];
  renderRows('execution-quality-list', rows);
}

function renderRecentTotals(data) {
  const title = document.querySelector('#recent-totals-section h2');
  if (title) title.textContent = 'Settled Performance';
  const perf = data.analytics_summary?.settled_performance || {};
  const overall = perf.overall || {};
  const core = perf.edge_bet || {};
  const fun = perf.fun_sgp || {};
  const rows = [
    ['Overall • Settled bets', overall.settled_bet_count ?? MISSING],
    ['Overall • Record', `${overall.win_count ?? 0}-${overall.loss_count ?? 0}`],
    ['Overall • Profit/Loss', overall.realized_profit || MISSING],
    ['Overall • Stake risked', overall.total_stake || MISSING],
    ['Overall • ROI', overall.roi_pct_label || MISSING],
    ['Overall • Win rate', overall.win_rate_pct_label || MISSING],
    ['Overall • Sample status', overall.sample_size_status || MISSING],
    ['Overall • Reliability note', overall.reliability_note || MISSING],
    ['EDGE_BET • Settled bets', core.settled_bet_count ?? MISSING],
    ['EDGE_BET • Profit/Loss', core.realized_profit || MISSING],
    ['EDGE_BET • ROI', core.roi_pct_label || MISSING],
    ['EDGE_BET • Win rate', core.win_rate_pct_label || MISSING],
    ['FUN_SGP • Settled bets', fun.settled_bet_count ?? MISSING],
    ['FUN_SGP • Profit/Loss', fun.realized_profit || MISSING],
    ['FUN_SGP • ROI', fun.roi_pct_label || MISSING],
    ['FUN_SGP • Win rate', fun.win_rate_pct_label || MISSING],
    ['Unit metrics', overall.unit_metrics_status === 'insufficient_data' ? 'Insufficient data' : MISSING],
    ['Unit metrics reason', overall.unit_metrics_reason || MISSING],
  ];
  renderRows('recent-totals-list', rows);
}

function renderQuantPerformance(data) {
  const expectationTitle = document.querySelector('#quant-performance-section h2');
  const clvTitle = document.querySelector('#edge-quality-section h2');
  if (expectationTitle) expectationTitle.textContent = 'Edge Validation Summary';
  if (clvTitle) clvTitle.textContent = 'CLV Coverage / Missing Anchors';

  const summary = data.edge_validation?.summary || {};
  const perf = data.edge_validation?.actual_vs_expected || {};
  const clv = data.edge_validation?.clv_coverage || data.analytics_summary?.clv_analytics || {};

  const perfRows = [
    ['Status', summary.status || 'Insufficient data'],
    ['Settled bet sample', summary.settled_bet_sample_size ?? MISSING],
    ['Settled sample status', summary.settled_sample_status || MISSING],
    ['Settled sample note', summary.settled_sample_note || MISSING],
    ['EV coverage', summary.ev_coverage_pct_label || MISSING],
    ['Average edge at bet', summary.average_edge_at_bet_pct_label || MISSING],
    ['Observed win rate', summary.observed_win_rate_pct_label || MISSING],
    ['Breakeven win rate', summary.breakeven_win_rate_pct_label || MISSING],
    ['95% win-rate interval', summary.win_rate_interval_95_label || MISSING],
    ['Variance context', summary.variance_context || MISSING],
    ['Reliability label', summary.reliability_label || MISSING],
    ['Actual vs expected', perf.status || 'Insufficient data'],
    ['Actual vs expected note', perf.note || perf.reason || MISSING],
    ['Expected profit', perf.expected_profit || MISSING],
    ['Realized profit', perf.realized_profit || MISSING],
    ['Divergence vs expected', perf.divergence_from_expected || MISSING],
  ];
  renderRows('quant-performance-list', perfRows);

  const edgeRows = [
    ['Coverage status', clv.coverage_status || clv.status || 'Insufficient data'],
    ['Settled bets eligible for CLV', clv.settled_bet_count ?? clv.eligible_settled_bet_count ?? MISSING],
    ['CLV anchored', clv.clv_anchored_count ?? clv.anchored_bet_count ?? MISSING],
    ['CLV missing', clv.clv_missing_count ?? clv.missing_clv_bet_count ?? MISSING],
    ['CLV coverage', clv.clv_coverage_pct_label || clv.coverage_pct_label || MISSING],
    ['Sample status', clv.sample_size_status || MISSING],
    ['Average CLV price delta', clv.average_clv_price_delta_label || MISSING],
    ['Positive CLV rate', clv.positive_clv_rate_label || MISSING],
    ['Coverage warning', clv.coverage_warning || clv.source_warning || MISSING],
  ];
  renderRows('edge-quality-list', edgeRows);
}

function renderBankrollContribution(data) {
  const policy = data.bankroll_contribution_policy || {};
  const openRisk = data.open_risk_summary || {};
  const bankroll = data.bankroll_summary || {};
  const asMoney = (n) => {
    const value = numberOrNull(n);
    return value === null ? MISSING : `${value >= 0 ? '+' : '-'}$${Math.abs(value).toFixed(2)}`;
  };

  if (policy.status === 'insufficient_data') {
    hideSection('bankroll-contribution-policy-section', true);
  } else {
    hideSection('bankroll-contribution-policy-section', false);
    const policyRows = [
      ['Status', policy.status || MISSING],
      ['Reason', policy.reason || MISSING],
    ];
    renderRows('bankroll-contribution-policy-list', policyRows);
  }

  const compositionRows = [
    ['Starting bankroll', bankroll.starting_bankroll || MISSING],
    ['Contributions', bankroll.contributions || MISSING],
    ['Realized profit', bankroll.realized_profit || MISSING],
    ['Derived bankroll', bankroll.actual_bankroll || MISSING],
    ['Last recorded bankroll', bankroll.last_recorded_bankroll || MISSING],
    ['Bankroll reconciliation difference', bankroll.bankroll_difference || MISSING],
    ['Open tickets (execution truth)', openRisk.pending_ticket_count ?? MISSING],
    ['Open stake at risk', openRisk.total_stake_at_risk || MISSING],
    ['Open exposure % of bankroll', openRisk.open_exposure_pct_of_bankroll || MISSING],
    ['EDGE_BET open exposure', (openRisk.by_bet_class || []).find((row) => row.bet_class === 'EDGE_BET')?.total_stake_at_risk || MISSING],
    ['FUN_SGP open exposure', (openRisk.by_bet_class || []).find((row) => row.bet_class === 'FUN_SGP')?.total_stake_at_risk || MISSING],
    ['Manual override exposure', openRisk.manual_override_stake_at_risk || MISSING],
    ['Interpretation', 'Ledger-derived bankroll only. Sportsbook balances are not used here.'],
  ];
  renderRows('bankroll-composition-list', compositionRows);
}

function inRange(dateText, anchorDate, range) {
  if (range === 'all_time') return true;
  const dateKey = String(dateText || '').match(/(\d{4}-\d{2}-\d{2})/);
  if (!dateKey) return false;
  const ts = Date.parse(`${dateKey[1]}T00:00:00Z`);
  if (!Number.isFinite(ts)) return false;
  if (range === 'today') return ts === anchorDate;
  return ts >= (anchorDate - (6 * 24 * 60 * 60 * 1000)) && ts <= anchorDate;
}

function renderAccountability(data, range) {
  const anchorKey = String(data.last_updated_ct || '').match(/(\d{4}-\d{2}-\d{2})/);
  const anchorDate = anchorKey ? Date.parse(`${anchorKey[1]}T00:00:00Z`) : Date.now();

  const tracker = data.passed_opportunity_tracker || { entries: [] };
  const entries = (tracker.entries || []).filter((row) => inRange(row.timestamp_ct, anchorDate, range));
  const graded = entries.filter((row) => row.outcome_if_bet && row.outcome_if_bet !== 'ungraded');
  const wins = graded.filter((row) => row.outcome_if_bet === 'win').length;
  const losses = graded.filter((row) => row.outcome_if_bet === 'loss').length;
  const pushes = graded.filter((row) => row.outcome_if_bet === 'push').length;
  const record = graded.length > 0 ? `${wins}-${losses}${pushes > 0 ? `-${pushes}` : ''}` : MISSING;

  const rangeStats = data.rejection_reason_ranges?.[range] || { total_rejections: null, top_rejection_reasons: [] };
  const clv = data.analytics_summary?.clv_analytics || {};
  const overrides = data.behavioral_accountability?.overrides || {};
  const learningScope = data.recommendation_learning_scope || {};

  const topReasons = (rangeStats.top_rejection_reasons || []).map((item) => {
    const match = String(item).match(/^([a-z_]+)\s*\((\d+)\)$/i);
    if (!match) return item;
    return `${prettyReason(match[1])} (${match[2]})`;
  });

  const rows = [
    ['Range', range === 'today' ? 'Today' : (range === 'last_7' ? 'Last 7' : 'All-time')],
    ['Pending bet count', data.pending_count ?? (Array.isArray(data.pending_bets) ? data.pending_bets.length : 0)],
    ['CLV coverage', clv.coverage_pct_label || MISSING],
    ['Positive CLV rate', clv.positive_clv_rate_label || MISSING],
    ['Recent results', data.lifetime_stats?.['Win Rate']],
    ['Passed opportunities', entries.length],
    ['Rejected opportunities', rangeStats.total_rejections],
    ['Excluded invalid rows', learningScope.excluded_invalid_row_count ?? MISSING],
    ['Passed record if bet', record],
    ['Overrides this month', overrides.monthly_override_count ?? MISSING],
    ['Top rejection reasons', topReasons.join(', ') || MISSING],
  ];

  renderRows('accountability-list', rows);
}

function renderRejectedOpportunities(data) {
  const rows = (data.rejected_opportunities || []).slice(0, 20).map((row) => ({
    Date: row.Date,
    Sport: row.Sport,
    Market: row.Market,
    Selection: row.Selection,
    'Edge %': row['Edge %'],
    Reason: prettyReason(row['Reason Code']),
  }));

  if (!rows.length) {
    hideSection('rejected-opportunities-section', true);
    return;
  }
  hideSection('rejected-opportunities-section', false);
  renderTable(
    'rejected-opportunities-table',
    rows,
    ['Date', 'Sport', 'Market', 'Selection', 'Edge %', 'Reason'],
    'No rejected opportunities logged.'
  );
}

function renderScanCoverage(data) {
  const budget = data.request_budget_model || {};
  const coverage = data.scan_coverage_policy || {};
  const cache = data.cache_reuse_policy || {};

  const budgetRows = [
    ['Configured daily budget', budget?.optimized_policy_estimate ? `${data.scan_coverage_artifacts?.configured_budget?.daily_requests ?? MISSING}` : MISSING],
    ['Configured weekly budget', data.scan_coverage_artifacts?.configured_budget?.weekly_requests ?? MISSING],
    ['Legacy broad morning scan', budget.legacy_broad_scan_estimate?.morning_edge_hunt_per_run ?? MISSING],
    ['Optimized morning typical', budget.jobs?.morning_edge_hunt?.optimized_typical_requests_per_run ?? MISSING],
    ['Optimized morning expanded', budget.jobs?.morning_edge_hunt?.optimized_expanded_requests_per_run ?? MISSING],
    ['Typical daily usage', budget.optimized_policy_estimate?.daily_typical ?? MISSING],
    ['Expanded daily usage', budget.optimized_policy_estimate?.daily_expanded ?? MISSING],
    ['Typical weekly usage', budget.optimized_policy_estimate?.weekly_typical ?? MISSING],
    ['Expanded weekly usage', budget.optimized_policy_estimate?.weekly_expanded ?? MISSING],
    ['Waste avoided vs legacy/day', budget.wasted_requests_today_if_unbounded ?? MISSING],
  ];
  renderRows('scan-budget-list', budgetRows);

  const coverageRows = [
    ['Tier A sports', (coverage.tier_a?.active_sports_this_month || []).join(', ') || MISSING],
    ['Tier A markets', (coverage.tier_a?.markets || []).join(', ') || MISSING],
    ['Tier A books', (coverage.tier_a?.books || []).join(', ') || MISSING],
    ['Tier B sports', (coverage.tier_b?.active_sports_this_month || []).join(', ') || MISSING],
    ['Tier C sports', (coverage.tier_c?.active_sports_this_month || []).join(', ') || MISSING],
    ['Expand to Tier B if sparse', coverage.expansion_rules?.expand_to_tier_b_if_sparse ? 'Yes' : 'No'],
    ['Tier C only with surplus', coverage.expansion_rules?.expand_to_tier_c_only_with_surplus ? 'Yes' : 'No'],
    ['Props default', coverage.expansion_rules?.props_enabled_default ? 'On' : 'Off'],
    ['Alt lines default', coverage.expansion_rules?.alt_lines_enabled_default ? 'On' : 'Off'],
    ['Tier A cache', cache.tier_a_cache_minutes ? `${cache.tier_a_cache_minutes} min` : MISSING],
    ['Tier B cache', cache.tier_b_cache_minutes ? `${cache.tier_b_cache_minutes} min` : MISSING],
    ['Tier C cache', cache.tier_c_cache_minutes ? `${cache.tier_c_cache_minutes} min` : MISSING],
    ['Scores cache', cache.scores_cache_minutes ? `${cache.scores_cache_minutes} min` : MISSING],
  ];
  renderRows('scan-priority-list', coverageRows);
}

function renderOperatorEdgeBoard(data) {
  const board = data.operator_edge_board || {};

  const actionable = (board.actionable_bets || []).map((row) => ({
    Time: row.timestamp_ct,
    Sport: row.sport,
    Market: row.market,
    Selection: row.selection,
    Book: row.book,
    Odds: row.odds_american,
    'Raw Edge %': row.raw_edge_pct,
    'Post-Conf %': row.post_conf_edge_pct,
    Confidence: row.confidence_score,
  }));
  renderTable(
    'actionable-board-table',
    actionable,
    ['Time', 'Sport', 'Market', 'Selection', 'Book', 'Odds', 'Raw Edge %', 'Post-Conf %', 'Confidence'],
    'No current BET decisions for the target scan window.'
  );

  const passBand = (board.pass_band || []).map((row) => ({
    Time: row.timestamp_ct,
    Sport: row.sport,
    Market: row.market,
    Selection: row.selection,
    Book: row.book,
    'Post-Conf %': row.post_conf_edge_pct,
    'Gap To T3': row.gap_to_t3_pct,
    Confidence: row.confidence_score,
    Reason: prettyReason(row.rejection_reason),
  }));
  renderTable(
    'pass-band-table',
    passBand,
    ['Time', 'Sport', 'Market', 'Selection', 'Book', 'Post-Conf %', 'Gap To T3', 'Confidence', 'Reason'],
    'No 0% to 2% pass-band opportunities logged for the target scan window.'
  );

  const suppressed = (board.suppressed_candidates || []).map((row) => ({
    Time: row.timestamp_ct,
    Sport: row.sport,
    Market: row.market,
    Selection: row.selection,
    Book: row.book,
    'Pre-Conf %': row.pre_conf_edge_pct,
    'Post-Conf %': row.post_conf_edge_pct,
    'Conf Penalty': row.confidence_penalty_pct,
    Stage: row.rejection_stage,
    Reason: prettyReason(row.rejection_reason),
  }));
  renderTable(
    'suppressed-board-table',
    suppressed,
    ['Time', 'Sport', 'Market', 'Selection', 'Book', 'Pre-Conf %', 'Post-Conf %', 'Conf Penalty', 'Stage', 'Reason'],
    'No suppressed threshold-clearing candidates logged for the target scan window.'
  );
}

function renderLiveExecution(data) {
  const execution = data.live_execution || {};
  const marketTruth = data.market_truth_summary || {};
  const snapshotSummary = marketTruth.placement_snapshot || {};
  const clvSummary = marketTruth.clv_anchor || {};
  const accountability = data.behavioral_accountability || {};
  const recentExecutionLog = (execution.recent_execution_log || []).flatMap((row) => Array.isArray(row) ? row : [row]);
  const summaryRows = [
    ['Candidates', execution.counts?.candidates ?? MISSING],
    ['Approved', execution.counts?.approved ?? MISSING],
    ['Rejected', execution.counts?.rejected ?? MISSING],
    ['Snapshot coverage', snapshotSummary.snapshot_coverage_pct_label || MISSING],
    ['Missing snapshots', snapshotSummary.snapshot_missing_count ?? MISSING],
    ['CLV coverage', clvSummary.clv_coverage_pct_label || MISSING],
    ['Missing CLV anchors', clvSummary.clv_missing_count ?? MISSING],
    ['Overrides this month', accountability.overrides?.monthly_override_count ?? MISSING],
    ['Post-mortem', accountability.post_mortem?.current_status || MISSING],
    ['Run classification', data.decision_payload_v1?.run_classification || MISSING],
  ];
  renderRows('live-execution-summary-list', summaryRows);

  const recs = (execution.recommendations || []).map((row) => ({
    Time: row.timestamp_ct,
    Sport: row.sport,
    Market: row.market_type,
    Selection: row.selection,
    Recommended: `${formatValue(row.recommended_odds_american)} ${formatValue(row.recommended_book)}`,
    Current: row.execution?.current_odds_american === null || row.execution?.current_odds_american === undefined
      ? MISSING
      : `${row.execution.current_odds_american} ${formatValue(row.execution.current_book)}`,
    Stake: row.execution?.stake_breakdown?.formatted?.final_stake || '$0.00',
    Drift: row.execution?.line_or_price_drift_label || MISSING,
    Status: row.execution?.execution_status || MISSING,
    Reason: prettyReason(row.execution?.rejection_reason),
  }));
  renderTable(
    'live-execution-table',
    recs,
    ['Time', 'Sport', 'Market', 'Selection', 'Recommended', 'Current', 'Stake', 'Drift', 'Status', 'Reason'],
    'No live execution candidates available.'
  );

  const logRows = recentExecutionLog.map((row) => ({
    'Bet Slip': row.bet_slip_timestamp || row.logged_at_utc,
    rec_id: row.rec_id,
    Match: row.match_status || MISSING,
    Sport: row.sport || MISSING,
    Event: row.event || row.event_label || MISSING,
    Market: row.market || row.market_type || MISSING,
    Recommended: `${formatValue(row.recommended_odds)} ${formatValue(row.recommended_sportsbook)}`,
    Actual: `${formatValue(row.actual_odds)} ${formatValue(row.actual_sportsbook)}`,
    Stake: row.actual_stake ?? row.recommended_stake ?? MISSING,
    Snapshot: row.placement_snapshot_status || MISSING,
    'Snapshot Source': row.placement_snapshot_source || MISSING,
    Approval: row.execution_approval_result || MISSING,
    Override: row.manual_override_flag ? 'Yes' : 'No',
  }));
  renderTable(
    'execution-log-table',
    logRows,
    ['Bet Slip', 'rec_id', 'Match', 'Sport', 'Event', 'Market', 'Recommended', 'Actual', 'Stake', 'Snapshot', 'Snapshot Source', 'Approval', 'Override'],
    'No execution log rows yet.'
  );
}

function renderDiagnostics(data) {
  const health = data.decision_payload_v1?.system_health || data.integrity_gate?.checks || {};
  const freshness = data.data_freshness || {};
  const integrity = data.integrity_gate || {};
  const accountability = data.behavioral_accountability || {};
  const weeklyTruth = accountability.weekly_truth_report_summary || {};
  const recentOverride = accountability.overrides?.recent_overrides?.[0] || null;
  const topBleeding = weeklyTruth.top_bleeding_categories?.[0] || null;
  const huntAudit = data.hunt_audit_summary || {};
  const latestInvalidRun = huntAudit.latest_invalid_run || null;
  const learningScope = data.recommendation_learning_scope || {};

  const diagRows = [
    ['Run classification', data.decision_payload_v1?.run_classification || MISSING],
    ['Data freshness', health.data_freshness],
    ['API integrity', health.api_integrity],
    ['Ledger integrity', health.ledger_integrity],
    ['Bankroll continuity', health.bankroll_continuity],
    ['State sync', health.state_sync],
    ['Payload rebuild', health.payload_rebuild],
    ['Decision engine status', health.decision_engine_status],
    ['Post-mortem status', accountability.post_mortem?.current_status || MISSING],
    ['Invalid hunt runs', huntAudit.invalid_run_count ?? MISSING],
    ['Latest invalid hunt', latestInvalidRun ? `${latestInvalidRun.run_id} (${latestInvalidRun.invalid_status})` : MISSING],
    ['Latest invalid reasons', latestInvalidRun ? (latestInvalidRun.reasons || []).join(', ') || MISSING : MISSING],
    ['Learning rows excluded', learningScope.excluded_invalid_row_count ?? MISSING],
    ['Learning run IDs excluded', (learningScope.excluded_run_ids || []).join(', ') || MISSING],
    ['Overrides this month', accountability.overrides?.monthly_override_count ?? MISSING],
    ['Blocked-run overrides', accountability.overrides?.blocked_run_override_count ?? MISSING],
    ['Off-model overrides', accountability.overrides?.off_model_override_count ?? MISSING],
    ['Weekly truth settled bets', weeklyTruth.settled_bet_count ?? MISSING],
    ['Recent override', recentOverride ? `${recentOverride.override_type} @ ${recentOverride.timestamp_utc}` : MISSING],
    ['Top bleeding slice', topBleeding ? `${topBleeding.label} (${topBleeding.realized_pl})` : MISSING],
    ['Recommendation log last row', freshness.recommendation_log_last_row_time],
    ['Grading cache last update', freshness.grading_cache_last_update],
    ['Payload build time (UTC)', freshness.payload_build_time_utc],
    ['Integrity warnings', (integrity.reasons || []).join(', ') || MISSING],
  ];
  renderRows('diagnostics-list', diagRows);

  const diagTableRows = [
    {
      'Missing scan days': (integrity.diagnostics?.missing_scan_days || []).join(', ') || MISSING,
      'Duplicate rec IDs': (integrity.diagnostics?.duplicate_rec_ids || []).join(', ') || MISSING,
      'Freshness hours': integrity.diagnostics?.freshness_hours_since_anchor ?? MISSING,
      'Latest data fail codes': (integrity.diagnostics?.latest_hunt_data_failure_codes || []).join(', ') || MISSING,
      'Current bankroll': integrity.diagnostics?.bankroll_continuity?.current_bankroll ?? MISSING,
      'Expected bankroll': integrity.diagnostics?.bankroll_continuity?.expected_bankroll ?? MISSING,
      'Bankroll delta': integrity.diagnostics?.bankroll_continuity?.delta ?? MISSING,
      'Raw fail codes': (integrity.reasons || []).join(', ') || MISSING,
      'Recent invalid run IDs': (huntAudit.recent_invalid_runs || []).map((row) => row.run_id).join(', ') || MISSING,
    },
  ];

  renderTable(
    'diagnostics-table',
    diagTableRows,
    ['Missing scan days', 'Duplicate rec IDs', 'Freshness hours', 'Latest data fail codes', 'Current bankroll', 'Expected bankroll', 'Bankroll delta', 'Raw fail codes', 'Recent invalid run IDs'],
    'No diagnostics available.'
  );
}

(async () => {
  try {
    const data = await loadData();
    renderMeta(data);
    renderSummaryCards(data);
    renderBets(data);
    renderPending(data);
    renderDailyRejectionSummary(data);
    renderExecutionQuality(data);
    renderRecentTotals(data);
    renderQuantPerformance(data);
    renderBankrollContribution(data);
    renderRejectedOpportunities(data);
    renderScanCoverage(data);
    renderOperatorEdgeBoard(data);
    renderLiveExecution(data);
    renderDiagnostics(data);

    let activeRange = 'all_time';
    const buttons = document.querySelectorAll('.range-btn');

    const rerenderAccountability = () => {
      renderAccountability(data, activeRange);
      buttons.forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.range === activeRange);
      });
    };

    buttons.forEach((btn) => {
      btn.addEventListener('click', () => {
        activeRange = btn.dataset.range || 'all_time';
        rerenderAccountability();
      });
    });

    rerenderAccountability();
  } catch (err) {
    document.body.innerHTML = `<p style="padding:20px;font-family:sans-serif;">Failed to load live log: ${err.message}</p>`;
  }
})();

async function loadData() {
  if (window.__LIVE_DATA__) return window.__LIVE_DATA__;
  const res = await fetch('./data.json', { cache: 'no-store' });
  if (!res.ok) throw new Error('Failed to load data.json');
  return res.json();
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
}

const MISSING = '\u2014';

function formatValue(value) {
  if (value === null || value === undefined || value === '') return MISSING;
  if (Array.isArray(value)) return value.length ? value.join(', ') : MISSING;
  if (typeof value === 'object') return MISSING;
  return String(value);
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
  const life = data.lifetime_stats || {};
  const decisionQuality = data.decision_quality || {};

  const processScore = status['Process Score (7d)'] || (decisionQuality.decision_quality_rate !== null && decisionQuality.decision_quality_rate !== undefined
    ? `${decisionQuality.decision_quality_rate}%`
    : null);

  top.innerHTML = '';
  top.appendChild(makeCard('Bankroll', status.Bankroll));
  top.appendChild(makeCard('Overall ROI', life['Overall ROI']));
  top.appendChild(makeCard('Average CLV', life['Average CLV']));
  top.appendChild(makeCard('Process Score', processScore));

  const clvRate = decisionQuality.positive_clv_rate;
  const clvQuality = clvRate === null || clvRate === undefined
    ? MISSING
    : (clvRate >= 55 ? 'Strong' : (clvRate >= 45 ? 'Neutral' : 'Watch'));
  const roiValue = parsePercent(life['Overall ROI']);
  const roiQuality = roiValue === null ? MISSING : (roiValue > 0 ? 'Positive' : (roiValue === 0 ? 'Flat' : 'Negative'));

  support.innerHTML = '';
  support.appendChild(makeCard('CLV Quality', clvQuality, true));
  support.appendChild(makeCard('ROI Quality', roiQuality, true));
}

function buildBetRows(data) {
  const lastUpdatedDay = String(data.last_updated_ct || '').match(/(\d{4}-\d{2}-\d{2})/)?.[1] || MISSING;
  const todayRows = (data.todays_bets || []).map((row) => ({
    Date: lastUpdatedDay,
    'Logged At': formatLoggedAt(lastUpdatedDay, row['Timestamp (CT)']),
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
    ['Date', 'Logged At', 'Sport', 'Market', 'Bet', 'Odds (US)', 'Odds (Dec)', 'Book', 'Stake', 'Tier', 'Result', 'P/L', 'CLV'],
    config.emptyText
  );
}

function renderPending(data) {
  const pending = (data.pending_bets || [])
    .map((value) => String(value || '').trim())
    .filter((value) => value && value.toLowerCase() !== 'none')
    .map((value, idx) => ({ '#': idx + 1, Bet: value }));

  renderTable('pending-table', pending, ['#', 'Bet'], 'No pending bets.');
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
  const eq = data.execution_quality || {};
  const rows = [
    ['Average slippage (last 25)', eq['Avg Slippage (last 25 bets)']],
    ['Implied probability delta', eq['Avg Slippage (implied prob delta)']],
    ['Execution warning', eq['Execution Warning']],
  ];
  renderRows('execution-quality-list', rows);
}

function renderRecentTotals(data) {
  const totals = data.weekly_running_totals || {};
  const review = data.weekly_performance_review || {};
  const clv = review.clv_metrics || {};
  const dq = review.decision_quality || {};
  const policy = review.bankroll_contribution_policy || {};
  const asMoney = (n) => (n === null || n === undefined ? MISSING : `${n >= 0 ? '+' : '-'}$${Math.abs(n).toFixed(2)}`);
  const asPct = (n) => (n === null || n === undefined ? MISSING : `${n >= 0 ? '+' : ''}${n}%`);
  const rows = [
    ['CLV Metrics • Average CLV', clv.average_clv !== null && clv.average_clv !== undefined ? `${clv.average_clv}%` : MISSING],
    ['CLV Metrics • Positive CLV rate', asPct(clv.positive_clv_rate)],
    ['CLV Metrics • CLV win rate', asPct(clv.clv_win_rate)],
    ['CLV Metrics • CLV interpretation', clv.clv_win_rate_interpretation || MISSING],
    ['CLV Metrics • Total bets evaluated', clv.total_bets_evaluated ?? MISSING],
    ['CLV Metrics • Execution slippage', clv.execution_slippage || MISSING],
    ['Decision Quality • Profit from bets', asMoney(dq.profit_from_bets)],
    ['Decision Quality • Profit if all sits bet', asMoney(dq.profit_if_all_sits_bet)],
    ['Decision Quality • Decision edge', asMoney(dq.decision_edge)],
    ['Decision Quality • Sit discipline rate', dq.sit_discipline_rate || MISSING],
    ['Contribution Policy • Realized monthly profit', asMoney(policy.realized_monthly_profit)],
    ['Contribution Policy • Actual bankroll', asMoney(policy.actual_bankroll)],
    ['Contribution Policy • Strategy equity', asMoney(policy.strategy_equity)],
    ['Contribution Policy • Realized betting profit (lifetime)', asMoney(policy.realized_betting_profit_lifetime)],
    ['Contribution Policy • Basis months', (policy.contribution_basis_months_used || []).join(', ') || MISSING],
    ['Contribution Policy • Next estimated contribution', asMoney(policy.next_estimated_contribution)],
    ['Contribution Policy • Total contributions to date', asMoney(policy.total_contributions_to_date)],
    ['Contribution Policy • Summary', policy.contribution_adjusted_summary || MISSING],
    ['Bets', totals.Bets],
    ['ROI', totals.ROI],
  ];
  renderRows('recent-totals-list', rows);
}

function renderQuantPerformance(data) {
  const q = data.quant_performance || {};
  const dq = data.decision_quality || {};
  const asMoney = (n) => (n === null || n === undefined ? MISSING : `${n >= 0 ? '+' : '-'}$${Math.abs(n).toFixed(2)}`);
  const asPct = (n) => (n === null || n === undefined ? MISSING : `${n >= 0 ? '+' : ''}${n}%`);
  const asRatio = (n) => (n === null || n === undefined ? MISSING : n.toFixed(2));
  const asRet = (n) => (n === null || n === undefined ? MISSING : `${(n * 100).toFixed(1)}%`);
  const asUnits = (n) => (n === null || n === undefined ? MISSING : `${n >= 0 ? '+' : '-'}${Math.abs(n).toFixed(2)}u`);
  const asUnitsUnsigned = (n) => (n === null || n === undefined ? MISSING : `${n.toFixed(2)}u`);
  const asPValue = (n) => (n === null || n === undefined ? MISSING : n.toFixed(4));

  const perfRows = [
    ['Bets settled', q.settled_bets_evaluated],
    ['Total units', asUnits(q.total_units)],
    ['Total staked units', asUnitsUnsigned(q.total_staked_units)],
    ['Average units per bet', asUnitsUnsigned(q.average_units_per_bet)],
    ['ROI (units)', asPct(q.roi_units)],
    ['Expected profit', asMoney(q.expected_profit)],
    ['Expected profit (units)', asUnits(q.expected_profit_units)],
    ['Actual profit', asMoney(q.actual_profit)],
    ['Actual profit (units)', asUnits(q.actual_profit_units)],
    ['Variance', asMoney(q.variance)],
    ['Variance (units)', asUnits(q.variance_units)],
    ['EV realization', asRatio(q.ev_realization_ratio)],
  ];
  renderRows('quant-performance-list', perfRows);

  const edgeRows = [
    ['Average CLV', asPct(dq.avg_clv)],
    ['Positive CLV rate', asPct(dq.positive_clv_rate)],
    ['Observed win rate', asPct(q.observed_win_rate)],
    ['Breakeven win rate', asPct(q.breakeven_win_rate)],
    ['Binomial p-value', asPValue(q.p_value)],
    ['Confidence level', asPct(q.confidence_level)],
    ['Sample status', q.sample_status || MISSING],
    ['Avg edge detected', asPct(q.edge_at_detection)],
    ['Avg edge at placement', asPct(q.edge_at_placement)],
    ['Avg edge at close', asPct(q.edge_at_close)],
    ['Edge retention', asRet(q.edge_retention)],
    ['Closing edge retention', asRet(q.closing_edge_retention)],
    ['Market efficiency impact', asRet(q.market_efficiency_impact)],
  ];
  renderRows('edge-quality-list', edgeRows);
}

function renderBankrollContribution(data) {
  const policy = data.bankroll_contribution_policy || {};
  const automation = data.bankroll_contribution_automation || {};
  const asMoney = (n) => (n === null || n === undefined ? MISSING : `${n >= 0 ? '+' : '-'}$${Math.abs(n).toFixed(2)}`);
  const basisMonths = (policy.contribution_basis_months_used || []).join(', ') || MISSING;
  const profitValues = (policy.realized_profit_values_used || []).map((n) => `${n >= 0 ? '+' : '-'}$${Math.abs(Number(n)).toFixed(2)}`).join(', ') || MISSING;

  const policyRows = [
    ['Last contribution', asMoney(policy.last_contribution_amount)],
    ['Last contribution date', policy.last_contribution_date || MISSING],
    ['Contribution basis months', basisMonths],
    ['Realized profit values used', profitValues],
    ['Rolling average realized profit', asMoney(policy.rolling_average_realized_profit)],
    ['Next estimated contribution', asMoney(policy.next_estimated_contribution)],
    ['Automation status', automation.status || MISSING],
    ['Automation last run', automation.last_run_ct || MISSING],
    ['Automation effective month', automation.effective_month || MISSING],
    ['Automation reason', automation.reason || MISSING],
    ['Next expected contribution cycle', automation.next_expected_cycle || MISSING],
  ];
  renderRows('bankroll-contribution-policy-list', policyRows);

  const compositionRows = [
    ['Actual bankroll', asMoney(policy.actual_bankroll)],
    ['Reported bankroll (status source)', asMoney(policy.reported_current_bankroll)],
    ['Bankroll reconciliation difference', asMoney(policy.bankroll_formula_difference)],
    ['Strategy equity', asMoney(policy.strategy_equity)],
    ['Total external contributions', asMoney(policy.total_external_contributions)],
    ['Realized betting profit', asMoney(policy.realized_betting_profit_lifetime)],
    ['Starting bankroll', asMoney(policy.starting_bankroll)],
    ['Bankroll growth from betting', asMoney(policy.bankroll_growth_from_betting)],
    ['Bankroll growth from contributions', asMoney(policy.bankroll_growth_from_contributions)],
    ['Realized monthly profit', asMoney(policy.realized_monthly_profit_ex_contributions)],
    ['Actual bankroll includes external contributions', 'Yes'],
    ['Strategy equity excludes external contributions', 'Yes'],
    ['Units remain primary strategy metric', 'Yes'],
    ['Interpretation', policy.monthly_interpretation || MISSING],
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
  const dq = data.decision_quality || {};

  const topReasons = (rangeStats.top_rejection_reasons || []).map((item) => {
    const match = String(item).match(/^([a-z_]+)\s*\((\d+)\)$/i);
    if (!match) return item;
    return `${prettyReason(match[1])} (${match[2]})`;
  });

  const rows = [
    ['Range', range === 'today' ? 'Today' : (range === 'last_7' ? 'Last 7' : 'All-time')],
    ['Pending bet count', (data.pending_bets || []).filter((p) => String(p).toLowerCase() !== 'none').length],
    ['Positive CLV rate', dq.positive_clv_rate !== null && dq.positive_clv_rate !== undefined ? `${dq.positive_clv_rate}%` : MISSING],
    ['Average CLV', dq.avg_clv],
    ['Recent results', data.lifetime_stats?.['Win Rate']],
    ['Passed opportunities', entries.length],
    ['Rejected opportunities', rangeStats.total_rejections],
    ['Passed record if bet', record],
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

  renderTable(
    'rejected-opportunities-table',
    rows,
    ['Date', 'Sport', 'Market', 'Selection', 'Edge %', 'Reason'],
    'No rejected opportunities logged.'
  );
}

function renderDiagnostics(data) {
  const health = data.decision_payload_v1?.system_health || data.integrity_gate?.checks || {};
  const freshness = data.data_freshness || {};
  const integrity = data.integrity_gate || {};

  const diagRows = [
    ['Data freshness', health.data_freshness],
    ['Ledger integrity', health.ledger_integrity],
    ['Bankroll continuity', health.bankroll_continuity],
    ['Decision engine status', health.decision_engine_status],
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
      'Freshness hours': integrity.diagnostics?.freshness_hours_since_last_recommendation ?? MISSING,
      'Current bankroll': integrity.diagnostics?.bankroll_continuity?.current_bankroll ?? MISSING,
      'Expected bankroll': integrity.diagnostics?.bankroll_continuity?.expected_bankroll ?? MISSING,
      'Bankroll delta': integrity.diagnostics?.bankroll_continuity?.delta ?? MISSING,
      'Raw fail codes': (integrity.reasons || []).join(', ') || MISSING,
    },
  ];

  renderTable(
    'diagnostics-table',
    diagTableRows,
    ['Missing scan days', 'Duplicate rec IDs', 'Freshness hours', 'Current bankroll', 'Expected bankroll', 'Bankroll delta', 'Raw fail codes'],
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

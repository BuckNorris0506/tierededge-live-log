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

const MISSING = '—';

function formatValue(value) {
  if (value === null || value === undefined || value === '') return MISSING;
  if (Array.isArray(value)) return value.length ? value.join(', ') : MISSING;
  if (typeof value === 'object') return MISSING;
  return String(value);
}

function renderMeta(data) {
  const meta = document.getElementById('meta');
  if (!meta) return;
  meta.innerHTML = '';
  meta.appendChild(el('div', '', `Updated (CT): ${data.last_updated_ct || 'unknown'}`));
  meta.appendChild(el('div', '', `Generated (UTC): ${data.generated_at_utc || 'unknown'}`));
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
    td.colSpan = 1;
    tr.appendChild(td);
    tbody.appendChild(tr);
    table.appendChild(tbody);
    return;
  }

  const headers = columns || Object.keys(rows[0]);
  const thead = el('thead');
  const trh = el('tr');
  for (const header of headers) trh.appendChild(el('th', '', header));
  thead.appendChild(trh);

  const tbody = el('tbody');
  for (const row of rows) {
    const tr = el('tr');
    for (const header of headers) {
      tr.appendChild(el('td', '', formatValue(row[header])));
    }
    tbody.appendChild(tr);
  }

  table.appendChild(thead);
  table.appendChild(tbody);
}

function renderTodayDecision(data) {
  const decision = data?.decision_console?.today || {};
  const gate = data?.integrity_gate || { pass: false, reasons: ['integrity_unknown'] };

  const rows = [];
  if (gate.pass !== true) {
    rows.push(['Status', 'BLOCKED']);
    rows.push(['Reason', 'Integrity gate failed']);
    rows.push(['Checks', (gate.reasons || []).join(', ')]);
    rows.push(['Next action', decision.next_action || 'Resolve integrity warnings']);
  } else {
    rows.push(['Status', decision.verdict || 'NO DECISION']);
    if (decision.no_bets_reason) rows.push(['Reason', decision.no_bets_reason]);
    rows.push(['Next action', decision.next_action || 'Review decisions']);
  }

  renderRows('today-decision-list', rows);

  const bets = (decision.bets || []).map((row) => ({
    Selection: row.selection,
    Market: row.market,
    'Edge %': row.edge_percent !== null && row.edge_percent !== undefined ? `${row.edge_percent}%` : MISSING,
    Tier: row.tier || MISSING,
    Stake: row.stake || MISSING,
    Reason: row.reason || MISSING,
  }));

  const sits = (decision.sits || []).map((row) => ({
    Scope: row.label || MISSING,
    Reason: row.reason || MISSING,
  }));

  renderTable('today-bets-table', bets, ['Selection', 'Market', 'Edge %', 'Tier', 'Stake', 'Reason'], 'No bets today.');
  renderTable('today-sits-table', sits, ['Scope', 'Reason'], 'No sit entries for current range.');
}

function renderSystemHealth(data) {
  const health = data?.decision_console?.system_health || {};
  const freshness = data?.data_freshness || {};
  const rows = [
    ['Data Freshness', health.data_freshness || 'fail'],
    ['Ledger Integrity', health.ledger_integrity || 'fail'],
    ['Decision Engine Status', health.decision_engine_status || 'blocked'],
    ['Recommendation log last row', freshness.recommendation_log_last_row_time || 'unknown'],
    ['Grading cache last update', freshness.grading_cache_last_update || 'unknown'],
    ['Payload build time (UTC)', freshness.payload_build_time_utc || 'unknown'],
  ];
  renderRows('system-health-list', rows);
}

function renderExecution(data) {
  const exec = data?.decision_console?.execution || {};
  const rows = [
    ['Bankroll', exec.bankroll || data?.current_status?.Bankroll || MISSING],
    ['Open Exposure', exec.open_exposure || data?.open_exposure || MISSING],
    ['Daily Exposure Used', exec.daily_exposure_used || data?.current_status?.['Daily Exposure Used'] || MISSING],
    ['Circuit Breaker', exec.circuit_breaker || data?.current_status?.['Circuit Breaker'] || MISSING],
  ];
  renderRows('execution-console-list', rows);
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
  const accountability = data?.decision_console?.accountability || {};
  const anchorKey = String(data?.last_updated_ct || '').match(/(\d{4}-\d{2}-\d{2})/);
  const anchorDate = anchorKey ? Date.parse(`${anchorKey[1]}T00:00:00Z`) : Date.now();

  const tracker = data?.passed_opportunity_tracker || { entries: [] };
  const entries = (tracker.entries || []).filter((row) => inRange(row.timestamp_ct, anchorDate, range));
  const graded = entries.filter((row) => row.outcome_if_bet && row.outcome_if_bet !== 'ungraded');
  const wins = graded.filter((row) => row.outcome_if_bet === 'win').length;
  const losses = graded.filter((row) => row.outcome_if_bet === 'loss').length;
  const pushes = graded.filter((row) => row.outcome_if_bet === 'push').length;
  const record = graded.length > 0 ? `${wins}-${losses}${pushes > 0 ? `-${pushes}` : ''}` : MISSING;

  const rejectionRange = data?.rejection_reason_ranges?.[range] || { total_rejections: null, top_rejection_reasons: [] };

  const rows = [
    ['Range', range === 'last_7' ? 'Last 7' : (range === 'all_time' ? 'All-time' : 'Today')],
    ['Pending Bets', accountability.pending_bets_count],
    ['Positive CLV Rate', accountability.positive_clv_rate !== null && accountability.positive_clv_rate !== undefined ? `${accountability.positive_clv_rate}%` : MISSING],
    ['Average CLV', accountability.avg_clv],
    ['Recent Results', accountability.recent_results],
    ['Passed Opportunities', entries.length],
    ['Passed Record If Bet', record],
    ['Top Rejection Reasons', (rejectionRange.top_rejection_reasons || []).join(', ') || MISSING],
    ['Rejected Opportunities', rejectionRange.total_rejections],
  ];

  renderRows('accountability-list', rows);
}

(async () => {
  try {
    const data = await loadData();
    renderMeta(data);

    let activeRange = 'all_time';
    const buttons = document.querySelectorAll('.range-btn');
    const rerender = () => {
      renderTodayDecision(data);
      renderSystemHealth(data);
      renderExecution(data);
      renderAccountability(data, activeRange);
      buttons.forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.range === activeRange);
      });
    };

    buttons.forEach((btn) => {
      btn.addEventListener('click', () => {
        activeRange = btn.dataset.range || 'all_time';
        rerender();
      });
    });

    rerender();
  } catch (err) {
    document.body.innerHTML = `<p style="padding:20px;font-family:sans-serif;">Failed to load decision console: ${err.message}</p>`;
  }
})();

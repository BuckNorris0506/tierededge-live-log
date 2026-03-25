async function loadData() {
  if (window.__LIVE_DATA__) return window.__LIVE_DATA__;
  const res = await fetch(`/tierededge-live-log/data.json?t=${Date.now()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error('Failed to load data.json');
  return res.json();
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
  if (typeof value === 'number' && !Number.isFinite(value)) return MISSING;
  if (Array.isArray(value)) return value.length ? value.join(', ') : MISSING;
  return String(value);
}

function titleCase(input) {
  return String(input || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function renderMeta(data) {
  const meta = document.getElementById('meta');
  if (!meta) return;
  meta.innerHTML = '';
  meta.appendChild(el('div', '', `Updated (CT): ${data.last_updated_ct || 'unknown'}`));
  meta.appendChild(el('div', '', `Generated (UTC): ${data.generated_at_utc || 'unknown'}`));
  meta.appendChild(el('div', 'quiet', `Schema: ${data.operator_dashboard?.schema || data.schema || 'unknown'}`));
}

function renderMetricList(metrics) {
  const list = el('dl', 'metric-list');
  for (const metric of metrics || []) {
    const row = el('div', 'metric-row');
    row.appendChild(el('dt', '', titleCase(metric.label)));
    row.appendChild(el('dd', '', formatValue(metric.value)));
    list.appendChild(row);
  }
  return list;
}

function renderCard(card) {
  const article = el('article', 'card');
  article.appendChild(el('h3', '', card.label || 'Card'));
  article.appendChild(renderMetricList(card.metrics || []));
  return article;
}

function renderSection(section) {
  const wrapper = el('section', 'dashboard-section');
  wrapper.appendChild(el('h2', '', section.title || 'Section'));
  const grid = el('div', `card-grid${section.key === 'watchlist_action_flags' ? ' card-grid-single' : ''}`);
  for (const card of section.cards || []) grid.appendChild(renderCard(card));
  wrapper.appendChild(grid);
  return wrapper;
}

function renderFlags(flags) {
  const panel = el('section', 'panel flags-panel');
  panel.appendChild(el('h3', '', 'Action Flags'));
  const groups = [
    { key: 'RED', label: 'RED' },
    { key: 'YELLOW', label: 'YELLOW' },
    { key: 'INFO', label: 'INFO' },
  ];

  for (const group of groups) {
    const rows = (flags || []).filter((flag) => flag.level === group.key);
    const block = el('div', `flag-group flag-group-${group.key.toLowerCase()}`);
    block.appendChild(el('h4', '', group.label));
    if (!rows.length) {
      block.appendChild(el('p', 'flag-empty', 'None'));
    } else {
      const list = el('ul', 'flag-list');
      for (const flag of rows) {
        const item = el('li', 'flag-item');
        item.appendChild(el('strong', '', flag.title || group.label));
        item.appendChild(el('span', '', ` ${formatValue(flag.message)}`));
        list.appendChild(item);
      }
      block.appendChild(list);
    }
    panel.appendChild(block);
  }

  return panel;
}

function renderTable(rows, columns) {
  const wrap = el('div', 'table-wrap');
  const table = el('table');
  if (!rows || !rows.length) {
    const tbody = el('tbody');
    const tr = el('tr');
    const td = el('td', '', 'Insufficient data');
    td.colSpan = columns.length || 1;
    tr.appendChild(td);
    tbody.appendChild(tr);
    table.appendChild(tbody);
    wrap.appendChild(table);
    return wrap;
  }

  const thead = el('thead');
  const trh = el('tr');
  for (const col of columns) trh.appendChild(el('th', '', col.label));
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = el('tbody');
  for (const row of rows) {
    const tr = el('tr');
    for (const col of columns) {
      tr.appendChild(el('td', '', formatValue(row[col.key])));
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  wrap.appendChild(table);
  return wrap;
}

function renderDrilldown(title, rows, columns) {
  const details = el('details', 'panel drilldown');
  const summary = el('summary', '', title);
  details.appendChild(summary);
  details.appendChild(renderTable(rows, columns));
  return details;
}

function renderDashboard(data) {
  const root = document.getElementById('dashboard-root');
  const drilldownsRoot = document.getElementById('drilldowns-root');
  if (!root || !drilldownsRoot) return;
  root.innerHTML = '';
  drilldownsRoot.innerHTML = '';

  const dashboard = data.operator_dashboard;
  if (!dashboard) {
    const panel = el('section', 'panel');
    panel.appendChild(el('h2', '', 'Operator Dashboard'));
    panel.appendChild(el('p', '', 'Insufficient data. operator_dashboard is not present in canonical/public state.'));
    root.appendChild(panel);
    return;
  }

  for (const section of dashboard.top_level_sections || []) {
    root.appendChild(renderSection(section));
    if (section.key === 'watchlist_action_flags') {
      root.appendChild(renderFlags(dashboard.action_flags || []));
    }
  }

  const drilldownsHeader = el('section', 'dashboard-section');
  drilldownsHeader.appendChild(el('h2', '', 'DRILL-DOWN VIEWS'));
  const note = el('p', 'section-note', 'Use these only when the top-level cards suggest something needs inspection.');
  drilldownsHeader.appendChild(note);
  drilldownsRoot.appendChild(drilldownsHeader);

  const drilldowns = dashboard.drilldowns || {};
  drilldownsRoot.appendChild(renderDrilldown(
    'Rejected Opportunity Detail',
    drilldowns.rejected_opportunity_detail || [],
    [
      { key: 'timestamp_ct', label: 'Timestamp (CT)' },
      { key: 'sport', label: 'Sport' },
      { key: 'market_type', label: 'Market' },
      { key: 'selection', label: 'Selection' },
      { key: 'sportsbook', label: 'Book' },
      { key: 'executable_book', label: 'Executable' },
      { key: 'edge_pct', label: 'Edge' },
      { key: 'rejection_reason', label: 'Rejection Reason' },
      { key: 'close_capture_status', label: 'Close Capture' },
    ]
  ));
  drilldownsRoot.appendChild(renderDrilldown(
    'Market-Type Performance',
    drilldowns.market_type_performance || [],
    [
      { key: 'sport', label: 'Sport' },
      { key: 'market_family', label: 'Market Family' },
      { key: 'market_type', label: 'Market Type' },
      { key: 'bets', label: 'Bets' },
      { key: 'sits', label: 'Sits' },
      { key: 'avg_edge', label: 'Avg Edge' },
      { key: 'rejection_reasons', label: 'Top Rejection Reasons' },
    ]
  ));
  drilldownsRoot.appendChild(renderDrilldown(
    'Snapshot Integrity Failures',
    drilldowns.snapshot_integrity_failures || [],
    [
      { key: 'timestamp_ct', label: 'Timestamp (CT)' },
      { key: 'sport', label: 'Sport' },
      { key: 'market_type', label: 'Market' },
      { key: 'selection', label: 'Selection' },
      { key: 'sportsbook', label: 'Book' },
      { key: 'snapshot_status', label: 'Snapshot Status' },
      { key: 'rejection_reason', label: 'Rejection Reason' },
      { key: 'spread_seconds', label: 'Spread Seconds' },
    ]
  ));
}

(async function init() {
  try {
    const data = await loadData();
    renderMeta(data);
    renderDashboard(data);
  } catch (error) {
    const root = document.getElementById('dashboard-root');
    if (root) {
      const panel = el('section', 'panel');
      panel.appendChild(el('h2', '', 'Dashboard Load Failure'));
      panel.appendChild(el('p', '', error.message || 'Unknown error'));
      root.innerHTML = '';
      root.appendChild(panel);
    }
    console.error(error);
  }
})();

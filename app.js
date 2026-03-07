async function loadData() {
  if (window.__LIVE_DATA__) {
    return window.__LIVE_DATA__;
  }
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

function parseNumber(value) {
  if (value === null || value === undefined) return null;
  const clean = String(value).replace(/[^0-9.-]/g, '');
  if (!clean || clean === '-' || clean === '.' || clean === '-.') return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

function qualityLabel(value, weakMax, decentMax) {
  if (value === null) return 'n/a';
  if (value < weakMax) return 'weak';
  if (value <= decentMax) return 'decent';
  return 'strong';
}

function renderCards(data) {
  const root = document.getElementById('cards');
  root.innerHTML = '';

  const avgClv = parseNumber(data.lifetime_stats['Average CLV']);
  const roi = parseNumber(data.lifetime_stats['Overall ROI']);
  const totalBets = parseNumber(data.lifetime_stats['Total Bets']);

  const clvQuality = qualityLabel(avgClv, 0.3, 1.0);
  const roiQuality = qualityLabel(roi, 1.0, 4.0);
  const sampleLabel = totalBets !== null && totalBets >= 200 ? 'stable' : 'provisional';

  const cards = [
    ['Bankroll', data.current_status['Bankroll'] || '-'],
    ['Overall ROI', data.lifetime_stats['Overall ROI'] || '-'],
    ['Average CLV', data.lifetime_stats['Average CLV'] || '-'],
    ['Process Score', data.current_status['Process Score (7d)'] || '-'],
    ['CLV Quality', `${clvQuality} (${sampleLabel})`],
    ['ROI Quality', `${roiQuality} (${sampleLabel})`],
  ];

  for (const [label, value] of cards) {
    const card = el('article', 'card');
    card.appendChild(el('h3', '', label));
    card.appendChild(el('p', '', value));
    root.appendChild(card);
  }
}

function renderMeta(data) {
  const meta = document.getElementById('meta');
  meta.innerHTML = '';
  meta.appendChild(el('div', '', `Schema: ${data.schema}`));
  meta.appendChild(el('div', '', `Updated (CT): ${data.last_updated_ct}`));
  meta.appendChild(el('div', '', `Generated (UTC): ${data.generated_at_utc}`));
}

function renderTable(id, rows, limit = null) {
  const table = document.getElementById(id);
  table.innerHTML = '';

  if (!rows || rows.length === 0) {
    const tr = el('tr');
    const td = el('td', '', 'No data');
    td.colSpan = 1;
    tr.appendChild(td);
    table.appendChild(tr);
    return;
  }

  const headers = Object.keys(rows[0]);

  const thead = el('thead');
  const trh = el('tr');
  for (const h of headers) trh.appendChild(el('th', '', h));
  thead.appendChild(trh);

  const tbody = el('tbody');
  const dataRows = limit ? rows.slice(0, limit) : rows;

  for (const row of dataRows) {
    const tr = el('tr');
    for (const h of headers) {
      tr.appendChild(el('td', '', row[h]));
    }
    tbody.appendChild(tr);
  }

  table.appendChild(thead);
  table.appendChild(tbody);
}

function renderList(id, mapOrList) {
  const list = document.getElementById(id);
  list.innerHTML = '';

  if (Array.isArray(mapOrList)) {
    if (mapOrList.length === 0) {
      list.appendChild(el('li', '', 'None'));
      return;
    }
    for (const item of mapOrList) {
      list.appendChild(el('li', '', item));
    }
    return;
  }

  const entries = Object.entries(mapOrList || {});
  if (entries.length === 0) {
    list.appendChild(el('li', '', 'No data'));
    return;
  }

  for (const [k, v] of entries) {
    list.appendChild(el('li', '', `${k}: ${v}`));
  }
}

function renderRejectionSummary(id, summary) {
  const list = document.getElementById(id);
  list.innerHTML = '';

  const labelMap = {
    'Total Markets Checked': 'Total markets checked',
    'Total Rejected': 'Total rejected',
    no_edge: 'No edge',
    low_confidence: 'Low confidence',
    stale_or_unverified_odds: 'Odds not verified',
    exposure_cap_reached: 'Exposure cap reached',
    breaker_active: 'Circuit breaker active',
  };

  const preferredOrder = [
    'Total Markets Checked',
    'Total Rejected',
    'no_edge',
    'low_confidence',
    'stale_or_unverified_odds',
    'exposure_cap_reached',
    'breaker_active',
  ];

  for (const key of preferredOrder) {
    if (!(key in (summary || {}))) continue;
    const label = labelMap[key] || key;
    list.appendChild(el('li', '', `${label}: ${summary[key]}`));
  }
}

(async () => {
  try {
    const data = await loadData();
    renderMeta(data);
    renderCards(data);
    renderTable('today-table', data.todays_bets);
    renderTable('log-table', data.bet_log, 50);
    renderList('pending-list', data.pending_bets);
    renderRejectionSummary('reject-list', data.daily_rejection_summary);
    renderList('execution-list', data.execution_quality);
    renderList('weekly-list', data.weekly_running_totals);
  } catch (err) {
    document.body.innerHTML = `<p style="padding:20px;font-family:sans-serif;">Failed to load live log: ${err.message}</p>`;
  }
})();

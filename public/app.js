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
    ['Decision Quality Rate', data.decision_quality?.decision_quality_rate !== null && data.decision_quality?.decision_quality_rate !== undefined ? `${data.decision_quality.decision_quality_rate}%` : '-'],
    ['Money Saved (Sits)', data.sit_accountability_summary?.money_saved_by_sitting !== null && data.sit_accountability_summary?.money_saved_by_sitting !== undefined ? `$${data.sit_accountability_summary.money_saved_by_sitting}` : '-'],
    ['Passed Bets W-L (If Bet)', data.sit_accountability_summary?.passed_bets_record_if_bet || '-'],
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

function renderDecisionQuality(id, dq) {
  const list = document.getElementById(id);
  list.innerHTML = '';
  const rows = [
    ['Decision Quality Rate', dq?.decision_quality_rate !== null && dq?.decision_quality_rate !== undefined ? `${dq.decision_quality_rate}%` : 'N/A'],
    ['Bet Quality Rate', dq?.bet_quality_rate !== null && dq?.bet_quality_rate !== undefined ? `${dq.bet_quality_rate}%` : 'N/A'],
    ['Sit Quality Rate', dq?.sit_quality_rate !== null && dq?.sit_quality_rate !== undefined ? `${dq.sit_quality_rate}%` : 'N/A'],
    ['Positive CLV Rate', dq?.positive_clv_rate !== null && dq?.positive_clv_rate !== undefined ? `${dq.positive_clv_rate}%` : 'N/A'],
    ['Average CLV', dq?.avg_clv !== null && dq?.avg_clv !== undefined ? `${dq.avg_clv}` : 'N/A'],
    ['Average Edge (Placed)', dq?.avg_edge_placed !== null && dq?.avg_edge_placed !== undefined ? `${dq.avg_edge_placed}%` : 'N/A'],
    ['Total Decisions Evaluated', dq?.total_decisions ?? 'N/A'],
    ['High-Quality Bet Decisions', dq?.high_quality_bet_decisions ?? 'N/A'],
    ['High-Quality Sit Decisions', dq?.high_quality_sit_decisions ?? 'N/A'],
    ['Placed Bets Count', dq?.placed_bets_count ?? 'N/A'],
    ['Rejected Plays Count', dq?.rejected_plays_count ?? 'N/A'],
    ['Rejected EV Total', dq?.rejected_ev_total !== null && dq?.rejected_ev_total !== undefined ? `${dq.rejected_ev_total}` : 'N/A'],
    ['Avoided Negative EV', dq?.avoided_negative_ev !== null && dq?.avoided_negative_ev !== undefined ? `${dq.avoided_negative_ev}` : 'N/A'],
    ['Rejected By Reason', dq?.rejected_by_reason ? JSON.stringify(dq.rejected_by_reason) : 'N/A'],
  ];
  for (const [k, v] of rows) {
    list.appendChild(el('li', '', `${k}: ${v}`));
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
    const value = (v && typeof v === 'object') ? JSON.stringify(v) : v;
    list.appendChild(el('li', '', `${k}: ${value}`));
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
    renderDecisionQuality('decision-quality-list', data.decision_quality);
    renderTable('today-table', data.todays_bets);
    renderTable('log-table', data.bet_log, 50);
    renderTable('rejected-table', data.rejected_opportunities, 50);
    renderList('pending-list', data.pending_bets);
    renderRejectionSummary('reject-list', data.daily_rejection_summary);
    renderList('sit-accountability-list', data.sit_accountability);
    renderList('sit-accountability-summary-list', data.sit_accountability_summary);
    renderList('scanner-stats-list', data.scanner_statistics);
    renderList('market-confidence-list', data.market_confidence);
    renderList('canonical-decision-engine-list', data.canonical_decision_engine);
    renderList('drawdown-governor-list', data.drawdown_governor);
    renderList('edge-distribution-list', data.edge_distribution);
    renderList('edge-distribution-transparency-list', data.edge_distribution_transparency);
    renderList('market-type-reliability-list', data.market_type_reliability_index);
    renderList('reliability-index-list', data.reliability_index);
    renderList('daily-summary-list', data.daily_summary);
    renderList('expectation-framing-list', data.expectation_framing);
    renderList('rule-ledger-pointer-list', data.rule_ledger_pointer);
    renderList('execution-list', data.execution_quality);
    renderList('weekly-list', data.weekly_running_totals);
  } catch (err) {
    document.body.innerHTML = `<p style="padding:20px;font-family:sans-serif;">Failed to load live log: ${err.message}</p>`;
  }
})();

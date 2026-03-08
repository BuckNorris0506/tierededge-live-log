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

const MISSING = '—';

function isMissing(value) {
  return value === null || value === undefined || value === '';
}

function formatValue(value) {
  if (isMissing(value)) return MISSING;
  if (typeof value === 'object') return MISSING;
  return String(value);
}

function titleCaseFromKey(key) {
  return String(key || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
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
    ['Decision Quality Rate', dq?.decision_quality_rate !== null && dq?.decision_quality_rate !== undefined ? `${dq.decision_quality_rate}%` : MISSING],
    ['Bet Quality Rate', dq?.bet_quality_rate !== null && dq?.bet_quality_rate !== undefined ? `${dq.bet_quality_rate}%` : MISSING],
    ['Sit Quality Rate', dq?.sit_quality_rate !== null && dq?.sit_quality_rate !== undefined ? `${dq.sit_quality_rate}%` : MISSING],
    ['Positive CLV Rate', dq?.positive_clv_rate !== null && dq?.positive_clv_rate !== undefined ? `${dq.positive_clv_rate}%` : MISSING],
    ['Average CLV', dq?.avg_clv !== null && dq?.avg_clv !== undefined ? `${dq.avg_clv}` : MISSING],
    ['Average Edge (Placed)', dq?.avg_edge_placed !== null && dq?.avg_edge_placed !== undefined ? `${dq.avg_edge_placed}%` : MISSING],
    ['Total Decisions Evaluated', formatValue(dq?.total_decisions)],
    ['High-Quality Bet Decisions', formatValue(dq?.high_quality_bet_decisions)],
    ['High-Quality Sit Decisions', formatValue(dq?.high_quality_sit_decisions)],
    ['Placed Bets Count', formatValue(dq?.placed_bets_count)],
    ['Rejected Plays Count', formatValue(dq?.rejected_plays_count)],
    ['Rejected EV Total', dq?.rejected_ev_total !== null && dq?.rejected_ev_total !== undefined ? `${dq.rejected_ev_total}` : MISSING],
    ['Avoided Negative EV', dq?.avoided_negative_ev !== null && dq?.avoided_negative_ev !== undefined ? `${dq.avoided_negative_ev}` : MISSING],
  ];
  for (const [k, v] of rows) {
    list.appendChild(el('li', '', `${k}: ${v}`));
  }
  const reasons = dq?.rejected_by_reason || {};
  const reasonEntries = Object.entries(reasons);
  if (reasonEntries.length > 0) {
    list.appendChild(el('li', '', 'Rejection Reasons:'));
    for (const [reason, count] of reasonEntries) {
      list.appendChild(el('li', '', `${titleCaseFromKey(reason)}: ${formatValue(count)}`));
    }
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
      tr.appendChild(el('td', '', formatValue(row[h])));
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
      list.appendChild(el('li', '', formatValue(item)));
    }
    return;
  }

  const entries = Object.entries(mapOrList || {});
  if (entries.length === 0) {
    list.appendChild(el('li', '', 'No data'));
    return;
  }

  for (const [k, v] of entries) {
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      const subEntries = Object.entries(v);
      if (subEntries.length === 0) {
        list.appendChild(el('li', '', `${titleCaseFromKey(k)}: ${MISSING}`));
        continue;
      }
      list.appendChild(el('li', '', `${titleCaseFromKey(k)}:`));
      for (const [sk, sv] of subEntries) {
        list.appendChild(el('li', '', `${titleCaseFromKey(sk)}: ${formatValue(sv)}`));
      }
      continue;
    }
    list.appendChild(el('li', '', `${titleCaseFromKey(k)}: ${formatValue(v)}`));
  }
}

function renderTodaysScan(id, data) {
  const list = document.getElementById(id);
  list.innerHTML = '';
  const summary = data.daily_decision_summary || data.daily_summary || {};
  const scanner = data.scanner_statistics || {};
  const dq = data.decision_quality || {};
  const rows = [
    ['Markets Scanned', summary.games_scanned ?? scanner['Games Scanned']],
    ['Edges Detected', summary.edges_detected ?? scanner['Edges Detected']],
    ['Bets Placed', summary.bets_placed ?? dq.placed_bets_count],
    ['Sit Decisions', summary.sits ?? dq.rejected_plays_count],
    ['Strongest Edge Found', summary.strongest_edge_found ?? scanner['Largest Edge Detected']],
    ['Largest Edge Rejected', summary.largest_edge_rejected ?? summary.strongest_edge_rejected ?? scanner['Largest Edge Rejected']],
    ['Final Daily Verdict', summary.final_daily_verdict],
  ];

  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderSitAccountability(id, sit, summary) {
  const list = document.getElementById(id);
  list.innerHTML = '';
  const rows = [
    ['Avoided Losses (count)', sit?.['Avoided Losses (count)']],
    ['Missed Winners (count)', sit?.['Missed Winners (count)']],
    ['Net P/L if all sits were followed', sit?.['Net P/L If Followed All Sits']],
    ['Net EV rejected', sit?.['Net EV Rejected']],
    ['Passed Bets W-L if bet', sit?.['Passed Bets W-L If Bet'] ?? summary?.passed_bets_record_if_bet],
    ['Sit Decision Win Rate if bet', sit?.['Sit Decision Win Rate If Bet'] ?? (summary?.passed_bets_win_rate_if_bet !== null && summary?.passed_bets_win_rate_if_bet !== undefined ? `${summary.passed_bets_win_rate_if_bet}%` : null)],
    ['Money Saved By Sitting', sit?.['Money Saved By Sitting'] ?? (summary?.money_saved_by_sitting !== null && summary?.money_saved_by_sitting !== undefined ? `$${summary.money_saved_by_sitting}` : null)],
  ];
  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderEdgeDistributionTransparency(id, payload) {
  const list = document.getElementById(id);
  list.innerHTML = '';
  const buckets = payload?.buckets || {};
  const bucketRows = [
    ['0-1% edge', buckets.edge_0_1 ?? 0],
    ['1-2% edge', buckets.edge_1_2 ?? 0],
    ['2-3% edge', buckets.edge_2_3 ?? 0],
    ['3-4% edge', buckets.edge_3_4 ?? 0],
    ['4-5% edge', buckets.edge_4_5 ?? 0],
    ['5%+ edge', buckets.edge_5_plus ?? 0],
  ];
  for (const [label, value] of bucketRows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }

  const bySport = Object.entries(payload?.by_sport || {});
  if (bySport.length > 0) {
    list.appendChild(el('li', '', 'Edges By Sport:'));
    for (const [sport, count] of bySport) {
      list.appendChild(el('li', '', `${sport}: ${formatValue(count)}`));
    }
  }

  const byMarket = Object.entries(payload?.by_market_type || {});
  if (byMarket.length > 0) {
    list.appendChild(el('li', '', 'Edges By Market Type:'));
    for (const [market, count] of byMarket) {
      list.appendChild(el('li', '', `${market}: ${formatValue(count)}`));
    }
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

function renderPassedOpportunityTracker(id, tracker) {
  const list = document.getElementById(id);
  list.innerHTML = '';
  if (!tracker) {
    list.appendChild(el('li', '', 'No tracker data'));
    return;
  }

  list.appendChild(el('li', '', `Total passed opportunities: ${tracker.total_passed_opportunities ?? 0}`));
  list.appendChild(el('li', '', `Graded: ${tracker.graded_count ?? 0} | Ungraded: ${tracker.ungraded_count ?? 0}`));
  if (tracker.record_if_bet) {
    list.appendChild(el('li', '', `Record if bet: ${tracker.record_if_bet}`));
  }

  const entries = tracker.entries || [];
  if (entries.length === 0) {
    list.appendChild(el('li', '', 'No passed-opportunity rows available yet.'));
    return;
  }

  for (const row of entries.slice(0, 12)) {
    list.appendChild(el('li', '', row.narrative || `${row.selection || 'Selection'}: ${row.outcome_if_bet || 'ungraded'}`));
  }
}

(async () => {
  try {
    const data = await loadData();
    renderMeta(data);
    renderCards(data);
    renderTodaysScan('todays-scan-list', data);
    renderDecisionQuality('decision-quality-list', data.decision_quality);
    renderTable('today-table', data.todays_bets);
    renderTable('log-table', data.bet_log, 50);
    renderTable('rejected-table', data.rejected_opportunities, 50);
    renderList('pending-list', data.pending_bets);
    renderRejectionSummary('reject-list', data.daily_rejection_summary);
    renderSitAccountability('sit-accountability-list', data.sit_accountability, data.sit_accountability_summary);
    renderList('sit-accountability-summary-list', data.sit_accountability_summary);
    renderPassedOpportunityTracker('passed-opportunity-tracker-list', data.passed_opportunity_tracker);
    renderList('scanner-stats-list', data.scanner_statistics);
    renderList('market-confidence-list', data.market_confidence);
    renderList('canonical-decision-engine-list', data.canonical_decision_engine);
    renderList('drawdown-governor-list', data.drawdown_governor);
    renderList('edge-distribution-list', data.edge_distribution);
    renderEdgeDistributionTransparency('edge-distribution-transparency-list', data.edge_distribution_transparency);
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

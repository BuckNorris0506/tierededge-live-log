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
  if (Array.isArray(value)) {
    if (value.length === 0) return MISSING;
    return value.map((item) => formatValue(item)).join(', ');
  }
  if (typeof value === 'object') return MISSING;
  return String(value);
}

function isMeaningfulValue(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === 'string') {
    const s = value.trim().toLowerCase();
    return s !== '' && s !== '-' && s !== '—' && s !== 'n/a' && s !== 'none' && s !== 'no data';
  }
  if (typeof value === 'number') return Number.isFinite(value);
  if (Array.isArray(value)) return value.some((item) => isMeaningfulValue(item));
  if (typeof value === 'object') return Object.values(value).some((item) => isMeaningfulValue(item));
  return true;
}

function toggleSection(sectionId, visible) {
  const node = document.getElementById(sectionId);
  if (!node) return;
  node.classList.toggle('hidden', !visible);
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

function parseDateKey(value) {
  const m = String(value || '').match(/(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function resolveAnchorDateMs(data) {
  const dateKey = parseDateKey(data?.last_updated_ct);
  if (!dateKey) return null;
  const ms = Date.parse(`${dateKey}T00:00:00Z`);
  return Number.isFinite(ms) ? ms : null;
}

function inRange(dateMs, anchorMs, range) {
  if (!Number.isFinite(dateMs) || !Number.isFinite(anchorMs)) return range === 'all_time';
  const oneDayMs = 24 * 60 * 60 * 1000;
  const dayFloor = (ts) => {
    const d = new Date(ts);
    d.setUTCHours(0, 0, 0, 0);
    return d.getTime();
  };
  const targetDay = dayFloor(dateMs);
  const anchorDay = dayFloor(anchorMs);
  if (range === 'today') return targetDay === anchorDay;
  if (range === 'last_7') return targetDay >= (anchorDay - (6 * oneDayMs)) && targetDay <= anchorDay;
  return true;
}

function resolveCanonicalSitAccountability(data) {
  const summary = data?.sit_accountability_summary || {};
  const manual = data?.sit_accountability || {};
  const hasComputed = isMeaningfulValue(summary?.passed_bets_graded);

  if (hasComputed) {
    return {
      source: 'computed',
      avoided_losses: null,
      missed_winners: null,
      net_pl_if_followed_all_sits: summary.net_counterfactual_pl_if_bet,
      net_ev_rejected: summary.net_ev_rejected,
      passed_bets_record_if_bet: summary.passed_bets_record_if_bet,
      passed_bets_win_rate_if_bet: summary.passed_bets_win_rate_if_bet,
      money_saved_by_sitting: summary.money_saved_by_sitting,
      passed_bets_graded: summary.passed_bets_graded,
      passed_bets_wins_if_bet: summary.passed_bets_wins_if_bet,
      passed_bets_losses_if_bet: summary.passed_bets_losses_if_bet,
      passed_bets_pushes_if_bet: summary.passed_bets_pushes_if_bet,
      missed_profit_by_sitting: summary.missed_profit_by_sitting,
    };
  }

  return {
    source: 'manual',
    avoided_losses: manual['Avoided Losses (count)'],
    missed_winners: manual['Missed Winners (count)'],
    net_pl_if_followed_all_sits: manual['Net P/L If Followed All Sits'],
    net_ev_rejected: manual['Net EV Rejected'],
    passed_bets_record_if_bet: manual['Passed Bets W-L If Bet'],
    passed_bets_win_rate_if_bet: parseNumber(manual['Sit Decision Win Rate If Bet']),
    money_saved_by_sitting: manual['Money Saved By Sitting'],
    passed_bets_graded: null,
    passed_bets_wins_if_bet: null,
    passed_bets_losses_if_bet: null,
    passed_bets_pushes_if_bet: null,
    missed_profit_by_sitting: null,
  };
}

function qualityLabel(value, weakMax, decentMax) {
  if (value === null) return 'n/a';
  if (value < weakMax) return 'weak';
  if (value <= decentMax) return 'decent';
  return 'strong';
}

function renderCards(data) {
  const root = document.getElementById('cards');
  if (!root) return;
  root.innerHTML = '';

  const avgClv = parseNumber(data.lifetime_stats['Average CLV']);
  const roi = parseNumber(data.lifetime_stats['Overall ROI']);
  const totalBets = parseNumber(data.lifetime_stats['Total Bets']);

  const clvQuality = qualityLabel(avgClv, 0.3, 1.0);
  const roiQuality = qualityLabel(roi, 1.0, 4.0);
  const sampleLabel = totalBets !== null && totalBets >= 200 ? 'stable' : 'provisional';
  const sit = resolveCanonicalSitAccountability(data);

  const cards = [
    ['Bankroll', data.current_status['Bankroll'] || '-'],
    ['Open Exposure', data.normalized?.open_exposure || data.open_exposure || data.current_status['Daily Exposure Used'] || '-'],
    ['Decision Quality Rate', data.decision_quality?.decision_quality_rate !== null && data.decision_quality?.decision_quality_rate !== undefined ? `${data.decision_quality.decision_quality_rate}%` : '-'],
    ['Money Saved (Sits)', sit?.money_saved_by_sitting !== null && sit?.money_saved_by_sitting !== undefined ? `$${sit.money_saved_by_sitting}` : '-'],
    ['Passed Bets W-L (If Bet)', sit?.passed_bets_record_if_bet || '-'],
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
  if (!list) return;
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
  if (!meta) return;
  meta.innerHTML = '';
  meta.appendChild(el('div', '', `Schema: ${data.schema}`));
  meta.appendChild(el('div', '', `Updated (CT): ${data.last_updated_ct}`));
  meta.appendChild(el('div', '', `Generated (UTC): ${data.generated_at_utc}`));
}

function renderTable(id, rows, limit = null) {
  const table = document.getElementById(id);
  if (!table) return;
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
  if (!list) return;
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
    if (Array.isArray(v)) {
      if (v.length === 0) {
        list.appendChild(el('li', '', `${titleCaseFromKey(k)}: ${MISSING}`));
        continue;
      }
      list.appendChild(el('li', '', `${titleCaseFromKey(k)}:`));
      for (const item of v) {
        list.appendChild(el('li', '', formatValue(item)));
      }
      continue;
    }
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
  if (!list) return;
  list.innerHTML = '';
  const summary = data.daily_decision_summary || data.daily_summary || {};
  const scanner = data.scanner_statistics || {};
  const dq = data.decision_quality || {};
  const rows = [
    ['Snapshot Date (CT)', data.last_updated_ct],
    ['Markets Scanned', summary.games_scanned ?? scanner['Games Scanned']],
    ['Edges Detected', summary.edges_detected ?? scanner['Edges Detected']],
    ['Bets Placed', summary.bets_placed ?? dq.placed_bets_count],
    ['Sit Decisions', summary.sits ?? dq.rejected_plays_count],
    ['Strongest Edge Found', summary.strongest_edge_found ?? scanner['Largest Edge Detected']],
    ['Largest Edge Rejected', summary.largest_edge_rejected ?? summary.strongest_edge_rejected ?? scanner['Largest Edge Rejected']],
    ['Top Rejection Reasons', summary.top_rejection_reasons],
    ['Final Daily Verdict', summary.final_daily_verdict ?? data.daily_verdict ?? data.normalized?.daily_verdict],
  ];

  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderSitAccountability(id, canonicalSit) {
  const list = document.getElementById(id);
  if (!list) return;
  list.innerHTML = '';
  const rows = [
    ['Source', canonicalSit?.source || 'unknown'],
    ['Avoided Losses (count)', canonicalSit?.avoided_losses],
    ['Missed Winners (count)', canonicalSit?.missed_winners],
    ['Net P/L if all sits were followed', canonicalSit?.net_pl_if_followed_all_sits],
    ['Net EV rejected', canonicalSit?.net_ev_rejected],
    ['Passed Bets W-L if bet', canonicalSit?.passed_bets_record_if_bet],
    ['Sit Decision Win Rate if bet', canonicalSit?.passed_bets_win_rate_if_bet !== null && canonicalSit?.passed_bets_win_rate_if_bet !== undefined ? `${canonicalSit.passed_bets_win_rate_if_bet}%` : null],
    ['Money Saved By Sitting', canonicalSit?.money_saved_by_sitting !== null && canonicalSit?.money_saved_by_sitting !== undefined ? `$${canonicalSit.money_saved_by_sitting}` : null],
  ];
  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderSitAccountabilitySummary(id, canonicalSit) {
  const list = document.getElementById(id);
  if (!list) return;
  list.innerHTML = '';

  const rows = [
    ['Passed opportunities graded', canonicalSit?.passed_bets_graded],
    ['Record if bet', canonicalSit?.passed_bets_record_if_bet],
    ['Wins if bet', canonicalSit?.passed_bets_wins_if_bet],
    ['Losses if bet', canonicalSit?.passed_bets_losses_if_bet],
    ['Pushes if bet', canonicalSit?.passed_bets_pushes_if_bet],
    ['Win rate if bet', canonicalSit?.passed_bets_win_rate_if_bet !== null && canonicalSit?.passed_bets_win_rate_if_bet !== undefined ? `${canonicalSit.passed_bets_win_rate_if_bet}%` : null],
    ['Net counterfactual P/L if bet', canonicalSit?.net_pl_if_followed_all_sits],
    ['Money saved by sitting', canonicalSit?.money_saved_by_sitting],
    ['Missed profit by sitting', canonicalSit?.missed_profit_by_sitting],
    ['Net EV rejected', canonicalSit?.net_ev_rejected],
  ];

  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderEdgeDistributionTransparency(id, payload) {
  const list = document.getElementById(id);
  if (!list) return;
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

function renderDataFreshness(id, freshness) {
  const list = document.getElementById(id);
  if (!list) return;
  list.innerHTML = '';
  const rows = [
    ['Recommendation log last row', freshness?.recommendation_log_last_row_time || 'unknown'],
    ['Grading cache last update', freshness?.grading_cache_last_update || 'unknown'],
    ['Payload build time (UTC)', freshness?.payload_build_time_utc || 'unknown'],
  ];
  for (const [label, value] of rows) {
    list.appendChild(el('li', '', `${label}: ${formatValue(value)}`));
  }
}

function renderRejectionSummaryRange(id, data, range) {
  const list = document.getElementById(id);
  if (!list) return;
  list.innerHTML = '';

  const rangeData = data?.rejection_reason_ranges?.[range];
  if (rangeData) {
    list.appendChild(el('li', '', `Range: ${range === 'last_7' ? 'Last 7' : (range === 'all_time' ? 'All-time' : 'Today')}`));
    list.appendChild(el('li', '', `Total rejected: ${formatValue(rangeData.total_rejections)}`));
    const sortedReasons = Object.entries(rangeData.by_reason || {}).sort((a, b) => b[1] - a[1]);
    if (sortedReasons.length > 0) {
      for (const [reason, count] of sortedReasons) {
        list.appendChild(el('li', '', `${titleCaseFromKey(reason)}: ${count}`));
      }
    } else {
      list.appendChild(el('li', '', 'No rejection reasons in this range.'));
    }
    return;
  }

  const summary = data?.daily_rejection_summary || {};
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
    if (!(key in summary)) continue;
    const label = labelMap[key] || key;
    list.appendChild(el('li', '', `${label}: ${summary[key]}`));
  }
}

function renderPassedOpportunityTracker(id, tracker, data, range) {
  const list = document.getElementById(id);
  if (!list) return;
  list.innerHTML = '';
  if (!tracker) {
    list.appendChild(el('li', '', 'No tracker data'));
    return;
  }

  const anchorMs = resolveAnchorDateMs(data) ?? Date.now();
  const entries = (tracker.entries || []).filter((row) => {
    if (range === 'all_time') return true;
    const rowMs = Date.parse(String(row.timestamp_ct || ''));
    return inRange(rowMs, anchorMs, range);
  });

  const graded = entries.filter((row) => row.outcome_if_bet && row.outcome_if_bet !== 'ungraded');
  const wins = graded.filter((row) => row.outcome_if_bet === 'win').length;
  const losses = graded.filter((row) => row.outcome_if_bet === 'loss').length;
  const pushes = graded.filter((row) => row.outcome_if_bet === 'push').length;
  const record = graded.length > 0 ? `${wins}-${losses}${pushes > 0 ? `-${pushes}` : ''}` : null;

  list.appendChild(el('li', '', `Range: ${range === 'last_7' ? 'Last 7' : (range === 'all_time' ? 'All-time' : 'Today')}`));
  list.appendChild(el('li', '', `Total passed opportunities: ${entries.length}`));
  list.appendChild(el('li', '', `Graded: ${graded.length} | Ungraded: ${Math.max(0, entries.length - graded.length)}`));
  if (record) list.appendChild(el('li', '', `Record if bet: ${record}`));

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
    const canonicalSit = resolveCanonicalSitAccountability(data);
    let activeRange = 'all_time';

    const renderRangeViews = () => {
      renderRejectionSummaryRange('reject-list', data, activeRange);
      renderPassedOpportunityTracker('passed-opportunity-tracker-list', data.passed_opportunity_tracker, data, activeRange);
      const buttons = document.querySelectorAll('.range-btn');
      buttons.forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.range === activeRange);
      });
    };

    document.querySelectorAll('.range-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        const nextRange = btn.dataset.range || 'all_time';
        activeRange = nextRange;
        renderRangeViews();
      });
    });

    renderMeta(data);
    renderCards(data);
    renderDataFreshness('freshness-list', data.data_freshness);
    renderTodaysScan('todays-scan-list', data);
    renderDecisionQuality('decision-quality-list', data.decision_quality);
    renderTable('today-table', data.todays_bets);
    renderTable('log-table', data.bet_log, 50);
    renderTable('rejected-table', data.rejected_opportunities, 50);
    renderSitAccountability('sit-accountability-list', canonicalSit);
    renderSitAccountabilitySummary('sit-accountability-summary-list', canonicalSit);
    renderList('scanner-stats-list', data.scanner_statistics);
    renderList('market-confidence-list', data.market_confidence);
    renderList('canonical-decision-engine-list', data.canonical_decision_engine);
    renderList('drawdown-governor-list', data.drawdown_governor);
    renderEdgeDistributionTransparency('edge-distribution-transparency-list', data.edge_distribution_transparency);
    renderList('market-type-reliability-list', data.market_type_reliability_index);
    renderList('reliability-index-list', data.reliability_index);
    renderList('daily-summary-list', data.daily_summary);
    renderList('expectation-framing-list', data.expectation_framing);
    renderList('rule-ledger-pointer-list', data.rule_ledger_pointer);
    renderList('execution-list', data.execution_quality);
    renderList('weekly-list', data.weekly_running_totals);
    renderRangeViews();

    toggleSection('rule-ledger-pointer-section', isMeaningfulValue(data.rule_ledger_pointer));
    toggleSection('expectation-framing-section', isMeaningfulValue(data.expectation_framing));
    toggleSection('reliability-index-section', isMeaningfulValue(data.reliability_index));
    toggleSection('market-confidence-section', isMeaningfulValue(data.market_confidence));
    toggleSection('drawdown-governor-section', isMeaningfulValue(data.drawdown_governor));
    toggleSection('governance-section', isMeaningfulValue(data.market_confidence) || isMeaningfulValue(data.drawdown_governor));
    toggleSection('integrity-section', isMeaningfulValue(data.reliability_index) || isMeaningfulValue(data.expectation_framing));
    toggleSection('engine-rule-section', isMeaningfulValue(data.canonical_decision_engine) || isMeaningfulValue(data.rule_ledger_pointer));
    toggleSection('market-distribution-section', isMeaningfulValue(data.market_type_reliability_index) || isMeaningfulValue(data.scanner_statistics));
  } catch (err) {
    document.body.innerHTML = `<p style="padding:20px;font-family:sans-serif;">Failed to load live log: ${err.message}</p>`;
  }
})();

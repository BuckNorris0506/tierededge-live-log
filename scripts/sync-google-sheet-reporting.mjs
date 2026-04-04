import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {
  readCanonicalBetPlacedLedger,
  readCanonicalBetSettledLedger,
} from './execution-layer-utils.mjs';

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const REPORTING_STATE_PATH = path.join(ROOT, 'data', 'google-sheets-reporting.json');
const DEFAULT_SHEET_TITLE = 'TieredEdge OddsJam Trial Reporting';
const REQUIRED_SHEETS = ['Open Bets', 'Settled Bets', 'Daily Summary', 'Dashboard'];
const TRIAL_COST = 400;
const ENV_PATHS = [
  path.join(ROOT, '.env.google-sheets'),
  path.join(ROOT, '.env.google-sheets.local'),
];

function parseArgs(argv) {
  const args = new Set(argv.slice(2));
  return {
    dryRun: args.has('--dry-run'),
    json: args.has('--json'),
  };
}

function parseEnvValue(raw) {
  const trimmed = String(raw || '').trim();
  if (!trimmed) return '';
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
    || (trimmed.startsWith("'") && trimmed.endsWith("'"))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function loadEnvFiles() {
  for (const envPath of ENV_PATHS) {
    if (!fs.existsSync(envPath)) continue;
    const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const separatorIndex = trimmed.indexOf('=');
      if (separatorIndex === -1) continue;
      const key = trimmed.slice(0, separatorIndex).trim();
      const value = parseEnvValue(trimmed.slice(separatorIndex + 1));
      if (!key || process.env[key]) continue;
      process.env[key] = value;
    }
  }
}

function readReportingState() {
  try {
    return JSON.parse(fs.readFileSync(REPORTING_STATE_PATH, 'utf8'));
  } catch {
    return {};
  }
}

function writeReportingState(next) {
  fs.writeFileSync(REPORTING_STATE_PATH, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
}

function normalizeText(value) {
  return String(value || '').trim();
}

function normalizeMoney(value) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.round(number * 100) / 100 : 0;
}

function normalizeOdds(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return '';
  if (/^[+-]/.test(raw)) return raw;
  if (/^-?\d+(\.\d+)?$/.test(raw)) {
    const numeric = Number(raw);
    if (Number.isFinite(numeric)) return numeric >= 0 ? `+${numeric}` : `${numeric}`;
  }
  return raw;
}

function formatMoney(value) {
  return normalizeMoney(value).toFixed(2);
}

function isProofOrTestRow(row) {
  const haystack = [
    row.execution_id,
    row.grading_id,
    row.rec_id,
    row.run_id,
    row.ledger_event,
    row.ledger_selection,
    row.ledger_notes,
  ].map((value) => normalizeText(value).toLowerCase()).join(' | ');

  return (
    haystack.includes('schema-proof')
    || /\bproof\b/.test(haystack)
    || /\btest\b/.test(haystack)
    || haystack.includes('non-production')
  );
}

function placedKey(row) {
  const executionId = normalizeText(row.execution_id);
  if (executionId) return `execution_id:${executionId}`;
  return [
    row.ledger_date,
    row.ledger_event,
    row.ledger_market,
    row.ledger_selection,
    row.ledger_book,
    row.ledger_odds,
    formatMoney(row.ledger_stake),
  ].map((value) => normalizeText(value).toLowerCase()).join('||');
}

function sortByDateThenSelection(rows, dateField = 'ledger_date') {
  return [...rows].sort((a, b) => {
    const dateCompare = normalizeText(a?.[dateField]).localeCompare(normalizeText(b?.[dateField]));
    if (dateCompare !== 0) return dateCompare;
    const eventCompare = normalizeText(a?.ledger_event).localeCompare(normalizeText(b?.ledger_event));
    if (eventCompare !== 0) return eventCompare;
    return normalizeText(a?.ledger_selection).localeCompare(normalizeText(b?.ledger_selection));
  });
}

function buildReportingRows() {
  const placed = sortByDateThenSelection(
    readCanonicalBetPlacedLedger().filter((row) => !isProofOrTestRow(row))
  );
  const settled = sortByDateThenSelection(
    readCanonicalBetSettledLedger().filter((row) => !isProofOrTestRow(row))
  );

  const settledKeys = new Set(settled.map((row) => placedKey(row)));
  const open = placed.filter((row) => !settledKeys.has(placedKey(row)));

  const openRows = open.map((row) => ([
    row.ledger_date || '',
    row.ledger_sport || '',
    row.ledger_event || '',
    row.ledger_market || '',
    row.ledger_selection || '',
    row.ledger_book || '',
    normalizeOdds(row.ledger_odds),
    normalizeMoney(row.ledger_stake),
    row.ledger_stake_type || '',
    row.ledger_promo || 'None',
    row.ledger_source || '',
    row.ledger_notes || '',
  ]));

  const settledRows = settled.map((row) => ([
    row.ledger_date || '',
    row.ledger_sport || '',
    row.ledger_event || '',
    row.ledger_market || '',
    row.ledger_selection || '',
    row.ledger_book || '',
    normalizeOdds(row.ledger_odds),
    normalizeMoney(row.ledger_stake),
    row.ledger_stake_type || '',
    row.ledger_promo || 'None',
    row.ledger_result || '',
    normalizeMoney(row.ledger_payout),
    normalizeMoney(row.ledger_pl),
    row.ledger_source || '',
    row.ledger_notes || '',
  ]));

  const dates = Array.from(new Set([
    ...placed.map((row) => row.ledger_date).filter(Boolean),
    ...settled.map((row) => row.ledger_date).filter(Boolean),
  ])).sort();

  const openByDate = groupBy(open, (row) => row.ledger_date || '');
  const placedByDate = groupBy(placed, (row) => row.ledger_date || '');
  const settledByDate = groupBy(settled, (row) => row.ledger_date || '');

  const dailyRows = dates.map((date) => {
    const datePlaced = placedByDate.get(date) || [];
    const dateOpen = openByDate.get(date) || [];
    const dateSettled = settledByDate.get(date) || [];
    return [
      date,
      '',
      '',
      normalizeMoney(dateSettled.reduce((sum, row) => sum + normalizeMoney(row.ledger_pl), 0)),
      dateSettled.length,
      datePlaced.length,
      normalizeMoney(datePlaced.reduce((sum, row) => sum + normalizeMoney(row.ledger_stake), 0)),
      normalizeMoney(dateOpen.reduce((sum, row) => sum + normalizeMoney(row.ledger_stake), 0)),
      '',
    ];
  });

  const totalBetsPlaced = placed.length;
  const totalOpenBets = open.length;
  const totalSettledBets = settled.length;
  const totalRisked = normalizeMoney(placed.reduce((sum, row) => sum + normalizeMoney(row.ledger_stake), 0));
  const totalOpenExposure = normalizeMoney(open.reduce((sum, row) => sum + normalizeMoney(row.ledger_stake), 0));
  const totalSettledPl = normalizeMoney(settled.reduce((sum, row) => sum + normalizeMoney(row.ledger_pl), 0));
  const netVsTrialCost = normalizeMoney(totalSettledPl - TRIAL_COST);
  const decision = totalSettledPl >= TRIAL_COST ? 'BUY ODDSJAM' : 'DO NOT BUY';

  const dashboardRows = [
    ['Metric', 'Value'],
    ['Total Bets Placed', totalBetsPlaced],
    ['Total Open Bets', totalOpenBets],
    ['Total Settled Bets', totalSettledBets],
    ['Total Risked ($)', totalRisked],
    ['Total Open Exposure ($)', totalOpenExposure],
    ['Total Settled P/L ($)', totalSettledPl],
    ['Trial Cost ($)', TRIAL_COST],
    ['Net vs Trial Cost ($)', netVsTrialCost],
    ['Decision', decision],
  ];

  return {
    metadata: {
      generated_at_utc: new Date().toISOString(),
      total_bets_placed: totalBetsPlaced,
      total_open_bets: totalOpenBets,
      total_settled_bets: totalSettledBets,
      excluded_rows_reason: 'schema-proof/test rows excluded by canonical filter',
    },
    tabs: {
      'Open Bets': [
        ['Date Placed', 'Sport', 'Event', 'Market', 'Selection', 'Sportsbook', 'Odds', 'Stake ($)', 'Stake Type', 'Promo', 'Source', 'Notes'],
        ...openRows,
      ],
      'Settled Bets': [
        ['Date Settled', 'Sport', 'Event', 'Market', 'Selection', 'Sportsbook', 'Odds', 'Stake ($)', 'Stake Type', 'Promo', 'Result', 'Payout ($)', 'P/L ($)', 'Source', 'Notes'],
        ...settledRows,
      ],
      'Daily Summary': [
        ['Date', 'Starting Bankroll ($)', 'Ending Bankroll ($)', 'Daily Net P/L ($)', 'Bets Settled (#)', 'Bets Placed (#)', 'Amount Risked ($)', 'Open Exposure ($)', 'Notes'],
        ...dailyRows,
      ],
      Dashboard: dashboardRows,
    },
    raw: {
      placed,
      settled,
      open,
    },
  };
}

function groupBy(items, getKey) {
  const map = new Map();
  for (const item of items) {
    const key = getKey(item);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  }
  return map;
}

function escapePrivateKey(value) {
  return String(value || '').replace(/\\n/g, '\n');
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }
  if (!response.ok) {
    throw new Error(`google_api_error:${response.status}:${typeof data === 'string' ? data : JSON.stringify(data)}`);
  }
  return data;
}

function base64Url(value) {
  return Buffer.from(value).toString('base64url');
}

async function getAccessToken() {
  const clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || '';
  const privateKey = escapePrivateKey(process.env.GOOGLE_PRIVATE_KEY || '');
  if (!clientEmail || !privateKey) {
    throw new Error('missing_google_service_account_credentials');
  }

  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const claim = {
    iss: clientEmail,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now,
  };
  const unsigned = `${base64Url(JSON.stringify(header))}.${base64Url(JSON.stringify(claim))}`;
  const signature = crypto.createSign('RSA-SHA256').update(unsigned).end().sign(privateKey, 'base64url');
  const assertion = `${unsigned}.${signature}`;

  const body = new URLSearchParams({
    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    assertion,
  });
  const token = await fetchJson('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  return token.access_token;
}

async function getSpreadsheet(accessToken, spreadsheetId) {
  return fetchJson(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}?fields=spreadsheetId,spreadsheetUrl,sheets(properties(sheetId,title))`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
}

async function createSpreadsheet(accessToken, title) {
  return fetchJson('https://sheets.googleapis.com/v4/spreadsheets', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      properties: { title },
      sheets: REQUIRED_SHEETS.map((sheetTitle) => ({
        properties: {
          title: sheetTitle,
          gridProperties: { frozenRowCount: 1 },
        },
      })),
    }),
  });
}

async function ensureRequiredSheets(accessToken, spreadsheetId, existing) {
  const titles = new Set((existing.sheets || []).map((sheet) => sheet.properties?.title).filter(Boolean));
  const requests = [];
  for (const title of REQUIRED_SHEETS) {
    if (!titles.has(title)) {
      requests.push({
        addSheet: {
          properties: {
            title,
            gridProperties: { frozenRowCount: 1 },
          },
        },
      });
    }
  }
  if (!requests.length) return existing;
  await fetchJson(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}:batchUpdate`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ requests }),
  });
  return getSpreadsheet(accessToken, spreadsheetId);
}

async function clearAndWriteSheetValues(accessToken, spreadsheetId, payload) {
  const ranges = REQUIRED_SHEETS.map((title) => `'${title}'`);
  await fetchJson(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values:batchClear`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ranges }),
  });

  const data = REQUIRED_SHEETS.map((title) => ({
    range: `'${title}'!A1`,
    majorDimension: 'ROWS',
    values: payload.tabs[title],
  }));

  await fetchJson(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values:batchUpdate`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      valueInputOption: 'USER_ENTERED',
      data,
    }),
  });
}

async function applyFormatting(accessToken, spreadsheetId, spreadsheet) {
  const requests = [];
  for (const sheet of spreadsheet.sheets || []) {
    const title = sheet.properties?.title;
    if (!REQUIRED_SHEETS.includes(title)) continue;
    requests.push({
      repeatCell: {
        range: {
          sheetId: sheet.properties.sheetId,
          startRowIndex: 0,
          endRowIndex: 1,
        },
        cell: {
          userEnteredFormat: {
            textFormat: { bold: true },
            backgroundColor: { red: 0.85, green: 0.9, blue: 0.98 },
          },
        },
        fields: 'userEnteredFormat(textFormat,backgroundColor)',
      },
    });
    requests.push({
      updateSheetProperties: {
        properties: {
          sheetId: sheet.properties.sheetId,
          gridProperties: { frozenRowCount: 1 },
        },
        fields: 'gridProperties.frozenRowCount',
      },
    });
  }

  if (!requests.length) return;
  await fetchJson(`https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}:batchUpdate`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ requests }),
  });
}

async function syncGoogleSheet(payload) {
  const accessToken = await getAccessToken();
  const state = readReportingState();
  const configuredId = process.env.GOOGLE_SHEETS_REPORTING_SPREADSHEET_ID || state.spreadsheet_id || '';
  const title = process.env.GOOGLE_SHEETS_REPORTING_TITLE || state.spreadsheet_title || DEFAULT_SHEET_TITLE;

  let spreadsheet;
  if (configuredId) {
    spreadsheet = await ensureRequiredSheets(accessToken, configuredId, await getSpreadsheet(accessToken, configuredId));
  } else {
    spreadsheet = await createSpreadsheet(accessToken, title);
  }

  await clearAndWriteSheetValues(accessToken, spreadsheet.spreadsheetId, payload);
  await applyFormatting(accessToken, spreadsheet.spreadsheetId, spreadsheet);

  const nextState = {
    spreadsheet_id: spreadsheet.spreadsheetId,
    spreadsheet_url: spreadsheet.spreadsheetUrl,
    spreadsheet_title: title,
    last_sync_at_utc: new Date().toISOString(),
    required_sheets: REQUIRED_SHEETS,
  };
  writeReportingState(nextState);
  return nextState;
}

async function main() {
  loadEnvFiles();
  const args = parseArgs(process.argv);
  const payload = buildReportingRows();

  if (args.dryRun) {
    const result = {
      status: 'dry_run',
      reporting_state_path: REPORTING_STATE_PATH,
      ...payload.metadata,
      tabs: Object.fromEntries(Object.entries(payload.tabs).map(([title, rows]) => [title, {
        row_count_excluding_header: Math.max(rows.length - 1, 0),
        header: rows[0],
        sample_rows: rows.slice(1, 4),
      }])),
    };
    console.log(args.json ? JSON.stringify(result, null, 2) : result);
    return;
  }

  const syncState = await syncGoogleSheet(payload);
  const result = {
    status: 'ok',
    spreadsheet_id: syncState.spreadsheet_id,
    spreadsheet_url: syncState.spreadsheet_url,
    last_sync_at_utc: syncState.last_sync_at_utc,
    ...payload.metadata,
  };
  console.log(args.json ? JSON.stringify(result, null, 2) : result);
}

main().catch((error) => {
  const payload = {
    status: 'failed',
    error: String(error.message || error),
    reporting_state_path: REPORTING_STATE_PATH,
  };
  console.error(JSON.stringify(payload, null, 2));
  process.exit(1);
});

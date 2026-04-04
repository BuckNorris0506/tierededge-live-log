# Google Sheets Reporting Setup

1. In Google Cloud, create or select a project.
2. Enable the `Google Sheets API` for that project.
3. Go to `IAM & Admin` -> `Service Accounts`.
4. Create a service account for TieredEdge reporting.
5. Open that service account and create a new JSON key.
6. Copy the service account email from the service account details page.
7. Open the JSON key file and copy the `private_key` value exactly.
8. In the repo root, copy `.env.google-sheets.example` to `.env.google-sheets`.
9. Fill in:
   - `GOOGLE_SERVICE_ACCOUNT_EMAIL` with the service account email
   - `GOOGLE_PRIVATE_KEY` with the full private key from the JSON, including `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----`, with embedded `\n` line breaks
   - `GOOGLE_SHEETS_REPORTING_SPREADSHEET_ID` only if you want to update an existing Google Sheet
10. If you want to use an existing Google Sheet, open it in Google Sheets and share it with the service account email as an editor.
11. If you do not set `GOOGLE_SHEETS_REPORTING_SPREADSHEET_ID`, the first sync run will create a new Google Sheet automatically.
12. Run:
    - `npm run reporting:google-sheets`

Notes:
- `.env.google-sheets` is loaded automatically by the sync script.
- `.env.google-sheets.local` is also loaded automatically and can override nothing already set in the shell.
- TieredEdge ledger files remain the source of truth. The Google Sheet is reporting only.

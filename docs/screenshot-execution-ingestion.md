## Screenshot Execution Ingestion

TieredEdge now supports two execution-capture paths:
- local preview/confirm from saved screenshots
- production WhatsApp screenshot ingestion with explicit confirmation
- production WhatsApp settled-ticket ingestion with explicit confirmation

### Local preview / confirm

Preview:

```bash
node scripts/preview-screenshot-executions.mjs --image /absolute/path/to/screenshot.png
```

Confirm:

```bash
node scripts/confirm-screenshot-executions.mjs --confirm CONFIRM_ALL
```

This local path:
- runs OCR against the screenshot
- extracts visible bet fields
- matches the bet against recommendation context when possible
- writes a preview file only
- appends nothing until confirmation

### Production WhatsApp flow

TieredEdge can now treat inbound WhatsApp sportsbook screenshots as a short-lived execution-ingestion job.

#### 1. Create preview from a WhatsApp message attachment

```bash
node scripts/whatsapp-execution-ingestion.mjs preview \
  --sender whatsapp:+18165551212 \
  --message-file /absolute/path/to/whatsapp-message.txt
```

Or pass image paths directly:

```bash
node scripts/whatsapp-execution-ingestion.mjs preview \
  --sender whatsapp:+18165551212 \
  --image /absolute/path/to/screenshot.png
```

This step:
- extracts `[media attached: ... (image/...)]` paths from the inbound WhatsApp message when available
- copies attachments into `data/whatsapp-screenshot-inbox/`
- runs OCR / parsing
- attempts recommendation matching
- stores a pending preview in `data/whatsapp-execution-pending.json`
- writes the per-preview artifact into `data/pending-whatsapp-previews/`
- returns a WhatsApp-friendly preview message
- appends nothing yet

#### 2. Confirm or edit by WhatsApp reply

```bash
node scripts/whatsapp-execution-ingestion.mjs command \
  --sender whatsapp:+18165551212 \
  --command "CONFIRM ALL" \
  --rebuild
```

Supported WhatsApp commands:
- `CONFIRM ALL`
- `CONFIRM 1,3`
- `REJECT`
- `CANCEL`
- `HELP`
- `EDIT 1 ODDS +136`
- `EDIT 1 STAKE 20.50`

On confirm:
- only the selected rows are appended to `data/execution-log.jsonl`
- unmatched rows stay unmatched and are marked as manual overrides
- recommendation truth is untouched
- optional rebuild refreshes canonical/public state

### Matching statuses

Each parsed bet is classified as one of:
- `matched_to_recommendation`
- `matched_with_low_confidence`
- `unmatched_manual_bet`
- `ambiguous_match`

Weak matches never invent a `rec_id`.

### Safety rules

- Screenshot-derived rows never append without confirmation.
- Pending WhatsApp previews are sender-scoped and expire automatically.
- New previews replace older pending previews for the same sender.
- Missing stake / odds / event identity block confirmation for that row.
- Unreadable screenshots return explicit warnings instead of partial garbage.
- Execution logging remains append-only and separate from recommendation logging.

### Production WhatsApp settled-ticket flow

Preview:

```bash
node scripts/whatsapp-settled-ticket-ingestion.mjs preview \
  --sender whatsapp:+18165551212 \
  --message-file /absolute/path/to/whatsapp-message.txt
```

Confirm:

```bash
node scripts/whatsapp-settled-ticket-ingestion.mjs command \
  --sender whatsapp:+18165551212 \
  --command "CONFIRM ALL" \
  --rebuild
```

This flow:
- extracts settled/open/cashed-out ticket fields from inbound sportsbook screenshots
- matches to execution-log rows first, recommendation context second
- stores a sender-scoped pending confirmation in `data/whatsapp-settlement-pending.json`
- appends only confirmed rows into `data/grading-ledger.jsonl`
- writes reconciliation events instead of silently changing old grading rows

Settled-ticket commands:
- `CONFIRM ALL`
- `CONFIRM 1,3`
- `REJECT`
- `CANCEL`
- `HELP`

Settled-ticket reconciliation flags:
- `grading_mismatch`
- `cashout_detected`
- `partial_settlement`
- `manual_reconciliation_required`

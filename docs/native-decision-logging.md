# Native Decision-Time Logging

TieredEdge now supports append-only native decision-time logging through:

- `npm run append:native-decision -- '<json-row-or-json-array>'`

Ledgers written under `data/native-decision-ledgers/`:

- `decision-observations.jsonl`
- `bets-ledger.jsonl`
- `0-to-2-pass-ledger.jsonl`
- `suppressed-candidates-ledger.jsonl`

Observation band only:

1. actual bets
2. Friday `FUN_SGP` bets
3. passes where `0% < edge < 2%`
4. suppressed candidates

Controlled values:

- `final_decision`: `BET`, `SIT`
- `rejection_stage`: `no_raw_edge`, `confidence_gate`, `threshold_gate`, `risk_gate`, `integrity_gate`, `state_sync_gate`

Downstream behavior:

- if native ledgers exist, `build-live-log.mjs` prefers them over recommendation-log reconstruction for pass/suppression traces
- betting logic is unchanged

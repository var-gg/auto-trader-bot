# Runtime Replay Corpus

Created: 2026-03-25
Scope: operational truth capture for live auto-trading replay validation.

## Goal
Freeze AS-IS runtime behavior from production evidence, not code inference.

Evidence sources used in this pass:
- Cloud Scheduler inventory (`ops/runtime_inventory_scheduler_jobs_20260325.json`)
- Cloud Run service inventory (`ops/runtime_inventory_run_services_20260325.json`)
- Cloud Run request logs
  - `ops/runtime_inventory_requests_trade_paths_20260325.json`
  - `ops/runtime_inventory_requests_money_paths_20260325.json`
- DB money-path rows (`ops/runtime_inventory_case_rows_20260325.json`)

## Current replay fixtures
Stored under `tests/replay_fixtures/`.

### Trading
- `kr_open_20260324.json`
  - endpoint: `/api/trading-hybrid/kr/open`
  - DB anchor: `order_batch.id=7156`
  - request trace anchored by Cloud Run request log around `2026-03-23T23:31Z`
- `us_open_20260324.json`
  - endpoint: `/api/trading-hybrid/us/open`
  - DB anchor: `order_batch.id=7175`
- `us_open_sell_20260324.json`
  - endpoint: `/api/trading-hybrid/us/open`
  - DB anchor: `order_batch.id=7176`
- `kr_intraday_20260324_1750kst.json`
  - endpoint: `/api/trading-hybrid/kr/intraday`
  - DB anchor: `order_batch.id=7181`
- `us_intraday_risk_cut_20260325_0010kst.json`
  - endpoint: `/api/trading-hybrid/us/intraday`
  - DB anchor: `order_batch.id=7219`

### PM / bootstrap
- `bootstrap_20260324_1600kst.json`
  - endpoint: `/kis-test/bootstrap`
  - linked PM signal run: `run_id=34`
- `pm_signal_us_20260324_0015kst.json`
  - endpoint: `/recommendations/batch-generate`
  - DB anchor: `pm_signal_run_header.run_id=34`
- `pm_signal_kr_20260324_1415kst.json`
  - endpoint: `/recommendations/kr/batch-generate`
  - DB anchor: `pm_signal_run_header.run_id=33`

### Fill / reconcile
- `fill_snapshot_20260325.json`
  - DB anchor only for now
  - most recent `order_fill` rows captured
  - fill collection scheduler/request endpoint still not positively identified from current inventory snapshot

## Correlation model used
Each replay fixture tries to retain at least one of:
- Cloud Run `trace`
- request timestamp + endpoint
- `order_batch.id`
- `order_plan.id`
- `order_leg.id`
- `broker_order.id`
- `order_fill.id`
- `pm_signal_run_header.run_id`

This is enough to reconnect fixture summaries back to Cloud logs and DB truth.

## Anonymization policy in this pass
- service host masked to `https://service.example.run.app`
- symbol strings replaced with stable aliases in replay fixtures (`SYM_####`)
- broker order numbers truncated/masked in fixture preview (`ORD#xxxx`)
- raw detailed source-of-truth dumps remain under `ops/` for local validation

## Gaps still open
1. Fill collection trigger endpoint is not yet identified from current Scheduler inventory.
2. PM risk refresh does not yet have a dedicated replay fixture in this pass.
3. External response payload bodies are only indirectly represented via Cloud Run request metadata and DB side effects, not full body capture.
4. Request headers are limited to what Cloud logs expose without app-level request-body capture.

## Intended use
- baseline AS-IS runtime truth before refactor changes
- future parity/shadow-run comparisons
- cutover validation and rollback investigation

## Minimum verification completed
- live money-path endpoints found in Scheduler inventory
- Cloud Run request logs exist for bootstrap/open/intraday/signal generation
- DB rows exist for order batch / plan / leg / broker / fill / PM signal decision history
- replay fixtures written with correlation keys back to runtime evidence

# DB Write / Read Parity

Created: 2026-03-25
Branch: `public-release-20260323`
Scope: live DB side effects and next-cycle read viability.

## Goal
Verify that TO-BE preserves the write-set meaning required by later live reads.

This means more than just creating orders.
The following have to stay connected:
- order batch / plan / leg / broker / fill chain
- PM execution history / outcome history chain
- risk snapshot freshness chain
- next-cycle readers for pending orders, maturity checks, and history postprocess

## Added tests
- `tests/parity/test_db_side_effect_parity_core.py`

## What the tests lock down

### 1) Order write-set keeps next-read keys intact
Test covers:
- `create_order_batch(...)`
- `create_plan_with_legs(...)`

Required keys preserved:
- `order_batch.id`
- `order_plan.id`
- `order_plan.ticker_id`
- `order_plan.recommendation_id`
- `order_plan.reverse_breach_day`
- `order_leg.plan_id`
- one broker submission attempt per leg

This matters because later reads depend on:
- `ticker_id` for PM linkage / maturity / market joins
- `reverse_breach_day` for maturity override
- `plan_id -> leg_id -> broker_order` traversal

### 2) Pending-order read remains viable
`portfolio_repository.load_pending_orders(...)` expects:
- latest `broker_order.status = 'SUBMITTED'`
- 24h window on `submitted_at`
- no `order_fill`, or `fill_status = 'UNFILLED'`
- `order_leg -> order_plan -> ticker.country` join intact

Parity implication:
- TO-BE must keep broker order write rows tied to `leg_id`
- status meaning must stay compatible with `SUBMITTED`
- fill collector must not invent conflicting fill rows for not-yet-filled orders

### 3) PM history postprocess remains viable
`PMHistoryBatchService.compute_tplus_outcomes(...)` reads from:
- `pm_order_execution_history.run_id`
- `ticker_id`
- `symbol`
- `avg_fill_price` or `submitted_price` / `intended_limit_price`
- `executed_at`

It writes to:
- `pm_outcome_tplus_history(run_id, ticker_id, horizon_days, ...)`

Parity implication:
- TO-BE must keep `run_id` and `ticker_id` on execution history
- a usable price field must remain present for outcome computation
- `executed_at` must remain meaningful as entry-date anchor

### 4) Position maturity remains viable
`PositionMaturityRepository.check_position_maturity(...)` depends on:
- latest BUY fill via `order_fill -> broker_order -> order_leg -> order_plan`
- `order_plan.reverse_breach_day`
- final OHLCV rows for business-day counting

Parity implication:
- reverse-breach provenance cannot disappear
- BUY fill chain must remain queryable
- if column names move, a canonical mapping layer is required

### 5) Headline risk freshness remains viable
`HeadlineRiskService.get_latest_active_snapshot(...)` expects:
- `market_headline_risk_snapshot.market_scope`
- `as_of_at`
- `expires_at`
- `discount_multiplier`
- other risk metadata fields

Parity implication:
- TTL semantics must survive schema moves
- latest-active query must still distinguish active vs stale snapshots

## Schema meaning map

| AS-IS table.column | Meaning | TO-BE expectation / canonical meaning |
|---|---|---|
| `order_batch.id` | batch identity | keep as batch correlation key |
| `order_batch.notes` | batch provenance/meta JSON | may move to structured columns later, but route/slot/provenance meaning must remain recoverable |
| `order_plan.id` | plan identity | keep as per-symbol execution correlation key |
| `order_plan.ticker_id` | durable ticker foreign key | mandatory; later reads depend on it |
| `order_plan.recommendation_id` | recommendation provenance | keep if PM traceability is required |
| `order_plan.reverse_breach_day` | maturity override source | must remain queryable for maturity logic |
| `order_plan.decision` | execute vs skip | canonical decision flag |
| `order_leg.plan_id` | plan->leg chain | mandatory |
| `order_leg.type/side/quantity/limit_price` | executable leg intent | canonical execution shape |
| `broker_order.leg_id` | leg->broker chain | mandatory |
| `broker_order.order_number` | broker-facing order id | keep when provider returns it |
| `broker_order.status` | submit/result state | must stay compatible with pending-order queries |
| `broker_order.reject_code/reject_message` | forensic failure reason | keep for incident analysis and unfilled backfill |
| `order_fill.broker_order_id` | broker->fill chain | mandatory |
| `order_fill.fill_qty/fill_price/fill_status/filled_at` | execution result meaning | canonical fill meaning |
| `pm_order_execution_history.run_id` | PM run correlation | mandatory for PM outcome history |
| `pm_order_execution_history.ticker_id` | ticker correlation | mandatory for price joins/outcomes |
| `pm_order_execution_history.submitted_price/intended_limit_price/avg_fill_price` | entry-price family | at least one usable canonical entry price must survive |
| `pm_order_execution_history.executed_at` | event time anchor | mandatory for lookback/outcome windows |
| `pm_outcome_tplus_history.*` | outcome materialization | same `(run_id, ticker_id, horizon_days)` meaning |
| `market_headline_risk_snapshot.expires_at` | snapshot TTL | active/stale distinction must survive |

## Verified live-read dependencies from current code
- pending order readers still join `broker_order -> order_leg -> order_plan -> ticker` and left join `order_fill`
- maturity readers still depend on filled BUY chain and `reverse_breach_day`
- PM outcome postprocess still depends on `pm_order_execution_history` price and timestamp fields
- headline risk still depends on TTL-active snapshot reads

## Known risks / gaps

### 1) `order_batch.notes` is still overloaded
It currently carries provenance/meta JSON in text form.
That is usable, but brittle as a long-term forensic contract.
For now, parity should preserve the meaning even if storage later becomes structured.

### 2) `pm_order_execution_history.run_id` is inferred in some write paths
Current submit path may derive run id from recent `pm_candidate_decision_history` by `ticker_id`.
That is workable, but weaker than explicit end-to-end provenance passing.

### 3) Pending-order semantics rely on `SUBMITTED`
If TO-BE introduces a new intermediate status without updating readers, next-cycle logic can silently miss live orders.

### 4) Fill collectors intentionally skip zero-fill / cancelled / rejected rows
That is current AS-IS meaning.
Do not change this behavior silently or pending-order / execution-history reasoning will drift.

## Bottom line
Current TO-BE still preserves the core write/read chain for live trading and PM postprocessing, provided these meanings stay intact:
- ticker-linked plan provenance,
- leg-linked broker rows,
- broker-linked fill rows,
- PM run-linked execution history,
- and TTL-valid risk snapshots.

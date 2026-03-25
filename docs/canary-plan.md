# Canary Plan

Created: 2026-03-25
Branch target: `public-release-20260323`
Purpose: run a tightly limited live canary only after route/decision/adapter/DB/shadow gates have passed.

## Canary principles
- start with **one market / one slot / smallest safe scope**
- do **not** combine canary with strategy tuning or config cleanup
- do **not** change scheduler topology during canary
- compare every canary run against AS-IS replay anchors and same-day operational expectations
- if any rollback trigger fires, revert immediately before widening scope

## Recommended first canary
### Phase 0: no-money live canary
Use TO-BE runtime with live reads but broker-safe mode (`test_mode` / paper / no-op submit path).

Recommended target:
- market: **US**
- slot: **open** (`/api/trading-hybrid/us/open`)
- reason: replay anchor exists (`order_batch.id=7175`), shadow replay already covers US open semantics, and the slot is central enough to prove the main live path.

Success requirement:
- runtime behavior is explainable for the current market/time context
- live reads succeed when they are expected to succeed in that context
- structured logs emitted
- selected candidates / plan shape / skip reasons are explainable against AS-IS expectations
- no production-money movement

Note:
- outside market hours, a stop/skip/error may be the correct outcome
- the canary fails only when behavior is semantically wrong or insufficiently traceable

### Phase 1: smallest-money live canary
Only after Phase 0 passes cleanly.

Recommended target:
- same market/slot pair first: **US open**
- restrict to smallest allowed budget or safest single-plan exposure
- no multi-slot expansion on the same day

Success requirement:
- plan generation matches expected semantic band
- outbound broker intent is complete and traceable
- fill/reconcile and next-cycle reads stay healthy

## Structured log contract for canary
Every canary run must emit a structured `live_run` record with at least:
- `run_id`
- `slot`
- `command`
- `strategy_version`
- `decision_summary`
- `order_batch_id`
- `order_plan_id`
- `broker_request_id`
- `broker_response_id`
- `extra.actor`
- `extra.channel`
- `extra.correlation`

Additionally, the run report for canary should capture:
- selected candidates
- order plan details (symbol, side, leg count, price ladder, quantity split)
- skip/reject reasons
- fill/reconcile outcome
- final status

## Canary comparison dashboard / report
For every canary run, produce a side-by-side record against AS-IS anchor(s):

| Field | AS-IS anchor | TO-BE canary | Result |
|---|---|---|---|
| endpoint / slot | replay corpus | structured log | pass/fail |
| selected candidates | replay summary | canary summary | same / explainable drift |
| order plan shape | replay semantic band | canary plan | pass/fail |
| skip/reject reasons | replay expectation | canary result | pass/fail |
| broker intent ids | broker/local correlation | structured log ids | present/missing |
| fill/reconcile | replay DB anchor / next-cycle check | canary follow-up | pass/fail |
| PM history / outcome linkage | run_id chain | canary DB/log chain | pass/fail |

This can be delivered as a daily markdown report or dashboard panel, but the comparison fields above are mandatory.

## Expansion ladder
Only expand if the prior step is clean.

1. **US open, broker-safe mode**
2. **US open, smallest-money live canary**
3. **US intraday, smallest-money live canary**
4. **KR open, broker-safe mode**
5. **KR open, smallest-money live canary**
6. broader slot coverage only after repeated clean runs

Do not add multiple markets and multiple slots in one expansion step.

## Mandatory evidence to keep per canary
- route entry log / request timestamp
- `live_run` structured log payload
- selected candidates summary
- order plan summary
- broker intent ids and submit status
- fill collection / reconcile result
- postprocess / next-cycle health check
- AS-IS comparison note

## Stop conditions during canary
Stop immediately and revert on any rollback trigger defined in `docs/rollback-runbook.md`.

## Explicit hold
Backtest / Optuna / strategy-improvement work remains paused until canary gates are passed.

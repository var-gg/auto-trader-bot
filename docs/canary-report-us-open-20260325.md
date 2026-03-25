# Canary Report — US Open — 2026-03-25

Date: 2026-03-25
Market: US
Slot: US_OPEN
Mode: broker-safe (`test_mode=true`)
Run ID: not emitted successfully
Command: `/api/trading-hybrid/us/open`
AS-IS anchor: `tests/replay_fixtures/us_open_20260324.json`
Operator: OpenClaw agent_trade_exec

## Summary
- Result: **FAIL**
- Expansion allowed: **NO**
- Rollback triggered: **YES (do not widen / keep AS-IS as active runtime)**

## Side-by-side comparison
| Field | AS-IS | TO-BE canary | Result | Note |
|---|---|---|---|---|
| Route / slot | `/api/trading-hybrid/us/open` | same target invoked | PASS | direct controller/command path used |
| Selected candidates | replay anchor exists | not reached cleanly | FAIL | run aborted before stable result |
| Order plan shape | semantic buy plan expected | not reached cleanly | FAIL | no trustworthy plan output |
| Side | BUY-side intent expected | not reached cleanly | FAIL | aborted |
| Leg count | replay-derived two-leg shape | not reached cleanly | FAIL | aborted |
| Quantity split | replay-derived split expected | not reached cleanly | FAIL | aborted |
| Price ladder meaning | replay-derived band expected | not reached cleanly | FAIL | aborted |
| Skip / reject reason | explainable parity expected | controller raised HTTP 500 | FAIL | schema/runtime fault, not a business skip |
| Broker request id | should be present in structured log | missing | FAIL | run log not completed |
| Broker response id | should be present in structured log | missing | FAIL | run log not completed |
| Fill / reconcile result | should complete or fail clearly | upstream account snapshot error observed | FAIL | `INVALID_CHECK_ACNO` surfaced |
| Next-cycle read health | should remain consistent | not proven | FAIL | run aborted |
| PM history linkage | should remain traceable | not proven | FAIL | no successful run_id chain |
| Final status | successful broker-safe run | failed | FAIL | do not widen |

## Observed failures
1. **Account snapshot path failure**
   - observed message: `INVALID_CHECK_ACNO`
   - effect: overseas account snapshot could not complete cleanly in sync stage

2. **Schema/runtime mismatch in earnings query**
   - observed error: `column e.confirmed_report_date does not exist`
   - source area: `pm_open_session_service._is_earnings_day(...)`
   - effect: transaction entered failed state before trading decision path could complete

3. **Transaction abort cascade**
   - observed final failure: `current transaction is aborted, commands ignored until end of transaction block`
   - source area: follow-up read in `PositionMaturityRepository.get_reverse_breach_day_from_last_buy_plan(...)`
   - effect: controller returned HTTP 500 instead of a usable broker-safe canary result

## Structured log excerpt
- run_id: not emitted successfully
- slot: intended `US_OPEN`
- command: intended `trading.run_open:US`
- strategy_version: intended `pm-core-v2`
- order_batch_id: missing
- order_plan_id: missing
- broker_request_id: missing
- broker_response_id: missing

## Rollback / gate decision
- Keep **AS-IS** as active runtime for this slot.
- Do **not** widen canary.
- Treat this as a **cutover blocker** under:
  - follow-up cycle / runtime anomaly
  - broker correlation gap
  - fill/sync failure

## Required fixes before retry
1. fix earnings query/schema mismatch for `confirmed_report_date`
2. ensure sync stage handles account snapshot failure deterministically for broker-safe canary
3. guarantee failed read does not poison entire trading transaction without controlled rollback/recovery
4. rerun the same **US open broker-safe canary only** after fixes

## Notes
This canary did useful work: it found a real runtime blocker before any cutover expansion.
That means the gate worked as intended.

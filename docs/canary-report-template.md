# Canary Report Template

Date:
Market:
Slot:
Mode: broker-safe / smallest-money live
Run ID:
Command:
AS-IS anchor:
Operator:

## Summary
- Result: PASS / FAIL
- Expansion allowed: YES / NO
- Rollback triggered: YES / NO

## Side-by-side comparison
| Field | AS-IS | TO-BE canary | Result | Note |
|---|---|---|---|---|
| Route / slot |  |  |  |  |
| Selected candidates |  |  |  |  |
| Order plan shape |  |  |  |  |
| Side |  |  |  |  |
| Leg count |  |  |  |  |
| Quantity split |  |  |  |  |
| Price ladder meaning |  |  |  |  |
| Skip / reject reason |  |  |  |  |
| Broker request id |  |  |  |  |
| Broker response id |  |  |  |  |
| Fill / reconcile result |  |  |  |  |
| Next-cycle read health |  |  |  |  |
| PM history linkage |  |  |  |  |
| Final status |  |  |  |  |

## Structured log excerpt
- run_id:
- slot:
- command:
- strategy_version:
- order_batch_id:
- order_plan_id:
- broker_request_id:
- broker_response_id:

## Decision
- Expand? 
- Repeat same scope? 
- Roll back? 

## Notes

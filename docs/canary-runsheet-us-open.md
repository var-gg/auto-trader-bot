# Canary Run-Sheet — US Open Broker-Safe

Created: 2026-03-25
Scope: one market / one slot only

## Step 0 — runtime preflight
Run:
```powershell
python scripts/check_canary_env.py
```
Stop if `ok=false`.

## Step 1 — execute canary
Use the actual TO-BE runtime context that owns valid KIS env.
Target:
- route: `/api/trading-hybrid/us/open`
- query: `test_mode=true`

## Step 2 — capture evidence
Capture:
- request timestamp
- route / slot
- run_id if emitted
- selected candidates
- order plan summary
- order_batch_id / order_plan_id
- broker request / response ids
- fill/reconcile result
- final status

## Step 3 — compare against AS-IS
AS-IS anchor:
- `tests/replay_fixtures/us_open_20260324.json`

Mark each as:
- PASS
- explainable drift
- FAIL

## Step 4 — gate decision
Only proceed if:
- no rollback trigger fired
- no missing broker correlation ids
- no sync/fill/account runtime failure
- report is complete

## Hard stop conditions
- invalid runtime env
- OVRS account snapshot failure
- duplicate-order risk
- missing structured traceability
- unexplained plan drift

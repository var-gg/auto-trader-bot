# Canary Execution Checklist

Created: 2026-03-25
Target first canary: US open only

## Before run
- [ ] confirm only one market / one slot is in scope
- [ ] confirm broker mode is safe (`test_mode` / paper / no-op) for Phase 0
- [ ] confirm no production DB sharing for shadow-style validation assets
- [ ] confirm AS-IS anchor selected: `tests/replay_fixtures/us_open_20260324.json`
- [ ] confirm structured logging is enabled for `live_run`
- [ ] confirm rollback operator has `docs/rollback-runbook.md` open
- [ ] confirm no parallel strategy tuning / Optuna / scheduler rewrites are happening

## During run
- [ ] capture request timestamp and route
- [ ] capture `run_id`
- [ ] capture `slot`
- [ ] capture `command`
- [ ] capture selected candidates
- [ ] capture order plan summary (symbol / side / leg count / qty split / price ladder)
- [ ] capture `order_batch_id`
- [ ] capture `order_plan_id`
- [ ] capture `broker_request_id`
- [ ] capture `broker_response_id`
- [ ] capture skip / reject reasons
- [ ] capture fill / reconcile outcome
- [ ] capture final status

## Immediate fail checks
- [ ] no missing order generation event
- [ ] no duplicate-order risk event
- [ ] no fill collection failure
- [ ] no broken next-cycle state read
- [ ] no PM signal/history linkage gap
- [ ] no broker correlation gap

## After run
- [ ] produce side-by-side canary report from template
- [ ] mark each compared field as pass / explainable drift / fail
- [ ] decide: stop / repeat same scope / widen scope
- [ ] if any fail condition fired, rollback immediately

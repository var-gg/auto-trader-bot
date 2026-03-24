# Schedule Slot Map

This step keeps the external scheduler surface small while making internal responsibility explicit through slot-based dispatch.

Primary artifacts:
- `live_app/config/schedule_manifest.json`
- `live_app/config/schedule_manifest.py`
- `live_app/application/schedule_slots.py`

---

## Goal

Keep job count low.
Do not create a large number of narrowly-scoped external scheduler jobs.
Instead:
- use a small set of scheduler entrypoints
- map them internally to named slots
- let slots declare which commands run

This yields:
- fewer scheduler objects
- clearer runtime ownership
- less controller orchestration logic

---

## Slot definitions

Current active slots:
- `KR_PREOPEN`
- `KR_OPEN`
- `KR_INTRADAY`
- `US_PREOPEN`
- `US_OPEN`
- `HOUSEKEEPING`

These are declared in `schedule_manifest.json`.

### `KR_PREOPEN`
Commands:
- `bootstrap.refresh_core_inputs`
- `risk.refresh_snapshot:KR`
- `signals.update_pm_v2:KR`

Operational meaning:
- preopen data refresh and PM preparation for Korea

### `KR_OPEN`
Commands:
- `trading.sync_account:KR`
- `trading.run_open:KR`

Operational meaning:
- Korea open-session trading path

### `KR_INTRADAY`
Commands:
- `trading.sync_account:KR`
- `trading.run_intraday:KR`

Operational meaning:
- Korea intraday cycle path

### `US_PREOPEN`
Commands:
- `bootstrap.refresh_core_inputs`
- `risk.refresh_snapshot:US`
- `signals.update_pm_v2:US`

Operational meaning:
- US preopen data refresh and PM preparation

### `US_OPEN`
Commands:
- `trading.sync_account:US`
- `trading.run_open:US`

Operational meaning:
- US open-session trading path

### `HOUSEKEEPING`
Commands:
- `history.backfill_unfilled`
- `history.compute_outcomes`
- `history.postprocess`

Operational meaning:
- post-trade cleanup and outcome maintenance

---

## Ingress policy

External ingress remains minimal and existing route paths stay intact.
Examples:
- `/kis-test/bootstrap`
- `/api/trading-hybrid/kr/open`
- `/api/trading-hybrid/us/open`
- `/api/trading-hybrid/kr/intraday`
- `/api/premarket/history/postprocess`

Controllers should stay thin:
- auth
- request parsing
- context creation
- command dispatch

They should **not** accumulate slot orchestration branches.

---

## Dispatcher role

`live_app/application/schedule_slots.py` provides:
- `SlotDispatchRequest`
- `ScheduleSlotDispatcher`

This is the internal slot/usecase seam.
A scheduler or internal trigger can call a slot directly without embedding orchestration in the controller.

---

## How current operational routes map to slots

### Korea
- bootstrap/preopen preparation -> `KR_PREOPEN`
- open trading endpoint -> `KR_OPEN`
- intraday trading endpoint -> `KR_INTRADAY`

### United States
- bootstrap/preopen preparation -> `US_PREOPEN`
- open trading endpoint -> `US_OPEN`
- intraday can remain under same family and be added as explicit active slot when needed

### Shared maintenance
- PM history repair/outcome processing -> `HOUSEKEEPING`

This means the current operating path can be explained without referencing controller internals.

---

## Why this keeps job count low

Instead of creating one scheduler job per tiny subtask, the model encourages:
- one slot invocation per operational window
- internal command sequence declared in manifest

So the number of external scheduler jobs can stay close to the number of operational windows, not the number of internal actions.

---

## Active path summary

Current intended active path reads as:
- preopen -> bootstrap + risk refresh + PM signal v2
- open -> trading_hybrid open path
- intraday -> trading_hybrid intraday path
- housekeeping -> PM history postprocess path

That is the single active reading intended by this step.

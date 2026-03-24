# Live App Boundary

This step introduces an explicit `live_app` boundary for Cloud Run deployment targets while keeping existing external routes unchanged.

Created packages:
- `live_app/api/*`
- `live_app/jobs/*`
- `live_app/application/*`

Principle:
- existing route paths stay the same
- controllers become thin entrypoints
- orchestration/business calls move into `application command/usecase` classes
- DB/runtime dependencies are represented as adapters and passed inward
- pure decision logic remains in `shared/domain`

---

## What changed

## 1. Thin controller boundary
Controllers now focus on:
- auth / dependency injection
- request parsing / validation
- `RunContext` creation
- command/query invocation
- HTTP error translation

They no longer directly own runtime business orchestration decisions.

Updated controllers:
- `app/features/kis_test/controllers/bootstrap_controller.py`
- `app/features/premarket/controllers/pm_signal_controller.py`
- `app/features/premarket/controllers/pm_risk_controller.py`
- `app/features/trading_hybrid/controllers/trading_hybrid_controller.py`

## 2. Application command/usecase layer
Added:
- `live_app/application/context.py`
- `live_app/application/adapters.py`
- `live_app/application/bootstrap_commands.py`
- `live_app/application/pm_signal_commands.py`
- `live_app/application/risk_commands.py`
- `live_app/application/trading_commands.py`

Role of this layer:
- own usecase invocation boundaries
- mediate controller -> service/runbook transitions
- be the future place where service internals are gradually replaced by `shared/domain` + adapters

## 3. Jobs boundary
Added:
- `live_app/jobs/bootstrap_job.py`
- `live_app/jobs/trading_jobs.py`

These do not add new scheduler jobs.
They only provide explicit Cloud Run/live entry wrappers for existing jobs/usecases.

## 4. API response helper
Added:
- `live_app/api/responses.py`

This centralizes the success/skipped envelope used by trading-hybrid endpoints.

---

## Boundary map

```text
existing route path
  -> controller
    -> RunContext creation
    -> application command/query
      -> adapters / existing services / runbooks
        -> shared/domain (for pure decision logic, future deeper migration)
        -> persistence / broker / marketdata adapters
```

Current step intentionally keeps most existing services/runbooks alive behind the command layer.
That preserves surface compatibility while establishing the seam.

---

## Surface compatibility

### Route paths changed?
No.
Existing paths were kept intact, including:
- `/kis-test/bootstrap`
- `/api/premarket/signals/*`
- `/api/premarket/risk/*`
- `/api/trading-hybrid/*`

### Scheduler jobs increased?
No.
No scheduler/job count increase was introduced.
`live_app/jobs/*` only wraps existing behaviors.

### New strategy branches added to controllers?
No.
Controllers only changed their invocation target.
They do not introduce new strategy logic or branching behavior.

---

## Why this is useful

Before:
- controller -> service/runbook directly
- business orchestration mixed with transport concerns

After:
- controller -> command/query -> existing runtime implementation
- a stable application seam exists for further migration

This means later refactors can:
- replace service internals with `shared/domain` calls
- add explicit repository/broker adapters
- keep HTTP surface untouched

---

## Current limitations

This step does **not** fully eliminate all legacy service/runbook coupling yet.
The command layer currently wraps existing runtime implementations rather than fully replacing them.

That is intentional.
This stage is about establishing the `live_app` boundary without breaking surface compatibility.

---

## Validation against requested checks

### Existing external surface preserved?
Yes. Route prefixes and endpoint paths were left unchanged.

### Pure calculation logic removed from controllers?
Yes for the covered controllers. They now create context and delegate to commands/queries.

### Golden behavior expected to remain?
Yes by design, because underlying runtime services/runbooks remain the same and are now only wrapped by the new application boundary.

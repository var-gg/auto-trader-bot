# Cutover Plan

This document defines the rollout order, cutover gates, rollback triggers, and observability expectations for the live/backtest split.

---

## 1. Cutover objective

Target state:
- live runtime uses the single active path described in `schedule_manifest`
- research/backtest runtime remains separate
- parity/golden tests are used as gates before strategy/runtime promotion

---

## 2. Deployment order

### Phase 0: pre-cutover checks
Required before rollout:
- `tests/golden/test_pure_decision_engine.py` passes
- `tests/golden/test_backtest_execution_model.py` passes
- `tests/parity/test_live_backtest_parity.py` passes
- `tests/golden/test_optuna_backtest_runtime.py` passes in backtest env
- `schedule_manifest` active path is reviewed
- live image builds from `Dockerfile.live`
- backtest image/runtime builds separately from `Dockerfile.backtest`

### Phase 1: deploy observability first
Deploy structured logging fields before changing active execution ownership.
Reason:
- if rollout fails, investigation already has correlation keys

### Phase 2: route ingress through thin command seams
Ensure active ingress uses:
- command/usecase dispatch
- slot manifest interpretation
- no new controller orchestration branches

Current implementation status:
- `/api/trading-hybrid/{market}/{phase}` -> `RunTradingHybridCommand`
- `/kis-test/bootstrap` -> `RunBootstrapCommand`
- `/api/premarket/history/*` -> history commands in `live_app.application.history_commands`
- structured logging helper is attached at command/controller boundary

### Phase 3: hold legacy paths as deprecated only
Do not promote dual-active paths.
If legacy path must remain temporarily, it is read-only/deprecated in policy terms, not an equal active branch.

### Phase 4: switch operational expectation
Operators should now read the system as:
- preopen -> bootstrap + risk + PM signal v2
- open -> trading_hybrid command path
- intraday -> trading_hybrid command path
- housekeeping -> PM history postprocess path

---

## 3. Cutover gates

A rollout is allowed only if all of the following hold:
- planning parity suite passes
- golden decision tests pass
- backtest execution model tests pass
- structured logging fields exist for live and backtest comparison
- current active path is singular and documented
- rollback target is known

Recommended hard gate tests:
- `tests/golden/test_pure_decision_engine.py`
- `tests/golden/test_backtest_execution_model.py`
- `tests/parity/test_live_backtest_parity.py`
- `tests/golden/test_optuna_backtest_runtime.py`

---

## 4. Rollback criteria

Rollback should be triggered when any of these happen after cutover:
- planning drift appears in live-vs-backtest comparison without intentional strategy change
- risk reject behavior changes unexpectedly
- slot/command mapping executes wrong capability
- structured logs are missing correlation keys for incident analysis
- controller path bypasses the active command seam

---

## 5. Rollback target

Rollback target is the last known good active path/configuration prior to cutover.
In practice this means:
- previous deployment image/tag
- previous `schedule_manifest` active-path interpretation
- previous command seam wiring if a new thin ingress change caused regression

Do not rollback blindly.
Tie rollback decision to:
- affected `run_id`
- failing slot
- failing parity/golden evidence

---

## 6. Where to compare during incident response

Compare here, in order:
1. structured live log for the affected `run_id`
2. matching backtest artifact/log for `scenario_id`
3. parity test output for corresponding fixture family
4. golden tests for shared decision engine behavior

If planning and parity are clean, move investigation to execution/broker behavior.
If planning is not clean, stop rollout and fix parity first.

---

## 7. Success criteria

Cutover is considered healthy when:
- live run and chosen backtest run can be compared side-by-side using correlation keys
- active path is singular and explainable
- rollback trigger and rollback target are both explicit
- research runtime stays isolated from live side effects

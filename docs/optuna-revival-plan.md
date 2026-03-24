# Optuna Revival Plan

Goal:
- reconnect parameter search to the trading stack
- keep live runtime clean
- require parity first
- make experiments reproducible

---

## Phase order

### Phase 1: parity first
Required before Optuna revival:
- canonical `shared/domain` planning seam exists
- live/backtest parity tests are passing
- simulated broker and result store exist

This phase is already the gate.
No Optuna should run before this.

### Phase 2: backtest-only Optuna runtime
Implemented in this step.

Optuna now lives only under:
- `backtest_app/optuna/*`

Objective function contract:
- consume historical fixture/data
- call `shared.domain.execution.build_order_plan_from_candidate`
- simulate execution with `backtest_app.simulated_broker`
- emit structured experiment artifacts

Forbidden:
- live controller calls
- live DB writes
- live broker execution

### Phase 3: promotion discipline
Before any tuned parameter set is considered for live promotion:
1. parity tests still pass
2. trial metadata is complete
3. decision engine version is known
4. result is replayable with same seed/data/config
5. promotion is done by explicit config/versioning step, not implicit trial side effects

---

## Why this avoids code pollution

This design keeps exploration out of `live_app`.
No controller/DB/broker path is modified to host optimization loops.
No runtime-mode branching is needed.
No best-score-only artifact is accepted.

---

## Current implementation summary

Files:
- `backtest_app/optuna/models.py`
- `backtest_app/optuna/objective.py`
- `backtest_app/optuna/study_runner.py`
- `backtest_app/optuna/artifacts.py`

Properties:
- backtest-only
- seedable
- artifact-backed
- decision-engine-version tracked
- parity-gated by process, not mixed into live runtime

---

## Promotion boundary

A best trial is **not** a live change.
It is only an experiment artifact.
Operational adoption must happen later through an explicit reviewed change to active config/path.

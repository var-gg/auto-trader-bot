# Experiment Tracking

This step reintroduces parameter search only on top of `backtest_app` after planning parity is in place.

Created:
- `backtest_app/optuna/*`
- `tests/golden/test_optuna_backtest_runtime.py`
- `docs/experiment-tracking.md`
- `docs/optuna-revival-plan.md`

---

## Tracking contract

Every experiment run must record more than a best score.
At minimum, each trial record includes:
- `strategy_version`
- `feature_version`
- `data_window`
- `universe`
- `objective_metric`
- `seed`
- `parameter_set_hash`
- `decision_engine_version`

This ensures trials are reproducible and auditable.

---

## Runtime boundary

Optuna objective execution is restricted to:
- `shared/domain` decision engine
- `backtest_app` historical loader
- `backtest_app` simulated broker

It must not call:
- live controllers
- live DB/session wiring
- live broker implementations

That keeps experiment loops free from live side effects.

---

## Artifacts

Artifacts are written as structured JSON via:
- `backtest_app/optuna/artifacts.py`

Stored payload contains:
- full experiment config
- all trial records
- best trial metadata

This is separate from live tables and separate from operational result stores.

---

## Reproducibility rules

Reproducibility requires:
- deterministic seed
- fixture/historical data path
- explicit strategy and feature versions
- explicit decision engine version string

The study runner seeds:
- Python random
- NumPy
- Optuna sampler

That makes same-seed studies replayable for the same dataset/config.

---

## What is traceable now

For the best trial, we can answer:
- which `strategy_version` produced it
- which `feature_version` it used
- which `data_window` and `universe` it searched over
- which parameter set hash won
- which shared decision-engine version generated plans

That is the minimum contract needed before any experiment result is considered promotable.

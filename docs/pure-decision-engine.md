# Pure Decision Engine

This step extracts pure decision logic into `shared/domain`.

Created packages:
- `shared/domain/signals/*`
- `shared/domain/execution/*`
- `shared/domain/risk/*`

Goal:
- same input -> same decision output
- no DB / HTTP / broker / current-time lookup inside decision functions
- live and backtest should be able to call the same functions

---

## Extracted pure areas

## 1. Signal scoring
File:
- `shared/domain/signals/scoring.py`

Functions:
- `normalize_ranked_candidates(...)`
- `compute_pm_signal(...)`

Source lineage:
- `pm_signal_service._compute_signal`
- `pm_signal_service_v2._compute_signal_v2` core behavior

What is preserved:
- shape/context rerank
- weighted score aggregation
- log-sum-exp evidence comparison
- `signal_1d` and best-target selection
- same `TOO_FEW` / `OK` / `LOW_CONF(...)` logic

What is excluded:
- DB fetch
- ANN search
- config load
- run header insert
- upsert / history write

---

## 2. Candidate selection and sizing
File:
- `shared/domain/execution/ladder.py`

Functions:
- `allocate_symbol_budgets(...)`
- `qty_from_budget(...)`

Source lineage:
- `pm_open_session_service.allocate_symbol_budgets_pm`
- same affordability / granularity / risk-parity style allocation

What is preserved:
- soft cap / hard cap logic
- minimum ladder affordability logic
- priority formula using signal strength and unit risk

What is excluded:
- DB enrich
- logger dependency in core signature
- account/session lookup

---

## 3. Ladder generation
File:
- `shared/domain/execution/ladder.py`

Functions:
- `generate_pm_ladder(...)`
- internal helpers for tick rounding and first-leg logic

Source lineage:
- `premarket/utils/pm_ladder_generator.py`

What is preserved:
- first-leg sizing rules
- TB label / IAE influence
- long recommendation discount easing
- tick-aware spread handling
- quantity split across legs

What is excluded:
- DB calls
- earnings-day lookup
- risk snapshot lookup
- wall-clock usage

---

## 4. Order plan build
File:
- `shared/domain/execution/planning.py`

Functions:
- `build_order_plan_from_candidate(...)`

Purpose:
- convert canonical `SignalCandidate` + budget + tuning into canonical `OrderPlan`

Why this matters:
- same function can feed live executor and backtest simulator
- keeps plan generation independent from persistence / broker submission

---

## 5. Risk transforms and reverse breach decision
File:
- `shared/domain/risk/policy.py`

Functions:
- `apply_buy_risk_multiplier(...)`
- `apply_sell_risk_multiplier(...)`
- `reverse_breach_triggered(...)`

Source lineage:
- buy/sell ladder multiplier transforms in PM and hybrid services
- reverse breach hold-day logic as a pure predicate

What is excluded:
- risk snapshot fetch
- position maturity DB lookup
- external policy loading

---

## 6. Outcome labeling
File:
- `shared/domain/execution/outcomes.py`

Functions:
- `label_outcome_from_pnl_bps(...)`
- `classify_unfilled_reason(...)`

Source lineage:
- `PMHistoryBatchService._label_from_pnl_bps`
- `PMHistoryBatchService._infer_unfilled_reason`

What is preserved:
- WIN / LOSS / FLAT mapping
- timeout / price constraint / quantity / fallback reject classification

---

## Canonical input/output discipline

All extracted functions now operate on:
- canonical models (`SignalCandidate`, `OrderPlan`, `LadderLeg`)
- Python primitives (`dict`, `list`, `float`, `str`)

They do **not** accept:
- SQLAlchemy rows
- FastAPI models
- KIS payload objects
- pandas DataFrames
- DB session handles
- implicit `datetime.now()`

---

## Validation status

Added pure-engine tests:
- `tests/golden/test_pure_decision_engine.py`

They verify:
- domain PM signal scoring matches current golden fixture output
- domain budget allocation + order planning matches current open-plan golden fixture output
- outcome helpers run without external dependencies

This gives a first parity bridge between current services and future extracted engine.

---

## Important limitation

This step intentionally extracts the core **without changing live services yet**.
Existing service code still owns:
- data loading
- DB writes
- scheduler/http orchestration
- broker execution

That is by design.
The current step creates the reusable engine first, while preserving behavior.

---

## What remains for later steps

Still outside pure engine for now:
- PM active-set DB queries
- intraday 5m prediction loaders
- position maturity repository lookups
- broker submission / fill collection
- runbook/controller orchestration

Next migration step should introduce adapter/mapping layers so existing services call into this pure engine instead of holding inline logic.

# Backtest Execution Model

This step adds explicit historical-data loading, simulated execution rules, and a separate backtest result store.

Created/expanded:
- `backtest_app/historical_data/*`
- `backtest_app/simulated_broker/*`
- `backtest_app/results/*`
- `docs/backtest-execution-model.md`

---

## 1. Historical data loader

Files:
- `backtest_app/historical_data/models.py`
- `backtest_app/historical_data/features.py`
- `backtest_app/historical_data/loader.py`

Responsibilities:
- load past bars from file-based source
- derive reusable bar features
- attach external-factor vector to candidate provenance
- supply canonical inputs to planning layer

Current canonical outputs:
- `MarketSnapshot`
- `SignalCandidate[]`
- `HistoricalBar[]`
- derived feature payload in `candidate.provenance`

Important property:
- no live DB/session dependency
- no FastAPI dependency

---

## 2. Simulated broker

Files:
- `backtest_app/simulated_broker/models.py`
- `backtest_app/simulated_broker/engine.py`

Input:
- canonical `OrderPlan`
- historical bars for that symbol
- explicit `SimulationRules`

Output:
- canonical `FillOutcome[]`

Modeled rules:
- fee bps
- slippage bps
- partial fill toggle
- partial fill ratio
- gap fill toggle
- session cutoff mode

This is important because the rules are **configuration**, not hidden service behavior.

### Fill decision model
- if limit is touched inside bar -> fill
- if `allow_gap_fill=true` and order is marketable at open gap -> fill at bar open
- otherwise unfilled at session cutoff

### Partial fills
When enabled and quantity > 1:
- quantity can be reduced according to `partial_fill_ratio`
- outcome marked `PARTIAL`

### Session cutoff
Currently modeled as a rule field (`session_cutoff_mode`) and stored in metadata.
This keeps the simulation contract explicit and replayable.

---

## 3. Separate result store

Files:
- `backtest_app/results/store.py`

Purpose:
- persist backtest results outside live execution tables
- store plans/fills/summary as JSON artifacts

Important guardrail:
- no reuse of live order/fill tables
- no mutation of live DB

---

## 4. Runner integration

Updated file:
- `backtest_app/runner/cli.py`

Current flow:

```text
historical fixture
  -> loader
  -> canonical candidates + derived vectors
  -> shared/domain planner
  -> simulated broker with explicit rules
  -> reporting summary
  -> backtest result store
```

Runner now supports:
- optional `results_dir` for persisted run artifacts

---

## 5. Deterministic replay

Determinism requirement means:
- same `OrderPlan`
- same historical bars
- same `SimulationRules`
- same outputs

Current implementation is deterministic because:
- no random path is used in fill decisions
- rule fields fully determine behavior
- result store writes structured JSON snapshots

This is validated by test coverage.

---

## 6. Why this satisfies constraints

### No live table reuse
Backtest results are written only to `backtest_app/results/store.py` JSON artifacts.
No live order/fill table integration exists here.

### No deep service hardcoding
Simulation logic lives in `SimulatedBroker` and is parameterized by `SimulationRules`.
It is not buried inside live services.

### No note-string-only results
Results are stored as structured:
- `OrderPlan`
- `FillOutcome`
- summary object

These are machine-readable and replayable.

---

## Validation against requested checks

### Deterministic replay for same `OrderPlan`?
Yes.
A dedicated test runs the same plan twice against the same bars and compares serialized fills.

### Fill rules controlled by config?
Yes.
`SimulationRules` explicitly controls gap fill, slippage, partial fill, fees, and session cutoff mode.

---

## Next likely step

Next useful follow-ups:
1. more detailed session cutoff semantics by market/session
2. richer slippage/queue models
3. readonly historical DB adapter in addition to json loader
4. parity fixtures comparing live executor intent vs simulated outcome

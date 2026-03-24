# Backtest App Structure

This step creates a separate `backtest_app` runtime inside the same repo.

Goal:
- keep research/backtest code in the same repository
- keep execution environment separate from Cloud Run/live runtime
- depend directly on `shared/domain`
- avoid direct dependency on FastAPI / live DB session / live broker implementation

Created structure:
- `backtest_app/runner/*`
- `backtest_app/historical_data/*`
- `backtest_app/simulated_broker/*`
- `backtest_app/reporting/*`
- `backtest_app/configs/*`

---

## Runtime separation

`backtest_app` is intentionally separate from `live_app`.

It does **not**:
- import FastAPI controllers
- depend on Cloud Run request handling
- depend on SQLAlchemy session wiring from live path
- call live broker execution code
- update live DB state

It **does**:
- import `shared/domain`
- build canonical `OrderPlan`
- simulate fills into canonical `FillOutcome`
- generate simple summaries/reports

This satisfies the “same repo, different runtime” requirement.

---

## Package roles

## 1. `configs`
Files:
- `backtest_app/configs/models.py`

Contains:
- `BacktestScenario`
- `BacktestConfig`
- `RunnerRequest`

Purpose:
- define scenario/period/market/symbol universe/parameter inputs
- keep runner inputs explicit and serializable

## 2. `historical_data`
Files:
- `backtest_app/historical_data/models.py`
- `backtest_app/historical_data/loader.py`

Purpose:
- provide historical bars and candidate fixtures to the simulator
- currently implemented as JSON file loader
- no live DB dependency

## 3. `simulated_broker`
Files:
- `backtest_app/simulated_broker/engine.py`

Purpose:
- consume canonical `OrderPlan`
- emit canonical `FillOutcome`
- basic fill logic: fill if limit price is touched by historical bar range

## 4. `reporting`
Files:
- `backtest_app/reporting/summary.py`

Purpose:
- summarize plans/fills into simple report objects
- keep output reviewable and script-friendly

## 5. `runner`
Files:
- `backtest_app/runner/cli.py`
- `backtest_app/runner/__main__.py`

Purpose:
- independent CLI/batch entrypoint
- inputs:
  - scenario id
  - market
  - period
  - symbol universe
  - historical fixture path
  - initial capital
  - optional output path

Run example:

```bash
python -m backtest_app.runner \
  --scenario-id demo-us-open \
  --market US \
  --start-date 2026-03-01 \
  --end-date 2026-03-24 \
  --symbols NVDA,AAPL \
  --data tests/fixtures/backtest_historical_fixture.json
```

---

## Shared/domain usage

`backtest_app` directly uses:
- `SignalCandidate`
- `OrderPlan`
- `LadderLeg`
- `FillOutcome`
- `MarketSnapshot`
- `shared.domain.execution.build_order_plan_from_candidate`

This proves a basic simulation can run from canonical domain types only.

---

## Current simulation flow

```text
scenario/config CLI input
  -> JSON historical loader
  -> canonical MarketSnapshot + SignalCandidate list
  -> shared/domain planner -> OrderPlan[]
  -> simulated broker -> FillOutcome[]
  -> reporting summary
```

This is intentionally minimal but complete enough to prove the runtime split.

---

## Guardrails satisfied

### No global RUN_MODE sprawl
No `RUN_MODE` branching was added to existing live code.
`backtest_app` is a separate package/runtime.

### No live DB mutation
Current implementation uses file-based historical input and a local simulator only.
It does not write to live DB.

### No direct live_app dependency
`backtest_app` imports `shared/domain` directly.
It does not import `live_app` controllers, commands, sessions, or broker adapters.

---

## Validation against requested checks

### Can `backtest_app` run independently of Cloud Run?
Yes.
It has its own CLI entrypoint and does not require FastAPI or Cloud Run request context.

### Can basic simulation run using `shared/domain` only?
Yes.
The runner uses canonical `SignalCandidate -> OrderPlan -> FillOutcome` flow without live transport/runtime objects.

---

## Next likely step

After this skeleton, the natural next moves are:
1. richer historical data adapters (csv/parquet/db-readonly)
2. more realistic fill/slippage/fee models
3. parity runner comparing live planner output vs backtest planner output on same fixtures
4. report exports for pnl / label / fill diagnostics

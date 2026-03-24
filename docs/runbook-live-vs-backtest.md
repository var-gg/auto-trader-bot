# Runbook: Live vs Backtest

This runbook defines how to compare live runs and backtest runs after the runtime split.

---

## 1. Required correlation keys

## Live (`live_app`)
Every material live run should emit structured fields including:
- `run_id`
- `slot`
- `command`
- `strategy_version`
- `decision_summary`
- `risk_reject_reason`
- `order_batch_id`
- `order_plan_id`
- `broker_request_id`
- `broker_response_id`

Recommended helper:
- `live_app/observability/structured_logging.py`

## Backtest (`backtest_app`)
Every material backtest run should emit structured fields including:
- `scenario_id`
- `data_range`
- `parameter_hash`
- `score_summary`
- `fill_summary`
- `strategy_version`
- `feature_version`
- `seed`

Recommended helper:
- `backtest_app/observability/structured_logging.py`

Without these keys, comparison and rollback investigation become guesswork.

---

## 2. Comparison procedure

Goal:
- line up one live run and one comparable backtest run
- compare decision behavior before discussing execution differences

### Step A: identify the live run
Collect from logs:
- `run_id`
- `slot`
- `command`
- `strategy_version`
- `order_plan_id`
- `risk_reject_reason`

### Step B: identify the backtest counterpart
Collect from artifacts/logs:
- `scenario_id`
- `data_range`
- `parameter_hash`
- `strategy_version`
- `feature_version`
- `seed`

### Step C: compare in this order
1. planning parity
   - symbol
   - side
   - ladder leg count
   - quantity split
   - decision metadata
   - risk reject / skip reason
2. broker/execution context
   - only after planning parity is understood
   - compare broker request/response ids and live execution evidence
3. result summary
   - live realized outcome vs backtest simulated outcome should be discussed separately

Important:
- do not treat live fill and simulated fill as identical signals
- compare planning first, execution second

---

## 3. What to do when a discrepancy appears

### Case 1: planning drift
Symptoms:
- different ladder
- different quantity split
- different skip reason
- different policy version/decision metadata

Actions:
1. run parity tests
2. inspect `tests/parity/*` fixture coverage
3. inspect `shared/domain` planning seam and normalization path
4. block cutover/promotion until parity is restored or intentionally updated

### Case 2: planning same, execution different
Symptoms:
- order plan is same
- live fills differ from simulated fills

Actions:
1. inspect broker request/response ids
2. inspect simulated broker rules used in backtest
3. document whether difference is due to slippage/gap/session timing/live venue behavior
4. do not call this a parity failure unless planning also drifted

### Case 3: risk gate mismatch
Symptoms:
- live rejects, backtest plans
- or backtest skips, live places plan

Actions:
1. compare `risk_reject_reason`
2. verify strategy version and feature version
3. verify fixture/data window alignment
4. if live path bypassed command seam, treat as migration bug

---

## 4. Failure recovery guidance

When live behavior looks wrong after refactor:
1. freeze further rollout
2. identify latest affected `run_id`
3. compare against nearest matching backtest `scenario_id`
4. run parity/golden suite
5. if active-path regression is confirmed, revert to prior active path/config
6. keep artifacts/logs for the incident window

---

## 5. Minimum gate before saying "investigation complete"

You should be able to answer:
- which live `run_id` is affected
- which slot/command executed
- which strategy version was active
- which backtest scenario was used for comparison
- whether the mismatch is planning drift or execution drift
- whether rollback is required

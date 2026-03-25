# Rollback Runbook

Created: 2026-03-25
Purpose: define exact rollback conditions and operator actions for TO-BE canary / cutover.

## Core rule
If a rollback trigger fires, revert to the previous runtime immediately.
Do not continue the experiment to 'gather more data'.

## Immediate rollback triggers

### 1) Order generation omission
Condition:
- expected candidate/plan should have produced an order plan, but TO-BE produced none without an already-approved skip reason.

Action:
- stop canary expansion
- switch scheduler/manual traffic back to AS-IS runtime
- preserve structured logs and comparison report

### 2) Duplicate-order risk
Condition:
- same symbol/slot/run can produce duplicate live submit intent, or pending-order readers fail to suppress a repeated submit risk.

Action:
- immediate rollback
- freeze further canary attempts until root cause is explained and tested

### 3) Fill collection failure
Condition:
- fill/reconcile stage errors, or next-cycle cannot determine correct pending/filled state from TO-BE write path.

Action:
- immediate rollback
- run fill/reconcile health check on AS-IS runtime
- compare `broker_order` / `order_fill` chain before any retry

### 4) Follow-up cycle decision anomaly
Condition:
- next cycle makes a materially different decision because pending/fill/maturity/risk state is broken or stale.

Examples:
- pending order not seen
- matured position not recognized
- stale risk snapshot treated as active
- duplicated sell/reduce because prior state was missed

Action:
- immediate rollback
- mark canary failed

### 5) PM signal/history gap
Condition:
- `run_id` chain missing,
- `pm_signal_run_header` / `pm_candidate_decision_history` / `pm_order_execution_history` / `pm_outcome_tplus_history` linkage breaks,
- or postprocess batch cannot run cleanly after canary.

Action:
- immediate rollback
- suspend expansion until provenance chain is repaired

### 6) Broker correlation gap
Condition:
- structured logs or DB writes do not retain enough ids to correlate request -> plan -> broker intent -> fill/reconcile.

Action:
- rollback
- do not widen canary until forensic traceability is restored

## Rollback actions
1. stop directing canary traffic to TO-BE runtime
2. re-enable previous AS-IS runtime for the affected slot/market only
3. confirm scheduler/manual route target is back on prior runtime
4. run one health verification on AS-IS path
5. freeze further TO-BE widening
6. produce incident note with:
   - affected run_id / slot / market
   - trigger fired
   - expected vs actual
   - data preserved (logs / batch ids / plan ids / broker ids)

## Data to preserve on rollback
- `live_run` structured log
- request timestamp / route
- order batch / plan / broker ids
- fill/reconcile summary
- PM history/postprocess summary
- AS-IS comparison note

## Comparison report requirement
Every canary must have a side-by-side report against AS-IS expectations.
Minimum fields:
- run_id
- slot
- command
- selected candidates
- order plan
- broker ids
- fill/reconcile result
- final status
- rollback trigger fired? yes/no

## Operator note
Rollback criteria are fixed before the experiment.
Do not loosen them during canary.
Do not combine rollback handling with strategy changes, Optuna reruns, or schedule edits.

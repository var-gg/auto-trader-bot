# Cutover Gates

Created: 2026-03-25
Purpose: define objective gates that must pass before widening canary or performing runtime cutover.

## Rule zero
No full cutover before these gates pass.
No strategy improvement work in parallel with canary.

## Gate set

### Gate 1 — Ingress parity
Must already be true:
- live routes still accept existing scheduler/manual triggers
- route/method/auth/required params documented
- route-to-command dispatch traceable

Evidence:
- `docs/route-and-scheduler-parity.md`
- `tests/contracts/test_live_routes_*.py`

### Gate 2 — Decision parity
Must already be true:
- representative replay-derived fixtures produce same semantic decision meaning
- diff output is readable when drift occurs

Evidence:
- `docs/decision-parity-rules.md`
- `tests/parity/test_decision_parity_*.py`

### Gate 3 — External adapter parity
Must already be true:
- broker outbound request meaning preserved
- failure mapping not silently swallowed
- fill/read adapters preserve request contracts

Evidence:
- `docs/external-io-parity.md`
- `tests/integration/test_broker_adapter_parity_*.py`
- `tests/integration/test_marketdata_adapter_contract_*.py`

### Gate 4 — DB write/read parity
Must already be true:
- order/broker/fill chain preserves next-cycle reads
- PM history/outcome writes remain viable
- risk snapshot TTL semantics remain viable

Evidence:
- `docs/db-write-read-parity.md`
- `tests/parity/test_db_side_effect_parity_*.py`

### Gate 5 — Shadow E2E parity
Must already be true:
- representative scheduler sequence replays end-to-end without real broker orders
- bootstrap -> trading -> postprocess chain completes
- run sequence and semantic anchors are explainable

Evidence:
- `docs/shadow-e2e-plan.md`
- `docs/shadow-run-result-report.md`
- `tests/e2e/test_shadow_replay_*.py`

### Gate 6 — Small canary logging completeness
Before any live-money canary, verify structured logs include:
- `run_id`
- `slot`
- `command`
- `selected candidates` (in canary report even if not all are in one log field)
- `order plan`
- `broker_request_id`
- `broker_response_id`
- fill/reconcile result
- final status

Pass condition:
- one broker-safe live canary run is fully traceable side-by-side against AS-IS.

### Gate 7 — No critical rollback trigger during broker-safe canary
Pass condition:
- zero missing-order events
- zero duplicate-order risk events
- zero fill-collection hard failures
- zero next-cycle decision anomalies
- zero PM signal/history linkage gaps

Interpretation rule:
- a runtime stop/skip/error can still be **correct behavior** if it matches market-hour / holiday / preflight / broker-safe expectations
- only unexplained or semantically wrong failures count as gate failures

### Gate 8 — Smallest-money live canary clean
Pass condition:
- no immediate rollback trigger fires
- fill/reconcile succeeds
- next-cycle read path agrees with expected state
- AS-IS comparison report marks any drift as explainable and non-material

## Immediate no-go items
Any one of these blocks expansion:
- route ingress break
- decision drift with money-going meaning change
- duplicate-order risk not ruled out
- broker correlation ids missing
- fill collection failure
- PM run / history linkage missing
- next-cycle pending/maturity/outcome logic disagrees with expectation

## Expansion permission
Widening scope is allowed only when:
1. the current gate is passed,
2. the result report is written,
3. rollback path remains ready,
4. no other concurrent strategy/config experiments are running.

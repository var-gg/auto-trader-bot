# Source of Truth

Prefer evidence over inference.
When documents and code disagree, identify the conflict first and gather evidence before choosing a winner.

## Global Precedence
1. `AGENTS.md`
2. `docs/harness/*`
3. Lane-specific runtime docs in `docs/*`
4. Replay fixtures and evidence under `tests/replay_fixtures/` and `runs/`
5. Code
6. Historical notes and one-off reports only when newer truth is missing

## Lane Read Order

### `live-cutover`
1. [../cutover-gates.md](../cutover-gates.md)
2. [../route-and-scheduler-parity.md](../route-and-scheduler-parity.md)
3. [../decision-parity-rules.md](../decision-parity-rules.md)
4. [../db-write-read-parity.md](../db-write-read-parity.md)
5. [../external-io-parity.md](../external-io-parity.md)
6. [../live-path-gap-list.md](../live-path-gap-list.md)

### `shadow-replay`
1. [../shadow-e2e-plan.md](../shadow-e2e-plan.md)
2. [../shadow-run-result-report.md](../shadow-run-result-report.md)
3. [../runtime-replay-corpus.md](../runtime-replay-corpus.md)
4. [../structured-logging-and-metrics.md](../structured-logging-and-metrics.md)

### `research-discovery`
1. [../research_run_protocol.md](../research_run_protocol.md)
2. [../local-backtest-postgres.md](../local-backtest-postgres.md)
3. [../experiment-tracking.md](../experiment-tracking.md)
4. [../runbook-local-backtest.md](../runbook-local-backtest.md)

### `promotion`
1. [promotion-gate.md](promotion-gate.md)
2. [../research_run_protocol.md](../research_run_protocol.md)
3. [../shadow-e2e-plan.md](../shadow-e2e-plan.md)
4. [../cutover-gates.md](../cutover-gates.md)
5. [../recommended-next-steps.md](../recommended-next-steps.md)

## Conflict Rules
- If code appears ahead of docs, record a doc freshness issue and verify with tests or fixtures.
- If docs appear ahead of code, record an evidence or implementation gap instead of assuming the code already does it.
- If production truth is missing, classify it as `evidence_gap`, not success.

## Readiness Hint
If you cannot name the owning lane and the first three source documents you read, you do not have enough context to change behavior safely.

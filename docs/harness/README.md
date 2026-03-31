# Harness Overlay

This directory is the agent-readable overlay for the repo.
It does not replace the existing cutover, shadow, or research documents.
It tells an agent what lane a task belongs to, what evidence is required, how failures are classified, and how research results become live candidates.

## Read Order
1. [north-star.md](north-star.md)
2. [runtime-lanes.md](runtime-lanes.md)
3. [task-taxonomy.md](task-taxonomy.md)
4. [source-of-truth.md](source-of-truth.md)
5. [evidence-contract.md](evidence-contract.md)
6. [failure-attribution.md](failure-attribution.md)
7. [promotion-gate.md](promotion-gate.md)
8. [checklists.md](checklists.md)
9. [doc-gardening.md](doc-gardening.md)

## Existing Source Documents
- Live cutover and gate status start with [../cutover-gates.md](../cutover-gates.md), [../cutover-summary.md](../cutover-summary.md), and [../route-and-scheduler-parity.md](../route-and-scheduler-parity.md).
- Shadow replay truth starts with [../shadow-e2e-plan.md](../shadow-e2e-plan.md), [../shadow-run-result-report.md](../shadow-run-result-report.md), and [../runtime-replay-corpus.md](../runtime-replay-corpus.md).
- Research truth starts with [../research_run_protocol.md](../research_run_protocol.md), [../local-backtest-postgres.md](../local-backtest-postgres.md), and [../experiment-tracking.md](../experiment-tracking.md).
- Known live truth gaps start with [../live-path-gap-list.md](../live-path-gap-list.md).

## Overlay Role
- Make runtime lanes explicit.
- Make task classification explicit.
- Make failure attribution explicit.
- Make promotion gates explicit.
- Keep output and evidence expectations uniform across tasks.

## What This Overlay Does Not Do
- It does not rewrite the existing live routes or scheduler topology.
- It does not change broker execution behavior.
- It does not promote research findings by backtest performance alone.

## Schema and Test Hooks
- Cross-lane artifact schemas live under [../../evals/schemas/failure_attribution.schema.json](../../evals/schemas/failure_attribution.schema.json) and [../../evals/schemas/scorecard.schema.json](../../evals/schemas/scorecard.schema.json).
- Mechanical checks live in [../../tests/harness/test_docs_index_consistency.py](../../tests/harness/test_docs_index_consistency.py) and [../../tests/harness/test_failure_taxonomy_schema.py](../../tests/harness/test_failure_taxonomy_schema.py).

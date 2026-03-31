# Runtime Lanes

This repo uses four runtime lanes.
Evidence is only comparable within a lane unless the promotion flow explicitly connects them.

## `live-cutover`
- Purpose: preserve AS-IS money-going meaning while moving toward the TOBE runtime boundary.
- Allowed work: route parity, decision parity, adapter parity, DB parity, broker-safe canary preparation.
- Required evidence: route contract, replay-derived parity, DB side-effect parity, structured log completeness, rollback readiness.
- Non-goals: strategy tuning, parameter sweeps, speculative refactors, replay-free claims of equivalence.
- Read first: [../cutover-gates.md](../cutover-gates.md), [../route-and-scheduler-parity.md](../route-and-scheduler-parity.md), [../db-write-read-parity.md](../db-write-read-parity.md)

## `shadow-replay`
- Purpose: reproduce live orchestration without real broker side effects.
- Allowed work: deterministic scheduler replay, paper or no-op broker identifiers, side-effect-free end-to-end orchestration checks.
- Required evidence: replay anchors, stage ordering, semantic parity at slot level, correlation ids, isolated environment assumptions.
- Non-goals: deployed staging claims that do not exist, live broker writes, strategy discovery.
- Read first: [../shadow-e2e-plan.md](../shadow-e2e-plan.md), [../shadow-run-result-report.md](../shadow-run-result-report.md), [../runtime-replay-corpus.md](../runtime-replay-corpus.md)

## `research-discovery`
- Purpose: use `backtest_app` to explore strategy, feature, and parameter changes in a reproducible local or mirror-only environment.
- Allowed work: mirror-only TOBE runs, legacy reference comparison, matrix batches, Optuna or parameter search, holdout analysis.
- Required evidence: `manifest.json`, `run_card.json`, `fold_report.json`, `diagnostics.json`, `report.md`, `leaderboard.csv`, reproducibility fields.
- Non-goals: live DB mutation, live broker execution, live canary claims, skipping holdout or reproducibility checks.
- Read first: [../research_run_protocol.md](../research_run_protocol.md), [../local-backtest-postgres.md](../local-backtest-postgres.md), [../experiment-tracking.md](../experiment-tracking.md)

## `promotion`
- Purpose: connect research evidence to live eligibility without collapsing discovery into cutover.
- Allowed work: replay-anchor comparison, shadow eligibility checks, canary packet assembly, evidence review.
- Required evidence: backtest reproducibility, holdout review, replay-anchor decision parity, shadow replay pass, canary eligibility packet.
- Non-goals: direct live promotion from backtest metrics alone, concurrent strategy tuning during canary, widening without gate evidence.
- Read first: [promotion-gate.md](promotion-gate.md), [../cutover-gates.md](../cutover-gates.md), [../recommended-next-steps.md](../recommended-next-steps.md)

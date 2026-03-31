# Task Taxonomy

Every task should be classified before any change is made.

## `live-parity`
- Primary lane: `live-cutover`
- Typical inputs: replay fixtures, route contracts, adapter requests, DB state expectations.
- Required invariants: external route meaning unchanged, money-going behavior unchanged, next-cycle reads still explainable.
- Forbidden side effects: strategy tuning, hidden live-path rewires.
- Default validation: golden, parity, contract, and DB parity checks.
- Reference docs: [../parity-test-policy.md](../parity-test-policy.md), [../decision-parity-rules.md](../decision-parity-rules.md)

## `shadow-replay`
- Primary lane: `shadow-replay`
- Typical inputs: replay corpus, deterministic scheduler sequence, paper broker ids.
- Required invariants: no real broker side effects, same orchestration order, explainable semantic anchors.
- Forbidden side effects: production DB reuse, live credential fallback, strategy exploration.
- Default validation: shadow replay tests and correlation checks.
- Reference docs: [../shadow-e2e-plan.md](../shadow-e2e-plan.md), [../shadow-run-result-report.md](../shadow-run-result-report.md)

## `research-run`
- Primary lane: `research-discovery`
- Typical inputs: mirror-only local Postgres, strategy mode, parameter set, seed, legacy reference scenarios.
- Required invariants: local or mirror-only data path, reproducibility fields present, holdout evidence retained.
- Forbidden side effects: live table mutation, live broker calls, cutover claims.
- Default validation: TOBE smoke, legacy reference, matrix batch in that order.
- Reference docs: [../research_run_protocol.md](../research_run_protocol.md), [../local-backtest-postgres.md](../local-backtest-postgres.md)

## `promotion-eval`
- Primary lane: `promotion`
- Typical inputs: research run outputs, replay anchors, shadow evidence, canary packet data.
- Required invariants: research and live evidence remain distinct, replay-anchor parity is checked before canary, canary remains narrow.
- Forbidden side effects: skipping shadow or replay, direct live uplift from performance metrics only.
- Default validation: promotion gate review plus lane-specific evidence packet checks.
- Reference docs: [promotion-gate.md](promotion-gate.md), [../cutover-gates.md](../cutover-gates.md)

## `harness-fix`
- Primary lane: choose the blocked lane, but report under harness work.
- Typical inputs: stale tests, missing plugins, brittle fixtures, broken schema or artifact writers.
- Required invariants: failure is classified before it is fixed, product regression claims stay separate.
- Forbidden side effects: presenting harness repair as strategy or runtime improvement.
- Default validation: failing test reproduction, targeted rerun, updated failure attribution.
- Reference docs: [failure-attribution.md](failure-attribution.md), [../parity-gate-failures-20260325.md](../parity-gate-failures-20260325.md)

## `docs-only`
- Primary lane: whichever lane the documentation belongs to, or overlay-only if it spans lanes.
- Typical inputs: stale documentation, missing cross-links, source-of-truth conflicts.
- Required invariants: documents stay consistent with current tested behavior and known gaps.
- Forbidden side effects: silent behavior claims without evidence.
- Default validation: docs consistency tests and cross-link review.
- Reference docs: [doc-gardening.md](doc-gardening.md), [../live-path-gap-list.md](../live-path-gap-list.md)

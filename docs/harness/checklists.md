# Checklists

Use these as flat completion checks, not as substitutes for evidence.

## `harness-fix`
- Classify the failure before editing anything.
- Name the blocked lane.
- Reproduce the failure with the smallest targeted command.
- Fix the harness or environment issue.
- Rerun the same targeted check.
- Update failure attribution with the new result.
- Record the next smallest step if product evidence is still missing.
- See also: [failure-attribution.md](failure-attribution.md), [../parity-gate-failures-20260325.md](../parity-gate-failures-20260325.md)

## `research-run`
- Confirm mirror-only or legacy-reference path intentionally.
- Record `strategy_version`, `feature_version`, `decision_engine_version`, `parameter_hash`, and `seed`.
- Run TOBE smoke before legacy reference.
- Preserve `manifest.json`, `run_card.json`, `fold_report.json`, `diagnostics.json`, `report.md`, and `leaderboard.csv`.
- Add `scorecard.json` and `failure_attribution.json` when cross-lane review is needed.
- Keep holdout evidence with the run.
- See also: [../research_run_protocol.md](../research_run_protocol.md), [../local-backtest-postgres.md](../local-backtest-postgres.md)

## `promotion-eval`
- Confirm the candidate run is reproducible.
- Review holdout behavior.
- Pick replay anchors and compare decision meaning.
- Confirm shadow replay evidence exists.
- Check unresolved live truth gaps.
- Build the canary eligibility packet.
- Stop if strategy tuning is still active.
- See also: [promotion-gate.md](promotion-gate.md), [../cutover-gates.md](../cutover-gates.md)

## `live-parity`
- Confirm the task belongs to `live-cutover`.
- Read the active cutover docs first.
- Extract invariants before changing code.
- Compare route, decision, adapter, and DB meaning as needed.
- Treat unexplained drift as a blocker.
- Keep rollback readiness visible.
- See also: [../cutover-gates.md](../cutover-gates.md), [../route-and-scheduler-parity.md](../route-and-scheduler-parity.md)

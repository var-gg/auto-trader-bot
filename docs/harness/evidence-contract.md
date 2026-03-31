# Evidence Contract

Meaningful work should leave evidence that another agent can read without replaying the entire history.

## Common Envelope
All lanes should be able to report:
- `task_id`
- `lane`
- `outcome`
- `touched_files`
- `tests_run`
- `artifacts`
- `next_gate`

Recommended additions when available:
- `task_class`
- `run_id`
- `files_read`
- `invariants`
- `remaining_risk`

## Artifact Root
- Future cross-lane outputs should use `runs/`.
- This patch does not create a new top-level `artifacts/` directory.
- Research outputs stay where the current research ledger already writes them.
- New cross-lane files should be written next to existing lane outputs, not in a second parallel tree.

## Research-Discovery Files
Research runs keep the existing required files from [../research_run_protocol.md](../research_run_protocol.md):
- `manifest.json`
- `run_card.json`
- `fold_report.json`
- `diagnostics.json`
- `report.md`
- `leaderboard.csv`

Optional cross-lane additions:
- `scorecard.json`
- `failure_attribution.json`

## Live-Cutover and Shadow-Replay Files
When a live or shadow task produces reusable evidence, prefer:
- `scorecard.json`
- `failure_attribution.json`
- `report.md`

If a lane already emits a richer report, keep that report and add the cross-lane files only when they improve readability.

## Lane-Specific Required Keys

### Shared keys
- `task_id`
- `lane`
- `outcome`
- `tests_run`
- `artifacts`
- `next_gate`

### `live-cutover`
- `slot`
- `command`
- `strategy_version`
- `order_batch_id` when present
- `order_plan_id` when present
- `broker_request_id` when present
- `broker_response_id` when present

### `shadow-replay`
- `replay_anchor_ids`
- `slot_sequence`
- `paper_broker_ids`

### `research-discovery`
- `strategy_version`
- `feature_version`
- `decision_engine_version`
- `parameter_hash`
- `seed`
- `data_range`
- `scenario_id`

### `promotion`
- `candidate_run_ids`
- `replay_anchor_ids`
- `shadow_run_ids`
- `canary_packet_path` when created

## Related References
- Logging contract: [../structured-logging-and-metrics.md](../structured-logging-and-metrics.md)
- Research run ledger: [../research_run_protocol.md](../research_run_protocol.md)
- Live vs backtest correlation: [../runbook-live-vs-backtest.md](../runbook-live-vs-backtest.md)

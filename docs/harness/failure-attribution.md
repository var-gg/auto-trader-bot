# Failure Attribution

Do not collapse all failures into pass or fail.
Classify the failure before deciding what to fix or rerun.

## Failure Classes

### `product_regression`
- Meaning: the code or strategy behavior is wrong for the intended task.
- Typical signals: decision parity drift with money-going meaning changes, route behavior changed semantically, next-cycle reads no longer match expectations.
- Rerun policy: `do_not_rerun_until_code_or_config_changes`

### `harness_bug`
- Meaning: the test, fixture, schema, monkeypatch, or artifact writer is wrong.
- Typical signals: `NameError` in test code, stale monkeypatch target, broken schema validation, brittle fixture setup.
- Rerun policy: `fix_harness_then_rerun`

### `environment_issue`
- Meaning: the runtime context is wrong or incomplete.
- Typical signals: missing plugin, missing secret, invalid shell context, placeholder env values, wrong DB target.
- Rerun policy: `fix_environment_then_rerun`

### `evidence_gap`
- Meaning: the truth needed to make a claim does not exist yet.
- Typical signals: missing scheduler anchor, missing replay corpus, unresolved live-path truth, unproven fill-collection ingress.
- Rerun policy: `collect_evidence_then_rerun`

### `task_spec_gap`
- Meaning: the task or acceptance criteria were underspecified or contradictory.
- Typical signals: required evidence shape unclear, unknown target behavior, conflicting docs with no resolution path.
- Rerun policy: `clarify_task_then_rerun`

## Blocker Rules
- `product_regression` blocks the lane until explained or fixed.
- `harness_bug` blocks confidence, not product quality claims.
- `environment_issue` blocks local or CI verdicts until the environment is repaired.
- `evidence_gap` blocks promotion or equivalence claims.
- `task_spec_gap` blocks execution when a reasonable assumption would be unsafe.

## Rerun Guidance
- Do not rerun `product_regression` blindly.
- Fix the harness before using its output as product evidence.
- Fix the environment before using the lane for acceptance.
- Collect the missing evidence before declaring parity or promotion.
- Clarify the task before widening scope.

## Current Known Mapping from 2026-03-25
- Decision parity drift in [../parity-gate-failures-20260325.md](../parity-gate-failures-20260325.md): `product_regression`
- DB side-effect parity `NameError`: `harness_bug`
- Premarket route contract stale target or dispatch mismatch: `harness_bug` until dispatch truth says otherwise, then reclassify if needed
- Shadow replay async plugin gap: `environment_issue`
- Fill collection scheduler truth and PM risk anchor gaps in [../live-path-gap-list.md](../live-path-gap-list.md): `evidence_gap`

## Required Report Fields
Every failure report should name:
- `failure_class`
- `summary`
- `evidence_refs`
- `rerun_policy`
- `remaining_risk`
- `next_smallest_step`

## Related References
- Known failures: [../parity-gate-failures-20260325.md](../parity-gate-failures-20260325.md)
- Live truth gaps: [../live-path-gap-list.md](../live-path-gap-list.md)
- Runtime env blocker example: [../test-env-blocker-20260325.md](../test-env-blocker-20260325.md)

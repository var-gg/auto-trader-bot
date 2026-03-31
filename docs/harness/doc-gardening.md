# Doc Gardening

Deep docs are only useful if agents can trust them.

## Freshness Rules
- When a new fact changes behavior, update the owning source-of-truth doc in the same change.
- If a test exposes stale docs, either update the docs or record an explicit evidence gap.
- If a doc claims a path is active, there must be route, scheduler, replay, or artifact evidence behind that claim.

## Stale-Doc Triggers
- Route or command dispatch changed.
- Required evidence file names changed.
- Failure classes or rerun policy changed.
- Replay anchor set changed materially.
- Live truth gaps were closed or newly discovered.

## Mechanical Checks
- `AGENTS.md` must link to `docs/harness/README.md`.
- `docs/harness/README.md` must index every overlay doc.
- Every overlay doc must link at least one pre-existing source-of-truth doc in `docs/`.
- Schema changes should be paired with a harness test update.

## Conflict Handling
- If docs and tests disagree, do not silently prefer one.
- Record whether the issue is a `harness_bug`, `evidence_gap`, or `task_spec_gap`.
- Fix the owning document or test in the same patch whenever possible.

## Writing Rule
- Keep `AGENTS.md` short.
- Keep the overlay focused on classification and evidence.
- Keep deep operational detail in the existing source documents such as [../cutover-gates.md](../cutover-gates.md), [../shadow-e2e-plan.md](../shadow-e2e-plan.md), and [../research_run_protocol.md](../research_run_protocol.md).

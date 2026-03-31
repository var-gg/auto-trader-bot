# Promotion Gate

Research output becomes a live candidate only after explicit evidence gates.
Backtest performance alone is never enough.

## Required Sequence
1. Backtest reproducibility pass
2. Holdout and robustness review
3. Replay-anchor decision parity
4. Shadow replay pass
5. Canary eligibility packet
6. Smallest-money canary
7. Widening

## Gate Details

### 1. Backtest reproducibility pass
- Confirm the run is repeatable with recorded `strategy_version`, `feature_version`, `decision_engine_version`, `parameter_hash`, and `seed`.
- Confirm the research ledger contains the required files from [../research_run_protocol.md](../research_run_protocol.md).

### 2. Holdout and robustness review
- Confirm holdout behavior is not materially worse than the chosen baseline.
- Record any sensitivity or calibration concerns before leaving the research lane.

### 3. Replay-anchor decision parity
- Choose representative replay anchors from [../runtime-replay-corpus.md](../runtime-replay-corpus.md).
- Show that the promotable candidate matches the intended decision meaning at the anchor level.
- If a candidate needs a planned semantic change, document it before proceeding.

### 4. Shadow replay pass
- Reproduce the orchestration in shadow without broker side effects.
- Confirm the replay sequence, correlation ids, and slot-level semantics are still explainable.
- Use [../shadow-e2e-plan.md](../shadow-e2e-plan.md) as the minimum shadow contract.

### 5. Canary eligibility packet
- Create a compact packet that names the research run ids, replay anchors, shadow evidence, expected live slot, rollback contact points, and known risks.
- Do not widen scope while this packet is incomplete.

### 6. Smallest-money canary
- Run the narrowest live canary that exercises the intended slot and keeps rollback trivial.
- Compare against [../cutover-gates.md](../cutover-gates.md) and the current canary templates.

### 7. Widening
- Widen only after the current gate passes, the result report is written, and there are no unresolved rollback triggers.

## Hard Holds
- No strategy tuning in parallel with canary or cutover.
- No widening when replay or shadow evidence is missing.
- No live uplift based on a leaderboard row alone.

## Required Inputs to Promotion Review
- Research ledger files
- `scorecard.json`
- `failure_attribution.json`
- replay anchor references
- shadow evidence
- live truth-gap check against [../live-path-gap-list.md](../live-path-gap-list.md)

## Related References
- Research protocol: [../research_run_protocol.md](../research_run_protocol.md)
- Cutover gates: [../cutover-gates.md](../cutover-gates.md)
- Recommended next steps: [../recommended-next-steps.md](../recommended-next-steps.md)

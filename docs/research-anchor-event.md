# Improved anchor/event research logic

Scope: research/backtest seam only. Live PM path is unchanged.

## North star
- The research target is not a sure-win trigger.
- The target is a weak but repeatable edge where some anchor families produce a meaningfully shaped forward distribution.
- A family is still research-worthy even if only a minority of anchors produce narrow or directional mixtures.
- Pre-Optuna failure does not mean "no signal exists." It can also mean the surfaced distribution is still an artifact of prototype collapse.

## Outcome horizon
- Canonical outcome horizon remains `5 trading days`.
- Entry anchor is still the first bar open in the forward evaluation window.
- Triple-barrier rules stay in force:
  - upper barrier: `entry * (1 + target_return_pct)`
  - lower barrier: `entry * (1 - stop_return_pct)`
  - fallback: `HORIZON_CLOSE`
- We also surface early outcome diagnostics to avoid degenerating into a pure 1-day optimization problem:
  - `close_return_d2_pct`
  - `close_return_d3_pct`
  - `resolved_by_d2`
  - `resolved_by_d3`

## Labeling rules
- Use forward event windows with triple-barrier + horizon close.
- Explicit non-directional classes:
  - `NO_TRADE` when no usable future window exists
  - `AMBIGUOUS` when upper/lower barrier are hit in the same bar and sequence cannot be inferred reliably

Derived diagnostics:
- `MAE` / `MFE`
- `days_to_hit`
- `after_cost_return_pct`
- `quality_score`
- early-day diagnostics for `D+2` and `D+3`

## Prototype role
- Anchors are grouped by `(anchor_code, side)`.
- Within group, embeddings above `dedup_similarity_threshold` can still be collapsed into a `StatePrototype`.
- But `StatePrototype` is now a coarse retrieval helper, not the final distribution unit.
- Final `q10/q50/q90`, `ESS`, `uncertainty`, `lower_bound`, and consensus telemetry must be computed from prototype member lineage or raw-event members, not from the prototype aggregate alone.
- This separation matters because a narrow forecast built from one representative prototype is not enough evidence for Optuna.

## Compression audit
- Every event-memory build should record whether raw event mass is thin or whether prototype compression is the real bottleneck.
- Required diagnostics:
  - `event_record_count`
  - `prototype_count`
  - `compression_ratio`
  - cluster-size distribution
  - regime/sector prototype distribution
- Official artifacts:
  - `prototype_compression_audit.json`
  - `prototype_compression_table.csv`

## Retrieval and scoring
- Prototype retrieval is coarse:
  - cosine-ranked prototype neighborhood
  - prototype reservoir only narrows the search region
- Member retrieval is decisive:
  - expand `prototype_membership.lineage`
  - re-rank members directly in transformed-feature space
  - dedupe only exact `(symbol, event_date, side)` duplicates
- Final member weight:

`member_weight = member_kernel * prototype_support_prior * member_freshness_prior * context_alignment`

Where:
- `member_kernel = exp(kernel_temperature * (member_similarity - 1.0))`
- `prototype_support_prior = 0.55 + 0.45 * min(1.0, prototype_decayed_support / 5.0)`
- `member_freshness_prior = 1 / (1 + age_days / 30)`
- `context_alignment = 0.40 + 0.60 * max(regime_alignment, sector_alignment)`

## Failure mode to watch
- `single_prototype_echo` means the surfaced distribution is still effectively dominated by one member/prototype lineage.
- That is a valid blocker for Optuna because it implies policy search would optimize an echo artifact instead of a repeatable mixture.
- It is not proof that the anchor family itself has no value.

## ANN seam
ANN is intentionally separated from business scoring.
Current split:
- repository: `AnchorSearchRepository`
- ranking/index seam: `CandidateIndex`
- current implementation: `ExactCosineCandidateIndex`

This keeps future ANN replacements independent from labeling and mixture logic.

## Persistence / artifacts
- canonical results are intended for `bt_result.*`
- diagnostic artifacts can be written separately as JSON/CSV under research artifact storage
- this preserves both reproducibility and inspection friendliness

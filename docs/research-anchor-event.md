# Improved anchor/event research logic

Scope: research/backtest seam only. Live PM path is unchanged.

## Labeling rules
- Use forward event windows with triple-barrier + horizon close.
- Entry anchor: first bar open in the forward evaluation window.
- Upper barrier: `entry * (1 + target_return_pct)`
- Lower barrier: `entry * (1 - stop_return_pct)`
- Horizon close fallback: if neither barrier is hit first, label as `HORIZON_CLOSE`.
- Explicit non-directional classes:
  - `NO_TRADE` when no usable future window exists
  - `AMBIGUOUS` when upper/lower barrier are hit in the same bar and sequence cannot be inferred reliably

Derived diagnostics:
- `MAE` / `MFE`
- `days_to_hit`
- `after_cost_return_pct`
- `quality_score`

## Prototype / dedup rule
- Anchors are grouped by `(anchor_code, side)`.
- Within group, embeddings with cosine similarity above `dedup_similarity_threshold` are collapsed into one prototype.
- Prototype representative prefers higher `anchor_quality`, then higher `liquidity_score`.
- No per-config physical tables; versioning stays in canonical columns.

## Scoring formula
First implementation uses exact cosine + interpretable additive terms:

`score = 0.45*similarity + 0.25*anchor_quality + 0.15*regime_match + 0.10*sector_match + 0.05*liquidity_score`

Where:
- `similarity`: exact cosine similarity via numpy
- `anchor_quality`: quality from historical label outcomes
- `regime_match`: 1 if candidate regime matches query regime, else 0
- `sector_match`: 1 if sector matches query sector, else 0
- `liquidity_score`: normalized liquidity quality term

Filtering:
- minimum liquidity threshold
- optional strict sector match

## ANN seam
ANN is intentionally separated from business scoring.
Current split:
- repository: `AnchorSearchRepository`
- ranking/index seam: `CandidateIndex`
- current implementation: `ExactCosineCandidateIndex`

So later replacement can swap in:
- pgvector ANN
- HNSW / IVF
- FAISS / ScaNN / service-backed ANN

without changing labeling or additive score composition.

## Persistence / artifacts
- canonical results are intended for `bt_result.*`
- diagnostic artifacts can be written separately as JSON under research artifact storage
- this preserves both reproducibility and inspection friendliness

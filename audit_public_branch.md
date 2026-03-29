# Public Branch Truth Audit

Audit date:
- `2026-03-29`

Branch under audit:
- `public-release-20260323`

Scope:
- `scripts/medium_viability_check.py`
- `backtest_app/historical_data/features.py`
- `backtest_app/research/pipeline.py`
- `backtest_app/research/prototype.py`

Status:
- `do_not_run_medium = true`
- Reason: authoritative truth is split between committed `HEAD` and the dirty working tree, and prior run artifacts do not pin that split with sufficient provenance.

## Important Note
- The previous `audit_public_branch.md` and `mismatch_list.json` were not authoritative branch-truth artifacts.
- They described the dirty working tree as if it were committed public-branch truth.
- This file replaces that interpretation and separates `git show HEAD:` truth from current working-tree truth.

## committed_head_truth

### 1) `scripts/medium_viability_check.py` CLI support
- `HEAD` does support `--run-labels` and `--only`.
- This part of the earlier audit was directionally correct.

### 2) `backtest_app/historical_data/features.py` feature contract
- `HEAD` does **not** contain `FeatureTransform`.
- `HEAD` still uses `FeatureScaler` only.
- `HEAD` still emits one similarity `context_features` block and does not expose a transformed-feature contract across event/query/prototype.

### 3) Default similarity contents in `HEAD`
- `HEAD` still includes absolute macro `*_level` features in similarity context.
- `HEAD` still includes raw absolute `dollar_volume` in liquidity features.
- `HEAD` does not split similarity context from regime-only context.

### 4) `build_event_memory_asof` semantics in `HEAD`
- `HEAD` event memory builds event embeddings from `macro_history={feature_end_date: macro_payload}`.
- That means the committed branch still uses same-day macro payload for event similarity semantics.
- `HEAD` then fits a separate `FeatureScaler` on `anchor_feature_rows` after raw event embeddings are already created.

### 5) Query vs event/prototype transform semantics in `HEAD`
- `HEAD` query embeddings use trailing macro history up to query date.
- `HEAD` event embeddings use same-day macro payload only.
- `HEAD` prototype construction stores embeddings, but there is no authoritative transformed-feature contract shared with query/event/prototype.
- So committed `HEAD` still has a query/event semantic mismatch.

## working_tree_truth

### 1) `scripts/medium_viability_check.py` current state
- Working tree still supports `--run-labels` and `--only`.
- It now also records provenance and metadata-application checks in summaries.
- Dirty/non-authoritative runs are prepared to be excluded from medium verdicts.

### 2) `backtest_app/historical_data/features.py` current state
- Working tree contains `FeatureTransform`.
- Canonical contract is now `raw_features -> transform -> transformed_features -> embedding`.
- Default similarity excludes macro `*_level` and raw absolute `dollar_volume`.
- Regime-level macro data is kept separately in `regime_context_features`.

### 3) `backtest_app/research/pipeline.py` current state
- Working tree builds event raw payloads with trailing macro history via `_macro_history_until(...)`.
- Same-day macro payload is still used only for regime classification.
- Event memory now stores `transform` and aliases `scaler` to `transform.scaler`.
- Query building uses the shared transform semantics.
- Rolling diagnostics now record runtime-applied support metadata such as `top_k` and `diagnostic_disable_ess_gate`.

### 4) `backtest_app/research/prototype.py` current state
- Working tree prototypes store transformed features and lineage metadata from the same contract.
- Prototype metadata is aligned with event-side transformed embeddings.

## run_provenance_truth

### Existing medium and diagnosis artifacts
- Existing `medium_viability_check*` outputs are not authoritative for strategy verdicts.
- Existing `feature_contract_diagnosis` outputs are not authoritative for branch-vs-fixed comparisons.

### Why they are not authoritative
- They mix committed `HEAD` assumptions with dirty working-tree behavior.
- They do not consistently persist `branch`, `head_commit`, `dirty_worktree`, `changed_tracked_files`, and `diff_fingerprint`.
- The old diagnosis flow compared synthetic variants inside one code state instead of running a clean baseline ref against a clean fixed state.

## Operational conclusion
- Do not use current medium results as evidence that TOBE v1 fails.
- First authoritative rerun order remains:
  1. clean fixed commit
  2. targeted tests
  3. tiny baseline-vs-fixed comparison
  4. `feature_contract_diagnosis`
  5. medium `best1`
  6. medium `best2`

## Output flag
- `do_not_run_medium = true`

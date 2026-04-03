# Optuna Revival Plan

Goal:
- reconnect parameter search to the trading stack
- keep `live_app` clean
- require pre-Optuna evidence first
- make Optuna fast enough to run on frozen evidence instead of rerunning discovery

---

## Official V1 baseline

The current Optuna baseline is:

- authoritative source: `best1`
- `pre_optuna_ready = true`
- policy scope: `directional_wide_only`

This means V1 Optuna is **not** strategy discovery.
It is execution-policy tuning on top of already discovered forecast evidence.

---

## Architecture

### 1. Offline discovery layer

This is the expensive layer:

- rolling similarity search
- prototype/member-mixture construction
- `pre_optuna_packet.json`
- `policy_family_candidates.csv`
- `prototype_compression_audit.json`

This layer produces the authoritative evidence bundle.
It must not run inside Optuna trials.

### 2. Frozen-seed Optuna layer

This is the official V1 search path.

Required artifacts:

- `optuna_replay_seed.parquet`
- `optuna_replay_seed_summary.json`
- `pre_optuna_packet.json`
- `source_chunks.json`
- `coverage_summary.json`

Study mode:

- `frozen_seed_v1`

Official seed profile:

- `calibration_universe_v1`

Debug-only seed profile:

- `proof_subset_v1`

Allowed trial work:

- replay frozen candidate rows
- compute buy/sell prices
- rank buy candidates under capital limits
- simulate fills and portfolio evolution
- score the trial

Forbidden trial work:

- rolling similarity rerun
- prototype rebuild
- member-mixture recomputation
- authoritative medium rerun

Interpretation rule:

- `best1` proves the family is Optuna-worthy
- it does **not** define the final training universe
- the final study runs on a separately built calibration bundle covering the full mirrored tradable universe and full mirrored date range
- `build-query-feature-cache` materializes query features and replay bars into local PostgreSQL first
- `build-train-snapshots` persists reusable prototype/scaler/transform/calibration snapshots
- `build-calibration-bundle` then runs replay-only chunk scoring
- chunk workers use `build-calibration-chunk`
- the official study then compacts that DB-backed bundle with `build-study-cache` into `study_cache/fold_001..003.parquet`
- `build-calibration-chunk` is forbidden from calling rolling discovery helpers; it must fail fast if heavy upstream is invoked

### 3. Daily pre-open snapshot layer

TOBE runtime is still snapshot-based.
The difference is that the snapshot is now built from:

- frozen library/evidence bundle
- tuned `policy_params.json`

Daily output:

- `preopen_signal_snapshot.parquet`
- `preopen_signal_snapshot.json`

Offline refresh layer:

- `build-query-feature-cache`
- writes `bt_result.calibration_query_feature_row` and `bt_result.calibration_replay_bar`
- `build-train-snapshots`
- writes `bt_result.calibration_snapshot_run` and JSON-safe train snapshot artifacts
- `build-calibration-bundle`
- chunked over the mirrored tradable universe
- writes chunk progress and phase timing to `bt_result.calibration_chunk_run`
- writes replay-only seed rows to `bt_result.calibration_seed_row`
- exports one merged calibration bundle only after DB materialization is complete
- writes `source_chunks.json` and `coverage_summary.json`
- `build-study-cache`
- exports the DB-backed calibration bundle into 3 chronological fold parquet caches for official replay

This layer computes:

- buy ranking
- buy limit prices
- sell repricing for all holdings

It does not run Optuna or heavy discovery.

---

## V1 execution scope

V1 tunes only `directional_wide_only`.

Objective:

- primary: `final_equity / initial_capital`

Penalties:

- drawdown above threshold
- idle cash ratio
- excessive concentration

Feasibility floors:

- minimum trade count
- minimum sell fill count

All orders are DAY orders.
Unfilled orders do not carry.
Every trading day reuses the latest pre-open snapshot and recalculates prices from scratch.

Warm-start rule:

- official first study uses `32 trials`
- first `8` trials come from warm-start / local perturbation
- remaining `24` trials sample the broader search space
- official bundle defaults: `chunk_size=10`, `worker_count=4`, `soft_timeout=10m`, `hard_timeout=30m`
- timeout tuning should move to `p95 chunk runtime + headroom` once enough pilot chunks exist
- Phase A keeps `model_version=daily_reuse_v1`
- Phase B official full-universe studies use `snapshot_cadence=monthly`, `model_version=monthly_snapshot_v1`

- official calibration studies should enqueue the current known-feasible `single_leg` baseline
- plus a `ladder_v1` twin and a few local perturbations
- only after that should the broader search space sample freely

---

## Ladder handling

Ladder is not treated as a tiny numeric parameter.
In V1 it is a **policy-family ablation**:

- `single_leg`
- `ladder_v1`

Meaning:

- compare the two structures on the same frozen seed
- only if ladder wins consistently should leg rules be promoted to a deeper search

Default ladder promotion rule:

- wins at least 2 of 3 chronological folds
- median `final_equity` uplift of at least `+1%`
- max drawdown degradation no worse than `2%p`

If ladder fails this rule, keep V1 on `single_leg`.

---

## Why this avoids code pollution

This design keeps exploration out of `live_app`.
No live controller or broker path hosts optimization loops.
No heavy research rerun is hidden inside trial evaluation.
The tuned output is a versioned artifact, not an implicit live change.

---

## Promotion boundary

A best trial is **not** a live change.
It is only an experiment artifact and a candidate `policy_params.json`.

Operational adoption still requires:

1. reproducible frozen-seed study evidence
2. snapshot contract compatibility
3. explicit review and promotion into the live-adjacent configuration path

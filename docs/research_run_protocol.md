# Research run protocol

This protocol is for **repeatable, comparable research backtest runs**.
The target is not a one-off JSON dump. The target is a ledger that lets us compare runs across time and isolate whether **feature**, **policy**, or **portfolio** changes moved the result.

## Principles

1. **Preflight first**
   - Confirm local-db is reachable.
   - Detect the earliest and latest dates where the full requested universe has coverage.
   - Prefer a 9-month discovery + 3-month holdout split.
   - Fall back to 6-month discovery + 2-month holdout if coverage is shorter.

2. **Stable identifiers**
   Every run should standardize:
   - `experiment_group`
   - `run_id`
   - `universe_hash`
   - `spec_hash`
   - `data_snapshot_id`
   - legacy `bt_event_window_snapshot_id` when comparing against reusable scenario snapshots

   `run_id` and legacy snapshot `scenario_id` are separate. Re-running a batch may change `run_id`, but it must not silently regenerate the legacy snapshot.

3. **One run directory per run**
   Each run must write a directory with the same core files.

4. **Append-only leaderboard**
   Never rewrite prior rows when comparing runs. Add a new row.

5. **Human-readable report**
   `report.md` should say what improved and what degraded without forcing someone to inspect raw JSON.

## Two snapshot concepts: mirror snapshot vs scenario snapshot

### Mirror snapshot
This is the local trading mirror created by:
- `python scripts/refresh_local_trading.py init-full`
- `python scripts/refresh_local_trading.py refresh-reference`
- `python scripts/refresh_local_trading.py refresh-market`

It provides the TOBE research runtime with local OHLCV / macro / sector/reference data.
This is enough for `research_similarity_v2` local-db runs.

### Scenario snapshot
This is the reusable legacy candidate set stored in `bt_event_window` plus manifest metadata.
It is materialized with `scripts/materialize_bt_event_window.py` and consumed by `legacy_event_window`.

Do not confuse these:
- mirror snapshot = local database state for TOBE path
- scenario snapshot = legacy reusable candidate rows for parity/reference path

A healthy mirror does **not** imply a legacy scenario snapshot exists.

## Preflight gates

Before running batches, confirm:
- local-db connectivity is healthy
- OHLCV common coverage exists for the full universe
- macro coverage is complete
- sector coverage is complete, unless intentionally overridden with `--allow-unknown-sector`
- if legacy reference is enabled, discovery + holdout scenario snapshots exist in the manifest

The batch scripts already enforce these gates through `preflight.json` and snapshot-manifest checks.

## Standard outputs per run

Each run directory should contain:

- `manifest.json`
- `run_card.json`
- `fold_report.json`
- `decisions.csv` (or parquet if supported)
- `trades.csv` (or parquet if supported)
- `forecast_panel.csv` (or parquet if supported)
- `pre_optuna_packet.json`
- `pattern_family_table.csv`
- `policy_family_candidates.csv`
- `prototype_compression_audit.json`
- `prototype_compression_table.csv`
- `diagnostics.json`
- `report.md`

At the experiment-group root:

- `preflight.json`
- `leaderboard.csv`

## run_card required fields

Required summary fields:

- `run_id`
- `strategy_mode`
- `discovery_start`
- `discovery_end`
- `holdout_start`
- `holdout_end`
- `symbols`
- core spec values
- `trade_count`
- `fill_count`
- `coverage`
- `no_trade_ratio`
- `expectancy_after_cost`
- `psr`
- `dsr`
- `calibration_error`
- `monotonicity`
- `max_drawdown`
- `long_split`
- `short_split`
- `regime_split`

Recommended additions:

- `holdout_expectancy_after_cost`
- `universe_hash`
- `spec_hash`
- `data_snapshot_id`
- `experiment_group`

## Prepare reusable bt_event_window snapshots

Before legacy-comparable batches, materialize the discovery/holdout scenario snapshots once:

```powershell
python scripts/materialize_bt_event_window.py --scenario-id legacy_discovery --phase discovery --source-json runs\legacy_discovery.json
python scripts/materialize_bt_event_window.py --scenario-id legacy_holdout --phase holdout --source-json runs\legacy_holdout.json
```

You can also reuse an existing scenario without regenerating rows:

```powershell
python scripts/materialize_bt_event_window.py --scenario-id legacy_discovery_v2 --phase discovery --copy-from-scenario-id legacy_discovery
```

## Official run order

### Step 1 — TOBE-only smoke
Validate the mirror-only path first:

```powershell
python -m backtest_app.runner --data-source local-db --strategy-mode research_similarity_v2 --scenario-id scn_001 --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

Expected outcome: a fresh mirror setup should run here without any scenario snapshot materialization.

### Step 2 — legacy reference
Only after Path A succeeds, validate legacy comparability by ensuring reusable scenario snapshots exist and then running the legacy reference row.

### Step 3 — matrix batch
Only after the TOBE smoke and legacy reference are healthy, run the policy × portfolio matrix.

This order matters because it cleanly separates:
- mirror/bootstrap failures
- missing legacy snapshot/setup failures
- actual research-policy differences

## First batch runner

Use:

```powershell
python -m scripts.research_first_batch
```

Optional:

```powershell
python -m scripts.research_first_batch --experiment-group first_batch_manual_20260327
python -m scripts.research_first_batch --skip-holdout
python -m scripts.research_first_batch --legacy-discovery-scenario-id legacy_discovery --legacy-holdout-scenario-id legacy_holdout
python -m scripts.research_first_batch --skip-legacy-reference
```

Behavior:
- TOBE runs should work from the mirror alone.
- Legacy reference requires scenario snapshots.
- `--skip-legacy-reference` is the right switch when you want mirror-only TOBE validation or when legacy snapshots are not ready yet.

## Matrix batch runner

For policy × portfolio decomposition after the first batch:

```powershell
python -m scripts.research_matrix_batch
```

This batch reuses the same legacy snapshot manifest for the legacy reference row and any matrix comparisons. It never regenerates the legacy snapshot in-script.

Optional:

```powershell
python -m scripts.research_matrix_batch --legacy-discovery-scenario-id legacy_discovery --legacy-holdout-scenario-id legacy_holdout
python -m scripts.research_matrix_batch --skip-legacy-reference
```

This writes the normal per-run ledger plus group-level outputs:
- `comparison_table.csv`
- `comparison.md`
- `axis_effect_summary.json`

## How to interpret the ledger

Use `leaderboard.csv` to answer, in order:

1. Did policy changes move coverage / no-trade more than expectancy?
2. Did portfolio changes move trade count and drawdown more than signal quality?
3. Only after those: did feature changes materially improve holdout behavior?

## Pre-Optuna gate

Do not use `forecast_selected_count` as the Optuna go/no-go gate.

The Optuna-ready question is:

- does the forecast surface contain at least one **repeated pattern family**
- and does that family map to an **execution policy family** that is not a member-level collapse echo

The runtime now writes a dedicated packet for this:

- `pre_optuna_packet.json`
- `pattern_family_table.csv`
- `policy_family_candidates.csv`
- `prototype_compression_audit.json`
- `prototype_compression_table.csv`

Default recurring-family rule:

- same `pattern_key`
- at least `3` distinct decision dates
- at least `5` anchor rows

The only valid verdicts are:

- `optuna_ready`
- `not_ready_single_prototype_collapse`
- `not_ready_no_repeated_patterns`
- `not_ready_contract_or_environment`

`optuna_ready` means:

- at least one recurring family exists
- at least one row is marked `optuna_eligible=true`
- `next_optuna_target_scope` tells you whether to tune `tight_consensus`, `directional_wide`, or both

`not_ready_single_prototype_collapse` now has a stricter meaning:

- the blocker must still be visible at the member-mixture layer
- not merely because prototype-level telemetry looked concentrated
- the compression artifacts should tell you whether the raw event pool was already thin or whether prototype compression over-collapsed it

Interpretation rule:

- `prototype_count` small and `event_record_count` also small: the raw historical pool is thin
- `prototype_count` small but `event_record_count` large: prototype compression is the suspected blocker
- member telemetry with `member_pre_truncation_count > 1` and `member_mixture_ess > 1` is the minimum sign that Optuna-adjacent policy work is becoming meaningful

## Recommended change order

1. **policy**
2. **portfolio**
3. **feature**

Reason: policy and portfolio are faster to interpret and less likely to confound whether the signal exists at all.

## Minimum acceptance checks for TOBE sanity batch

- fold report is non-empty
- frozen validation is true
- scenario-end open positions are zero
- coverage > 0
- no_trade_ratio < 0.95
- holdout expectancy_after_cost >= 0 or better than legacy baseline
- calibration / monotonicity are not fully broken

## Failure handling

If preflight fails:
- do not fabricate research conclusions
- record the blocker
- preserve the experiment-group directory if partial metadata was already written

If a run completes but quality checks fail:
- keep the row in `leaderboard.csv`
- mark the interpretation in `report.md`
- do not silently overwrite the run with a rerun

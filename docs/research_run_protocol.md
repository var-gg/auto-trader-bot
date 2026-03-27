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

3. **One run directory per run**
   Each run must write a directory with the same core files.

4. **Append-only leaderboard**
   Never rewrite prior rows when comparing runs. Add a new row.

5. **Human-readable report**
   `report.md` should say what improved and what degraded without forcing someone to inspect raw JSON.

## Standard outputs per run

Each run directory should contain:

- `manifest.json`
- `run_card.json`
- `fold_report.json`
- `decisions.csv` (or parquet if supported)
- `trades.csv` (or parquet if supported)
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

## First batch runner

Use:

```powershell
python scripts/research_first_batch.py
```

Optional:

```powershell
python scripts/research_first_batch.py --experiment-group first_batch_manual_20260327
python scripts/research_first_batch.py --skip-holdout
```

## How to interpret the ledger

Use `leaderboard.csv` to answer, in order:

1. Did policy changes move coverage / no-trade more than expectancy?
2. Did portfolio changes move trade count and drawdown more than signal quality?
3. Only after those: did feature changes materially improve holdout behavior?

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

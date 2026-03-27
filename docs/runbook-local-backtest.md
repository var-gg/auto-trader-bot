# Local backtest runbook

## Rules
- No Alembic.
- No ORM `create_all` / autogenerate / runtime DDL.
- SQL ownership stays in checked-in `db/sql/**/*.sql`.
- Live money path behavior must not change.

## Why this path
We standardize on a SQL-first local bootstrap so local backtest setup is repeatable and reviewable.

We also standardize on a dump-first local trading mirror:
- local keeps the `trading` schema name
- only a whitelist subset is copied from production/proxy
- live order / fill / execution / position / portfolio state is excluded
- refresh cadence is separate from backtest execution cadence

## Two official execution paths
### Path A — mirror-only TOBE
Use this by default.
- strategy mode: `research_similarity_v2`
- requires only the local mirror + SQL bootstrap
- intended to succeed on a fresh mirror setup

### Path B — snapshot-backed legacy
Use this only when you need legacy comparability.
- strategy mode: `legacy_event_window`
- requires the same local mirror **plus** pre-materialized `bt_event_window` scenario snapshots
- intended to fail fast if the requested snapshot is missing

## Supported procedure
### 1) Provision local DB and env
Required:
- local PostgreSQL running
- `BACKTEST_DB_URL` (or `BACKTEST_DB_*`)
- `SOURCE_DB_URL`

### 2) Apply SQL bootstrap/patches
```bash
python scripts/db_apply_sql.py --db-url "env:BACKTEST_DB_URL"
```

Optional verify pass:
```bash
python scripts/db_apply_sql.py --db-url "env:BACKTEST_DB_URL" --groups verify
```

### 3) Create initial local mirror
```bash
python scripts/refresh_local_trading.py init-full
```

### 4) Normal refresh loop
```bash
python scripts/refresh_local_trading.py refresh-reference
python scripts/refresh_local_trading.py refresh-market
```

### 5) Run Path A smoke first
```bash
python -m backtest_app.runner --data-source local-db --strategy-mode research_similarity_v2 --scenario-id scn_001 --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

### 6) Optional Path B legacy reference
Materialize snapshots first:
```bash
python scripts/materialize_bt_event_window.py --scenario-id legacy_discovery --phase discovery --source-json runs/legacy_discovery.json
python scripts/materialize_bt_event_window.py --scenario-id legacy_holdout --phase holdout --source-json runs/legacy_holdout.json
```

Then run:
```bash
python -m backtest_app.runner --data-source local-db --strategy-mode legacy_event_window --scenario-id legacy_discovery --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

If the snapshot is missing, stop and follow the CLI guidance instead of debugging the mirror path. Missing legacy snapshot is a setup issue, not a TOBE mirror failure.

### 7) Drift recovery
```bash
python scripts/refresh_local_trading.py resync-full
```

## Batch run notes
- `python -m scripts.research_first_batch --help`
- `python -m scripts.research_matrix_batch --help`
- Both batch entrypoints support:
  - `--skip-legacy-reference`
  - `--legacy-discovery-scenario-id`
  - `--legacy-holdout-scenario-id`

## Deprecated commands
Do not use these as the official bootstrap anymore:
```bash
python scripts/apply_local_sql.py ...
python scripts/mirror_trading_whitelist.py
```

## Manual steps still required
- start/stop local PostgreSQL yourself
- point `SOURCE_DB_URL` at the intended production/proxy source
- seed or import research-owned local event data only if you need legacy snapshot-backed runs
- review SQL patches before production apply

## Safety notes
- `backtest_app` local-db mode is guarded against Cloud SQL/live wiring
- mirror refresh and backtest execution are intentionally separate
- this runbook is for local research/backtest only, not live cutover

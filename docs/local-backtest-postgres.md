# Local backtest Postgres

## Goal
- Keep the live Cloud SQL / money path untouched.
- Run backtest + research against local PostgreSQL only.
- Keep schema/bootstrap SQL-first from `db/sql/**/*.sql` only.
- Standardize local bootstrap on the supported local-db paths.

## Official strategy
The local database strategy has two official execution paths:

### Path A — mirror-only TOBE path
- preferred default for fresh local setup
- production/proxy remains the source of truth
- a whitelist subset is refreshed into local Postgres under the same `trading` schema name
- backtest reads local state only
- uses `--strategy-mode research_similarity_v2`
- does **not** require pre-materialized `bt_event_window` scenario rows

### Path B — snapshot-backed legacy path
- use only for legacy parity/reference runs
- still depends on the same local mirror for price/reference tables
- uses `--strategy-mode legacy_event_window`
- requires a matching pre-materialized `bt_event_window` scenario snapshot and manifest row

This keeps mirror bootstrap simple while making legacy-comparison requirements explicit.

## Supported bootstrap path for both A and B
### 0) Set env
- `SOURCE_DB_URL` -> production/proxy source DB for mirror refresh
- `BACKTEST_DB_URL` -> local PostgreSQL target
  - or use `BACKTEST_DB_HOST`, `BACKTEST_DB_PORT`, `BACKTEST_DB_NAME`, `BACKTEST_DB_USER`, `BACKTEST_DB_PASSWORD`

### 1) Apply SQL-first bootstrap + patches
```bash
python scripts/db_apply_sql.py --db-url "env:BACKTEST_DB_URL"
```

Default groups are:
- `db/sql/bootstrap/*.sql`
- `db/sql/patches/*.sql`

Optional verification:
```bash
python scripts/db_apply_sql.py --db-url "env:BACKTEST_DB_URL" --groups verify
```

### 2) Build the first local trading mirror
```bash
python scripts/refresh_local_trading.py init-full
```

### 3) Normal refresh loop
```bash
python scripts/refresh_local_trading.py refresh-reference
python scripts/refresh_local_trading.py refresh-market
```

## Path A execution — mirror-only TOBE
Run this first on a fresh mirror:

```bash
python -m backtest_app.runner --data-source local-db --strategy-mode research_similarity_v2 --scenario-id scn_001 --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

This is the official default local-db example because a fresh mirror should succeed here without any legacy scenario bootstrap.

## Path B execution — snapshot-backed legacy
Before running legacy mode, materialize reusable scenario snapshots:

```bash
python scripts/materialize_bt_event_window.py --scenario-id legacy_discovery --phase discovery --source-json runs/legacy_discovery.json
python scripts/materialize_bt_event_window.py --scenario-id legacy_holdout --phase holdout --source-json runs/legacy_holdout.json
```

Then run the legacy path with one of those scenario ids:

```bash
python -m backtest_app.runner --data-source local-db --strategy-mode legacy_event_window --scenario-id legacy_discovery --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

If the snapshot is missing, the CLI is expected to fail fast with a resolution message telling you to either:
- switch to `--strategy-mode research_similarity_v2`, or
- materialize the legacy snapshot first

## Research batch expectation
- `scripts/research_first_batch.py` and `scripts/research_matrix_batch.py` default to TOBE + optional legacy reference behavior.
- Both support:
  - `--skip-legacy-reference`
  - `--legacy-discovery-scenario-id`
  - `--legacy-holdout-scenario-id`
- Recommended run order is documented in `docs/research_run_protocol.md`.

## Deprecated path
Do **not** use these as the primary bootstrap anymore:
- `python scripts/apply_local_sql.py ...`
- `python scripts/mirror_trading_whitelist.py`
- direct manual application of `db/sql/001_local_backtest_schema.sql` / `002_local_backtest_seed.sql`

Those scripts now exist only as compatibility wrappers that redirect to the single supported flow.

## Manual steps still required
- provision/start local PostgreSQL
- set `SOURCE_DB_URL`
- set `BACKTEST_DB_URL` or `BACKTEST_DB_*`
- materialize legacy scenario snapshots if you need Path B
- run backtests explicitly; bootstrap does not auto-run research jobs

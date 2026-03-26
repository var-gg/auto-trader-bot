# Local backtest Postgres

## Goal
- Keep the live Cloud SQL / money path untouched.
- Run backtest + research against local PostgreSQL only.
- Keep schema/bootstrap SQL-first from `db/sql/**/*.sql` only.
- Standardize local bootstrap on **one supported path**.

## Official strategy
The supported local strategy is **dump-first local trading mirror**:
- production/proxy remains the source of truth
- a whitelist subset is refreshed into local Postgres under the same `trading` schema name
- backtest reads local state only
- live order/fill/account/execution tables are intentionally excluded

This minimizes branching while avoiding any live-money path mutation.

## Supported bootstrap path
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

### 4) Run local backtest
```bash
python -m backtest_app.runner --data-source local-db --scenario-id scn_001 --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

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
- load research-owned local event/anchor data if your scenario depends on it
- run backtests explicitly; bootstrap does not auto-run research jobs

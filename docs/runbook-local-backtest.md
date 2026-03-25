# Local backtest runbook

## Why we do not use Alembic
- This repo needs SQL to remain the single source of truth for schema reconstruction.
- Backtest/research DB shape must be inspectable from checked-in SQL alone.
- Runtime/autogenerate migration flows make it easier to drift from the intended live-vs-local split.
- We want explicit patch review, deterministic apply order, and no hidden DDL side effects.

## Why we use a local trading mirror
- Backtests need production-like table names (`trading`) with minimal code branching.
- Research should run on a local, partial mirror rather than the live Cloud SQL path.
- Only whitelist tables required for research/backtest are copied.
- This preserves reproducibility while protecting live order/fill/execution state.

## Procedure
### 1) Initialize local DB
- provision local Postgres
- create the target database
- set `BACKTEST_DB_URL`

### 2) Apply SQL bootstrap + patches
```bash
python scripts/db_apply_sql.py --db-url "$BACKTEST_DB_URL"
```

### 3) Refresh local trading mirror
First run:
```bash
python scripts/refresh_local_trading.py init-full
```

Normal loop:
```bash
python scripts/refresh_local_trading.py refresh-reference
python scripts/refresh_local_trading.py refresh-market
```

### 4) Run backtest
```bash
python -m backtest_app.runner --data-source local-db --scenario-id scn_001 --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

### 5) Long drift recovery
If local mirror drift is suspected after a long idle period:
```bash
python scripts/refresh_local_trading.py resync-full
```

## Manual steps still required
- provision/start local Postgres
- point `SOURCE_DB_URL` at the production/proxy source
- load/refresh research-owned event data into canonical local tables
- review SQL patches before applying them to production

## Safety notes
- backtest_app local-db mode is guarded against Cloud SQL/live wiring by default
- refresh and backtest are separate steps; do not full refresh before every run
- live cutover is not done by this runbook; this flow is for research/backtest only

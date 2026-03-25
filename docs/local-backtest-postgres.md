# Local backtest Postgres plan

## Goal
- Keep live Cloud SQL money path untouched.
- Run backtest/research only against local Postgres.
- Manage local research schema with `db/sql/*.sql` only.
- Mirror only whitelisted live tables into local `trading` schema.

## New local tables
Apply in order:
1. `db/sql/001_local_backtest_schema.sql`
2. `db/sql/002_local_backtest_seed.sql`

Tables introduced:
- `trading.bt_mirror_ticker`
- `trading.bt_mirror_ohlcv_daily`
- `trading.bt_event_window`
- `trading.bt_mirror_whitelist`

## Mirror flow
1. Point `SOURCE_DB_URL` at live/proxy source.
2. Point `BACKTEST_DB_URL` (or BACKTEST_DB_* envs) at local Postgres.
3. Run:
   - `python scripts/apply_local_sql.py db/sql/001_local_backtest_schema.sql db/sql/002_local_backtest_seed.sql`
   - `python scripts/mirror_trading_whitelist.py`
4. Load research-generated event snapshots into `trading.bt_event_window` using explicit SQL/import scripts.

## Backtest runner
- Fixture mode remains supported:
  - `python -m backtest_app.runner --data-source json --data tests/fixtures/backtest_historical_fixture.json ...`
- Local DB mode:
  - `python -m backtest_app.runner --data-source local-db --scenario-id my_scenario --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT`

## Safety boundaries
- `backtest_app` uses its own `backtest_app.db.local_session` wiring, separate from live FastAPI sessions.
- `BACKTEST_DB_URL` must point to local Postgres by default.
- Cloud SQL socket URLs and live env markers are rejected for backtest mode.
- Backtest sessions set `search_path=trading,bt_result,meta,public` and `default_transaction_read_only=on`.
- Result artifacts still go to JSON files; no live DB writes are introduced.

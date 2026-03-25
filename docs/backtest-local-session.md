# Backtest local DB session split

## New loader interface
Backtest data loading now has two explicit paths:
- `JsonHistoricalDataLoader` for fixture-driven runs
- `LocalPostgresLoader` for local Postgres runs

`LocalPostgresLoader` constructor:
- `LocalPostgresLoader(session_factory, schema="trading")`

Primary entrypoint:
- `load_for_scenario(scenario_id, market, start_date, end_date, symbols)`

This keeps loader responsibility on read-only historical input assembly and avoids reuse of live app session wiring.

## Env / config example
```bash
BACKTEST_DB_URL=postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/auto_trader_backtest
BACKTEST_DB_SCHEMA=trading
BACKTEST_DB_SEARCH_PATH=trading,bt_result,meta,public
BACKTEST_DB_REQUIRE_LOCAL=true
```

CLI examples:
```bash
python -m backtest_app.runner \
  --data-source local-db \
  --scenario-id scn_001 \
  --market US \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --symbols AAPL,MSFT

python -m backtest_app.runner \
  --data-source json \
  --data tests/fixtures/backtest_historical_fixture.json \
  --scenario-id scn_fixture \
  --market US \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --symbols AAPL
```

## Split point vs live session
Live app session wiring remains under the live app / FastAPI stack.
Backtest now uses:
- `backtest_app.db.local_session.LocalBacktestDbConfig`
- `create_backtest_session_factory(...)`
- `local_session_scope(...)`

So `backtest_app` no longer needs live `SessionLocal` wiring to read local Postgres.

## Guard strategy
`guard_backtest_local_only(...)` blocks:
- Cloud SQL socket URLs (`/cloudsql/...`)
- Google Cloud SQL style host markers
- non-local hosts when `BACKTEST_DB_REQUIRE_LOCAL=true`
- live env signals like `INSTANCE_CONNECTION_NAME` unless explicitly overridden

Additionally, session creation enforces:
- `search_path=trading,bt_result,meta,public`
- `default_transaction_read_only=on`

No DDL is emitted. ORM usage remains read/query only.

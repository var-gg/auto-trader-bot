# Local trading mirror

## Goal
Keep a local PostgreSQL database with the same schema name (`trading`) while copying only the whitelist subset needed for research/backtests.

## Official stance: dump-first local mirror
The supported strategy is **dump-first local trading mirror**.

That means:
- source of truth remains production/proxy DB
- local bootstrap first applies checked-in SQL
- mirror refresh then copies approved tables into local Postgres
- backtests read local mirror state only
- live execution/account/order/fill state is not mirrored

This is the official local bootstrap strategy for backtests.

## Config
Config file: `config/local_trading_mirror.json`

Top-level fields:
- `schema`: local schema name (kept as `trading`)
- `mirror_tables`: ordered list of mirror specs
- `exclude_prefixes`: guardrail prefixes that must not be mirrored by default

## Refresh modes
### `init-full`
- official first bootstrap mode
- full dump of the whitelist subset into local Postgres
- initializes `meta.local_mirror_state`

### `refresh-reference`
- refreshes slow-moving reference tables

### `refresh-market`
- refreshes faster-moving market/research tables

### `resync-full`
- manual recovery mode when local drift is suspected

## Commands
```bash
python scripts/db_apply_sql.py --db-url "env:BACKTEST_DB_URL"
python scripts/refresh_local_trading.py init-full
python scripts/refresh_local_trading.py refresh-reference
python scripts/refresh_local_trading.py refresh-market
python scripts/refresh_local_trading.py resync-full
```

## Exclusion policy
Default exclude classes:
- live order / leg / broker / fill / execution tables
- portfolio / position / account-state tables
- anything whose primary purpose is live money path mutation or broker trace

## Bootstrap summary
1. set `BACKTEST_DB_URL`
2. set `SOURCE_DB_URL`
3. apply SQL with `scripts/db_apply_sql.py`
4. run `scripts/refresh_local_trading.py init-full`
5. run backtests against local-db mode

## Deprecated path
Legacy helpers are deprecated:
- `scripts/apply_local_sql.py`
- `scripts/mirror_trading_whitelist.py`

They should not be referenced by new docs/runbooks.

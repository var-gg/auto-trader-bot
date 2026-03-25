# Local trading mirror

## Goal
Keep a local Postgres database with the same schema name (`trading`) but only a whitelist subset of production tables/data needed for research and backtests.

## Why local trading mirror
- It keeps schema names aligned with production while avoiding broad code rewrites.
- It avoids direct research/backtest dependence on live Cloud SQL.
- It limits copied data to a controlled whitelist instead of mirroring order/fill/account state.
- It separates refresh cadence from backtest execution cadence.

Backtest execution and mirror refresh are separate steps:
- refresh updates local mirror tables
- backtest reads local state only

## Whitelist file format
Config file: `config/local_trading_mirror.json`

Top-level fields:
- `schema`: local schema name (kept as `trading`)
- `mirror_tables`: ordered list of mirror specs
- `exclude_prefixes`: guardrail prefixes that must not be mirrored by default

Each mirror spec supports:
- `name`: logical table id
- `source_table`: source table on production/proxy DB
- `target_table`: local target table
- `refresh_group`: `reference` or `market`
- `refresh_strategy`: `full` | `cursor_upsert` | `resync`
- `cursor_column`: preferred incremental cursor
- `fallback_cursor_column`: fallback cursor when timestamps are not available
- `source_sql`: SELECT used on source DB
- `insert_sql`: target-side INSERT/UPSERT SQL
- `truncate_sql`: optional, used for full/resync modes

## Refresh modes
### `init-full`
- first local bootstrap
- copies all whitelisted tables
- truncates tables that declare `truncate_sql`
- seeds `meta.local_mirror_state`

### `refresh-reference`
- refreshes slow-moving reference tables only
- examples: ticker metadata, macro series definitions, sector/anchor mapping tables
- prefers incremental cursor refresh when available

### `refresh-market`
- refreshes faster-moving market/research data only
- examples: daily OHLCV, macro values, event snapshots
- incremental-first; no full dump by default

### `resync-full`
- manual recovery mode when drift is suspected or local state is stale/corrupt
- replays the full whitelist set
- allowed to truncate/reload target tables

## Exclusion policy
Default exclude classes:
- live order / leg / broker / fill / execution tables
- portfolio / position / account-state tables
- anything whose primary purpose is live money path mutation or broker trace

Examples of excluded prefixes in config:
- `trading.order_`
- `trading.broker_`
- `trading.fill`
- `trading.execution`
- `trading.position`
- `trading.portfolio`

## Operational loop example
### First-time local setup
1. apply SQL bootstrap/patches to local DB
2. run `init-full`
3. load research-owned event windows if needed
4. run backtest against local DB

### Normal daily loop
1. before research/backtest window, run `refresh-reference`
2. then run `refresh-market`
3. execute one or more backtests locally
4. do **not** refresh on every single backtest invocation

### Drift recovery
- if schema/data drift is suspected after a long gap, run `resync-full`

## Commands
```bash
python scripts/refresh_local_trading.py init-full
python scripts/refresh_local_trading.py refresh-reference
python scripts/refresh_local_trading.py refresh-market
python scripts/refresh_local_trading.py resync-full
```

Required env:
- `SOURCE_DB_URL` -> production/proxy source DB
- `BACKTEST_DB_URL` (or `BACKTEST_DB_*`) -> local Postgres target

## Same-schema strategy
The local DB keeps schema name `trading` to minimize code changes.
Only a partial mirror is maintained; absence of live-only tables is intentional.

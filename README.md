# auto-trader-bot

Auto Trader Bot is a FastAPI-based trading operations service for research, signal generation, market data ingestion, and broker-facing execution workflows.

This public repository is a sanitized version intended for code review, architecture sharing, and development reference.
No production secrets, live account credentials, or private runbooks should be committed here.

## What it includes
- FastAPI application entrypoint and routers
- Trading workflow modules for:
  - premarket signal processing
  - market data ingestion
  - earnings and fundamentals collection
  - portfolio and fill collection flows
  - trading-hybrid planning / execution logic
- SQLAlchemy models and repositories
- Dockerfile and `start.sh` for containerized deployment
- Secret Manager / Cloud Run deployment guidance
- SQL-first local backtest path backed by local PostgreSQL

## What is intentionally excluded
- Real `.env` files and local secrets
- Service account keys
- Private operational inventory and runbooks
- Production-only docs with sensitive infrastructure details
- Logs, runtime state, and local editor artifacts

## Runtime model
### Local development
Use environment variables or a local `.env` file that is **not** committed.

### Cloud Run
Production-style deployment is expected to use:
- Cloud Run
- Cloud SQL for PostgreSQL
- Secret Manager for sensitive values
- Cloud Scheduler for periodic jobs

The app now performs fail-fast startup validation in deploy environments when required runtime values are missing or still set to placeholders.

## Minimum required environment variables
### Database
- `DB_USER`
- `DB_PASS`
- `DB_NAME`
- `INSTANCE_CONNECTION_NAME` (Cloud Run)
- or `DB_URL` for local fallback usage

### Local backtest database
- `BACKTEST_DB_URL`
- or `BACKTEST_DB_HOST`
- `BACKTEST_DB_PORT`
- `BACKTEST_DB_NAME`
- `BACKTEST_DB_USER`
- `BACKTEST_DB_PASSWORD`
- `SOURCE_DB_URL` for dump-first local mirror refresh

### Broker / execution
- `KIS_APPKEY`
- `KIS_APPSECRET`
- `KIS_CANO`
- `KIS_ACNT_PRDT_CD`
- `KIS_VIRTUAL=true|false`
- `KIS_VIRTUAL_CANO` when virtual mode is enabled

### Other integrations
- `OPENAI_API_KEY`
- `FRED_API_KEY`
- `DART_API_KEY` (optional in some flows)

See `.env.example` and deployment docs for the public-safe setup shape.

## Running locally
### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Prepare environment
```bash
cp .env.example .env
# then fill in your own local values
```

### 3. Start the API
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8080
```

Or run the container entrypoint behavior with:
```bash
bash start.sh
```

## Local backtest bootstrap (official)
Use **one supported bootstrap path** only.

### 1) Apply SQL-first bootstrap/patches to local Postgres
```bash
python scripts/db_apply_sql.py --db-url "env:BACKTEST_DB_URL"
```

### 2) Build the dump-first local trading mirror
```bash
python scripts/refresh_local_trading.py init-full
```

### 3) Normal refresh loop
```bash
python scripts/refresh_local_trading.py refresh-reference
python scripts/refresh_local_trading.py refresh-market
```

### 4) Run local-db backtest
```bash
python -m backtest_app.runner --data-source local-db --scenario-id scn_001 --market US --start-date 2026-01-01 --end-date 2026-01-31 --symbols AAPL,MSFT
```

Deprecated bootstrap helpers:
- `python scripts/apply_local_sql.py ...`
- `python scripts/mirror_trading_whitelist.py`

See also:
- `docs/local-backtest-postgres.md`
- `docs/runbook-local-backtest.md`
- `docs/local-trading-mirror.md`
- `docs/db-sql-first.md`

## Deploying
Public-safe deployment notes are included in:
- `DEPLOY_SECRET_MANAGER.md`
- `DEPLOY_VALIDATION_CHECKLIST.md`
- `docs/db-sql-first.md`

These documents assume Secret Manager-backed Cloud Run deployment and post-deploy validation of startup, health, and order/fill flow behavior.
Production schema changes must be applied explicitly from checked-in SQL files; runtime code must not create schemas/tables implicitly.

## API surface
The repository contains feature modules such as:
- `premarket`
- `marketdata`
- `earnings`
- `portfolio`
- `trading_hybrid`
- `signals`
- `recommendation`
- `kis_test`

For a live project, expose only the endpoints you intend to operate and keep production routing, scheduling, and auth policy under separate private ops control.

## Safety note
This repository may contain broker-facing execution code paths.
Do not point it at real accounts unless you have explicitly configured and validated secrets, environment, risk controls, and monitoring.

-- Local backtest/research schema.
-- Apply manually against local Postgres only.
-- No Alembic. No runtime DDL.

CREATE SCHEMA IF NOT EXISTS trading;

-- Mirror whitelist targets (copied from live Cloud SQL into local Postgres only).
CREATE TABLE IF NOT EXISTS trading.bt_mirror_ticker (
    ticker_id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    country TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_bt_mirror_ticker_symbol_exchange
    ON trading.bt_mirror_ticker(symbol, exchange);

CREATE TABLE IF NOT EXISTS trading.bt_mirror_sector (
    sector_id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_bt_mirror_sector_code
    ON trading.bt_mirror_sector(code);

CREATE TABLE IF NOT EXISTS trading.bt_mirror_industry (
    industry_id INTEGER PRIMARY KEY,
    sector_id INTEGER NOT NULL REFERENCES trading.bt_mirror_sector(sector_id),
    code TEXT NOT NULL,
    name TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_bt_mirror_industry_sector_id
    ON trading.bt_mirror_industry(sector_id);

CREATE UNIQUE INDEX IF NOT EXISTS ix_bt_mirror_industry_code
    ON trading.bt_mirror_industry(code);

CREATE TABLE IF NOT EXISTS trading.bt_mirror_ticker_industry (
    ticker_id INTEGER NOT NULL REFERENCES trading.bt_mirror_ticker(ticker_id),
    industry_id INTEGER NOT NULL REFERENCES trading.bt_mirror_industry(industry_id),
    is_primary BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (ticker_id, industry_id)
);

CREATE INDEX IF NOT EXISTS ix_bt_mirror_ticker_industry_industry_id
    ON trading.bt_mirror_ticker_industry(industry_id);

CREATE INDEX IF NOT EXISTS ix_bt_mirror_ticker_industry_ticker_primary
    ON trading.bt_mirror_ticker_industry(ticker_id, is_primary);

CREATE TABLE IF NOT EXISTS trading.bt_mirror_ohlcv_daily (
    ticker_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC(18,6) NOT NULL,
    high NUMERIC(18,6) NOT NULL,
    low NUMERIC(18,6) NOT NULL,
    close NUMERIC(18,6) NOT NULL,
    volume BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker_id, trade_date)
);

CREATE INDEX IF NOT EXISTS ix_bt_mirror_ohlcv_daily_symbol_date
    ON trading.bt_mirror_ohlcv_daily(symbol, trade_date);

-- Research-owned event/anchor snapshots for backtest inputs.
-- Loaded by offline SQL/scripts only; read by backtest_app.
CREATE TABLE IF NOT EXISTS trading.bt_event_window (
    id BIGSERIAL PRIMARY KEY,
    scenario_id TEXT NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    ticker_id INTEGER,
    event_time TIMESTAMPTZ NOT NULL,
    anchor_date DATE,
    reference_date DATE NOT NULL,
    side_bias TEXT NOT NULL,
    signal_strength NUMERIC(18,8) NOT NULL,
    confidence NUMERIC(18,8),
    current_price NUMERIC(18,6),
    atr_pct NUMERIC(18,8),
    target_return_pct NUMERIC(18,8),
    max_reverse_pct NUMERIC(18,8),
    expected_horizon_days INTEGER,
    reverse_breach_day INTEGER,
    outcome_label TEXT,
    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
    diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_bt_event_window_scenario_market_refdate
    ON trading.bt_event_window(scenario_id, market, reference_date);

CREATE INDEX IF NOT EXISTS ix_bt_event_window_symbol_event_time
    ON trading.bt_event_window(symbol, event_time);

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.bt_scenario_snapshot_manifest (
    snapshot_id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL UNIQUE,
    phase TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    market TEXT NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    universe_hash TEXT NOT NULL,
    spec_hash TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT,
    source_path TEXT,
    copied_from_scenario_id TEXT
);

CREATE INDEX IF NOT EXISTS ix_bt_scenario_snapshot_manifest_phase_market
    ON meta.bt_scenario_snapshot_manifest(phase, market, window_start, window_end);

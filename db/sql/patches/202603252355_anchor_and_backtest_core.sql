-- Purpose: canonical trading/bt_result tables for anchor/event research and backtest outputs.
-- Scope: production-compatible SQL (assumes required extensions/types already exist where needed).
-- Notes:
--   * No dynamic per-config physical tables. Use canonical tables + version/config columns.
--   * Reconstructable from SQL files alone.
--   * ORM models must map only; no create_all/autocreate.

CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS bt_result;

-- Anchor label run: one row per labeling run/config/version execution.
CREATE TABLE IF NOT EXISTS trading.anchor_label_run (
    id BIGSERIAL PRIMARY KEY,
    run_key TEXT NOT NULL,
    config_version TEXT NOT NULL,
    label_version TEXT NOT NULL,
    market TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'CREATED',
    source_range_start DATE,
    source_range_end DATE,
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_anchor_label_run_run_key UNIQUE (run_key)
);

CREATE INDEX IF NOT EXISTS ix_anchor_label_run_status_started_at
    ON trading.anchor_label_run(status, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_anchor_label_run_config_version
    ON trading.anchor_label_run(config_version, label_version);

-- Anchor event: canonical event table, versioned by config/label metadata.
CREATE TABLE IF NOT EXISTS trading.anchor_event (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES trading.anchor_label_run(id) ON DELETE CASCADE,
    ticker_id INTEGER,
    symbol TEXT NOT NULL,
    market TEXT NOT NULL,
    anchor_code TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    anchor_date DATE,
    reference_date DATE NOT NULL,
    side_bias TEXT,
    config_version TEXT NOT NULL,
    label_version TEXT NOT NULL,
    horizon_days INTEGER,
    target_return_pct NUMERIC(18,8),
    max_reverse_pct NUMERIC(18,8),
    outcome_label TEXT,
    confidence NUMERIC(18,8),
    event_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_anchor_event_canonical UNIQUE (run_id, symbol, anchor_code, event_time, config_version, label_version)
);

CREATE INDEX IF NOT EXISTS ix_anchor_event_symbol_time
    ON trading.anchor_event(symbol, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_anchor_event_market_refdate
    ON trading.anchor_event(market, reference_date DESC);
CREATE INDEX IF NOT EXISTS ix_anchor_event_run_id
    ON trading.anchor_event(run_id);
CREATE INDEX IF NOT EXISTS ix_anchor_event_ticker_refdate
    ON trading.anchor_event(ticker_id, reference_date DESC);

-- Anchor vector: canonical embedding store, versioned by config/model.
-- Requires pgvector or equivalent vector type to be present before apply.
CREATE TABLE IF NOT EXISTS trading.anchor_vector (
    id BIGSERIAL PRIMARY KEY,
    anchor_event_id BIGINT REFERENCES trading.anchor_event(id) ON DELETE CASCADE,
    anchor_code TEXT NOT NULL,
    config_version TEXT NOT NULL,
    embedding_model TEXT NOT NULL,
    embedding_version TEXT NOT NULL,
    vector_dim INTEGER,
    embedding_vector VECTOR,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_anchor_vector_canonical UNIQUE (anchor_event_id, embedding_model, embedding_version)
);

CREATE INDEX IF NOT EXISTS ix_anchor_vector_anchor_code
    ON trading.anchor_vector(anchor_code, embedding_model, embedding_version);
CREATE INDEX IF NOT EXISTS ix_anchor_vector_event_id
    ON trading.anchor_vector(anchor_event_id);

-- Backtest run: canonical run header in local/result schema.
CREATE TABLE IF NOT EXISTS bt_result.backtest_run (
    id BIGSERIAL PRIMARY KEY,
    run_key TEXT NOT NULL,
    scenario_id TEXT NOT NULL,
    strategy_id TEXT,
    market TEXT,
    config_version TEXT NOT NULL,
    data_source TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'CREATED',
    initial_capital NUMERIC(18,6),
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_backtest_run_run_key UNIQUE (run_key)
);

CREATE INDEX IF NOT EXISTS ix_backtest_run_scenario_started_at
    ON bt_result.backtest_run(scenario_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_backtest_run_status_started_at
    ON bt_result.backtest_run(status, started_at DESC);

-- Backtest trade: per-trade/per-leg executed artifact for a run.
CREATE TABLE IF NOT EXISTS bt_result.backtest_trade (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES bt_result.backtest_run(id) ON DELETE CASCADE,
    plan_key TEXT,
    ticker_id INTEGER,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    opened_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    quantity NUMERIC(18,8),
    entry_price NUMERIC(18,8),
    exit_price NUMERIC(18,8),
    gross_pnl NUMERIC(18,8),
    net_pnl NUMERIC(18,8),
    return_pct NUMERIC(18,8),
    fill_status TEXT,
    trade_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_backtest_trade_run_id
    ON bt_result.backtest_trade(run_id);
CREATE INDEX IF NOT EXISTS ix_backtest_trade_symbol_opened_at
    ON bt_result.backtest_trade(symbol, opened_at DESC);
CREATE INDEX IF NOT EXISTS ix_backtest_trade_ticker_opened_at
    ON bt_result.backtest_trade(ticker_id, opened_at DESC);

-- Backtest metric: canonical metric/key-value table per run/config/version.
CREATE TABLE IF NOT EXISTS bt_result.backtest_metric (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES bt_result.backtest_run(id) ON DELETE CASCADE,
    metric_group TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC(24,10),
    metric_text TEXT,
    config_version TEXT NOT NULL,
    metric_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_backtest_metric_canonical UNIQUE (run_id, metric_group, metric_name, config_version)
);

CREATE INDEX IF NOT EXISTS ix_backtest_metric_run_group
    ON bt_result.backtest_metric(run_id, metric_group);
CREATE INDEX IF NOT EXISTS ix_backtest_metric_name
    ON bt_result.backtest_metric(metric_name);

-- Purpose: materialized calibration cache for full-universe frozen-seed Optuna.
-- Scope: local research/backtest PostgreSQL only.

CREATE TABLE IF NOT EXISTS bt_result.calibration_bundle_run (
    id BIGSERIAL PRIMARY KEY,
    bundle_key TEXT NOT NULL UNIQUE,
    market TEXT NOT NULL,
    strategy_mode TEXT NOT NULL,
    policy_scope TEXT NOT NULL,
    seed_profile TEXT NOT NULL,
    proof_reference_run TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    chunk_size INTEGER NOT NULL,
    worker_count INTEGER NOT NULL,
    universe_symbol_count INTEGER NOT NULL DEFAULT 0,
    buy_candidate_count INTEGER NOT NULL DEFAULT 0,
    sell_replay_row_count INTEGER NOT NULL DEFAULT 0,
    source_chunk_count INTEGER NOT NULL DEFAULT 0,
    failed_chunk_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CHECK (status IN ('pending', 'running', 'ok', 'partial', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_calibration_bundle_run_status_started_at
    ON bt_result.calibration_bundle_run(status, started_at DESC);

CREATE TABLE IF NOT EXISTS bt_result.calibration_chunk_run (
    id BIGSERIAL PRIMARY KEY,
    bundle_run_id BIGINT NOT NULL REFERENCES bt_result.calibration_bundle_run(id) ON DELETE CASCADE,
    chunk_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    symbols_json TEXT NOT NULL,
    symbol_count INTEGER NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    elapsed_ms BIGINT,
    soft_timeout_exceeded BOOLEAN NOT NULL DEFAULT FALSE,
    last_error TEXT,
    seed_row_count INTEGER NOT NULL DEFAULT 0,
    replay_bar_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (bundle_run_id, chunk_id),
    CHECK (status IN ('pending', 'running', 'reused', 'ok', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_calibration_chunk_run_bundle_status_chunk
    ON bt_result.calibration_chunk_run(bundle_run_id, status, chunk_id);

CREATE TABLE IF NOT EXISTS bt_result.calibration_seed_row (
    bundle_run_id BIGINT NOT NULL REFERENCES bt_result.calibration_bundle_run(id) ON DELETE CASCADE,
    decision_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    market TEXT NOT NULL,
    policy_family TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    lower_bound DOUBLE PRECISION,
    q10_return DOUBLE PRECISION,
    q50_return DOUBLE PRECISION,
    q90_return DOUBLE PRECISION,
    interval_width DOUBLE PRECISION,
    uncertainty DOUBLE PRECISION,
    member_mixture_ess DOUBLE PRECISION,
    member_top1_weight_share DOUBLE PRECISION,
    member_pre_truncation_count INTEGER,
    forecast_selected BOOLEAN NOT NULL DEFAULT FALSE,
    optuna_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    recurring_family BOOLEAN NOT NULL DEFAULT FALSE,
    single_prototype_collapse BOOLEAN NOT NULL DEFAULT FALSE,
    regime_code TEXT,
    sector_code TEXT,
    member_consensus_signature TEXT,
    q50_d2_return DOUBLE PRECISION,
    q50_d3_return DOUBLE PRECISION,
    p_resolved_by_d2 DOUBLE PRECISION,
    p_resolved_by_d3 DOUBLE PRECISION,
    t1_open DOUBLE PRECISION,
    PRIMARY KEY (bundle_run_id, decision_date, symbol, side),
    CHECK (side IN ('BUY', 'SELL'))
);

CREATE INDEX IF NOT EXISTS ix_calibration_seed_row_bundle_date_side
    ON bt_result.calibration_seed_row(bundle_run_id, decision_date, side);
CREATE INDEX IF NOT EXISTS ix_calibration_seed_row_bundle_symbol_date
    ON bt_result.calibration_seed_row(bundle_run_id, symbol, decision_date);
CREATE INDEX IF NOT EXISTS ix_calibration_seed_row_bundle_family_collapse_side
    ON bt_result.calibration_seed_row(bundle_run_id, policy_family, single_prototype_collapse, side);

CREATE TABLE IF NOT EXISTS bt_result.calibration_replay_bar (
    bundle_run_id BIGINT NOT NULL REFERENCES bt_result.calibration_bundle_run(id) ON DELETE CASCADE,
    decision_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    bar_n INTEGER NOT NULL,
    session_date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    PRIMARY KEY (bundle_run_id, decision_date, symbol, side, bar_n),
    CHECK (side IN ('BUY', 'SELL')),
    CHECK (bar_n BETWEEN 1 AND 5)
);

CREATE INDEX IF NOT EXISTS ix_calibration_replay_bar_bundle_date_symbol_side
    ON bt_result.calibration_replay_bar(bundle_run_id, decision_date, symbol, side);

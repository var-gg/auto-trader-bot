-- Purpose: add snapshot-reuse materialization tables and timing ledger columns
-- for replay-only calibration bundle generation.

ALTER TABLE bt_result.calibration_bundle_run
    ADD COLUMN IF NOT EXISTS snapshot_cadence TEXT NOT NULL DEFAULT 'daily',
    ADD COLUMN IF NOT EXISTS model_version TEXT NOT NULL DEFAULT 'daily_reuse_v1';

CREATE TABLE IF NOT EXISTS bt_result.calibration_query_feature_row (
    bundle_run_id BIGINT NOT NULL REFERENCES bt_result.calibration_bundle_run(id) ON DELETE CASCADE,
    decision_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    execution_date DATE,
    t1_open DOUBLE PRECISION,
    regime_code TEXT,
    sector_code TEXT,
    feature_anchor_ts_utc TIMESTAMPTZ,
    macro_asof_ts_utc TIMESTAMPTZ,
    raw_features_json TEXT NOT NULL,
    transformed_features_json TEXT NOT NULL,
    embedding_json TEXT NOT NULL,
    query_meta_json TEXT NOT NULL,
    PRIMARY KEY (bundle_run_id, decision_date, symbol)
);

CREATE INDEX IF NOT EXISTS ix_calibration_query_feature_row_bundle_date_symbol
    ON bt_result.calibration_query_feature_row(bundle_run_id, decision_date, symbol);

CREATE INDEX IF NOT EXISTS ix_calibration_query_feature_row_bundle_symbol_date
    ON bt_result.calibration_query_feature_row(bundle_run_id, symbol, decision_date);

CREATE TABLE IF NOT EXISTS bt_result.calibration_snapshot_run (
    id BIGSERIAL PRIMARY KEY,
    bundle_run_id BIGINT NOT NULL REFERENCES bt_result.calibration_bundle_run(id) ON DELETE CASCADE,
    snapshot_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    train_start DATE,
    train_end DATE NOT NULL,
    spec_hash TEXT NOT NULL,
    memory_version TEXT NOT NULL,
    model_version TEXT NOT NULL DEFAULT 'daily_reuse_v1',
    snapshot_cadence TEXT NOT NULL DEFAULT 'daily',
    status TEXT NOT NULL DEFAULT 'pending',
    artifact_path TEXT,
    event_record_count INTEGER NOT NULL DEFAULT 0,
    prototype_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    UNIQUE (bundle_run_id, snapshot_id),
    CHECK (status IN ('pending', 'running', 'ok', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_calibration_snapshot_run_bundle_snapshot_date
    ON bt_result.calibration_snapshot_run(bundle_run_id, snapshot_date, status);

ALTER TABLE bt_result.calibration_chunk_run
    ADD COLUMN IF NOT EXISTS load_ms BIGINT,
    ADD COLUMN IF NOT EXISTS query_feature_ms BIGINT,
    ADD COLUMN IF NOT EXISTS snapshot_load_ms BIGINT,
    ADD COLUMN IF NOT EXISTS score_ms BIGINT,
    ADD COLUMN IF NOT EXISTS db_write_ms BIGINT,
    ADD COLUMN IF NOT EXISTS decision_date_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS query_row_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS raw_event_row_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prototype_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS forbidden_call_violation BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS forbidden_call_name TEXT;

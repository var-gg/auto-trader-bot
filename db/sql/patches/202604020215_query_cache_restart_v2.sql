-- Purpose: add bundle heartbeat fields and query-cache chunk ledger for safe restart.

ALTER TABLE bt_result.calibration_bundle_run
    ADD COLUMN IF NOT EXISTS current_step TEXT,
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_pid INTEGER,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE TABLE IF NOT EXISTS bt_result.calibration_query_chunk_run (
    id BIGSERIAL PRIMARY KEY,
    bundle_run_id BIGINT NOT NULL REFERENCES bt_result.calibration_bundle_run(id) ON DELETE CASCADE,
    chunk_id INTEGER NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    symbols_json TEXT NOT NULL,
    symbol_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    elapsed_ms BIGINT,
    load_ms BIGINT,
    feature_build_ms BIGINT,
    db_write_ms BIGINT,
    decision_date_count INTEGER NOT NULL DEFAULT 0,
    query_row_count INTEGER NOT NULL DEFAULT 0,
    replay_bar_count INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at TIMESTAMPTZ,
    last_error TEXT,
    UNIQUE (bundle_run_id, chunk_id),
    CHECK (status IN ('pending', 'running', 'reused', 'ok', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_calibration_query_chunk_run_bundle_status_chunk
    ON bt_result.calibration_query_chunk_run(bundle_run_id, status, chunk_id);

CREATE INDEX IF NOT EXISTS ix_calibration_query_chunk_run_bundle_window
    ON bt_result.calibration_query_chunk_run(bundle_run_id, window_start, window_end, chunk_id);

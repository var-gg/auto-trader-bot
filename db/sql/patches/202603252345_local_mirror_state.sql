-- Local trading mirror state/log tables.
-- Apply to local Postgres only.

CREATE TABLE IF NOT EXISTS meta.local_mirror_state (
    table_name TEXT PRIMARY KEY,
    last_cursor_text TEXT,
    last_refreshed_at TIMESTAMPTZ,
    last_mode TEXT,
    row_count BIGINT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS meta.local_mirror_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    mode TEXT NOT NULL,
    table_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'STARTED',
    rows_copied BIGINT NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS ix_local_mirror_run_log_started_at
    ON meta.local_mirror_run_log(started_at DESC);

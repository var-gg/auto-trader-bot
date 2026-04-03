-- Purpose: add intra-phase running counters for snapshot event memory builds.

ALTER TABLE bt_result.calibration_snapshot_run
    ADD COLUMN IF NOT EXISTS current_symbol TEXT,
    ADD COLUMN IF NOT EXISTS symbols_done INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS symbols_total INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS raw_event_row_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pending_record_count INTEGER NOT NULL DEFAULT 0;

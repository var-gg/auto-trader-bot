-- Purpose: add prototype-stage checkpoint and running counter columns
-- for resumable snapshot prototype clustering.

ALTER TABLE bt_result.calibration_snapshot_run
    ADD COLUMN IF NOT EXISTS prototype_rows_total INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prototype_rows_done INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cluster_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_checkpoint_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS checkpoint_path TEXT;

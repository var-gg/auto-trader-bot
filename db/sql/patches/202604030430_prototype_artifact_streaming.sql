ALTER TABLE bt_result.calibration_snapshot_run
    ADD COLUMN IF NOT EXISTS artifact_rows_total INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS artifact_rows_done INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS artifact_part_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS artifact_bytes_written BIGINT NOT NULL DEFAULT 0;

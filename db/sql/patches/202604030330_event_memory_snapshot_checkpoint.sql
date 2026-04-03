ALTER TABLE bt_result.calibration_snapshot_run
    ADD COLUMN IF NOT EXISTS event_candidate_total INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS event_candidate_done INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS current_event_date DATE NULL,
    ADD COLUMN IF NOT EXISTS last_event_checkpoint_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS event_checkpoint_path TEXT NULL;

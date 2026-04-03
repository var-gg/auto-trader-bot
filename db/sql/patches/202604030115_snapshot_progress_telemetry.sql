-- Purpose: add snapshot phase timing and heartbeat telemetry so long-running
-- train snapshot builds can be distinguished from stale harness state.

ALTER TABLE bt_result.calibration_snapshot_run
    ADD COLUMN IF NOT EXISTS current_phase TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS event_memory_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS transform_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prototype_ms BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS artifact_write_ms BIGINT NOT NULL DEFAULT 0;

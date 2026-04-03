-- Purpose: verify snapshot telemetry columns exist after snapshot progress patch.

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'bt_result'
  AND table_name = 'calibration_snapshot_run'
  AND column_name IN (
    'current_phase',
    'last_heartbeat_at',
    'event_memory_ms',
    'transform_ms',
    'prototype_ms',
    'artifact_write_ms'
  )
ORDER BY column_name;

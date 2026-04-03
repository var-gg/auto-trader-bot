-- Purpose: verify prototype snapshot checkpoint columns exist.

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'bt_result'
  AND table_name = 'calibration_snapshot_run'
  AND column_name IN (
    'prototype_rows_total',
    'prototype_rows_done',
    'cluster_count',
    'last_checkpoint_at',
    'checkpoint_path'
  )
ORDER BY column_name;

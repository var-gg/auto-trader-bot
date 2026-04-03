-- Purpose: verify snapshot running counter columns exist.

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'bt_result'
  AND table_name = 'calibration_snapshot_run'
  AND column_name IN (
    'current_symbol',
    'symbols_done',
    'symbols_total',
    'raw_event_row_count',
    'pending_record_count'
  )
ORDER BY column_name;

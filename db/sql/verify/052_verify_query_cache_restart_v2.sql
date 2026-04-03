-- Purpose: verify bundle heartbeat columns and query-cache chunk ledger exist.

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'bt_result'
  AND table_name = 'calibration_bundle_run'
  AND column_name IN (
    'current_step',
    'last_heartbeat_at',
    'last_pid',
    'last_error'
  )
ORDER BY column_name;

SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname = 'bt_result'
  AND tablename IN ('calibration_query_chunk_run')
ORDER BY tablename;

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'bt_result'
  AND indexname IN (
    'ix_calibration_query_chunk_run_bundle_status_chunk',
    'ix_calibration_query_chunk_run_bundle_window'
  )
ORDER BY indexname;

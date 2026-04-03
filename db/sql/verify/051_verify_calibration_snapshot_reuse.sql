-- Purpose: verify snapshot-reuse calibration cache tables and columns exist.

SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname = 'bt_result'
  AND tablename IN (
    'calibration_query_feature_row',
    'calibration_snapshot_run'
  )
ORDER BY tablename;

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'bt_result'
  AND indexname IN (
    'ix_calibration_query_feature_row_bundle_date_symbol',
    'ix_calibration_query_feature_row_bundle_symbol_date',
    'ix_calibration_snapshot_run_bundle_snapshot_date'
  )
ORDER BY indexname;

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'bt_result'
  AND table_name = 'calibration_chunk_run'
  AND column_name IN (
    'load_ms',
    'query_feature_ms',
    'snapshot_load_ms',
    'score_ms',
    'db_write_ms',
    'decision_date_count',
    'query_row_count',
    'raw_event_row_count',
    'prototype_count',
    'forbidden_call_violation',
    'forbidden_call_name'
  )
ORDER BY column_name;

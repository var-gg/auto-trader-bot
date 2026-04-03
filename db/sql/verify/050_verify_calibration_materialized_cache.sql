-- Purpose: verify calibration materialized cache tables and indexes exist.

SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname = 'bt_result'
  AND tablename IN (
    'calibration_bundle_run',
    'calibration_chunk_run',
    'calibration_seed_row',
    'calibration_replay_bar'
  )
ORDER BY tablename;

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'bt_result'
  AND indexname IN (
    'ix_calibration_bundle_run_status_started_at',
    'ix_calibration_chunk_run_bundle_status_chunk',
    'ix_calibration_seed_row_bundle_date_side',
    'ix_calibration_seed_row_bundle_symbol_date',
    'ix_calibration_seed_row_bundle_family_collapse_side',
    'ix_calibration_replay_bar_bundle_date_symbol_side'
  )
ORDER BY indexname;

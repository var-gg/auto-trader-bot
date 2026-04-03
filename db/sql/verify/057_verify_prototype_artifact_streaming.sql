SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = 'bt_result'
   AND table_name = 'calibration_snapshot_run'
   AND column_name IN (
       'artifact_rows_total',
       'artifact_rows_done',
       'artifact_part_count',
       'artifact_bytes_written'
   )
 ORDER BY column_name;

SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = 'bt_result'
   AND table_name = 'calibration_snapshot_run'
   AND column_name IN (
       'event_candidate_total',
       'event_candidate_done',
       'current_event_date',
       'last_event_checkpoint_at',
       'event_checkpoint_path'
   )
 ORDER BY column_name;

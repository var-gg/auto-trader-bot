SELECT table_name, column_name
  FROM information_schema.columns
 WHERE table_schema = 'bt_result'
   AND (
       (
           table_name = 'calibration_snapshot_run'
           AND column_name IN (
               'event_cache_build_ms',
               'eligible_event_count',
               'scaler_reconstruct_ms',
               'prototype_prepare_ms'
           )
       )
       OR (
           table_name = 'calibration_chunk_run'
           AND column_name IN (
               'snapshot_core_load_ms',
               'query_parse_ms',
               'query_transform_ms',
               'prototype_score_ms',
               'member_lazy_load_ms',
               'query_block_count'
           )
       )
   )
 ORDER BY table_name, column_name;

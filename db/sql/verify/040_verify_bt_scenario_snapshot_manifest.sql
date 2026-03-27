SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'meta'
  AND table_name = 'bt_scenario_snapshot_manifest'
  AND column_name IN (
    'snapshot_id',
    'scenario_id',
    'phase',
    'source_kind',
    'market',
    'window_start',
    'window_end',
    'universe_hash',
    'spec_hash',
    'row_count',
    'created_at',
    'notes',
    'source_path',
    'copied_from_scenario_id'
  )
ORDER BY column_name;

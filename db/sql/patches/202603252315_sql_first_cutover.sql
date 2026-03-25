-- Marker patch for SQL-first cutover.
-- Intentionally lightweight: records the transition under sql_patch_log.
-- Future schema changes should be added as new files in db/sql/patches.

SELECT 1;

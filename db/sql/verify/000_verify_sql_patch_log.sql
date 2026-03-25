SELECT patch_name, patch_group, checksum_sha256, applied_at, applied_by, tool_name, success, execution_ms
FROM meta.sql_patch_log
ORDER BY applied_at DESC, patch_name DESC;

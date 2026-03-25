-- Purpose: bootstrap shared metadata schemas/tables for SQL-first patching and sync tracking.
-- Scope: production + local.
-- Notes:
--   * This file is safe to re-run.
--   * DDL ownership lives here; ORM must only map/query these tables.

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.sql_patch_log (
    patch_name TEXT PRIMARY KEY,
    patch_group TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by TEXT NOT NULL DEFAULT CURRENT_USER,
    tool_name TEXT NOT NULL DEFAULT 'scripts/db_apply_sql.py',
    success BOOLEAN NOT NULL DEFAULT TRUE,
    execution_ms INTEGER,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS ix_sql_patch_log_applied_at
    ON meta.sql_patch_log(applied_at DESC);

CREATE TABLE IF NOT EXISTS meta.sync_state (
    sync_key TEXT PRIMARY KEY,
    sync_group TEXT NOT NULL,
    last_cursor_text TEXT,
    last_synced_at TIMESTAMPTZ,
    last_status TEXT,
    row_count BIGINT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS ix_sync_state_group_synced_at
    ON meta.sync_state(sync_group, last_synced_at DESC);

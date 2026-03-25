-- SQL-first patch bootstrap. Apply before any db/sql/patches/*.sql files.
-- DDL ownership is SQL-only; ORM is mapping-only.

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

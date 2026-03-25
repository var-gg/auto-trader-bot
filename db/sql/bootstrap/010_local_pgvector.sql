-- Purpose: local-only bootstrap for vector support.
-- Scope: local Postgres only. Do NOT apply blindly to production.
-- Notes:
--   * Required before creating vector-backed local tables if pgvector is not already installed.
--   * Keep extension/bootstrap separate from production patches.

CREATE EXTENSION IF NOT EXISTS vector;

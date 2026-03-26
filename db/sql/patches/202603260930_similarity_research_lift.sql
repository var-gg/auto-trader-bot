-- Purpose: extend canonical anchor_event / anchor_vector for practical similarity backtests.
-- Scope: local + production SQL-first schema evolution.
-- Rules:
--   * keep canonical tables, no per-config physical tables
--   * do not hide core similarity/backtest metrics only inside JSON payloads
--   * preserve backward compatibility with existing anchor_event / anchor_vector rows

ALTER TABLE trading.anchor_event
    ADD COLUMN IF NOT EXISTS mae_pct NUMERIC(18,8),
    ADD COLUMN IF NOT EXISTS mfe_pct NUMERIC(18,8),
    ADD COLUMN IF NOT EXISTS days_to_hit INTEGER,
    ADD COLUMN IF NOT EXISTS after_cost_return_pct NUMERIC(18,8),
    ADD COLUMN IF NOT EXISTS quality_score NUMERIC(18,8),
    ADD COLUMN IF NOT EXISTS regime_code TEXT,
    ADD COLUMN IF NOT EXISTS sector_code TEXT,
    ADD COLUMN IF NOT EXISTS liquidity_score NUMERIC(18,8),
    ADD COLUMN IF NOT EXISTS prototype_id TEXT,
    ADD COLUMN IF NOT EXISTS prototype_membership JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS ix_anchor_event_quality_score
    ON trading.anchor_event(quality_score DESC, reference_date DESC);
CREATE INDEX IF NOT EXISTS ix_anchor_event_regime_sector
    ON trading.anchor_event(regime_code, sector_code, reference_date DESC);
CREATE INDEX IF NOT EXISTS ix_anchor_event_prototype_id
    ON trading.anchor_event(prototype_id);

ALTER TABLE trading.anchor_vector
    ADD COLUMN IF NOT EXISTS shape_vector VECTOR,
    ADD COLUMN IF NOT EXISTS ctx_vector VECTOR,
    ADD COLUMN IF NOT EXISTS vector_version TEXT,
    ADD COLUMN IF NOT EXISTS shape_vector_dim INTEGER,
    ADD COLUMN IF NOT EXISTS ctx_vector_dim INTEGER,
    ADD COLUMN IF NOT EXISTS prototype_membership JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE trading.anchor_vector
SET vector_version = COALESCE(vector_version, embedding_version)
WHERE vector_version IS NULL;

CREATE INDEX IF NOT EXISTS ix_anchor_vector_vector_version
    ON trading.anchor_vector(anchor_code, embedding_model, vector_version);
CREATE INDEX IF NOT EXISTS ix_anchor_vector_dims
    ON trading.anchor_vector(vector_dim, shape_vector_dim, ctx_vector_dim);

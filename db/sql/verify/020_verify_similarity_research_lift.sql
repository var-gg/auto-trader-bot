-- Purpose: verify similarity-backtest lift columns exist on canonical anchor tables.
-- Scope: local + production verification.

SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'trading'
  AND (
    (table_name = 'anchor_event' AND column_name IN (
        'mae_pct',
        'mfe_pct',
        'days_to_hit',
        'after_cost_return_pct',
        'quality_score',
        'regime_code',
        'sector_code',
        'liquidity_score',
        'prototype_id',
        'prototype_membership'
    ))
    OR
    (table_name = 'anchor_vector' AND column_name IN (
        'shape_vector',
        'ctx_vector',
        'vector_dim',
        'vector_version',
        'embedding_model',
        'shape_vector_dim',
        'ctx_vector_dim',
        'prototype_membership'
    ))
  )
ORDER BY table_name, column_name;

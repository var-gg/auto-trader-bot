-- Purpose: verify canonical anchor/backtest/meta tables exist after SQL-first apply.
-- Scope: production + local verification.

SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema, table_name) IN (
    ('trading', 'anchor_label_run'),
    ('trading', 'anchor_event'),
    ('trading', 'anchor_vector'),
    ('bt_result', 'backtest_run'),
    ('bt_result', 'backtest_trade'),
    ('bt_result', 'backtest_metric'),
    ('meta', 'sql_patch_log'),
    ('meta', 'sync_state')
)
ORDER BY table_schema, table_name;

-- Purpose: verify bt_result analysis indexes exist.
-- Scope: local + production verification.

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'bt_result'
  AND indexname IN (
    'ix_backtest_run_strategy_market_started_at',
    'ix_backtest_run_data_source_started_at',
    'ix_backtest_trade_plan_key',
    'ix_backtest_trade_fill_status',
    'ix_backtest_metric_group_name'
  )
ORDER BY indexname;

-- Purpose: add analysis-oriented indexes for bt_result persistence/query paths.
-- Scope: local + production SQL-first schema evolution.

CREATE INDEX IF NOT EXISTS ix_backtest_run_strategy_market_started_at
    ON bt_result.backtest_run(strategy_id, market, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_backtest_run_data_source_started_at
    ON bt_result.backtest_run(data_source, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_backtest_trade_plan_key
    ON bt_result.backtest_trade(plan_key);
CREATE INDEX IF NOT EXISTS ix_backtest_trade_fill_status
    ON bt_result.backtest_trade(fill_status, closed_at DESC);
CREATE INDEX IF NOT EXISTS ix_backtest_metric_group_name
    ON bt_result.backtest_metric(metric_group, metric_name, created_at DESC);

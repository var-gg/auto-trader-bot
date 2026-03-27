-- Optional seed/ops metadata for local backtest database.
-- Apply manually after 001_local_backtest_schema.sql.

CREATE TABLE IF NOT EXISTS trading.bt_mirror_whitelist (
    source_table TEXT PRIMARY KEY,
    target_table TEXT NOT NULL,
    notes TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO trading.bt_mirror_whitelist(source_table, target_table, notes)
VALUES
    ('trading.ticker', 'trading.bt_mirror_ticker', 'Minimal symbol universe mirror used by local backtest/research'),
    ('trading.sector', 'trading.bt_mirror_sector', 'Sector reference mirror used by TOBE/local research preflight and candidate context'),
    ('trading.industry', 'trading.bt_mirror_industry', 'Industry reference mirror used by TOBE/local research preflight and candidate context'),
    ('trading.ticker_industry', 'trading.bt_mirror_ticker_industry', 'Ticker-to-industry reference mirror used by TOBE/local research preflight and candidate context'),
    ('trading.ohlcv_daily', 'trading.bt_mirror_ohlcv_daily', 'Daily bar mirror used by local backtest/research')
ON CONFLICT (source_table) DO UPDATE
SET target_table = EXCLUDED.target_table,
    notes = EXCLUDED.notes,
    enabled = TRUE;

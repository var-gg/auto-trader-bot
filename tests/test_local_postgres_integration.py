import os
import uuid
from contextlib import suppress

import pytest
from sqlalchemy import create_engine, text

from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.runner import cli


ADMIN_DB_URL = os.getenv("BACKTEST_TEST_ADMIN_DB_URL", "postgresql+psycopg2://postgres:7508@127.0.0.1:5433/postgres")


def _admin_engine():
    return create_engine(ADMIN_DB_URL, future=True)


def _can_connect() -> bool:
    try:
        with _admin_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _can_connect(), reason="local ephemeral postgres not available")


@pytest.fixture()
def temp_backtest_db():
    db_name = f"bt_it_{uuid.uuid4().hex[:8]}"
    admin = _admin_engine()
    with admin.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    db_url = f"postgresql+psycopg2://postgres:7508@127.0.0.1:5433/{db_name}"
    engine = create_engine(db_url, future=True)
    with engine.begin() as conn:
        conn.connection.cursor().execute(
            """
            CREATE SCHEMA trading;
            CREATE TABLE trading.bt_mirror_ohlcv_daily (
                ticker_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open NUMERIC(18,6) NOT NULL,
                high NUMERIC(18,6) NOT NULL,
                low NUMERIC(18,6) NOT NULL,
                close NUMERIC(18,6) NOT NULL,
                volume BIGINT,
                PRIMARY KEY (ticker_id, trade_date)
            );
            CREATE TABLE trading.bt_event_window (
                id BIGSERIAL PRIMARY KEY,
                scenario_id TEXT NOT NULL,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                ticker_id INTEGER,
                event_time TIMESTAMPTZ NOT NULL,
                anchor_date DATE,
                reference_date DATE NOT NULL,
                side_bias TEXT NOT NULL,
                signal_strength NUMERIC(18,8) NOT NULL,
                confidence NUMERIC(18,8),
                current_price NUMERIC(18,6),
                atr_pct NUMERIC(18,8),
                target_return_pct NUMERIC(18,8),
                max_reverse_pct NUMERIC(18,8),
                expected_horizon_days INTEGER,
                reverse_breach_day INTEGER,
                outcome_label TEXT,
                provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
                diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
                notes JSONB NOT NULL DEFAULT '[]'::jsonb
            );
            CREATE TABLE trading.macro_data_series (
                id BIGINT PRIMARY KEY,
                fred_series_id TEXT,
                name TEXT,
                frequency TEXT,
                unit TEXT,
                is_active BOOLEAN,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            );
            CREATE TABLE trading.macro_data_series_value (
                id BIGINT PRIMARY KEY,
                series_id BIGINT,
                obs_date DATE,
                value NUMERIC(18,8),
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ
            );
            """
        )
        conn.execute(text("""
            INSERT INTO trading.bt_mirror_ohlcv_daily(ticker_id, symbol, trade_date, open, high, low, close, volume)
            VALUES
              (1, 'AAPL', '2026-01-01', 100, 104, 99, 103, 1000000),
              (1, 'AAPL', '2026-01-02', 103, 105, 101, 104, 1100000),
              (1, 'AAPL', '2026-01-03', 104, 106, 102, 105, 1200000),
              (1, 'AAPL', '2026-01-04', 105, 107, 103, 106, 1300000),
              (1, 'AAPL', '2026-01-05', 106, 108, 104, 107, 1250000),
              (1, 'AAPL', '2026-01-06', 107, 109, 105, 108, 1280000),
              (1, 'AAPL', '2026-01-07', 108, 111, 107, 110, 1400000),
              (1, 'AAPL', '2026-01-08', 110, 112, 109, 111, 1410000),
              (1, 'AAPL', '2026-01-09', 111, 114, 110, 113, 1500000),
              (1, 'AAPL', '2026-01-10', 113, 116, 112, 115, 1550000);
        """))
        conn.execute(text("""
            INSERT INTO trading.bt_event_window(
                scenario_id, market, symbol, ticker_id, event_time, anchor_date, reference_date,
                side_bias, signal_strength, confidence, current_price, atr_pct, target_return_pct,
                max_reverse_pct, expected_horizon_days, reverse_breach_day, outcome_label,
                provenance, diagnostics, notes
            ) VALUES (
                'scn-it', 'US', 'AAPL', 1, '2026-01-05T00:00:00+00:00', '2026-01-05', '2026-01-05',
                'BUY', 0.8, 0.7, 107, 0.03, 0.05, 0.02, 5, NULL, 'UNKNOWN', '{}'::jsonb, '{}'::jsonb, '[]'::jsonb
            );
        """))
        conn.execute(text("""
            INSERT INTO trading.macro_data_series(id, fred_series_id, name, frequency, unit, is_active, created_at, updated_at)
            VALUES (1, 'GROWTH', 'growth', 'D', 'pct', TRUE, NOW(), NOW());
            INSERT INTO trading.macro_data_series_value(id, series_id, obs_date, value, created_at, updated_at)
            VALUES (1, 1, '2026-01-10', 0.2, NOW(), NOW());
        """))
    try:
        yield db_url
    finally:
        with suppress(Exception):
            engine.dispose()
        with admin.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            with suppress(Exception):
                conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))


def test_actual_local_postgres_loader_path(temp_backtest_db):
    cfg = LocalBacktestDbConfig(url=temp_backtest_db, schema="trading")
    loader = LocalPostgresLoader(create_backtest_session_factory(cfg), schema="trading")
    historical = loader.load_for_scenario(
        scenario_id="scn-it",
        market="US",
        start_date="2026-01-01",
        end_date="2026-01-10",
        symbols=["AAPL"],
        strategy_mode="legacy_event_window",
    )
    assert historical.candidates
    assert historical.candidates[0].symbol == "AAPL"
    assert historical.bars_by_symbol["AAPL"]


def test_same_db_same_config_same_seed_same_result(temp_backtest_db, monkeypatch):
    monkeypatch.setenv("BACKTEST_DB_URL", temp_backtest_db)
    monkeypatch.setenv("BACKTEST_DB_SCHEMA", "trading")
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(
            scenario_id="scn-it",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-10",
            symbols=["AAPL"],
        ),
        config=cli.BacktestConfig(initial_capital=10000.0, metadata={"seed": "0"}),
    )
    left = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-it", strategy_mode="legacy_event_window")
    right = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-it", strategy_mode="legacy_event_window")
    assert left["summary"] == right["summary"]
    assert left["plans"] == right["plans"]
    assert left["fills"] == right["fills"]


def test_research_similarity_v2_actual_loader_and_runner(temp_backtest_db, monkeypatch):
    monkeypatch.setenv("BACKTEST_DB_URL", temp_backtest_db)
    monkeypatch.setenv("BACKTEST_DB_SCHEMA", "trading")
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(
            scenario_id="scn-it-v2",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-10",
            symbols=["AAPL"],
        ),
        config=cli.BacktestConfig(initial_capital=10000.0, metadata={"seed": "0"}),
    )
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-it-v2", strategy_mode="research_similarity_v2")
    assert result["strategy_mode"] == "research_similarity_v2"
    assert isinstance(result["artifacts"]["signal_panel"], list)
    assert result["diagnostics"].get("signal_panel") is not None

from datetime import datetime

import pytest

from backtest_app.db.local_session import guard_backtest_local_only
from backtest_app.runner import cli


def test_guard_blocks_cloud_sql_url():
    with pytest.raises(ValueError):
        guard_backtest_local_only("postgresql://user:pass@/db?host=/cloudsql/project:region:instance")


def test_run_backtest_uses_local_db_loader(monkeypatch):
    historical = type("Historical", (), {"market_snapshot": type("Snap", (), {"as_of": datetime(2026, 1, 1, 0, 0, 0)})(), "candidates": [], "bars_by_symbol": {}})()

    class FakeLoader:
        def __init__(self, session_factory, schema="trading"):
            self.session_factory = session_factory
            self.schema = schema

        def load_for_scenario(self, **kwargs):
            assert kwargs["scenario_id"] == "scn-1"
            return historical

    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeLoader)

    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(scenario_id="scn-1", market="US", start_date="2026-01-01", end_date="2026-01-31", symbols=["AAPL"]),
        config=cli.BacktestConfig(initial_capital=10000.0),
    )

    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-1")
    assert result["scenario"] == "scn-1"
    assert result["summary"]["scenario_id"] == "scn-1"

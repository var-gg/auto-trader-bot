from datetime import datetime

from backtest_app.runner import cli


class FakeLoader:
    def __init__(self, session_factory, schema="trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, **kwargs):
        return type(
            "Historical",
            (),
            {
                "market_snapshot": type("Snap", (), {"as_of": datetime(2026, 1, 1, 0, 0, 0)})(),
                "candidates": [],
                "bars_by_symbol": {},
            },
        )()


def _request():
    return cli.RunnerRequest(
        scenario=cli.BacktestScenario(
            scenario_id="same-seed",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-31",
            symbols=["AAPL"],
        ),
        config=cli.BacktestConfig(initial_capital=10000.0),
    )


def test_same_config_same_result(monkeypatch):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeLoader)
    req = _request()
    left = cli.run_backtest(req, None, data_source="local-db", scenario_id="same-seed")
    right = cli.run_backtest(req, None, data_source="local-db", scenario_id="same-seed")
    assert left == right


def test_backtest_local_db_path_does_not_write_live(monkeypatch):
    writes = []

    def _forbid(*args, **kwargs):
        writes.append((args, kwargs))
        raise AssertionError("live write attempted")

    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeLoader)
    monkeypatch.setattr("backtest_app.db.local_session.guard_backtest_local_only", lambda url: url)
    result = cli.run_backtest(_request(), None, data_source="local-db", scenario_id="same-seed")
    assert result["scenario"] == "same-seed"
    assert writes == []

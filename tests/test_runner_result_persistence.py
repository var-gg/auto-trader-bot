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
                "metadata": {"diagnostics": {"pipeline": {"anchor_count": 1}}, "dump_ref": "local-dump-1"},
            },
        )()


class FakeSqlStore:
    def __init__(self, db_url):
        self.db_url = db_url

    def save_run(self, **kwargs):
        assert kwargs["strategy_mode"] == "research_similarity_v1"
        assert kwargs["snapshot_info"]["historical_metadata"]["dump_ref"] == "local-dump-1"
        return 999


def test_run_backtest_can_persist_sql_results(monkeypatch):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeLoader)
    monkeypatch.setattr(cli, "SqlResultStore", FakeSqlStore)

    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(
            scenario_id="scn-persist",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-31",
            symbols=["AAPL"],
        ),
        config=cli.BacktestConfig(initial_capital=10000.0, metadata={"label_version": "lv1", "vector_version": "vv1"}),
    )

    result = cli.run_backtest(
        request,
        None,
        data_source="local-db",
        scenario_id="scn-persist",
        strategy_mode="research_similarity_v1",
        sql_db_url="postgresql://local",
        save_json=False,
    )
    assert result["sql_run_id"] == 999
    assert result["diagnostics"]["pipeline"]["anchor_count"] == 1

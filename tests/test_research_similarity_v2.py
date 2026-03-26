from datetime import datetime

from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice
from backtest_app.research.pipeline import generate_similarity_candidates_rolling
from backtest_app.runner import cli
from shared.domain.models import MarketCode, MarketSnapshot


def _bars(symbol: str):
    return [
        HistoricalBar(symbol=symbol, timestamp=f"2026-01-{i:02d}", open=100 + i, high=102 + i, low=99 + i, close=101 + i, volume=1000000 + i * 1000)
        for i in range(1, 15)
    ]


def test_generate_similarity_candidates_rolling_builds_panel_without_future_library_leakage():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT")}
    macro_history = {f"2026-01-{i:02d}": {"growth": i / 100.0} for i in range(1, 15)}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market="US", macro_history_by_date=macro_history, abstain_margin=0.0)
    assert isinstance(candidates, list)
    assert diagnostics["signal_panel"]
    first = diagnostics["signal_panel"][0]
    assert first["library"]["anchor_count"] >= 0
    for row in diagnostics["signal_panel"]:
        for side in ("long", "short"):
            for match in row["top_matches"][side]:
                assert "prototype_id" in match
                assert "weight" in match


class FakeRollingLoader:
    def __init__(self, session_factory, schema="trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, **kwargs):
        assert kwargs["strategy_mode"] == "research_similarity_v2"
        return HistoricalSlice(
            market_snapshot=MarketSnapshot(market=MarketCode.US, as_of=datetime(2026, 1, 31, 0, 0, 0), session_label="BACKTEST", is_open=False),
            bars_by_symbol={"AAPL": _bars("AAPL")},
            candidates=[],
            metadata={"diagnostics": {"2026-01-10:AAPL": {"scores": {"abstained": True}}}, "signal_panel_artifact": [{"decision_date": "2026-01-10", "symbol": "AAPL"}]},
        )


def test_run_backtest_supports_research_similarity_v2(monkeypatch):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeRollingLoader)
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(
            scenario_id="scn-r2",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-31",
            symbols=["AAPL"],
        ),
        config=cli.BacktestConfig(initial_capital=10000.0),
    )
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-r2", strategy_mode="research_similarity_v2")
    assert result["strategy_mode"] == "research_similarity_v2"
    assert result["artifacts"]["signal_panel"][0]["decision_date"] == "2026-01-10"

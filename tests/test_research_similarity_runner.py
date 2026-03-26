from datetime import datetime

from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice
from backtest_app.research.pipeline import generate_similarity_candidates
from backtest_app.runner import cli
from shared.domain.models import MarketCode, MarketSnapshot


class FakeSimilarityLoader:
    def __init__(self, session_factory, schema="trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, **kwargs):
        assert kwargs["strategy_mode"] == "research_similarity_v1"
        historical = HistoricalSlice(
            market_snapshot=MarketSnapshot(market=MarketCode.US, as_of=datetime(2026, 1, 31, 0, 0, 0), session_label="BACKTEST", is_open=False),
            bars_by_symbol={
                "AAPL": [HistoricalBar(symbol="AAPL", timestamp="2026-01-01", open=100, high=103, low=99, close=102, volume=1000000)]
            },
            candidates=[],
            metadata={"diagnostics": {"AAPL": {"scores": {"abstained": True}}}},
        )
        return historical


def test_generate_similarity_candidates_abstains_when_margin_is_weak():
    bars_by_symbol = {
        "AAA": [
            HistoricalBar(symbol="AAA", timestamp=f"2025-10-{((i - 1) % 28) + 1:02d}" if i <= 28 else (f"2025-11-{((i - 29) % 28) + 1:02d}" if i <= 56 else f"2025-12-{((i - 57) % 28) + 1:02d}"), open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1000000)
            for i in range(1, 85)
        ]
    }
    candidates, diagnostics = generate_similarity_candidates(
        bars_by_symbol=bars_by_symbol,
        market="US",
        macro_payload={"growth": 0.0, "inflation": 0.0},
        abstain_margin=10.0,
    )
    assert candidates == []
    assert diagnostics["AAA"]["scores"]["abstained"] is True


def test_run_backtest_supports_research_similarity_strategy(monkeypatch):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeSimilarityLoader)
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(
            scenario_id="scn-r1",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-31",
            symbols=["AAPL"],
        ),
        config=cli.BacktestConfig(initial_capital=10000.0),
    )
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-r1", strategy_mode="research_similarity_v1")
    assert result["strategy_mode"] == "research_similarity_v1"
    assert result["diagnostics"]["AAPL"]["scores"]["abstained"] is True
    assert "portfolio" in result

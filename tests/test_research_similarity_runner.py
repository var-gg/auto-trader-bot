from datetime import datetime

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice
from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.pipeline import build_event_memory_asof, generate_similarity_candidates
from backtest_app.runner import cli
from shared.domain.models import MarketCode, MarketSnapshot


class FakeSimilarityLoader:
    def __init__(self, session_factory, schema="trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, **kwargs):
        assert kwargs["strategy_mode"] == "research_similarity_v1"
        assert kwargs["research_spec"].feature_window_bars == 20
        historical = HistoricalSlice(
            market_snapshot=MarketSnapshot(market=MarketCode.US, as_of=datetime(2026, 1, 31, 0, 0, 0), session_label="BACKTEST", is_open=False),
            bars_by_symbol={"AAPL": [HistoricalBar(symbol="AAPL", timestamp="2026-01-01", open=100, high=103, low=99, close=102, volume=1000000)]},
            candidates=[],
            metadata={"diagnostics": {"AAPL": {"scores": {"abstained": True}}}},
        )
        return historical


def test_generate_similarity_candidates_accepts_spec_override():
    bars_by_symbol = {
        "AAA": [HistoricalBar(symbol="AAA", timestamp=f"2025-10-{((i - 1) % 28) + 1:02d}" if i <= 28 else (f"2025-11-{((i - 29) % 28) + 1:02d}" if i <= 56 else f"2025-12-{((i - 57) % 28) + 1:02d}"), open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1000000) for i in range(1, 85)]
    }
    spec = ResearchExperimentSpec(feature_window_bars=20, horizon_days=3, target_return_pct=0.02, stop_return_pct=0.02)
    candidates, diagnostics = generate_similarity_candidates(bars_by_symbol=bars_by_symbol, market="US", macro_payload={"growth": 0.0, "inflation": 0.0}, abstain_margin=10.0, spec=spec)
    assert isinstance(candidates, list)
    assert diagnostics["pipeline"]["spec"]["feature_window_bars"] == 20
    assert diagnostics["pipeline"]["spec_hash"] == spec.spec_hash()


def test_run_backtest_supports_research_similarity_strategy(monkeypatch):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeSimilarityLoader)
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(scenario_id="scn-r1", market="US", start_date="2026-01-01", end_date="2026-01-31", symbols=["AAPL"]),
        config=cli.BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(feature_window_bars=20)),
    )
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-r1", strategy_mode="research_similarity_v1")
    assert result["strategy_mode"] == "research_similarity_v1"
    assert result["diagnostics"]["AAPL"]["scores"]["abstained"] is True
    assert "portfolio" in result


def test_build_event_memory_asof_is_reproducible_and_leak_free(tmp_path):
    spec = ResearchExperimentSpec(feature_window_bars=20, horizon_days=3, target_return_pct=0.02, stop_return_pct=0.02)
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1000000) for i in range(1, 29)]
    bars_by_symbol = {"AAA": bars}
    memory = build_event_memory_asof(decision_date="2025-11-25", spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date={}, sector_map={}, market="US")
    again = build_event_memory_asof(decision_date="2025-11-25", spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date={}, sector_map={}, market="US")
    assert memory["spec_hash"] == again["spec_hash"]
    assert memory["coverage"] == again["coverage"]
    assert all(r.outcome_end_date < "2025-11-25" for r in memory["event_records"])
    store = JsonResearchArtifactStore(str(tmp_path))
    store.save_snapshot(run_id="r1", name="memory_snapshot", spec={**spec.to_dict(), "spec_hash": spec.spec_hash()}, as_of_date="2025-11-25", coverage=memory["coverage"], excluded_reasons=memory["excluded_reasons"], payload={"event_records": [{"event_date": r.event_date, "outcome_end_date": r.outcome_end_date} for r in memory["event_records"]]}, format="json")
    loaded = store.load_snapshot(run_id="r1", name="memory_snapshot", format="json")
    assert loaded is not None
    assert loaded["spec_hash"] == spec.spec_hash()
    assert loaded["as_of_date"] == "2025-11-25"

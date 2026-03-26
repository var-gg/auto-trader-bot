from datetime import datetime

from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice
from backtest_app.research.pipeline import DECISION_CONVENTION, generate_similarity_candidates_rolling
from backtest_app.runner import cli
from shared.domain.models import MarketCode, MarketSnapshot


def _bars(symbol: str):
    rows = []
    price = 100.0
    from datetime import date, timedelta
    start = date(2025, 10, 1)
    for i in range(84):
        d = start + timedelta(days=i)
        open_ = price
        close = price * 1.002
        high = close * 1.01
        low = open_ * 0.99
        rows.append(HistoricalBar(symbol=symbol, timestamp=d.isoformat(), open=open_, high=high, low=low, close=close, volume=1000000 + (i + 1) * 1000))
        price = close
    return rows


def test_generate_similarity_candidates_rolling_builds_panel_without_future_library_leakage():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT")}
    macro_history = {f"2026-01-{i:02d}": {"growth": i / 100.0} for i in range(1, 15)}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market="US", macro_history_by_date=macro_history, abstain_margin=0.0)
    assert isinstance(candidates, list)
    assert diagnostics["signal_panel"]
    assert diagnostics["throughput"]["n_symbols"] == 2
    assert diagnostics["throughput"]["n_decision_dates"] >= 1
    assert diagnostics["signal_panel_jsonl"]
    assert diagnostics["cache_keys"]["library_cache_keys"]
    assert diagnostics["event_records"]
    populated_batches = [batch["records"] for batch in diagnostics["event_records"] if batch["records"]]
    assert populated_batches
    first_batch = populated_batches[0]
    assert "BUY" in first_batch[0]["side_outcomes"]
    assert "SELL" in first_batch[0]["side_outcomes"]
    for row in diagnostics["signal_panel"]:
        assert row["query"]["decision_convention"] == DECISION_CONVENTION
        assert row["query"]["feature_window_bars"] >= 60
        assert row["query"]["feature_coverage_bars"] >= 60
        assert row["query"]["insufficient_history"] is False
        assert row["library"]["max_outcome_end_before_decision"] is None or row["library"]["max_outcome_end_before_decision"] < row["decision_date"]
        if row["library"]["max_outcome_end_before_decision"] is not None:
            assert row["library"]["event_record_count"] >= 1
        for side in ("long", "short"):
            for match in row["top_matches"][side]:
                assert "prototype_id" in match
                assert "weight" in match


def test_generate_similarity_candidates_rolling_uses_next_open_as_current_price():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT")}
    macro_history = {f"2026-01-{i:02d}": {"growth": 0.0} for i in range(1, 15)}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market="US", macro_history_by_date=macro_history, abstain_margin=0.0)
    if candidates:
        c = candidates[0]
        decision_date = c.provenance["decision_date"]
        bars = bars_by_symbol[c.symbol]
        idx = next(i for i, b in enumerate(bars) if str(b.timestamp)[:10] == decision_date)
        assert c.current_price == bars[idx + 1].open


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

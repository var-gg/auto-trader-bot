from datetime import datetime

import types

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice
from backtest_app.research.pipeline import DECISION_CONVENTION, generate_similarity_candidates_rolling
from backtest_app.runner import cli
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.models import ExecutionVenue, FillStatus, LadderLeg, MarketCode, MarketSnapshot, OrderPlan, OrderType, Side


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
    assert diagnostics["prototype_compression_batches"]
    assert diagnostics["prototype_compression_audit"]["batch_count"] >= 1
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
        assert row["query"]["execution_date"] > row["decision_date"]
        assert row["query"]["price_reference_source"] == "next_open"
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
        execution_date = c.provenance["execution_date"]
        bars = bars_by_symbol[c.symbol]
        idx = next(i for i, b in enumerate(bars) if str(b.timestamp)[:10] == decision_date)
        assert c.current_price == bars[idx + 1].open
        assert execution_date == str(bars[idx + 1].timestamp)[:10]
        assert hasattr(c.anchor_date, "isoformat")
        assert hasattr(c.reference_date, "isoformat")


def test_generate_similarity_candidates_rolling_surfaces_debug_funnel_and_spec_horizons():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT")}
    macro_history = {f"2026-01-{i:02d}": {"growth": 0.0} for i in range(1, 15)}
    spec = ResearchExperimentSpec(feature_window_bars=20, horizon_days=3, lookback_horizons=[2, 4, 8])
    _, diagnostics = generate_similarity_candidates_rolling(
        bars_by_symbol=bars_by_symbol,
        market="US",
        macro_history_by_date=macro_history,
        spec=spec,
        abstain_margin=0.25,
        metadata={"quote_min_effective_sample_size": "1.7", "quote_uncertainty_cap": "0.11", "quote_max_return_interval_width": "0.09", "quote_min_regime_alignment": "0.4", "quote_ev_threshold": "0.006"},
    )
    assert diagnostics["pipeline"]["ev_config"]["min_effective_sample_size"] == 1.7
    assert diagnostics["pipeline"]["ev_config"]["max_uncertainty"] == 0.11
    assert diagnostics["pipeline"]["ev_config"]["max_return_interval_width"] == 0.09
    assert diagnostics["pipeline"]["ev_config"]["min_regime_alignment"] == 0.4
    assert diagnostics["pipeline"]["ev_config"]["min_expected_utility"] == 0.006
    row = diagnostics["signal_panel"][0]
    assert row["query"]["shape_horizons"] == [2, 4, 8]
    assert "query_panel_count" in row["query"]
    assert "prototype_pool_size" in row["decision_surface"]
    assert "chosen_effective_sample_size" in row["decision_surface"]
    assert "chosen_uncertainty" in row["decision_surface"]
    assert "buy" in row["scorer_diagnostics"]
    assert "sell" in row["scorer_diagnostics"]
    assert "fallback_raw_ev" in row["scorer_diagnostics"]["buy"]
    assert "top_matches_summary" in row["scorer_diagnostics"]["buy"]
    assert "member_top_matches_summary" in row["scorer_diagnostics"]["buy"]
    assert "top1_weight_share" in row["scorer_diagnostics"]["buy"]
    assert "consensus_signature" in row["scorer_diagnostics"]["buy"]
    assert "mixture_ess" in row["scorer_diagnostics"]["buy"]
    assert "member_mixture_ess" in row["scorer_diagnostics"]["buy"]
    assert "q50_d2_return" in row["scorer_diagnostics"]["buy"]
    assert isinstance(diagnostics["artifacts"]["excluded_reasons_histogram"], dict)


def test_generate_similarity_candidates_rolling_supports_diagnostic_lower_bound_ablation():
    bars_by_symbol = {"AAPL": _bars("AAPL"), "MSFT": _bars("MSFT")}
    macro_history = {f"2026-01-{i:02d}": {"growth": 0.0} for i in range(1, 15)}
    _, diagnostics = generate_similarity_candidates_rolling(
        bars_by_symbol=bars_by_symbol,
        market="US",
        macro_history_by_date=macro_history,
        abstain_margin=0.0,
        metadata={"diagnostic_disable_lower_bound_gate": "true"},
    )
    row = diagnostics["signal_panel"][0]
    assert diagnostics["pipeline"]["ev_config"]["diagnostic_disable_lower_bound_gate"] is True
    assert row["decision_surface"]["gate_ablation"]["diagnostic_disable_lower_bound_gate"] is True
    assert row["decision_surface"]["decision_rule"]["diagnostic_disable_lower_bound_gate"] is True


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


def test_simulated_broker_blocks_same_day_fill_before_execution_start_for_research_v2():
    broker = SimulatedBroker(rules=SimulationRules())
    plan = OrderPlan(
        plan_id="p1",
        symbol="AAPL",
        ticker_id=1,
        side=Side.BUY,
        generated_at=datetime(2026, 1, 10, 15, 30, 0),
        status="READY",
        rationale="test",
        venue=ExecutionVenue.BACKTEST,
        requested_budget=1000.0,
        requested_quantity=1,
        legs=[LadderLeg(leg_id="l1", side=Side.BUY, order_type=OrderType.LIMIT, quantity=1, limit_price=100.0)],
        metadata={"earliest_fill_ts": "2026-01-11T09:00:00", "quote_policy": {}},
    )
    bars = [
        HistoricalBar(symbol="AAPL", timestamp="2026-01-10", open=101.0, high=102.0, low=99.0, close=101.0, volume=1000),
        HistoricalBar(symbol="AAPL", timestamp="2026-01-11", open=103.0, high=104.0, low=101.0, close=103.0, volume=1000),
    ]
    fills = broker.simulate_plan(plan, bars)
    assert fills[0].fill_status == FillStatus.UNFILLED


def test_research_v2_planned_exit_uses_first_fill_date_plus_horizon(monkeypatch):
    bars = {"AAPL": [HistoricalBar(symbol="AAPL", timestamp=f"2026-01-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100 + i, volume=1000) for i in range(1, 12)]}

    class Loader:
        def __init__(self, *args, **kwargs):
            pass
        def load_for_scenario(self, **kwargs):
            candidate = types.SimpleNamespace(symbol="AAPL", side_bias=Side.BUY, current_price=102.0, expected_horizon_days=5, diagnostics={}, reference_date="2026-01-01", anchor_date="2026-01-01")
            return HistoricalSlice(market_snapshot=MarketSnapshot(market=MarketCode.US, as_of=datetime(2026, 1, 31), session_label="BACKTEST", is_open=False), bars_by_symbol=bars, candidates=[candidate], metadata={"diagnostics": {}, "signal_panel_artifact": []})

    monkeypatch.setattr(cli, "_load_historical", lambda *args, **kwargs: Loader().load_for_scenario())
    monkeypatch.setattr(cli, "build_portfolio_decisions", lambda **kwargs: [types.SimpleNamespace(candidate=kwargs["candidates"][0], selected=True, kill_reason=None, diagnostics={}, requested_budget=1000.0, side=Side.BUY, size_multiplier=1.0, expected_horizon_days=5)])
    monkeypatch.setattr(cli, "compare_policy_ab", lambda *args, **kwargs: {"quote_policy_v1": {"chosen_policy_reason": "test"}})
    def fake_plan(*args, **kwargs):
        return OrderPlan(plan_id="p1", symbol="AAPL", ticker_id=1, side=Side.BUY, generated_at=datetime(2026, 1, 1), status="READY", rationale="x", venue=ExecutionVenue.BACKTEST, requested_budget=1000.0, requested_quantity=1, legs=[] , metadata={"executable_from_date": "2026-01-02", "quote_policy": {}}), None
    monkeypatch.setattr(cli, "build_order_plan_from_candidate", fake_plan)
    monkeypatch.setattr(cli.SimulatedBroker, "simulate_plan", lambda self, plan, bars: [types.SimpleNamespace(plan_id="p1", leg_id="l1", symbol="AAPL", side=Side.BUY, fill_status=FillStatus.FULL, venue=ExecutionVenue.BACKTEST, event_time=datetime(2026, 1, 2, 0, 1), requested_quantity=1, filled_quantity=1, requested_price=102.0, average_fill_price=102.0, slippage_bps=0.0, metadata={"fee_bps": 0.0}, to_dict=lambda: {"plan_id": "p1", "fill_status": "FULL", "event_time": "2026-01-02T00:01:00", "filled_quantity": 1, "average_fill_price": 102.0, "metadata": {"fee_bps": 0.0}})])
    request = cli.RunnerRequest(scenario=cli.BacktestScenario(scenario_id="scn-r2", market="US", start_date="2026-01-01", end_date="2026-01-05", symbols=["AAPL"]), config=cli.BacktestConfig(initial_capital=10000.0))
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-r2", strategy_mode="research_similarity_v2", enable_validation=False)
    assert result["plans"][0]["metadata"]["first_fill_date"] == "2026-01-02"
    assert result["plans"][0]["metadata"]["planned_exit_date"] == "2026-01-07"


def test_legacy_mode_same_day_fill_convention_is_unchanged():
    broker = SimulatedBroker(rules=SimulationRules())
    plan = OrderPlan(
        plan_id="p2",
        symbol="AAPL",
        ticker_id=1,
        side=Side.BUY,
        generated_at=datetime(2026, 1, 10, 15, 30, 0),
        status="READY",
        rationale="legacy",
        venue=ExecutionVenue.BACKTEST,
        requested_budget=1000.0,
        requested_quantity=1,
        legs=[LadderLeg(leg_id="l1", side=Side.BUY, order_type=OrderType.LIMIT, quantity=1, limit_price=100.0)],
        metadata={"quote_policy": {}},
    )
    bars = [HistoricalBar(symbol="AAPL", timestamp="2026-01-10", open=101.0, high=102.0, low=99.0, close=101.0, volume=1000)]
    fills = broker.simulate_plan(plan, bars)
    assert fills[0].fill_status in {FillStatus.FULL, FillStatus.PARTIAL}
    assert str(fills[0].event_time)[:10] == "2026-01-10"

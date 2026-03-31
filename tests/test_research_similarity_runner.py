import csv
import json
import sys
from datetime import date, datetime
from pathlib import Path

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice
from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.pipeline import build_event_memory_asof, generate_similarity_candidates
from backtest_app.runner import cli
from shared.domain.models import MarketCode, MarketSnapshot, OutcomeLabel, Side, SignalCandidate


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


class FakeRollingDateLoader:
    def __init__(self, session_factory, schema="trading"):
        self.session_factory = session_factory
        self.schema = schema

    def load_for_scenario(self, **kwargs):
        bars = {
            "AAPL": [HistoricalBar(symbol="AAPL", timestamp=f"2026-01-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100 + i, volume=1000000) for i in range(1, 10)],
            "MSFT": [HistoricalBar(symbol="MSFT", timestamp=f"2026-01-{i:02d}", open=110 + i, high=111 + i, low=109 + i, close=110 + i, volume=1000000) for i in range(1, 10)],
            "XOM": [HistoricalBar(symbol="XOM", timestamp=f"2026-01-{i:02d}", open=90 + i, high=91 + i, low=89 + i, close=90 + i, volume=1000000) for i in range(1, 10)],
        }
        def cand(symbol, d, strength):
            return SignalCandidate(symbol=symbol, ticker_id=1, market=MarketCode.US, side_bias=Side.BUY, signal_strength=strength, confidence=0.8, anchor_date=date.fromisoformat(d), reference_date=date.fromisoformat(d), current_price=100.0, atr_pct=0.02, target_return_pct=0.04, max_reverse_pct=0.03, expected_horizon_days=2, outcome_label=OutcomeLabel.UNKNOWN, diagnostics={"query": {"regime_code": "RISK_ON", "sector_code": "TECH", "estimated_cost_bps": 10.0}, "ev": {"long": {"calibrated_ev": strength, "expected_mae": 0.01, "expected_mfe": 0.03, "uncertainty": 0.02, "effective_sample_size": 3.0}}, "decision_surface": {"buy": {"q10": -0.005, "q50": strength, "q90": strength + 0.03, "expected_mae": 0.01, "expected_mfe": 0.03, "p_target_first": 0.8, "effective_sample_size": 3.0, "uncertainty": 0.02}}})
        return HistoricalSlice(
            market_snapshot=MarketSnapshot(market=MarketCode.US, as_of=datetime(2026, 1, 31, 0, 0, 0), session_label="BACKTEST", is_open=False),
            bars_by_symbol=bars,
            candidates=[cand("AAPL", "2026-01-01", 0.09), cand("MSFT", "2026-01-01", 0.08), cand("XOM", "2026-01-02", 0.07), cand("AAPL", "2025-12-31", 0.12)],
            metadata={
                "diagnostics": {},
                "signal_panel_artifact": [
                    {
                        "decision_date": "2026-01-01",
                        "symbol": "AAPL",
                        "query": {
                            "execution_date": "2026-01-02",
                            "decision_convention": "EOD_T_SIGNAL__T1_OPEN_EXECUTION",
                            "price_reference_source": "next_open",
                        },
                        "decision_surface": {
                            "chosen_side": "BUY",
                            "abstain": False,
                            "abstain_reasons": [],
                            "chosen_lower_bound": 0.01,
                            "chosen_interval_width": 0.03,
                        },
                        "scorer_diagnostics": {
                            "buy": {
                                "expected_net_return": 0.05,
                                "q10": 0.01,
                                "q50": 0.03,
                                "q90": 0.08,
                                "expected_mae": 0.01,
                                "expected_mfe": 0.04,
                                "n_eff": 3.0,
                                "uncertainty": 0.02,
                                "top_matches_summary": [{"prototype_id": "buy-1", "support": 12.0, "similarity": 0.25}],
                            },
                            "sell": {
                                "expected_net_return": -0.01,
                                "q10": -0.04,
                                "q50": -0.01,
                                "q90": 0.01,
                                "expected_mae": 0.02,
                                "expected_mfe": 0.01,
                                "n_eff": 2.0,
                                "uncertainty": 0.03,
                                "top_matches_summary": [{"prototype_id": "sell-1", "support": 7.0, "similarity": 0.12}],
                            },
                        },
                        "ev": {
                            "buy": {"regime_alignment": 1.0, "abstain_reasons": []},
                            "sell": {"regime_alignment": 0.0, "abstain_reasons": ["low_ev"]},
                        },
                        "missingness": {"zero_imputed_feature_count": 0},
                    }
                ],
            },
        )


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


def test_run_backtest_research_similarity_v2_is_date_by_date_and_excludes_warmup(monkeypatch):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeRollingDateLoader)
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(scenario_id="scn-r2", market="US", start_date="2026-01-01", end_date="2026-01-02", symbols=["AAPL", "MSFT", "XOM"]),
        config=cli.BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(feature_window_bars=20, horizon_days=2)),
    )
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-r2", strategy_mode="research_similarity_v2")
    dates = [row["decision_date"] for row in result["portfolio"]["date_artifacts"]]
    assert dates == ["2026-01-01", "2026-01-02"]
    assert len(result["portfolio"]["date_artifacts"][0]["selected"]) <= 2
    assert len(result["portfolio"]["date_artifacts"][1]["selected"]) <= 2
    assert any(row["decision_date"] == "2025-12-31" for row in result["artifacts"]["warmup_candidates"])
    assert all(d["decision_date"] >= "2026-01-01" for d in result["portfolio"]["cash_path"])


def test_run_backtest_persists_forecast_panel_sidecars(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeRollingDateLoader)
    request = cli.RunnerRequest(
        scenario=cli.BacktestScenario(scenario_id="scn-r2", market="US", start_date="2026-01-01", end_date="2026-01-02", symbols=["AAPL", "MSFT", "XOM"]),
        config=cli.BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(feature_window_bars=20, horizon_days=2)),
    )
    output_dir = tmp_path / "nested" / "artifacts"
    result = cli.run_backtest(request, None, data_source="local-db", scenario_id="scn-r2", strategy_mode="research_similarity_v2", output_dir=str(output_dir), enable_validation=False)
    forecast_panel = dict((result.get("artifacts") or {}).get("forecast_panel") or {})
    assert forecast_panel["row_count"] == 1
    assert Path(str(forecast_panel["csv_path"])).exists()
    assert Path(str(forecast_panel["parquet_path"])).exists()
    assert (output_dir / "authoritative_summary.json").exists()
    with Path(str(forecast_panel["csv_path"])).open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["chosen_side_before_deploy"] == "BUY"
    assert float(row["q10"]) == 0.01
    assert float(row["q50"]) == 0.03
    assert float(row["q90"]) == 0.08
    assert float(row["effective_sample_size"]) == 3.0
    assert float(row["lower_bound"]) == 0.01
    assert float(row["interval_width"]) == 0.03
    assert json.loads(row["buy_top_matches_summary"])[0]["prototype_id"] == "buy-1"
    assert json.loads(row["sell_top_matches_summary"])[0]["prototype_id"] == "sell-1"
    assert int(row["buy_top_match_count"]) == 1
    assert int(row["sell_top_match_count"]) == 1
    assert float(row["buy_top_match_support_sum"]) == 12.0
    assert float(row["sell_top_match_support_sum"]) == 7.0
    assert float(row["buy_top_match_max_similarity"]) == 0.25
    assert float(row["sell_top_match_max_similarity"]) == 0.12


def test_cli_main_serializes_non_json_native_result_payloads(monkeypatch, tmp_path):
    def fake_execute(*args, **kwargs):
        return {
            "bars": [HistoricalBar(symbol="AAPL", timestamp="2026-01-01", open=100, high=101, low=99, close=100, volume=1_000_000)],
            "portfolio": {"decisions": []},
        }

    output_path = tmp_path / "cli_result.json"
    monkeypatch.setattr(cli, "execute_research_backtest", fake_execute)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--scenario-id",
            "scn-cli",
            "--market",
            "US",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-02",
            "--symbols",
            "AAPL",
            "--data-source",
            "json",
            "--data",
            "ignored.json",
            "--output",
            str(output_path),
        ],
    )

    assert cli.main() == 0
    assert output_path.exists()
    assert "HistoricalBar(" in output_path.read_text(encoding="utf-8")


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

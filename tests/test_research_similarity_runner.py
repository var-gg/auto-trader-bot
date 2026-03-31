import csv
import sys
from datetime import date, datetime
from pathlib import Path

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.models import HistoricalBar, HistoricalSlice, SymbolSessionMetadata
from backtest_app.research.artifacts import JsonResearchArtifactStore
import pytest

from backtest_app.research.pipeline import build_event_memory_asof, build_query_embedding, generate_similarity_candidates, generate_similarity_candidates_rolling
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
            session_metadata_by_symbol={
                "AAPL": SymbolSessionMetadata(symbol="AAPL", exchange_code="NMS", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00"),
                "MSFT": SymbolSessionMetadata(symbol="MSFT", exchange_code="NMS", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00"),
                "XOM": SymbolSessionMetadata(symbol="XOM", exchange_code="NYQ", country_code="US", exchange_tz="America/New_York", session_close_local_time="16:00"),
            },
            metadata={"diagnostics": {}, "signal_panel_artifact": [{"decision_date": "2026-01-01", "symbol": "AAPL", "query": {"exchange_code": "NMS", "exchange_tz": "America/New_York", "session_date_local": "2026-01-01", "session_close_ts_utc": "2026-01-01T21:00:00+00:00", "feature_anchor_ts_utc": "2026-01-01T21:00:00+00:00", "macro_asof_ts_utc": "2026-01-01T21:00:00+00:00", "macro_freshness_summary": {"vix": {"is_stale_flag": False}}}, "decision_surface": {"chosen_side": "BUY", "abstain": False, "abstain_reasons": [], "chosen_lower_bound": 0.01, "chosen_interval_width": 0.03}, "scorer_diagnostics": {"buy": {"expected_net_return": 0.05, "q10": 0.01, "q50": 0.03, "q90": 0.08, "expected_mae": 0.01, "expected_mfe": 0.04, "n_eff": 3.0, "uncertainty": 0.02, "top_matches_summary": []}, "sell": {"expected_net_return": -0.01, "q10": -0.04, "q50": -0.01, "q90": 0.01, "expected_mae": 0.02, "expected_mfe": 0.01, "n_eff": 2.0, "uncertainty": 0.03, "top_matches_summary": []}}, "ev": {"buy": {"regime_alignment": 1.0, "abstain_reasons": []}, "sell": {"regime_alignment": 0.0, "abstain_reasons": ["low_ev"]}}, "missingness": {"zero_imputed_feature_count": 0}}], "session_metadata_by_symbol": {"AAPL": {"exchange_code": "NMS"}, "MSFT": {"exchange_code": "NMS"}, "XOM": {"exchange_code": "NYQ"}}},
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


def _macro_history_sample():
    return {
        "2025-11-01": {"vix": 18.0, "rate": 4.00, "dollar": 100.0, "oil": 70.0, "breadth": 0.10},
        "2025-11-02": {"vix": 19.0, "rate": 4.05, "dollar": 100.5, "oil": 71.0, "breadth": 0.20},
        "2025-11-03": {"vix": 21.0, "rate": 4.10, "dollar": 101.0, "oil": 72.0, "breadth": 0.30},
        "2025-11-04": {"vix": 20.0, "rate": 4.20, "dollar": 100.8, "oil": 73.0, "breadth": 0.25},
        "2025-11-05": {"vix": 22.0, "rate": 4.15, "dollar": 101.2, "oil": 74.0, "breadth": 0.35},
        "2025-11-06": {"vix": 23.0, "rate": 4.18, "dollar": 101.4, "oil": 75.0, "breadth": 0.40},
        "2025-11-07": {"vix": 24.0, "rate": 4.22, "dollar": 101.8, "oil": 76.0, "breadth": 0.45},
        "2025-11-08": {"vix": 25.0, "rate": 4.25, "dollar": 102.0, "oil": 77.0, "breadth": 0.50},
        "2025-11-09": {"vix": 26.0, "rate": 4.30, "dollar": 102.5, "oil": 78.0, "breadth": 0.55},
        "2025-11-10": {"vix": 27.0, "rate": 4.35, "dollar": 103.0, "oil": 79.0, "breadth": 0.60},
    }


def _macro_history_for_bars(bars):
    out = {}
    for idx, bar in enumerate(bars, start=1):
        key = str(bar.timestamp)[:10]
        out[key] = {
            "vix": 18.0 + idx * 0.2,
            "rate": 4.0 + idx * 0.01,
            "dollar": 100.0 + idx * 0.15,
            "oil": 70.0 + idx * 0.25,
            "breadth": -0.1 + idx * 0.03,
        }
    return out


def _session_metadata(symbol: str = "AAA"):
    return {
        symbol: SymbolSessionMetadata(
            symbol=symbol,
            exchange_code="NMS",
            country_code="US",
            exchange_tz="America/New_York",
            session_close_local_time="16:00",
        )
    }


def test_event_and_query_use_identical_feature_contract_for_same_date():
    spec = ResearchExperimentSpec(feature_window_bars=5, horizon_days=2, target_return_pct=0.02, stop_return_pct=0.02)
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1_000_000 + i * 1000) for i in range(1, 15)]
    bars_by_symbol = {"AAA": bars}
    macro_history = _macro_history_sample()
    session_metadata = _session_metadata("AAA")
    memory = build_event_memory_asof(decision_date="2025-11-12", spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history, sector_map={}, market="US", session_metadata_by_symbol=session_metadata)
    target = next(r for r in memory["event_records"] if r.event_date == "2025-11-09")
    query_idx = next(i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == "2025-11-09")
    query_embedding, meta = build_query_embedding(symbol="AAA", bars=bars[query_idx - spec.feature_window_bars + 1: query_idx + 1], bars_by_symbol=bars_by_symbol, macro_history={k: v for k, v in macro_history.items() if k <= "2025-11-09"}, sector_map={}, cutoff_date="2025-11-09", spec=spec, transform=memory["transform"], session_metadata_by_symbol=session_metadata)
    assert memory["transform"] is not None
    assert target.diagnostics["transform_version"] == meta["transform_version"]
    assert target.diagnostics["raw_features"] == meta["raw_features"]
    assert target.diagnostics["transformed_features"] == meta["transformed_features"]
    assert target.diagnostics["embedding"] == query_embedding
    assert target.feature_anchor_ts_utc == meta["feature_anchor_ts_utc"]
    assert target.exchange_code == meta["exchange_code"] == "NMS"
    assert meta["breadth_policy"] == "diagnostics_only_v1"
    assert target.diagnostics["breadth_policy"] == "diagnostics_only_v1"
    assert meta["macro_series_present_count"] == 4
    matching_prototype = next(p for p in memory["prototypes"] if p.representative_date == target.event_date)
    assert matching_prototype.embedding == target.diagnostics["embedding"]
    assert matching_prototype.metadata["transformed_features"] == target.diagnostics["transformed_features"]
    assert matching_prototype.feature_anchor_ts_utc == target.feature_anchor_ts_utc


def test_build_event_memory_uses_transform_scaler_alias():
    spec = ResearchExperimentSpec(feature_window_bars=5, horizon_days=2, target_return_pct=0.02, stop_return_pct=0.02)
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1_000_000 + i * 1000) for i in range(1, 15)]
    memory = build_event_memory_asof(decision_date="2025-11-12", spec=spec, bars_by_symbol={"AAA": bars}, macro_history_by_date=_macro_history_sample(), sector_map={}, market="US", session_metadata_by_symbol=_session_metadata("AAA"))
    assert memory["transform"] is not None
    assert memory["scaler"] is memory["transform"].scaler


def test_single_day_macro_history_contract_collapse_is_detected():
    spec = ResearchExperimentSpec(feature_window_bars=5, horizon_days=2, target_return_pct=0.02, stop_return_pct=0.02)
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1_000_000 + i * 1000) for i in range(1, 15)]
    bars_by_symbol = {"AAA": bars}
    macro_history = _macro_history_sample()
    session_metadata = _session_metadata("AAA")
    memory = build_event_memory_asof(decision_date="2025-11-12", spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history, sector_map={}, market="US", session_metadata_by_symbol=session_metadata)
    target = next(r for r in memory["event_records"] if r.event_date == "2025-11-09")
    query_idx = next(i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == "2025-11-09")
    assert target.diagnostics["raw_features"]["vix_change_5"] != 0.0
    assert target.diagnostics["raw_features"]["vix_zscore_20"] != 0.0
    with pytest.raises(AssertionError, match="single-day macro history collapses context semantics"):
        build_query_embedding(symbol="AAA", bars=bars[query_idx - spec.feature_window_bars + 1: query_idx + 1], bars_by_symbol=bars_by_symbol, macro_history={"2025-11-09": macro_history["2025-11-09"]}, sector_map={}, cutoff_date="2025-11-09", spec=spec, transform=memory["transform"], session_metadata_by_symbol=session_metadata)


def test_generate_similarity_candidates_rolling_records_runtime_support_metadata():
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{((i - 1) % 28) + 1:02d}" if i <= 28 else (f"2025-12-{((i - 29) % 28) + 1:02d}" if i <= 56 else f"2026-01-{((i - 57) % 28) + 1:02d}"), open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1_000_000 + i * 1000) for i in range(1, 85)]
    bars_by_symbol = {"AAA": bars}
    macro_history = _macro_history_for_bars(bars)
    spec = ResearchExperimentSpec(feature_window_bars=20, horizon_days=3, target_return_pct=0.02, stop_return_pct=0.02)
    _candidates, diagnostics = generate_similarity_candidates_rolling(
        bars_by_symbol=bars_by_symbol,
        market="US",
        macro_history_by_date=macro_history,
        session_metadata_by_symbol=_session_metadata("AAA"),
        sector_map={},
        top_k=3,
        abstain_margin=0.01,
        spec=spec,
        metadata={
            "top_k": "5",
            "kernel_temperature": "8.0",
            "use_kernel_weighting": "false",
            "min_effective_sample_size": "2.5",
            "diagnostic_disable_ess_gate": "true",
        },
    )
    pipeline = diagnostics["pipeline"]
    assert pipeline["top_k"] == 5
    assert pipeline["top_k_requested"] == 3
    assert pipeline["ev_config"]["kernel_temperature"] == 8.0
    assert pipeline["ev_config"]["use_kernel_weighting"] is False
    assert pipeline["ev_config"]["min_effective_sample_size"] == 2.5
    assert pipeline["ev_config"]["diagnostic_disable_ess_gate"] is True
    assert pipeline["macro_join"]["breadth_policy"] == "diagnostics_only_v1"
    panel = diagnostics["signal_panel"]
    assert panel
    assert all((((row.get("decision_surface") or {}).get("gate_ablation") or {}).get("diagnostic_disable_ess_gate")) is True for row in panel)
    assert all(((row.get("query") or {}).get("exchange_code")) == "NMS" for row in panel)
    assert all(((row.get("query") or {}).get("breadth_policy")) == "diagnostics_only_v1" for row in panel)


def test_generate_similarity_candidates_rolling_emits_candidate_generation_progress_phase():
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{((i - 1) % 28) + 1:02d}" if i <= 28 else (f"2025-12-{((i - 29) % 28) + 1:02d}" if i <= 56 else f"2026-01-{((i - 57) % 28) + 1:02d}"), open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1_000_000 + i * 1000) for i in range(1, 85)]
    progress_updates = []
    generate_similarity_candidates_rolling(
        bars_by_symbol={"AAA": bars},
        market="US",
        macro_history_by_date=_macro_history_for_bars(bars),
        session_metadata_by_symbol=_session_metadata("AAA"),
        sector_map={},
        top_k=3,
        abstain_margin=0.01,
        spec=ResearchExperimentSpec(feature_window_bars=20, horizon_days=3, target_return_pct=0.02, stop_return_pct=0.02),
        metadata={"top_k": "5"},
        progress_callback=progress_updates.append,
    )

    phases = {str(update.get("phase")) for update in progress_updates}
    assert "candidate_generation" in phases
    assert "load_historical" not in phases


def test_macro_asof_join_uses_only_prior_source_timestamp():
    spec = ResearchExperimentSpec(feature_window_bars=5, horizon_days=2, target_return_pct=0.02, stop_return_pct=0.02)
    bars = [HistoricalBar(symbol="AAA", timestamp=f"2025-11-{i:02d}", open=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=1_000_000 + i * 1000) for i in range(1, 15)]
    bars_by_symbol = {"AAA": bars}
    session_metadata = {
        "AAA": SymbolSessionMetadata(symbol="AAA", exchange_code="KOE", country_code="KR", exchange_tz="Asia/Seoul", session_close_local_time="15:30")
    }
    macro_history = {
        "2025-11-08": {"vix": 18.0, "rate": 4.0, "dollar": 100.0, "oil": 70.0},
        "2025-11-09": {"vix": 99.0, "rate": 9.0, "dollar": 199.0, "oil": 170.0},
    }
    macro_series_history = [
        {"obs_date": "2025-11-08", "name": "vix", "value": 18.0, "source_ts_utc": "2025-11-08T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-08", "name": "rate", "value": 4.0, "source_ts_utc": "2025-11-08T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-08", "name": "dollar", "value": 100.0, "source_ts_utc": "2025-11-08T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-08", "name": "oil", "value": 70.0, "source_ts_utc": "2025-11-08T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-09", "name": "vix", "value": 99.0, "source_ts_utc": "2025-11-09T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-09", "name": "rate", "value": 9.0, "source_ts_utc": "2025-11-09T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-09", "name": "dollar", "value": 199.0, "source_ts_utc": "2025-11-09T21:00:00+00:00", "source_ts_is_derived": True},
        {"obs_date": "2025-11-09", "name": "oil", "value": 170.0, "source_ts_utc": "2025-11-09T21:00:00+00:00", "source_ts_is_derived": True},
    ]
    memory = build_event_memory_asof(decision_date="2025-11-12", spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history, macro_series_history=macro_series_history, sector_map={}, market="US", session_metadata_by_symbol=session_metadata)
    target = next(r for r in memory["event_records"] if r.event_date == "2025-11-09")
    assert target.feature_anchor_ts_utc == "2025-11-09T06:30:00+00:00"
    assert target.macro_asof_ts_utc == "2025-11-08T21:00:00+00:00"
    assert target.diagnostics["macro_freshness_summary"]["vix"]["source_ts_utc"] == "2025-11-08T21:00:00+00:00"

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from backtest_app.configs.models import BacktestConfig, BacktestScenario, OptunaObjectiveConfig, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest
from backtest_app.historical_data.features import FeatureScaler, FeatureTransform
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research import pipeline as research_pipeline
from backtest_app.research.pipeline import build_query_embedding
from backtest_app.research_runtime import calibration_cache
from backtest_app.research_runtime.calibration_cache import (
    ForbiddenCalibrationBundleCall,
    _forbidden_bundle_calls_guard,
    _load_train_snapshot_payload,
    build_query_feature_cache_rows,
)
from backtest_app.research_runtime.frozen_seed import (
    CALIBRATION_UNIVERSE_SEED_PROFILE,
    PROOF_SUBSET_SEED_PROFILE,
    build_study_cache,
    build_optuna_replay_seed,
    build_preopen_signal_snapshot,
    evaluate_frozen_seed_params,
    evaluate_frozen_seed_params_from_cache,
    filter_optuna_seed_rows,
    load_optuna_replay_seed,
    summarize_execution_mode_comparison,
    write_study_cache_from_rows,
    write_optuna_replay_seed_artifacts,
)
from backtest_app.research_runtime.optuna_runner import OptunaResearchRunner
from backtest_app.runner import cli


def _path_payload(open_px: float, high_px: float, low_px: float, close_px: float, session_date: str) -> str:
    return json.dumps(
        [
            {
                "session_date": session_date,
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": 1_000_000.0,
            }
        ],
        ensure_ascii=False,
    )


def _bars(symbol: str, closes: list[float], start_day: int = 1) -> list[HistoricalBar]:
    rows: list[HistoricalBar] = []
    for idx, close_px in enumerate(closes, start=start_day):
        open_px = close_px - 0.5
        rows.append(
            HistoricalBar(
                symbol=symbol,
                timestamp=f"2026-01-{idx:02d}",
                open=open_px,
                high=close_px + 1.0,
                low=open_px - 1.0,
                close=close_px,
                volume=1_000_000.0 + idx,
            )
        )
    return rows


def _seed_row(*, decision_date: str, execution_date: str, symbol: str, side: str, t1_open: float, d1_high: float, d1_low: float, d1_close: float, optuna_eligible: bool = True, forecast_selected: bool = True) -> dict:
    return {
        "decision_date": decision_date,
        "execution_date": execution_date,
        "symbol": symbol,
        "side": side,
        "run_label": "best1",
        "policy_scope": "directional_wide_only",
        "pattern_key": f"{side}|sig-{symbol}|RISK_ON|TECH|wide",
        "policy_family": "directional_wide",
        "optuna_eligible": optuna_eligible,
        "forecast_selected": forecast_selected if side == "BUY" else False,
        "chosen_side_before_deploy": "BUY" if forecast_selected else "ABSTAIN",
        "abstain": not forecast_selected if side == "BUY" else False,
        "single_prototype_collapse": False,
        "policy_edge_score": None,
        "q10_return": 0.01 if side == "BUY" else -0.01,
        "q50_return": 0.03 if side == "BUY" else 0.02,
        "q90_return": 0.06 if side == "BUY" else 0.04,
        "lower_bound": 0.01 if side == "BUY" else -0.01,
        "interval_width": 0.05,
        "uncertainty": 0.02,
        "member_mixture_ess": 2.5,
        "member_top1_weight_share": 0.45,
        "member_pre_truncation_count": 4,
        "member_support_sum": 12.0,
        "member_consensus_signature": f"{symbol}:sig",
        "member_candidate_count": 4,
        "positive_weight_member_count": 3,
        "q50_d2_return": 0.015,
        "q50_d3_return": 0.02,
        "p_resolved_by_d2": 0.4,
        "p_resolved_by_d3": 0.7,
        "regime_code": "RISK_ON",
        "sector_code": "TECH",
        "country_code": "US",
        "exchange_code": "NMS",
        "exchange_tz": "America/New_York",
        "shape_bucket": "wide",
        "market": "US",
        "t1_open": t1_open,
        "d1_open": t1_open,
        "d1_high": d1_high,
        "d1_low": d1_low,
        "d1_close": d1_close,
        "bar_path_d1_to_d5": _path_payload(t1_open, d1_high, d1_low, d1_close, execution_date),
        "path_length": 1,
        "last_path_close": d1_close,
        "recurring_family": True,
    }


def _write_seed_bundle(tmp_path: Path, rows: list[dict]) -> Path:
    seed_root = tmp_path / "research" / "seed123"
    write_optuna_replay_seed_artifacts(
        run_dir=seed_root,
        replay_seed={
            "seed_rows": rows,
            "summary": {
                "row_count": len(rows),
                "buy_row_count": sum(1 for row in rows if row["side"] == "BUY"),
                "sell_row_count": sum(1 for row in rows if row["side"] == "SELL"),
                "optuna_eligible_row_count": sum(1 for row in rows if row["side"] == "BUY" and row["optuna_eligible"]),
                "policy_scope": "directional_wide_only",
                "source_run_label": "best1",
            },
        },
    )
    return seed_root


def test_feature_scaler_and_transform_payload_roundtrip_is_lossless():
    scaler = FeatureScaler(means={"a": 1.5, "b": -2.0}, stds={"a": 0.5, "b": 3.0})
    transform = FeatureTransform(scaler=scaler, feature_keys=["a", "b"], version="feature_contract_v1")
    restored_scaler = FeatureScaler.from_payload(scaler.to_payload())
    restored_transform = FeatureTransform.from_payload(transform.to_payload())
    assert restored_scaler == scaler
    assert restored_transform == transform
    assert restored_transform.apply({"a": 2.0, "b": 1.0}) == transform.apply({"a": 2.0, "b": 1.0})


def test_train_snapshot_payload_roundtrip_is_lossless(tmp_path):
    store = JsonResearchArtifactStore(str(tmp_path))
    payload = {
        "snapshot_id": "snap-001",
        "as_of_date": "2026-01-10",
        "memory_version": "v1",
        "prototype_snapshot_name": "prototype_snapshot",
        "event_record_count": 11,
        "prototype_count": 3,
        "prototypes": [],
        "scaler": FeatureScaler(means={"a": 1.0}, stds={"a": 2.0}).to_payload(),
        "transform": FeatureTransform(
            scaler=FeatureScaler(means={"a": 1.0}, stds={"a": 2.0}),
            feature_keys=["a"],
            version="feature_contract_v1",
        ).to_payload(),
        "calibration": {"method": "logistic", "slope": 1.0, "intercept": 0.0},
        "quote_policy_calibration": {"abstain_margin": 0.05},
        "metadata": {"portfolio_top_n": 3},
        "session_metadata_by_symbol": {},
        "macro_series_history": [],
        "snapshot_ids": {},
        "artifact_kind": "train_snapshot_v1",
    }
    path = store.save_train_snapshot(
        run_id="bundle_snapshots",
        name="train_snapshot_20260110",
        as_of_date="2026-01-10",
        memory_version="v1",
        payload=payload,
    )
    restored = _load_train_snapshot_payload(path)
    assert isinstance(restored["scaler"], FeatureScaler)
    assert isinstance(restored["transform"], FeatureTransform)
    assert restored["scaler"].means == {"a": 1.0}
    assert restored["transform"].feature_keys == ["a"]
    assert restored["event_record_count"] == 11


def test_forbidden_bundle_guard_rejects_rolling_similarity_calls():
    with pytest.raises(ForbiddenCalibrationBundleCall) as excinfo:
        with _forbidden_bundle_calls_guard():
            research_pipeline.generate_similarity_candidates_rolling()
    assert excinfo.value.call_name == "generate_similarity_candidates_rolling"


def test_query_feature_cache_rows_match_query_embedding_contract():
    spec = ResearchExperimentSpec(feature_window_bars=5, lookback_horizons=[1, 3, 5], horizon_days=5)
    bars_by_symbol = {
        "AAPL": _bars("AAPL", [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0]),
        "MSFT": _bars("MSFT", [200.0, 199.0, 201.0, 202.0, 203.0, 204.0, 205.0]),
    }
    sector_map = {"AAPL": "TECH", "MSFT": "TECH"}
    cache = build_query_feature_cache_rows(
        symbols=["AAPL"],
        bars_by_symbol=bars_by_symbol,
        macro_history_by_date={},
        sector_map=sector_map,
        session_metadata_by_symbol={},
        macro_series_history=[],
        spec=spec,
        start_date="2026-01-05",
        end_date="2026-01-06",
        metadata={},
    )
    assert cache["query_row_count"] > 0
    row = cache["query_rows"][0]
    aapl_bars = bars_by_symbol["AAPL"]
    idx = next(i for i, bar in enumerate(aapl_bars) if bar.timestamp == row["decision_date"])
    embedding, meta = build_query_embedding(
        symbol="AAPL",
        bars=aapl_bars[idx - spec.feature_window_bars + 1 : idx + 1],
        bars_by_symbol=bars_by_symbol,
        macro_history={},
        sector_map=sector_map,
        cutoff_date=row["decision_date"],
        spec=spec,
        scaler=None,
        transform=None,
        use_macro_level_in_similarity=False,
        use_dollar_volume_absolute=False,
        session_metadata_by_symbol={},
        macro_series_history=[],
    )
    assert json.loads(row["embedding_json"]) == embedding
    assert json.loads(row["raw_features_json"]) == meta["raw_features"]
    assert row["regime_code"] == meta["regime_code"]
    assert row["execution_date"] == "2026-01-06"


def test_preopen_snapshot_roundtrip_supports_ladder_buy_and_sell_legs(tmp_path):
    seed_root = _write_seed_bundle(
        tmp_path,
        [
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=106.0, d1_low=99.0, d1_close=104.0),
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="MSFT", side="SELL", t1_open=120.0, d1_high=125.0, d1_low=118.0, d1_close=123.0),
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="NVDA", side="BUY", t1_open=200.0, d1_high=202.0, d1_low=198.0, d1_close=201.0, optuna_eligible=False),
        ],
    )
    seed_bundle = load_optuna_replay_seed(str(seed_root))
    snapshot = build_preopen_signal_snapshot(
        seed_rows=seed_bundle["rows"],
        as_of_date="2026-01-01",
        policy_params={
            "execution_mode": "ladder_v1",
            "max_new_buys": 2,
            "buy_budget_fraction": 1.0,
            "per_name_cap_fraction": 0.50,
            "w_lb": 1.0,
            "w_q50": 1.0,
            "w_width": 0.25,
            "w_uncertainty": 0.25,
            "w_ess": 0.50,
            "min_buy_score": -0.5,
            "min_lower_bound": -0.5,
            "min_member_ess": 1.0,
            "buy_leg_count": 3,
            "buy_first_leg_offset_pct": 0.01,
            "buy_last_leg_offset_pct": 0.03,
            "buy_leg_weight_alpha": 1.0,
            "sell_leg_count": 2,
            "sell_first_leg_markup_pct": 0.01,
            "sell_last_leg_markup_pct": 0.03,
            "sell_leg_weight_alpha": 1.0,
            "fallback_min_sell_markup": 0.002,
        },
        available_cash=10_000.0,
        holdings=[{"symbol": "MSFT", "quantity": 6, "avg_price": 118.0}],
        seed_profile=PROOF_SUBSET_SEED_PROFILE,
        seed_filter="pre_optuna_family",
    )
    assert snapshot["buy_count"] == 1
    assert snapshot["sell_count"] == 1
    buy_row = next(row for row in snapshot["snapshot_rows"] if row["side"] == "BUY")
    sell_row = next(row for row in snapshot["snapshot_rows"] if row["side"] == "SELL")
    buy_prices = json.loads(buy_row["buy_limit_prices"])
    sell_prices = json.loads(sell_row["sell_limit_prices"])
    assert buy_row["symbol"] == "AAPL"
    assert buy_row["buy_rank"] == 1
    assert len(buy_prices) == 3
    assert buy_prices == sorted(buy_prices, reverse=True)
    assert json.loads(buy_row["buy_leg_quantities"])
    assert sell_row["symbol"] == "MSFT"
    assert len(sell_prices) == 2
    assert sell_prices == sorted(sell_prices)
    assert snapshot["execution_mode"] == "ladder_v1"


def test_preopen_snapshot_uses_optuna_eligible_buys_even_when_old_forecast_selected_is_false():
    snapshot = build_preopen_signal_snapshot(
        seed_rows=[
            _seed_row(
                decision_date="2026-01-01",
                execution_date="2026-01-02",
                symbol="AAPL",
                side="BUY",
                t1_open=100.0,
                d1_high=104.0,
                d1_low=99.0,
                d1_close=103.0,
                optuna_eligible=True,
                forecast_selected=False,
            )
        ],
        as_of_date="2026-01-01",
        policy_params={
            "execution_mode": "single_leg",
            "max_new_buys": 1,
            "buy_budget_fraction": 1.0,
            "per_name_cap_fraction": 1.0,
            "w_lb": 1.0,
            "w_q50": 1.0,
            "w_width": 0.0,
            "w_uncertainty": 0.0,
            "w_ess": 0.0,
            "min_buy_score": -1.0,
            "min_lower_bound": -1.0,
            "min_member_ess": 1.0,
            "buy_entry_offset_pct": 0.0,
            "sell_markup_pct": 0.0,
            "fallback_min_sell_markup": 0.0,
        },
        available_cash=1_000.0,
        seed_profile=PROOF_SUBSET_SEED_PROFILE,
        seed_filter="pre_optuna_family",
    )
    assert snapshot["buy_count"] == 1
    assert snapshot["snapshot_rows"][0]["symbol"] == "AAPL"


def test_pre_optuna_seed_filter_keeps_tradeable_buys_and_follow_on_sells_only():
    filtered = filter_optuna_seed_rows(
        seed_rows=[
            _seed_row(
                decision_date="2026-01-03",
                execution_date="2026-01-06",
                symbol="AAPL",
                side="BUY",
                t1_open=100.0,
                d1_high=104.0,
                d1_low=99.0,
                d1_close=103.0,
                optuna_eligible=True,
                forecast_selected=False,
            ),
            _seed_row(
                decision_date="2026-01-01",
                execution_date="2026-01-02",
                symbol="AAPL",
                side="SELL",
                t1_open=100.0,
                d1_high=101.0,
                d1_low=99.0,
                d1_close=100.0,
            ),
            _seed_row(
                decision_date="2026-01-04",
                execution_date="2026-01-07",
                symbol="AAPL",
                side="SELL",
                t1_open=101.0,
                d1_high=102.0,
                d1_low=100.0,
                d1_close=101.0,
            ),
            _seed_row(
                decision_date="2026-01-04",
                execution_date="2026-01-07",
                symbol="MSFT",
                side="SELL",
                t1_open=120.0,
                d1_high=121.0,
                d1_low=119.0,
                d1_close=120.0,
            ),
        ],
        policy_scope="directional_wide_only",
        seed_filter="pre_optuna_family",
    )
    assert [(row["decision_date"], row["symbol"], row["side"]) for row in filtered] == [
        ("2026-01-03", "AAPL", "BUY"),
        ("2026-01-04", "AAPL", "SELL"),
    ]


def test_calibration_universe_filter_is_wider_than_proof_subset_and_keeps_follow_on_sells():
    seed_rows = [
        _seed_row(
            decision_date="2026-01-03",
            execution_date="2026-01-06",
            symbol="AAPL",
            side="BUY",
            t1_open=100.0,
            d1_high=104.0,
            d1_low=99.0,
            d1_close=103.0,
            optuna_eligible=False,
            forecast_selected=False,
        ),
        _seed_row(
            decision_date="2026-01-02",
            execution_date="2026-01-05",
            symbol="AAPL",
            side="SELL",
            t1_open=99.0,
            d1_high=100.0,
            d1_low=98.0,
            d1_close=99.0,
        ),
        _seed_row(
            decision_date="2026-01-04",
            execution_date="2026-01-07",
            symbol="AAPL",
            side="SELL",
            t1_open=101.0,
            d1_high=102.0,
            d1_low=100.0,
            d1_close=101.0,
        ),
        {
            **_seed_row(
                decision_date="2026-01-05",
                execution_date="2026-01-08",
                symbol="MSFT",
                side="BUY",
                t1_open=120.0,
                d1_high=123.0,
                d1_low=118.0,
                d1_close=122.0,
                optuna_eligible=False,
                forecast_selected=False,
            ),
            "single_prototype_collapse": True,
        },
        _seed_row(
            decision_date="2026-01-06",
            execution_date="2026-01-09",
            symbol="MSFT",
            side="SELL",
            t1_open=121.0,
            d1_high=122.0,
            d1_low=120.0,
            d1_close=121.0,
        ),
    ]
    proof_rows = filter_optuna_seed_rows(
        seed_rows=seed_rows,
        policy_scope="directional_wide_only",
        seed_profile=PROOF_SUBSET_SEED_PROFILE,
        seed_filter="pre_optuna_family",
    )
    calibration_rows = filter_optuna_seed_rows(
        seed_rows=seed_rows,
        policy_scope="directional_wide_only",
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
    )
    assert proof_rows == []
    assert [(row["decision_date"], row["symbol"], row["side"]) for row in calibration_rows] == [
        ("2026-01-03", "AAPL", "BUY"),
        ("2026-01-04", "AAPL", "SELL"),
    ]


def test_frozen_seed_evaluation_splits_folds_by_buy_episode_windows():
    filtered = filter_optuna_seed_rows(
        seed_rows=[
            _seed_row(decision_date="2026-01-03", execution_date="2026-01-06", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=104.0, d1_low=99.0, d1_close=103.0, optuna_eligible=True, forecast_selected=False),
            _seed_row(decision_date="2026-01-04", execution_date="2026-01-07", symbol="AAPL", side="SELL", t1_open=101.0, d1_high=102.0, d1_low=100.0, d1_close=101.0),
            _seed_row(decision_date="2026-02-03", execution_date="2026-02-04", symbol="MSFT", side="BUY", t1_open=110.0, d1_high=114.0, d1_low=109.0, d1_close=113.0, optuna_eligible=True, forecast_selected=False),
            _seed_row(decision_date="2026-02-04", execution_date="2026-02-05", symbol="MSFT", side="SELL", t1_open=111.0, d1_high=112.0, d1_low=110.0, d1_close=111.0),
            _seed_row(decision_date="2026-03-03", execution_date="2026-03-04", symbol="NVDA", side="BUY", t1_open=120.0, d1_high=124.0, d1_low=119.0, d1_close=123.0, optuna_eligible=True, forecast_selected=False),
            _seed_row(decision_date="2026-03-04", execution_date="2026-03-05", symbol="NVDA", side="SELL", t1_open=121.0, d1_high=122.0, d1_low=120.0, d1_close=121.0),
        ],
        policy_scope="directional_wide_only",
        seed_filter="pre_optuna_family",
    )
    evaluation = evaluate_frozen_seed_params(
        seed_rows=filtered,
        params={
            "execution_mode": "single_leg",
            "w_lb": 1.0,
            "w_q50": 1.0,
            "w_width": 0.0,
            "w_uncertainty": 0.0,
            "w_ess": 0.0,
            "min_buy_score": -1.0,
            "min_lower_bound": -1.0,
            "min_member_ess": 1.0,
            "max_new_buys": 1,
            "buy_budget_fraction": 1.0,
            "per_name_cap_fraction": 1.0,
            "buy_entry_offset_pct": 0.0,
            "sell_markup_pct": 0.0,
            "fallback_min_sell_markup": 0.0,
        },
        initial_capital=10_000.0,
        objective_cfg=OptunaObjectiveConfig(
            lambda_drawdown=0.0,
            allowed_drawdown=1.0,
            lambda_idle_cash=0.0,
            lambda_concentration=0.0,
            concentration_cap=1.0,
            min_trade_count=0,
            min_sell_fill_count=0,
        ),
    )
    assert [(fold["start_date"], fold["end_date"]) for fold in evaluation["folds"]] == [
        ("2026-01-03", "2026-01-04"),
        ("2026-02-03", "2026-02-04"),
        ("2026-03-03", "2026-03-04"),
    ]


def test_future_path_changes_do_not_change_buy_ranking_but_do_change_replay_outcome():
    seed_rows = [
        _seed_row(
            decision_date="2026-01-01",
            execution_date="2026-01-02",
            symbol="AAPL",
            side="BUY",
            t1_open=100.0,
            d1_high=105.0,
            d1_low=100.0,
            d1_close=104.0,
            optuna_eligible=False,
            forecast_selected=False,
        ),
        {
            **_seed_row(
                decision_date="2026-01-01",
                execution_date="2026-01-02",
                symbol="MSFT",
                side="BUY",
                t1_open=120.0,
                d1_high=123.0,
                d1_low=119.0,
                d1_close=122.0,
                optuna_eligible=False,
                forecast_selected=False,
            ),
            "q50_return": 0.02,
            "lower_bound": 0.005,
        },
        _seed_row(
            decision_date="2026-01-02",
            execution_date="2026-01-03",
            symbol="AAPL",
            side="SELL",
            t1_open=101.0,
            d1_high=102.0,
            d1_low=100.0,
            d1_close=101.0,
        ),
    ]
    params = {
        "execution_mode": "single_leg",
        "w_lb": 1.0,
        "w_q50": 1.0,
        "w_width": 0.0,
        "w_uncertainty": 0.0,
        "w_ess": 0.0,
        "min_buy_score": -1.0,
        "min_lower_bound": -1.0,
        "min_member_ess": 1.0,
        "max_new_buys": 2,
        "buy_budget_fraction": 1.0,
        "per_name_cap_fraction": 1.0,
        "buy_entry_offset_pct": 0.01,
        "sell_markup_pct": 0.0,
        "fallback_min_sell_markup": 0.0,
    }
    snapshot_a = build_preopen_signal_snapshot(
        seed_rows=seed_rows,
        as_of_date="2026-01-01",
        policy_params=params,
        available_cash=10_000.0,
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
    )
    mutated_rows = [dict(row) for row in seed_rows]
    mutated_rows[0]["d1_low"] = 98.0
    mutated_rows[0]["bar_path_d1_to_d5"] = _path_payload(100.0, 105.0, 98.0, 104.0, "2026-01-02")
    snapshot_b = build_preopen_signal_snapshot(
        seed_rows=mutated_rows,
        as_of_date="2026-01-01",
        policy_params=params,
        available_cash=10_000.0,
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
    )
    assert [row["symbol"] for row in snapshot_a["snapshot_rows"] if row["side"] == "BUY"] == [
        row["symbol"] for row in snapshot_b["snapshot_rows"] if row["side"] == "BUY"
    ]
    evaluation_a = evaluate_frozen_seed_params(
        seed_rows=filter_optuna_seed_rows(seed_rows=seed_rows, policy_scope="directional_wide_only", seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE),
        params=params,
        initial_capital=10_000.0,
        objective_cfg=OptunaObjectiveConfig(
            lambda_drawdown=0.0,
            allowed_drawdown=1.0,
            lambda_idle_cash=0.0,
            lambda_concentration=0.0,
            concentration_cap=1.0,
            min_trade_count=0,
            min_sell_fill_count=0,
        ),
    )
    evaluation_b = evaluate_frozen_seed_params(
        seed_rows=filter_optuna_seed_rows(seed_rows=mutated_rows, policy_scope="directional_wide_only", seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE),
        params=params,
        initial_capital=10_000.0,
        objective_cfg=OptunaObjectiveConfig(
            lambda_drawdown=0.0,
            allowed_drawdown=1.0,
            lambda_idle_cash=0.0,
            lambda_concentration=0.0,
            concentration_cap=1.0,
            min_trade_count=0,
            min_sell_fill_count=0,
        ),
    )
    assert evaluation_a["aggregate"]["trade_count"] != evaluation_b["aggregate"]["trade_count"]


def test_optuna_research_runner_frozen_seed_mode_replays_seed_without_upstream(tmp_path):
    seed_root = _write_seed_bundle(
        tmp_path,
        [
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=104.0, d1_low=99.0, d1_close=103.0),
            _seed_row(decision_date="2026-01-02", execution_date="2026-01-03", symbol="MSFT", side="BUY", t1_open=110.0, d1_high=114.0, d1_low=109.0, d1_close=113.0),
            _seed_row(decision_date="2026-01-03", execution_date="2026-01-06", symbol="NVDA", side="BUY", t1_open=120.0, d1_high=124.0, d1_low=119.0, d1_close=123.0),
        ],
    )
    request = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id="frozen-seed-study",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-03",
            symbols=["AAPL", "MSFT", "NVDA"],
        ),
        config=BacktestConfig(
            initial_capital=10_000.0,
            optuna=OptunaSearchConfig(
                experiment_id="frozen-seed-smoke",
                mode="frozen_seed_v1",
                n_trials=2,
                seed=7,
                seed_artifact_root=str(seed_root),
                policy_scope="directional_wide_only",
                seed_filter="pre_optuna_family",
                objective_metric="final_equity",
                search_space={
                    "execution_mode": {"type": "categorical", "choices": ["single_leg"]},
                    "w_lb": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                    "w_q50": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                    "w_width": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.1},
                    "w_uncertainty": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.1},
                    "w_ess": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.1},
                    "min_buy_score": {"type": "float", "low": -1.0, "high": -1.0, "step": 0.1},
                    "min_lower_bound": {"type": "float", "low": -1.0, "high": -1.0, "step": 0.1},
                    "min_member_ess": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                    "max_new_buys": {"type": "int", "low": 1, "high": 1, "step": 1},
                    "buy_budget_fraction": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                    "per_name_cap_fraction": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                    "buy_entry_offset_pct": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "sell_markup_pct": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "fallback_min_sell_markup": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "buy_leg_count": {"type": "int", "low": 1, "high": 1, "step": 1},
                    "buy_first_leg_offset_pct": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "buy_last_leg_offset_pct": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "buy_leg_weight_alpha": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                    "sell_leg_count": {"type": "int", "low": 1, "high": 1, "step": 1},
                    "sell_first_leg_markup_pct": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "sell_last_leg_markup_pct": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.001},
                    "sell_leg_weight_alpha": {"type": "float", "low": 1.0, "high": 1.0, "step": 0.1},
                },
                objective=OptunaObjectiveConfig(
                    lambda_drawdown=0.0,
                    allowed_drawdown=1.0,
                    lambda_idle_cash=0.0,
                    lambda_concentration=0.0,
                    concentration_cap=1.0,
                    min_trade_count=1,
                    min_sell_fill_count=0,
                ),
            ),
        ),
    )

    def _should_not_run(**kwargs):
        raise AssertionError("generic upstream runner must not be called in frozen_seed_v1 mode")

    result = OptunaResearchRunner(str(tmp_path / "results")).run(
        request=request,
        runner_fn=_should_not_run,
        validation_fn=_should_not_run,
        data_source="local-db",
    )
    assert result["status"] == "ok"
    assert result["best_trial"] is not None
    assert result["best_trial"]["upstream_recomputed"] is False
    assert result["holdout_report"]["status"] == "not_available"
    assert Path(result["study_outputs"]["best_params_path"]).exists()
    assert Path(result["study_outputs"]["trial_table_path"]).exists()
    assert Path(result["study_outputs"]["study_summary_path"]).exists()
    study_summary = json.loads(Path(result["study_outputs"]["study_summary_path"]).read_text(encoding="utf-8"))
    assert study_summary["warm_start_trial_count"] > 0


def test_optuna_research_runner_prefers_study_cache_when_present(tmp_path, monkeypatch):
    seed_root = _write_seed_bundle(
        tmp_path,
        [
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=104.0, d1_low=99.0, d1_close=103.0, optuna_eligible=False, forecast_selected=False),
            _seed_row(decision_date="2026-01-02", execution_date="2026-01-03", symbol="AAPL", side="SELL", t1_open=101.0, d1_high=102.0, d1_low=100.0, d1_close=101.0),
            _seed_row(decision_date="2026-02-01", execution_date="2026-02-02", symbol="MSFT", side="BUY", t1_open=110.0, d1_high=114.0, d1_low=109.0, d1_close=113.0, optuna_eligible=False, forecast_selected=False),
            _seed_row(decision_date="2026-02-02", execution_date="2026-02-03", symbol="MSFT", side="SELL", t1_open=111.0, d1_high=112.0, d1_low=110.0, d1_close=111.0),
            _seed_row(decision_date="2026-03-01", execution_date="2026-03-02", symbol="NVDA", side="BUY", t1_open=120.0, d1_high=124.0, d1_low=119.0, d1_close=123.0, optuna_eligible=False, forecast_selected=False),
            _seed_row(decision_date="2026-03-02", execution_date="2026-03-03", symbol="NVDA", side="SELL", t1_open=121.0, d1_high=122.0, d1_low=120.0, d1_close=121.0),
        ],
    )
    build_study_cache(seed_artifact_root=str(seed_root), policy_scope="directional_wide_only", seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE)
    from backtest_app.research_runtime import optuna_runner as optuna_runner_module

    monkeypatch.setattr(optuna_runner_module, "load_optuna_replay_seed", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("seed bundle should not be loaded when study cache exists")))

    request = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id="frozen-seed-cache-study",
            market="US",
            start_date="2026-01-01",
            end_date="2026-03-02",
            symbols=["AAPL", "MSFT", "NVDA"],
        ),
        config=BacktestConfig(
            initial_capital=10_000.0,
            optuna=OptunaSearchConfig(
                experiment_id="frozen-seed-cache-smoke",
                mode="frozen_seed_v1",
                n_trials=2,
                seed=7,
                seed_artifact_root=str(seed_root),
                policy_scope="directional_wide_only",
                seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
                objective=OptunaObjectiveConfig(
                    lambda_drawdown=0.0,
                    allowed_drawdown=1.0,
                    lambda_idle_cash=0.0,
                    lambda_concentration=0.0,
                    concentration_cap=1.0,
                    min_trade_count=0,
                    min_sell_fill_count=0,
                ),
            ),
        ),
    )

    result = OptunaResearchRunner(str(tmp_path / "results_cache")).run(
        request=request,
        runner_fn=lambda **kwargs: (_ for _ in ()).throw(AssertionError("generic upstream runner must not be called")),
        validation_fn=lambda **kwargs: (_ for _ in ()).throw(AssertionError("generic validation must not be called")),
        data_source="local-db",
    )
    assert result["status"] in {"ok", "no_feasible_trial"}
    study_summary = json.loads(Path(result["study_outputs"]["study_summary_path"]).read_text(encoding="utf-8"))
    assert study_summary["study_cache_root"] is not None


def test_build_optuna_replay_seed_preserves_pre_optuna_truth_on_chosen_side():
    replay_seed = build_optuna_replay_seed(
        forecast_rows=[
            {
                "decision_date": "2026-01-01",
                "symbol": "AAPL",
                "chosen_side_before_deploy": "ABSTAIN",
                "dominant_side": "BUY",
                "forecast_selected": False,
                "pattern_key": "BUY|precomputed|RISK_ON|TECH|wide",
                "policy_family": "directional_wide",
                "optuna_eligible": True,
                "recurring_family": True,
                "buy_q10": 0.01,
                "buy_q50": 0.03,
                "buy_q90": 0.06,
                "buy_member_mixture_ess": 2.5,
                "buy_member_top1_weight_share": 0.40,
                "buy_member_pre_truncation_count": 4,
                "buy_member_candidate_count": 4,
                "buy_positive_weight_member_count": 3,
                "buy_member_consensus_signature": "AAPL:sig",
                "buy_interval_width": 0.05,
                "buy_uncertainty": 0.02,
                "buy_q50_d2_return": 0.02,
                "buy_q50_d3_return": 0.03,
                "buy_p_resolved_by_d2": 0.4,
                "buy_p_resolved_by_d3": 0.7,
                "query_regime_code": "RISK_ON",
                "query_sector_code": "TECH",
                "country_code": "US",
                "exchange_code": "NMS",
                "exchange_tz": "America/New_York",
            }
        ],
        bars_by_symbol={},
        run_label="best1",
        policy_scope="directional_wide_only",
    )
    buy_row = next(row for row in replay_seed["seed_rows"] if row["side"] == "BUY")
    sell_row = next(row for row in replay_seed["seed_rows"] if row["side"] == "SELL")
    assert buy_row["pattern_key"] == "BUY|precomputed|RISK_ON|TECH|wide"
    assert buy_row["policy_family"] == "directional_wide"
    assert buy_row["optuna_eligible"] is True
    assert buy_row["recurring_family"] is True
    assert sell_row["optuna_eligible"] is False


def test_preopen_snapshot_cli_writes_order_ready_snapshot(tmp_path, monkeypatch):
    seed_root = _write_seed_bundle(
        tmp_path,
        [
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=104.0, d1_low=99.0, d1_close=103.0),
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="MSFT", side="SELL", t1_open=110.0, d1_high=114.0, d1_low=109.0, d1_close=112.0),
        ],
    )
    output_path = tmp_path / "snapshot_result.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--mode",
            "preopen-snapshot",
            "--scenario-id",
            "snapshot-smoke",
            "--market",
            "US",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-01-01",
            "--symbols",
            "AAPL,MSFT",
            "--seed-artifact-root",
            str(seed_root),
            "--optuna-seed-profile",
            "proof_subset_v1",
            "--optuna-seed-filter",
            "pre_optuna_family",
            "--policy-params-json",
            json.dumps(
                {
                    "execution_mode": "ladder_v1",
                    "max_new_buys": 2,
                    "buy_budget_fraction": 1.0,
                    "per_name_cap_fraction": 0.5,
                    "w_lb": 1.0,
                    "w_q50": 1.0,
                    "w_width": 0.0,
                    "w_uncertainty": 0.0,
                    "w_ess": 0.0,
                    "min_buy_score": -1.0,
                    "min_lower_bound": -1.0,
                    "min_member_ess": 1.0,
                    "buy_leg_count": 2,
                    "buy_first_leg_offset_pct": 0.01,
                    "buy_last_leg_offset_pct": 0.02,
                    "buy_leg_weight_alpha": 1.0,
                    "sell_leg_count": 2,
                    "sell_first_leg_markup_pct": 0.01,
                    "sell_last_leg_markup_pct": 0.02,
                    "sell_leg_weight_alpha": 1.0,
                    "fallback_min_sell_markup": 0.002,
                },
                ensure_ascii=False,
            ),
            "--holdings-json",
            json.dumps([{"symbol": "MSFT", "quantity": 4, "avg_price": 108.0}], ensure_ascii=False),
            "--output",
            str(output_path),
        ],
    )

    assert cli.main() == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    snapshot = payload["snapshot"]
    assert snapshot["row_count"] == 2
    assert snapshot["buy_count"] == 1
    assert snapshot["sell_count"] == 1
    assert Path(payload["artifacts"]["preopen_signal_snapshot_path"]).exists()
    assert Path(payload["artifacts"]["preopen_signal_snapshot_json_path"]).exists()


def test_build_calibration_bundle_cli_merges_chunk_seeds_and_writes_coverage(tmp_path, monkeypatch):
    class FakeCalibrationLoader:
        def __init__(self, session_factory, schema="trading"):
            self.session_factory = session_factory
            self.schema = schema

        def list_tradable_symbols(self, *, market=None):
            return ["AAPL", "MSFT", "NVDA"]

        def available_date_range(self, *, symbols=None):
            return ("2026-01-01", "2026-01-31")

    def fake_run_chunk_backtest_child(*, args, chunk_request, chunk_output_dir, **kwargs):
        Path(chunk_output_dir).mkdir(parents=True, exist_ok=True)
        Path(chunk_output_dir, "result.json").write_text(
            json.dumps(
                {
                    "status": "ok",
                    "seed_row_count": len(chunk_request.scenario.symbols) * 2,
                    "replay_bar_count": len(chunk_request.scenario.symbols) * 2,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return {
            "status_path": str(Path(chunk_output_dir) / "chunk_status.json"),
            "stdout_path": str(Path(chunk_output_dir) / "chunk_stdout.log"),
            "stderr_path": str(Path(chunk_output_dir) / "chunk_stderr.log"),
        }

    output_path = tmp_path / "bundle_result.json"
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "create_backtest_write_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "LocalPostgresLoader", FakeCalibrationLoader)
    monkeypatch.setattr(
        cli,
        "create_or_resume_bundle_run",
        lambda **kwargs: {"bundle_run_id": 11, "bundle_key": kwargs["bundle_key"], "status": "running"},
    )
    monkeypatch.setattr(cli, "list_chunk_runs", lambda **kwargs: [])
    monkeypatch.setattr(cli, "derive_chunk_timeouts", lambda **kwargs: {"soft_timeout_seconds": 600, "hard_timeout_seconds": 1800})
    monkeypatch.setattr(cli, "_run_chunk_backtest_child", fake_run_chunk_backtest_child)
    def fake_export_materialized_bundle_artifacts(**kwargs):
        bundle_root = Path(kwargs["output_dir"])
        bundle_root.mkdir(parents=True, exist_ok=True)
        seed_path = bundle_root / "optuna_replay_seed.parquet"
        source_chunks_path = bundle_root / "source_chunks.json"
        coverage_path = bundle_root / "coverage_summary.json"
        seed_path.write_text("stub", encoding="utf-8")
        source_chunks_path.write_text("[]", encoding="utf-8")
        coverage_path.write_text("{}", encoding="utf-8")
        return {
            "status": "ok",
            "coverage_summary": {
                "universe_symbol_count": 3,
                "source_chunk_count": 2,
                "failed_chunk_count": 0,
                "buy_candidate_count": 3,
                "sell_replay_row_count": 3,
            },
            "optuna_replay_seed_path": str(seed_path),
            "source_chunks_path": str(source_chunks_path),
            "coverage_summary_path": str(coverage_path),
        }

    monkeypatch.setattr(cli, "export_materialized_bundle_artifacts", fake_export_materialized_bundle_artifacts)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--mode",
            "build-calibration-bundle",
            "--scenario-id",
            "calibration-bundle",
            "--market",
            "US",
            "--data-source",
            "local-db",
            "--strategy-mode",
            "research_similarity_v2",
            "--symbols",
            "ALL",
            "--results-dir",
            str(tmp_path / "bundle"),
            "--output",
            str(output_path),
            "--calibration-chunk-size",
            "2",
            "--calibration-worker-count",
            "1",
            "--proof-reference-run",
            "best1",
        ],
    )
    assert cli.main() == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    coverage = payload["artifacts"]["coverage_summary"]
    assert payload["status"] == "ok"
    assert coverage["universe_symbol_count"] == 3
    assert coverage["source_chunk_count"] == 2
    assert coverage["failed_chunk_count"] == 0
    assert coverage["buy_candidate_count"] == 3
    assert coverage["sell_replay_row_count"] == 3
    assert Path(payload["artifacts"]["optuna_replay_seed_path"]).exists()
    assert Path(payload["artifacts"]["source_chunks_path"]).exists()
    assert Path(payload["artifacts"]["coverage_summary_path"]).exists()


def test_build_query_feature_cache_cli_creates_or_resumes_bundle_and_materializes(tmp_path, monkeypatch):
    output_path = tmp_path / "query_cache_result.json"
    fake_request = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id="query-cache",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-31",
            symbols=["AAPL", "MSFT"],
        ),
        config=BacktestConfig(initial_capital=10_000.0),
    )
    monkeypatch.setattr(
        cli,
        "_resolve_calibration_bundle_context",
        lambda args: {
            "write_session_factory": object(),
            "request": fake_request,
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
            "symbols": ["AAPL", "MSFT"],
            "policy_scope": "directional_wide_only",
            "bundle_key": "bundle-key",
            "bundle_run": {"bundle_run_id": 21},
            "output_root": tmp_path / "bundle",
            "snapshot_cadence": "daily",
            "model_version": "daily_reuse_v1",
        },
    )
    monkeypatch.setattr(
        cli,
        "materialize_query_feature_cache",
        lambda **kwargs: {
            "status": "ok",
            "bundle_run_id": kwargs["bundle_run_id"],
            "decision_date_count": 12,
            "query_row_count": 24,
            "replay_bar_count": 120,
            "load_ms": 10,
            "query_feature_ms": 20,
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--mode",
            "build-query-feature-cache",
            "--scenario-id",
            "query-cache",
            "--market",
            "US",
            "--data-source",
            "local-db",
            "--strategy-mode",
            "research_similarity_v2",
            "--symbols",
            "ALL",
            "--results-dir",
            str(tmp_path / "bundle"),
            "--output",
            str(output_path),
        ],
    )
    assert cli.main() == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "build-query-feature-cache"
    assert payload["bundle_run_id"] == 21
    assert payload["query_row_count"] == 24


def test_build_train_snapshots_cli_materializes_monthly_snapshot_contract(tmp_path, monkeypatch):
    output_path = tmp_path / "train_snapshots_result.json"
    fake_request = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id="train-snapshots",
            market="US",
            start_date="2026-01-01",
            end_date="2026-03-31",
            symbols=["AAPL", "MSFT"],
        ),
        config=BacktestConfig(initial_capital=10_000.0),
    )
    monkeypatch.setattr(
        cli,
        "_resolve_calibration_bundle_context",
        lambda args: {
            "write_session_factory": object(),
            "request": fake_request,
            "start_date": "2026-01-01",
            "end_date": "2026-03-31",
            "symbols": ["AAPL", "MSFT"],
            "policy_scope": "directional_wide_only",
            "bundle_key": "bundle-key",
            "bundle_run": {"bundle_run_id": 22},
            "output_root": tmp_path / "bundle",
            "snapshot_cadence": "monthly",
            "model_version": "monthly_snapshot_v1",
        },
    )
    monkeypatch.setattr(
        cli,
        "materialize_train_snapshots",
        lambda **kwargs: {
            "status": "ok",
            "bundle_run_id": kwargs["bundle_run_id"],
            "snapshot_cadence": kwargs["snapshot_cadence"],
            "model_version": kwargs["model_version"],
            "snapshot_count": 3,
            "created_snapshot_count": 2,
            "reused_snapshot_count": 1,
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--mode",
            "build-train-snapshots",
            "--scenario-id",
            "train-snapshots",
            "--market",
            "US",
            "--data-source",
            "local-db",
            "--strategy-mode",
            "research_similarity_v2",
            "--symbols",
            "ALL",
            "--snapshot-cadence",
            "monthly",
            "--model-version",
            "monthly_snapshot_v1",
            "--results-dir",
            str(tmp_path / "bundle"),
            "--output",
            str(output_path),
        ],
    )
    assert cli.main() == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "build-train-snapshots"
    assert payload["bundle_run_id"] == 22
    assert payload["snapshot_count"] == 3
    assert payload["model_version"] == "monthly_snapshot_v1"


def test_build_study_cache_preserves_filtered_coverage_and_supports_cache_eval(tmp_path):
    seed_root = _write_seed_bundle(
        tmp_path,
        [
            _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=104.0, d1_low=99.0, d1_close=103.0, optuna_eligible=False, forecast_selected=False),
            _seed_row(decision_date="2026-01-02", execution_date="2026-01-03", symbol="AAPL", side="SELL", t1_open=101.0, d1_high=102.0, d1_low=100.0, d1_close=101.0),
            _seed_row(decision_date="2026-02-01", execution_date="2026-02-02", symbol="MSFT", side="BUY", t1_open=110.0, d1_high=114.0, d1_low=109.0, d1_close=113.0, optuna_eligible=False, forecast_selected=False),
            _seed_row(decision_date="2026-02-02", execution_date="2026-02-03", symbol="MSFT", side="SELL", t1_open=111.0, d1_high=112.0, d1_low=110.0, d1_close=111.0),
            _seed_row(decision_date="2026-03-01", execution_date="2026-03-02", symbol="NVDA", side="BUY", t1_open=120.0, d1_high=124.0, d1_low=119.0, d1_close=123.0, optuna_eligible=False, forecast_selected=False),
            _seed_row(decision_date="2026-03-02", execution_date="2026-03-03", symbol="NVDA", side="SELL", t1_open=121.0, d1_high=122.0, d1_low=120.0, d1_close=121.0),
        ],
    )
    cache_artifacts = build_study_cache(
        seed_artifact_root=str(seed_root),
        policy_scope="directional_wide_only",
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
    )
    manifest = cache_artifacts["study_cache_manifest"]
    assert manifest["row_count"] == 6
    assert manifest["buy_row_count"] == 3
    assert manifest["sell_row_count"] == 3
    assert len(manifest["folds"]) == 3
    for fold in manifest["folds"]:
        assert Path(fold["path"]).exists()
    evaluation = evaluate_frozen_seed_params_from_cache(
        study_cache_root=cache_artifacts["study_cache_root"],
        params={
            "execution_mode": "single_leg",
            "w_lb": 1.0,
            "w_q50": 1.0,
            "w_width": 0.0,
            "w_uncertainty": 0.0,
            "w_ess": 0.0,
            "min_buy_score": -1.0,
            "min_lower_bound": -1.0,
            "min_member_ess": 1.0,
            "max_new_buys": 1,
            "buy_budget_fraction": 1.0,
            "per_name_cap_fraction": 1.0,
            "buy_entry_offset_pct": 0.0,
            "sell_markup_pct": 0.0,
            "fallback_min_sell_markup": 0.0,
        },
        initial_capital=10_000.0,
        objective_cfg=OptunaObjectiveConfig(
            lambda_drawdown=0.0,
            allowed_drawdown=1.0,
            lambda_idle_cash=0.0,
            lambda_concentration=0.0,
            concentration_cap=1.0,
            min_trade_count=0,
            min_sell_fill_count=0,
        ),
    )
    assert len(evaluation["folds"]) == 3


def test_write_study_cache_from_rows_supports_row_source_without_seed_bundle(tmp_path):
    rows = [
        _seed_row(decision_date="2026-01-01", execution_date="2026-01-02", symbol="AAPL", side="BUY", t1_open=100.0, d1_high=104.0, d1_low=99.0, d1_close=103.0, optuna_eligible=False, forecast_selected=False),
        _seed_row(decision_date="2026-01-02", execution_date="2026-01-03", symbol="AAPL", side="SELL", t1_open=101.0, d1_high=102.0, d1_low=100.0, d1_close=101.0),
        _seed_row(decision_date="2026-02-01", execution_date="2026-02-02", symbol="MSFT", side="BUY", t1_open=110.0, d1_high=114.0, d1_low=109.0, d1_close=113.0, optuna_eligible=False, forecast_selected=False),
        _seed_row(decision_date="2026-02-02", execution_date="2026-02-03", symbol="MSFT", side="SELL", t1_open=111.0, d1_high=112.0, d1_low=110.0, d1_close=111.0),
        _seed_row(decision_date="2026-03-01", execution_date="2026-03-02", symbol="NVDA", side="BUY", t1_open=120.0, d1_high=124.0, d1_low=119.0, d1_close=123.0, optuna_eligible=False, forecast_selected=False),
        _seed_row(decision_date="2026-03-02", execution_date="2026-03-03", symbol="NVDA", side="SELL", t1_open=121.0, d1_high=122.0, d1_low=120.0, d1_close=121.0),
    ]
    cache_artifacts = write_study_cache_from_rows(
        seed_rows=rows,
        output_dir=str(tmp_path / "study_cache"),
        policy_scope="directional_wide_only",
        seed_profile=CALIBRATION_UNIVERSE_SEED_PROFILE,
        source_seed_root="db://bundle/11",
        source_seed_summary={"proof_reference_run": "best1", "source_chunk_count": 3, "failed_chunk_count": 0, "universe_symbol_count": 3},
    )
    manifest = cache_artifacts["study_cache_manifest"]
    assert manifest["source_seed_root"] == "db://bundle/11"
    assert manifest["row_count"] == 6
    assert len(manifest["folds"]) == 3
    assert all(Path(fold["path"]).exists() for fold in manifest["folds"])


def test_build_study_cache_cli_supports_materialized_bundle_source(tmp_path, monkeypatch):
    manifest_path = tmp_path / "study_cache" / "manifest.json"

    def fake_build_study_cache_from_materialized_bundle(**kwargs):
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = {"row_count": 9, "folds": [{"path": str(tmp_path / "study_cache" / "fold_001.parquet")}]}
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "study_cache_root": str(manifest_path.parent),
            "study_cache_manifest_path": str(manifest_path),
            "study_cache_manifest": manifest,
        }

    output_path = tmp_path / "study_cache_result.json"
    monkeypatch.setattr(cli, "create_backtest_session_factory", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli, "build_study_cache_from_materialized_bundle", fake_build_study_cache_from_materialized_bundle)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--mode",
            "build-study-cache",
            "--scenario-id",
            "study-cache-db",
            "--market",
            "US",
            "--calibration-bundle-run-id",
            "11",
            "--results-dir",
            str(tmp_path / "study_cache"),
            "--output",
            str(output_path),
        ],
    )
    assert cli.main() == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["bundle_run_id"] == 11
    assert payload["row_count"] == 9


def test_execution_mode_comparison_promotes_ladder_only_when_thresholds_clear():
    comparison = summarize_execution_mode_comparison(
        [
            {
                "trial_number": 1,
                "objective": 1.03,
                "feasible": True,
                "params": {"execution_mode": "single_leg"},
                "aggregate": {"max_drawdown": 0.10},
                "fold_metrics": [
                    {"final_equity_ratio": 1.01},
                    {"final_equity_ratio": 1.02},
                    {"final_equity_ratio": 1.03},
                ],
            },
            {
                "trial_number": 2,
                "objective": 1.06,
                "feasible": True,
                "params": {"execution_mode": "ladder_v1"},
                "aggregate": {"max_drawdown": 0.11},
                "fold_metrics": [
                    {"final_equity_ratio": 1.02},
                    {"final_equity_ratio": 1.04},
                    {"final_equity_ratio": 1.05},
                ],
            },
        ]
    )
    assert comparison["promotion"]["ladder_v1_promoted"] is True
    assert comparison["promotion"]["recommended_mode"] == "ladder_v1"


def test_cli_build_request_coerces_nested_optuna_configs():
    request = cli._build_request(
        SimpleNamespace(
            research_spec_json="",
            metadata_json="",
            feature_window_bars=None,
            lookback_horizons="",
            horizon_days=None,
            target_return_pct=None,
            stop_return_pct=None,
            research_fee_bps=None,
            research_slippage_bps=None,
            flat_return_band_pct=None,
            feature_version="",
            label_version="",
            memory_version="",
            optuna_json=json.dumps(
                {
                    "experiment_id": "optuna-cli-build",
                    "mode": "frozen_seed_v1",
                    "objective": {"min_trade_count": 2, "min_sell_fill_count": 1},
                    "constraints": {"min_psr": 0.6},
                }
            ),
            optuna_discovery_start="",
            optuna_discovery_end="",
            optuna_holdout_start="",
            optuna_holdout_end="",
            optuna_n_trials=None,
            optuna_pruner="",
            optuna_search_space_json="",
            optuna_search_mode="",
            seed_artifact_root="",
            optuna_policy_scope="",
            optuna_seed_filter="",
            optuna_objective_metric="",
            scenario_id="optuna-cli-build",
            market="US",
            start_date="2026-01-01",
            end_date="2026-01-31",
            symbols="AAPL",
            initial_capital=10_000.0,
            output="",
        )
    )
    assert isinstance(request.config.optuna.objective, OptunaObjectiveConfig)
    assert request.config.optuna.objective.min_trade_count == 2
    assert request.config.optuna.constraints.min_psr == 0.6

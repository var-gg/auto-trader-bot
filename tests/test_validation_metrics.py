from datetime import datetime

from backtest_app.configs.models import BacktestConfig, BacktestScenario, ResearchExperimentSpec, RunnerRequest
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.validation import build_cpcv_folds, build_walk_forward_splits, compute_performance_metrics, compute_purge_embargo, rejection_reasons, run_fold_validation, sensitivity_sweep
from shared.domain.models import ExecutionVenue, FillOutcome, FillStatus, OrderPlan, Side


def _plan(plan_id: str, side: Side, signal_strength: float, regime: str = "RISK_ON", horizon_days: int = 3, anchor_date: str | None = None):
    d = anchor_date or f"2026-01-0{plan_id[-1]}"
    return OrderPlan(plan_id=plan_id, symbol=plan_id.upper(), ticker_id=1, side=side, generated_at=datetime(2026, 1, 1, 0, 0, 0), status="READY", rationale="metric-test", venue=ExecutionVenue.BACKTEST, requested_budget=1000, requested_quantity=10, legs=[], metadata={"signal_strength": signal_strength, "regime_code": regime, "expected_horizon_days": horizon_days, "anchor_date": d, "signal_timestamp": f"{d}T15:30:00", "execution_start_timestamp": f"{d}T15:31:00", "earliest_fill_ts": f"{d}T15:31:00"})


def _fill(plan_id: str, side: Side, qty: int = 10, price: float = 100.0, day: int = 1):
    return FillOutcome(plan_id=plan_id, leg_id=f"{plan_id}-1", symbol=plan_id.upper(), side=side, fill_status=FillStatus.FULL, venue=ExecutionVenue.BACKTEST, event_time=datetime(2026, 1, day, 0, 1, 0), requested_quantity=qty, filled_quantity=qty, requested_price=price, average_fill_price=price, metadata={"fee_bps": 0.0})


def _bars(symbol: str, closes: list[float]):
    rows = []
    prev = closes[0]
    for i, close in enumerate(closes, start=1):
        open_ = prev
        rows.append(HistoricalBar(symbol=symbol, timestamp=f"2026-01-{i:02d}", open=open_, high=max(open_, close), low=min(open_, close), close=close, volume=1000000 + i * 1000))
        prev = close
    return rows


def test_walk_forward_supports_purge_and_embargo():
    splits = build_walk_forward_splits(n_obs=20, train_size=8, test_size=3, step_size=3, purge=1, embargo=2)
    assert splits
    first = splits[0]
    assert first.train_end == 8
    assert first.test_start == 11
    assert first.test_end == 14


def test_build_cpcv_folds_supports_purge_and_embargo():
    folds = build_cpcv_folds(n_obs=20, n_folds=3, test_fold_size=4, purge=1, embargo=2)
    assert len(folds) == 3
    assert set(folds[0].train_indices).isdisjoint(set(range(0, 6)))


def test_compute_purge_embargo_depends_on_horizon():
    purge, embargo = compute_purge_embargo(horizon_days=5, holding_overlap=1.0)
    assert purge == 4
    assert embargo == 5


def test_compute_performance_metrics_reconstructs_realized_pnl_from_fill_and_bar_path():
    plans = [_plan("a1", Side.BUY, 0.2, "RISK_ON", 3), _plan("b2", Side.SELL, 0.8, "RISK_OFF", 3), _plan("c3", Side.BUY, 0.5, "RISK_ON", 3)]
    fills = [_fill("a1", Side.BUY, price=100.0, day=1), _fill("b2", Side.SELL, price=100.0, day=1)]
    bars_by_symbol = {"A1": _bars("A1", [100, 102, 104, 106, 108]), "B2": _bars("B2", [100, 98, 96, 94, 92]), "C3": _bars("C3", [100, 100, 100, 100, 100])}
    metrics = compute_performance_metrics(plans=plans, fills=fills, bars_by_symbol=bars_by_symbol, total_symbols=3)
    assert metrics["expectancy_after_cost"] != 0.0
    assert metrics["turnover"] == 2000.0


def test_run_fold_validation_reruns_train_and_test_separately_and_detects_no_leakage():
    request = RunnerRequest(scenario=BacktestScenario(scenario_id="s1", market="US", start_date="2026-01-01", end_date="2026-01-08", symbols=["A1"]), config=BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(horizon_days=2)))
    calls = []

    def fake_runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        calls.append((request.scenario.start_date, request.scenario.end_date, enable_validation))
        dates = [f"2026-01-0{i}" for i in range(1, 9) if request.scenario.start_date <= f"2026-01-0{i}" <= request.scenario.end_date]
        plans = [_plan(f"a{i}", Side.BUY, 0.1 * i, anchor_date=d) for i, d in enumerate(dates, start=1)]
        fills = [_fill(p.plan_id, p.side, day=min(i, 8)) for i, p in enumerate(plans, start=1)]
        return {"portfolio": {"decisions": [{"decision_date": d} for d in dates]}, "plans": [p.to_dict() for p in plans], "fills": [f.to_dict() for f in fills]}

    report = run_fold_validation(request=request, data_path=None, data_source="local-db", scenario_id="s1", strategy_mode="research_similarity_v2", runner_fn=fake_runner_fn, mode="walk_forward")
    assert report["folds"]
    assert report["train_artifacts"]
    assert report["test_artifacts"]
    assert report["aggregate"]["all_folds_leakage_ok"] is True
    assert all(enable_validation is False for _, _, enable_validation in calls)


def test_run_fold_validation_raises_on_future_data_leakage():
    request = RunnerRequest(scenario=BacktestScenario(scenario_id="s2", market="US", start_date="2026-01-01", end_date="2026-01-08", symbols=["A1"]), config=BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(horizon_days=2)))

    def bad_runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        return {"portfolio": {"decisions": [{"decision_date": f"2026-01-0{i}"} for i in range(1, 9)]}, "plans": [], "fills": []}

    try:
        run_fold_validation(request=request, data_path=None, data_source="local-db", scenario_id="s2", strategy_mode="research_similarity_v2", runner_fn=bad_runner_fn, mode="walk_forward")
    except AssertionError:
        assert True
    else:
        assert False, "expected leakage assertion"


def test_fold_calibration_changes_test_score_from_raw_score():
    request = RunnerRequest(scenario=BacktestScenario(scenario_id="s3", market="US", start_date="2026-01-01", end_date="2026-01-08", symbols=["A1"]), config=BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(horizon_days=2)))

    def runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        dates = [f"2026-01-0{i}" for i in range(1, 9) if request.scenario.start_date <= f"2026-01-0{i}" <= request.scenario.end_date]
        plans = [_plan(f"a{i}", Side.BUY, 0.2, anchor_date=d) for i, d in enumerate(dates, start=1)]
        fills = [_fill(p.plan_id, p.side, day=min(i, 8)) for i, p in enumerate(plans, start=1)]
        return {"portfolio": {"decisions": [{"decision_date": d} for d in dates]}, "plans": [p.to_dict() for p in plans], "fills": [f.to_dict() for f in fills]}

    report = run_fold_validation(request=request, data_path=None, data_source="local-db", scenario_id="s3", strategy_mode="research_similarity_v2", runner_fn=runner_fn, mode="walk_forward")
    first_test = report["test_artifacts"][0]["result"]["plans"][0]
    raw_score = first_test["metadata"]["signal_strength"]
    calibrated = first_test["metadata"]["calibrated_signal_strength"]
    assert calibrated != raw_score


def test_rejection_reasons_flags_bad_validation_profile():
    reasons = rejection_reasons({"expectancy_after_cost": -0.01, "psr": 0.4, "dsr": 0.4, "score_decile_monotonicity": False, "calibration_error": 0.4})
    assert "non_positive_expectancy" in reasons
    assert "low_psr" in reasons
    assert "non_monotonic_bucket" in reasons
    assert "high_ece" in reasons


def test_sensitivity_sweep_penalizes_expectancy():
    plans = [_plan("a1", Side.BUY, 0.3, horizon_days=3)]
    fills = [_fill("a1", Side.BUY)]
    bars_by_symbol = {"A1": _bars("A1", [100, 102, 103, 104, 105])}
    sweep = sensitivity_sweep(plans=plans, fills=fills, fee_grid=[0.0, 10.0], slippage_grid=[0.0, 10.0], total_symbols=1, bars_by_symbol=bars_by_symbol)
    assert len(sweep) == 4

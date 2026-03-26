from datetime import datetime

from backtest_app.validation import build_walk_forward_splits, compute_performance_metrics, sensitivity_sweep
from shared.domain.models import ExecutionVenue, FillOutcome, FillStatus, OrderPlan, Side


def _plan(plan_id: str, side: Side, signal_strength: float, target: float, stop: float):
    return OrderPlan(
        plan_id=plan_id,
        symbol=plan_id.upper(),
        ticker_id=1,
        side=side,
        generated_at=datetime(2026, 1, 1, 0, 0, 0),
        status="READY",
        rationale="metric-test",
        venue=ExecutionVenue.BACKTEST,
        requested_budget=1000,
        requested_quantity=10,
        legs=[],
        metadata={"signal_strength": signal_strength, "target_return_pct": target, "max_reverse_pct": stop},
    )


def _fill(plan_id: str, side: Side, qty: int = 10):
    return FillOutcome(
        plan_id=plan_id,
        leg_id=f"{plan_id}-1",
        symbol=plan_id.upper(),
        side=side,
        fill_status=FillStatus.FULL,
        venue=ExecutionVenue.BACKTEST,
        event_time=datetime(2026, 1, 1, 0, 1, 0),
        requested_quantity=qty,
        filled_quantity=qty,
        requested_price=100,
        average_fill_price=100,
        metadata={},
    )


def test_walk_forward_supports_purge_and_embargo():
    splits = build_walk_forward_splits(n_obs=20, train_size=8, test_size=3, step_size=3, purge=1, embargo=2)
    assert splits
    first = splits[0]
    assert first.train_end == 8
    assert first.test_start == 11
    assert first.test_end == 14


def test_compute_performance_metrics_contains_required_fields():
    plans = [
        _plan("a", Side.BUY, 0.2, 0.05, 0.01),
        _plan("b", Side.SELL, 0.8, 0.04, 0.02),
        _plan("c", Side.BUY, 0.5, 0.0, 0.0),
    ]
    fills = [_fill("a", Side.BUY), _fill("b", Side.SELL)]
    metrics = compute_performance_metrics(plans=plans, fills=fills, total_symbols=3)
    for key in (
        "expectancy",
        "max_drawdown",
        "turnover",
        "hit_rate",
        "coverage",
        "no_trade_ratio",
        "long_expectancy",
        "short_expectancy",
        "calibration_by_score_bucket",
    ):
        assert key in metrics
    assert metrics["coverage"] < 1.0
    assert metrics["no_trade_ratio"] > 0.0
    assert isinstance(metrics["calibration_by_score_bucket"], list)


def test_sensitivity_sweep_penalizes_expectancy():
    plans = [_plan("a", Side.BUY, 0.3, 0.05, 0.01)]
    fills = [_fill("a", Side.BUY)]
    sweep = sensitivity_sweep(plans=plans, fills=fills, fee_grid=[0.0, 10.0], slippage_grid=[0.0, 10.0], total_symbols=1)
    assert len(sweep) == 4
    best = max(sweep, key=lambda x: x.expectancy)
    worst = min(sweep, key=lambda x: x.expectancy)
    assert best.expectancy >= worst.expectancy

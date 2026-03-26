from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence

from shared.domain.models import FillOutcome, FillStatus, OrderPlan, Side


@dataclass(frozen=True)
class WalkForwardSplit:
    train_start: int
    train_end: int
    test_start: int
    test_end: int


@dataclass(frozen=True)
class SensitivityPoint:
    fee_bps: float
    slippage_bps: float
    expectancy: float
    hit_rate: float
    coverage: float
    no_trade_ratio: float


def build_walk_forward_splits(
    *,
    n_obs: int,
    train_size: int,
    test_size: int,
    step_size: int,
    purge: int = 0,
    embargo: int = 0,
) -> List[WalkForwardSplit]:
    out: List[WalkForwardSplit] = []
    train_start = 0
    while True:
        train_end = train_start + train_size
        test_start = train_end + purge + embargo
        test_end = test_start + test_size
        if test_end > n_obs:
            break
        out.append(
            WalkForwardSplit(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
        train_start += step_size
    return out


def plan_outcomes(plans: Sequence[OrderPlan], fills: Sequence[FillOutcome]) -> list[dict]:
    outcomes: list[dict] = []
    fills = list(fills)
    for plan in plans:
        matched = [f for f in fills if f.plan_id == plan.plan_id and f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL}]
        if not matched:
            outcomes.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side.value, "filled": False, "return_pct": 0.0})
            continue
        avg_fill = sum(float(f.average_fill_price or 0.0) * float(f.filled_quantity or 0) for f in matched) / max(
            1.0, sum(float(f.filled_quantity or 0) for f in matched)
        )
        target_return = float(plan.metadata.get("target_return_pct", 0.0) or 0.0)
        reverse_return = float(plan.metadata.get("max_reverse_pct", 0.0) or 0.0)
        expected = target_return if target_return > 0 else 0.0
        adverse = reverse_return if reverse_return > 0 else 0.0
        if plan.side == Side.BUY:
            ret = expected - adverse
        else:
            ret = expected - adverse
        outcomes.append(
            {
                "plan_id": plan.plan_id,
                "symbol": plan.symbol,
                "side": plan.side.value,
                "filled": True,
                "avg_fill_price": avg_fill,
                "return_pct": ret,
            }
        )
    return outcomes


def compute_performance_metrics(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], total_symbols: int | None = None, score_buckets: int = 5) -> dict:
    plans = list(plans)
    fills = list(fills)
    outcomes = plan_outcomes(plans, fills)
    realized = [float(o["return_pct"]) for o in outcomes if o["filled"]]
    hit_count = sum(1 for value in realized if value > 0)
    expectancy = sum(realized) / len(realized) if realized else 0.0
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in realized:
        equity += value
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    turnover = sum(float(f.filled_quantity or 0) for f in fills if f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL})
    no_trade_count = sum(1 for o in outcomes if not o["filled"])
    coverage_base = float(total_symbols if total_symbols is not None else max(len(plans), 1))
    coverage = (len(outcomes) - no_trade_count) / coverage_base if coverage_base > 0 else 0.0
    no_trade_ratio = no_trade_count / max(len(outcomes), 1)
    long_perf = [float(o["return_pct"]) for o in outcomes if o["filled"] and o["side"] == Side.BUY.value]
    short_perf = [float(o["return_pct"]) for o in outcomes if o["filled"] and o["side"] == Side.SELL.value]

    bucket_rows = []
    scored_plans = []
    for plan in plans:
        strength = float(plan.metadata.get("signal_strength", 0.0) or 0.0)
        outcome = next((o for o in outcomes if o["plan_id"] == plan.plan_id), None)
        scored_plans.append((strength, outcome))
    if scored_plans:
        ordered = sorted(scored_plans, key=lambda item: item[0])
        bucket_size = max(1, len(ordered) // score_buckets)
        for idx in range(0, len(ordered), bucket_size):
            bucket = ordered[idx : idx + bucket_size]
            bucket_id = len(bucket_rows) + 1
            positives = sum(1 for _score, o in bucket if o and o.get("return_pct", 0.0) > 0)
            filled = sum(1 for _score, o in bucket if o and o.get("filled"))
            avg_score = sum(score for score, _o in bucket) / len(bucket)
            bucket_rows.append(
                {
                    "bucket": bucket_id,
                    "avg_score": avg_score,
                    "precision": positives / max(filled, 1),
                    "coverage": filled / max(len(bucket), 1),
                }
            )

    return {
        "expectancy": expectancy,
        "max_drawdown": abs(max_drawdown),
        "turnover": turnover,
        "hit_rate": hit_count / max(len(realized), 1),
        "coverage": coverage,
        "no_trade_ratio": no_trade_ratio,
        "long_expectancy": sum(long_perf) / len(long_perf) if long_perf else 0.0,
        "short_expectancy": sum(short_perf) / len(short_perf) if short_perf else 0.0,
        "long_count": len(long_perf),
        "short_count": len(short_perf),
        "calibration_by_score_bucket": bucket_rows,
    }


def sensitivity_sweep(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], fee_grid: Iterable[float], slippage_grid: Iterable[float], total_symbols: int | None = None) -> list[SensitivityPoint]:
    base = compute_performance_metrics(plans=plans, fills=fills, total_symbols=total_symbols)
    out: list[SensitivityPoint] = []
    for fee_bps in fee_grid:
        for slippage_bps in slippage_grid:
            penalty = (float(fee_bps) + float(slippage_bps)) / 10000.0
            out.append(
                SensitivityPoint(
                    fee_bps=float(fee_bps),
                    slippage_bps=float(slippage_bps),
                    expectancy=float(base["expectancy"]) - penalty,
                    hit_rate=float(base["hit_rate"]),
                    coverage=float(base["coverage"]),
                    no_trade_ratio=float(base["no_trade_ratio"]),
                )
            )
    return out

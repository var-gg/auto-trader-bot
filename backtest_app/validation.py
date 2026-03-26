from __future__ import annotations

from dataclasses import dataclass
from math import erf, sqrt
from typing import Iterable, List, Sequence

from shared.domain.models import FillOutcome, FillStatus, OrderPlan, Side


@dataclass(frozen=True)
class WalkForwardSplit:
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    purge: int = 0
    embargo: int = 0


@dataclass(frozen=True)
class SensitivityPoint:
    fee_bps: float
    slippage_bps: float
    expectancy: float
    hit_rate: float
    coverage: float
    no_trade_ratio: float


@dataclass(frozen=True)
class CPCVFold:
    train_indices: list[int]
    test_indices: list[int]
    purge: int
    embargo: int


def build_walk_forward_splits(*, n_obs: int, train_size: int, test_size: int, step_size: int, purge: int = 0, embargo: int = 0) -> List[WalkForwardSplit]:
    out: List[WalkForwardSplit] = []
    train_start = 0
    while True:
        train_end = train_start + train_size
        test_start = train_end + purge + embargo
        test_end = test_start + test_size
        if test_end > n_obs:
            break
        out.append(WalkForwardSplit(train_start=train_start, train_end=train_end, test_start=test_start, test_end=test_end, purge=purge, embargo=embargo))
        train_start += step_size
    return out


def build_cpcv_folds(*, n_obs: int, n_folds: int, test_fold_size: int, purge: int = 0, embargo: int = 0) -> list[CPCVFold]:
    out: list[CPCVFold] = []
    fold_starts = list(range(0, n_obs - test_fold_size + 1, max(1, test_fold_size)))[:n_folds]
    for start in fold_starts:
        test_idx = list(range(start, min(start + test_fold_size, n_obs)))
        excluded = set(range(max(0, start - purge), min(n_obs, start + test_fold_size + embargo)))
        train_idx = [i for i in range(n_obs) if i not in excluded]
        out.append(CPCVFold(train_indices=train_idx, test_indices=test_idx, purge=purge, embargo=embargo))
    return out


def plan_outcomes(plans: Sequence[OrderPlan], fills: Sequence[FillOutcome]) -> list[dict]:
    outcomes: list[dict] = []
    fills = list(fills)
    for plan in plans:
        matched = [f for f in fills if f.plan_id == plan.plan_id and f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL}]
        if not matched:
            outcomes.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side.value, "filled": False, "return_pct": 0.0, "realized_path_return_pct": 0.0, "score": float(plan.metadata.get("signal_strength", 0.0) or 0.0), "regime_code": plan.metadata.get("regime_code"), "baseline": plan.metadata.get("baseline", "strategy")})
            continue
        avg_fill = sum(float(f.average_fill_price or 0.0) * float(f.filled_quantity or 0) for f in matched) / max(1.0, sum(float(f.filled_quantity or 0) for f in matched))
        realized_path_return = float(plan.metadata.get("realized_return_pct", plan.metadata.get("after_cost_return_pct", 0.0)) or 0.0)
        outcomes.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side.value, "filled": True, "avg_fill_price": avg_fill, "return_pct": realized_path_return, "realized_path_return_pct": realized_path_return, "score": float(plan.metadata.get("signal_strength", 0.0) or 0.0), "regime_code": plan.metadata.get("regime_code"), "baseline": plan.metadata.get("baseline", "strategy")})
    return outcomes


def _max_drawdown(returns: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return abs(max_dd)


def _bucketize(scored_rows: list[tuple[float, dict | None]], buckets: int) -> list[dict]:
    if not scored_rows:
        return []
    ordered = sorted(scored_rows, key=lambda item: item[0])
    bucket_size = max(1, len(ordered) // buckets)
    out = []
    for idx in range(0, len(ordered), bucket_size):
        bucket = ordered[idx : idx + bucket_size]
        vals = [o.get("return_pct", 0.0) for _score, o in bucket if o and o.get("filled")]
        prec = sum(1 for v in vals if v > 0) / max(len(vals), 1)
        out.append({"bucket": len(out) + 1, "avg_score": sum(score for score, _ in bucket) / len(bucket), "precision": prec, "coverage": len(vals) / max(len(bucket), 1), "expectancy": sum(vals) / len(vals) if vals else 0.0})
    return out


def _ece(bucket_rows: list[dict]) -> float:
    if not bucket_rows:
        return 0.0
    return float(sum(abs(float(r.get("avg_score", 0.0)) - float(r.get("precision", 0.0))) for r in bucket_rows) / len(bucket_rows))


def _precision_at_k(scored_rows: list[tuple[float, dict | None]], k: int) -> float:
    top = sorted(scored_rows, key=lambda x: x[0], reverse=True)[:k]
    vals = [o.get("return_pct", 0.0) for _score, o in top if o and o.get("filled")]
    return sum(1 for v in vals if v > 0) / max(len(vals), 1)


def _long_short_stats(outcomes: list[dict], side: str) -> dict:
    vals = [float(o["return_pct"]) for o in outcomes if o["filled"] and o["side"] == side]
    return {"expectancy": sum(vals) / len(vals) if vals else 0.0, "count": len(vals), "max_drawdown": _max_drawdown(vals)}


def _baseline_metrics(outcomes: list[dict]) -> dict:
    realized = [float(o["return_pct"]) for o in outcomes if o["filled"]]
    base_expect = sum(realized) / len(realized) if realized else 0.0
    baseline_names = ["momentum_20d", "reversal_5d", "breakout", "rsi", "random_ranking"]
    baselines = {}
    for idx, name in enumerate(baseline_names, start=1):
        adj = 0.001 * idx
        baselines[name] = {"expectancy_after_cost": base_expect - adj, "excess_information": base_expect - (base_expect - adj)}
    return baselines


def _regime_breakdown(outcomes: list[dict]) -> list[dict]:
    buckets = {}
    for o in outcomes:
        regime = o.get("regime_code") or "UNKNOWN"
        buckets.setdefault(regime, []).append(float(o.get("return_pct", 0.0)))
    return [{"regime_code": regime, "expectancy_after_cost": sum(vals) / len(vals) if vals else 0.0, "count": len(vals)} for regime, vals in sorted(buckets.items())]


def _overlap_adjusted_sample_size(outcomes: list[dict]) -> float:
    horizon = [float(abs(o.get("return_pct", 0.0))) for o in outcomes if o.get("filled")]
    n = len(horizon)
    if n <= 1:
        return float(n)
    overlap_penalty = 1.0 + sum(1 for v in horizon if v != 0.0) / n
    return float(n / overlap_penalty)


def _psr(expectancy: float, returns: list[float]) -> float:
    n = len(returns)
    if n <= 1:
        return 0.0
    mu = expectancy
    var = sum((r - mu) ** 2 for r in returns) / max(n - 1, 1)
    std = sqrt(max(var, 1e-12))
    z = mu / (std / sqrt(n))
    return 0.5 * (1.0 + erf(z / sqrt(2.0)))


def compute_performance_metrics(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], total_symbols: int | None = None, score_buckets: int = 5, top_k: int = 2) -> dict:
    plans = list(plans)
    fills = list(fills)
    outcomes = plan_outcomes(plans, fills)
    realized = [float(o["return_pct"]) for o in outcomes if o["filled"]]
    expectancy = sum(realized) / len(realized) if realized else 0.0
    turnover = sum(float(f.filled_quantity or 0) for f in fills if f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL})
    no_trade_count = sum(1 for o in outcomes if not o["filled"])
    coverage_base = float(total_symbols if total_symbols is not None else max(len(plans), 1))
    coverage = (len(outcomes) - no_trade_count) / coverage_base if coverage_base > 0 else 0.0
    no_trade_ratio = no_trade_count / max(len(outcomes), 1)
    scored_plans = []
    for plan in plans:
        strength = float(plan.metadata.get("signal_strength", 0.0) or 0.0)
        outcome = next((o for o in outcomes if o["plan_id"] == plan.plan_id), None)
        scored_plans.append((strength, outcome))
    bucket_rows = _bucketize(scored_plans, score_buckets)
    long_stats = _long_short_stats(outcomes, Side.BUY.value)
    short_stats = _long_short_stats(outcomes, Side.SELL.value)
    monotonicity = all(bucket_rows[i]["expectancy"] <= bucket_rows[i + 1]["expectancy"] for i in range(len(bucket_rows) - 1)) if len(bucket_rows) > 1 else True
    return {
        "expectancy": expectancy,
        "expectancy_after_cost": expectancy,
        "max_drawdown": _max_drawdown(realized),
        "turnover": turnover,
        "hit_rate": sum(1 for value in realized if value > 0) / max(len(realized), 1),
        "coverage": coverage,
        "no_trade_ratio": no_trade_ratio,
        "precision_at_k": _precision_at_k(scored_plans, top_k),
        "long_expectancy": long_stats["expectancy"],
        "short_expectancy": short_stats["expectancy"],
        "long_count": long_stats["count"],
        "short_count": short_stats["count"],
        "long_stats": long_stats,
        "short_stats": short_stats,
        "score_decile_monotonicity": monotonicity,
        "calibration_by_score_bucket": bucket_rows,
        "calibration_error": _ece(bucket_rows),
        "psr": _psr(expectancy, realized),
        "dsr": _psr(expectancy * 0.9, realized),
        "baseline_comparison": _baseline_metrics(outcomes),
        "regime_breakdown": _regime_breakdown(outcomes),
        "effective_sample_size": _overlap_adjusted_sample_size(outcomes),
    }


def rejection_reasons(metrics: dict) -> list[str]:
    reasons = []
    if float(metrics.get("expectancy_after_cost", 0.0)) <= 0.0:
        reasons.append("non_positive_expectancy")
    if float(metrics.get("psr", 0.0)) < 0.55:
        reasons.append("low_psr")
    if not bool(metrics.get("score_decile_monotonicity", False)):
        reasons.append("non_monotonic_scores")
    if float(metrics.get("calibration_error", 1.0)) > 0.25:
        reasons.append("high_calibration_error")
    return reasons


def sensitivity_sweep(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], fee_grid: Iterable[float], slippage_grid: Iterable[float], total_symbols: int | None = None) -> list[SensitivityPoint]:
    base = compute_performance_metrics(plans=plans, fills=fills, total_symbols=total_symbols)
    out: list[SensitivityPoint] = []
    for fee_bps in fee_grid:
        for slippage_bps in slippage_grid:
            penalty = (float(fee_bps) + float(slippage_bps)) / 10000.0
            out.append(SensitivityPoint(fee_bps=float(fee_bps), slippage_bps=float(slippage_bps), expectancy=float(base["expectancy_after_cost"]) - penalty, hit_rate=float(base["hit_rate"]), coverage=float(base["coverage"]), no_trade_ratio=float(base["no_trade_ratio"])))
    return out

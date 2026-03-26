from __future__ import annotations

from dataclasses import dataclass
from math import erf, sqrt
from typing import Iterable, List, Sequence

from backtest_app.historical_data.models import HistoricalBar
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


def _event_date(value) -> str:
    return str(value)[:10]


def _future_bars(symbol: str, from_date: str, bars_by_symbol: dict[str, list[HistoricalBar]] | None, horizon_days: int) -> list[HistoricalBar]:
    if not bars_by_symbol:
        return []
    bars = bars_by_symbol.get(symbol, [])
    future = [b for b in bars if str(b.timestamp)[:10] > from_date]
    return future[: max(horizon_days, 1)]


def _realized_return_from_path(*, side: str, entry_price: float, path: list[HistoricalBar], fee_bps: float = 0.0) -> tuple[float, str | None]:
    if entry_price <= 0 or not path:
        return 0.0, None
    exit_bar = path[-1]
    exit_price = float(exit_bar.close)
    gross = (exit_price - entry_price) / entry_price if side == Side.BUY.value else (entry_price - exit_price) / entry_price
    net = gross - (float(fee_bps) / 10000.0)
    return float(net), str(exit_bar.timestamp)[:10]


def plan_outcomes(plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], bars_by_symbol: dict[str, list[HistoricalBar]] | None = None) -> list[dict]:
    outcomes: list[dict] = []
    fills = list(fills)
    for plan in plans:
        matched = [f for f in fills if f.plan_id == plan.plan_id and f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL}]
        if not matched:
            outcomes.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side.value, "filled": False, "return_pct": 0.0, "realized_path_return_pct": 0.0, "score": float(plan.metadata.get("signal_strength", 0.0) or 0.0), "regime_code": plan.metadata.get("regime_code"), "baseline": plan.metadata.get("baseline", "strategy"), "horizon_days": int(plan.metadata.get("expected_horizon_days", plan.metadata.get("horizon_days", 5)) or 5)})
            continue
        total_qty = max(1.0, sum(float(f.filled_quantity or 0) for f in matched))
        avg_fill = sum(float(f.average_fill_price or 0.0) * float(f.filled_quantity or 0) for f in matched) / total_qty
        entry_date = min(_event_date(f.event_time) for f in matched)
        horizon_days = int(plan.metadata.get("expected_horizon_days", plan.metadata.get("horizon_days", 5)) or 5)
        fee_bps = max(float((f.metadata or {}).get("fee_bps", 0.0) or 0.0) for f in matched)
        path = _future_bars(plan.symbol, entry_date, bars_by_symbol, horizon_days)
        realized_path_return, exit_date = _realized_return_from_path(side=plan.side.value, entry_price=avg_fill, path=path, fee_bps=fee_bps)
        outcomes.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side.value, "filled": True, "avg_fill_price": avg_fill, "entry_date": entry_date, "exit_date": exit_date, "return_pct": realized_path_return, "realized_path_return_pct": realized_path_return, "score": float(plan.metadata.get("signal_strength", 0.0) or 0.0), "regime_code": plan.metadata.get("regime_code"), "baseline": plan.metadata.get("baseline", "strategy"), "horizon_days": horizon_days})
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


def _price_on_date(bars: list[HistoricalBar], trade_date: str) -> float | None:
    for bar in bars:
        if str(bar.timestamp)[:10] == trade_date:
            return float(bar.close)
    return None


def _baseline_signal(name: str, bars: list[HistoricalBar], idx: int) -> float:
    if name == "momentum_20d":
        if idx < 20:
            return 0.0
        return (float(bars[idx].close) - float(bars[idx - 20].close)) / max(float(bars[idx - 20].close), 1e-12)
    if name == "reversal_5d":
        if idx < 5:
            return 0.0
        return -((float(bars[idx].close) - float(bars[idx - 5].close)) / max(float(bars[idx - 5].close), 1e-12))
    if name == "breakout":
        if idx < 20:
            return 0.0
        look = bars[idx - 20 : idx]
        return 1.0 if float(bars[idx].close) > max(float(b.high) for b in look) else 0.0
    if name == "rsi":
        if idx < 14:
            return 0.0
        diffs = [float(bars[j].close) - float(bars[j - 1].close) for j in range(idx - 13, idx + 1)]
        gains = sum(max(d, 0.0) for d in diffs) / 14.0
        losses = sum(abs(min(d, 0.0)) for d in diffs) / 14.0
        rs = gains / max(losses, 1e-12)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return (50.0 - rsi) / 50.0
    if name == "random_ranking":
        return ((idx * 17) % 100) / 100.0 - 0.5
    return 0.0


def _baseline_metrics(outcomes: list[dict], bars_by_symbol: dict[str, list[HistoricalBar]] | None) -> dict:
    baseline_names = ["momentum_20d", "reversal_5d", "breakout", "rsi", "random_ranking"]
    baselines = {}
    for name in baseline_names:
        vals = []
        for o in outcomes:
            if not o.get("filled") or not bars_by_symbol:
                continue
            bars = bars_by_symbol.get(o["symbol"], [])
            entry_date = o.get("entry_date")
            if not entry_date:
                continue
            idxs = [i for i, b in enumerate(bars) if str(b.timestamp)[:10] == entry_date]
            if not idxs:
                continue
            idx = idxs[0]
            signal = _baseline_signal(name, bars, idx)
            horizon = int(o.get("horizon_days", 5) or 5)
            path = bars[idx + 1 : idx + 1 + horizon]
            if not path:
                continue
            entry = float(path[0].open)
            ret, _ = _realized_return_from_path(side=Side.BUY.value if signal >= 0 else Side.SELL.value, entry_price=entry, path=path, fee_bps=0.0)
            vals.append(ret)
        expectancy = sum(vals) / len(vals) if vals else 0.0
        baselines[name] = {"expectancy_after_cost": expectancy, "count": len(vals)}
    strategy_expect = sum(float(o["return_pct"]) for o in outcomes if o.get("filled")) / max(sum(1 for o in outcomes if o.get("filled")), 1)
    for name in baseline_names:
        baselines[name]["excess_information"] = strategy_expect - baselines[name]["expectancy_after_cost"]
    return baselines


def _regime_breakdown(outcomes: list[dict]) -> list[dict]:
    buckets = {}
    for o in outcomes:
        regime = o.get("regime_code") or "UNKNOWN"
        buckets.setdefault(regime, []).append(float(o.get("return_pct", 0.0)))
    return [{"regime_code": regime, "expectancy_after_cost": sum(vals) / len(vals) if vals else 0.0, "count": len(vals)} for regime, vals in sorted(buckets.items())]


def _overlap_adjusted_sample_size(outcomes: list[dict]) -> float:
    filled = [o for o in outcomes if o.get("filled")]
    n = len(filled)
    if n <= 1:
        return float(n)
    overlap = 0.0
    for i, left in enumerate(filled):
        for right in filled[i + 1 :]:
            if left.get("symbol") != right.get("symbol"):
                continue
            if not left.get("entry_date") or not left.get("exit_date") or not right.get("entry_date") or not right.get("exit_date"):
                continue
            if not (left["exit_date"] < right["entry_date"] or right["exit_date"] < left["entry_date"]):
                overlap += 1.0
    penalty = 1.0 + overlap / max(n, 1)
    return float(n / penalty)


def _psr(expectancy: float, returns: list[float]) -> float:
    n = len(returns)
    if n <= 1:
        return 0.0
    mu = expectancy
    var = sum((r - mu) ** 2 for r in returns) / max(n - 1, 1)
    std = sqrt(max(var, 1e-12))
    z = mu / (std / sqrt(n))
    return 0.5 * (1.0 + erf(z / sqrt(2.0)))


def format_validation_report(metrics: dict) -> str:
    lines = ["strategy | expectancy | max_dd | turnover | coverage"]
    lines.append(f"strategy | {metrics.get('expectancy_after_cost', 0.0):.4f} | {metrics.get('max_drawdown', 0.0):.4f} | {metrics.get('turnover', 0.0):.1f} | {metrics.get('coverage', 0.0):.2f}")
    for name, row in (metrics.get("baseline_comparison") or {}).items():
        lines.append(f"{name} | {float(row.get('expectancy_after_cost', 0.0)):.4f} | - | - | -")
    return "\n".join(lines)


def compute_performance_metrics(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], bars_by_symbol: dict[str, list[HistoricalBar]] | None = None, total_symbols: int | None = None, score_buckets: int = 5, top_k: int = 2) -> dict:
    plans = list(plans)
    fills = list(fills)
    outcomes = plan_outcomes(plans, fills, bars_by_symbol=bars_by_symbol)
    realized = [float(o["return_pct"]) for o in outcomes if o["filled"]]
    expectancy = sum(realized) / len(realized) if realized else 0.0
    turnover = sum(float((f.filled_quantity or 0) * (f.average_fill_price or 0.0)) for f in fills if f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL})
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
    metrics = {
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
        "baseline_comparison": _baseline_metrics(outcomes, bars_by_symbol),
        "regime_breakdown": _regime_breakdown(outcomes),
        "effective_sample_size": _overlap_adjusted_sample_size(outcomes),
    }
    metrics["validation_report"] = format_validation_report(metrics)
    return metrics


def rejection_reasons(metrics: dict) -> list[str]:
    reasons = []
    if float(metrics.get("expectancy_after_cost", 0.0)) <= 0.0:
        reasons.append("non_positive_expectancy")
    if max(float(metrics.get("psr", 0.0)), float(metrics.get("dsr", 0.0))) < 0.55:
        reasons.append("low_psr_or_dsr")
    if not bool(metrics.get("score_decile_monotonicity", False)):
        reasons.append("non_monotonic_score_buckets")
    if float(metrics.get("calibration_error", 1.0)) > 0.25:
        reasons.append("high_calibration_error")
    return reasons


def sensitivity_sweep(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], fee_grid: Iterable[float], slippage_grid: Iterable[float], total_symbols: int | None = None, bars_by_symbol: dict[str, list[HistoricalBar]] | None = None) -> list[SensitivityPoint]:
    base = compute_performance_metrics(plans=plans, fills=fills, bars_by_symbol=bars_by_symbol, total_symbols=total_symbols)
    out: list[SensitivityPoint] = []
    for fee_bps in fee_grid:
        for slippage_bps in slippage_grid:
            penalty = (float(fee_bps) + float(slippage_bps)) / 10000.0
            out.append(SensitivityPoint(fee_bps=float(fee_bps), slippage_bps=float(slippage_bps), expectancy=float(base["expectancy_after_cost"]) - penalty, hit_rate=float(base["hit_rate"]), coverage=float(base["coverage"]), no_trade_ratio=float(base["no_trade_ratio"])))
    return out

from __future__ import annotations

from dataclasses import dataclass
from math import ceil, erf, sqrt
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


def compute_purge_embargo(*, horizon_days: int, holding_overlap: float = 1.0) -> tuple[int, int]:
    horizon = max(1, int(horizon_days))
    purge = max(1, horizon - 1)
    embargo = max(1, int(ceil(horizon * max(0.0, float(holding_overlap)))))
    return purge, embargo


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
        outcomes.append({"plan_id": plan.plan_id, "symbol": plan.symbol, "side": plan.side.value, "filled": True, "avg_fill_price": avg_fill, "entry_date": entry_date, "exit_date": exit_date, "exit_holding_overlap_end": exit_date, "return_pct": realized_path_return, "realized_path_return_pct": realized_path_return, "score": float(plan.metadata.get("signal_strength", 0.0) or 0.0), "regime_code": plan.metadata.get("regime_code"), "baseline": plan.metadata.get("baseline", "strategy"), "horizon_days": horizon_days})
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
    baseline = _baseline_metrics(outcomes, bars_by_symbol)
    metrics = {
        "expectancy": expectancy,
        "expectancy_after_cost": expectancy,
        "realized_path_pnl": sum(realized),
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
        "baseline_comparison": baseline,
        "baseline_excess_information": {name: row.get("excess_information", 0.0) for name, row in baseline.items()},
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
        reasons.extend(["low_psr", "low_psr_or_dsr"])
    if not bool(metrics.get("score_decile_monotonicity", False)):
        reasons.extend(["non_monotonic_bucket", "non_monotonic_score_buckets"])
    if float(metrics.get("calibration_error", 1.0)) > 0.25:
        reasons.extend(["high_ece", "high_calibration_error"])
    return reasons


def _fold_dates(plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], bars_by_symbol: dict[str, list[HistoricalBar]] | None) -> list[str]:
    outcomes = plan_outcomes(plans, fills, bars_by_symbol)
    dates = sorted({str(o.get("entry_date") or "") for o in outcomes if o.get("entry_date")})
    return [d for d in dates if d]


def _subset_by_dates(plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], date_set: set[str]) -> tuple[list[OrderPlan], list[FillOutcome]]:
    kept_plans = [p for p in plans if str((p.metadata or {}).get("anchor_date") or p.generated_at.date().isoformat())[:10] in date_set]
    kept_ids = {p.plan_id for p in kept_plans}
    kept_fills = [f for f in fills if f.plan_id in kept_ids]
    return kept_plans, kept_fills


def run_fold_validation(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], bars_by_symbol: dict[str, list[HistoricalBar]] | None, total_symbols: int, horizon_days: int, mode: str = "walk_forward") -> dict:
    dates = _fold_dates(plans, fills, bars_by_symbol)
    purge, embargo = compute_purge_embargo(horizon_days=horizon_days, holding_overlap=1.0)
    if len(dates) < 3:
        aggregate = compute_performance_metrics(plans=plans, fills=fills, bars_by_symbol=bars_by_symbol, total_symbols=total_symbols)
        return {"mode": mode, "purge": purge, "embargo": embargo, "folds": [], "aggregate": aggregate, "rejection_reasons": rejection_reasons(aggregate), "train_artifacts": [], "test_artifacts": []}
    if mode == "cpcv":
        fold_defs = build_cpcv_folds(n_obs=len(dates), n_folds=min(3, max(1, len(dates) // 2)), test_fold_size=max(1, len(dates) // 3), purge=purge, embargo=embargo)
        normalized = [{"train_dates": [dates[i] for i in f.train_indices], "test_dates": [dates[i] for i in f.test_indices], "purge": f.purge, "embargo": f.embargo} for f in fold_defs]
    else:
        train_size = max(1, len(dates) // 2)
        test_size = max(1, min(horizon_days, max(1, len(dates) - train_size - purge - embargo)))
        fold_defs = build_walk_forward_splits(n_obs=len(dates), train_size=train_size, test_size=test_size, step_size=test_size, purge=purge, embargo=embargo)
        normalized = [{"train_dates": dates[f.train_start:f.train_end], "test_dates": dates[f.test_start:f.test_end], "purge": f.purge, "embargo": f.embargo} for f in fold_defs]
    folds = []
    train_artifacts = []
    test_artifacts = []
    for idx, fold in enumerate(normalized, start=1):
        train_plans, train_fills = _subset_by_dates(plans, fills, set(fold["train_dates"]))
        test_plans, test_fills = _subset_by_dates(plans, fills, set(fold["test_dates"]))
        train_metrics = compute_performance_metrics(plans=train_plans, fills=train_fills, bars_by_symbol=bars_by_symbol, total_symbols=total_symbols)
        test_metrics = compute_performance_metrics(plans=test_plans, fills=test_fills, bars_by_symbol=bars_by_symbol, total_symbols=total_symbols)
        leakage_ok = True
        if train_plans and test_plans:
            train_outcomes = plan_outcomes(train_plans, train_fills, bars_by_symbol)
            test_outcomes = plan_outcomes(test_plans, test_fills, bars_by_symbol)
            max_train_exit = max((o.get("exit_date") or "") for o in train_outcomes if o.get("exit_date")) if train_outcomes else ""
            min_test_entry = min((o.get("entry_date") or "") for o in test_outcomes if o.get("entry_date")) if test_outcomes else ""
            leakage_ok = (not max_train_exit) or (not min_test_entry) or (max_train_exit < min_test_entry)
        fold_row = {"fold_id": f"fold_{idx}", "split": fold, "train_metrics": train_metrics, "test_metrics": test_metrics, "leakage_ok": leakage_ok, "rejection_reasons": rejection_reasons(test_metrics)}
        folds.append(fold_row)
        train_artifacts.append({"fold_id": fold_row["fold_id"], "kind": "train_only", "dates": fold["train_dates"], "metrics": train_metrics})
        test_artifacts.append({"fold_id": fold_row["fold_id"], "kind": "test_only", "dates": fold["test_dates"], "metrics": test_metrics})
    test_metrics_rows = [f["test_metrics"] for f in folds]
    aggregate = {
        "expectancy_after_cost": sum(float(m.get("expectancy_after_cost", 0.0)) for m in test_metrics_rows) / max(len(test_metrics_rows), 1),
        "realized_path_pnl": sum(float(m.get("realized_path_pnl", 0.0)) for m in test_metrics_rows),
        "psr": sum(float(m.get("psr", 0.0)) for m in test_metrics_rows) / max(len(test_metrics_rows), 1),
        "dsr": sum(float(m.get("dsr", 0.0)) for m in test_metrics_rows) / max(len(test_metrics_rows), 1),
        "calibration_error": sum(float(m.get("calibration_error", 0.0)) for m in test_metrics_rows) / max(len(test_metrics_rows), 1),
        "score_decile_monotonicity": all(bool(m.get("score_decile_monotonicity", False)) for m in test_metrics_rows),
        "baseline_excess_information": {name: sum(float(m.get("baseline_excess_information", {}).get(name, 0.0)) for m in test_metrics_rows) / max(len(test_metrics_rows), 1) for name in {k for m in test_metrics_rows for k in (m.get("baseline_excess_information", {}) or {}).keys()}},
        "effective_sample_size": sum(float(m.get("effective_sample_size", 0.0)) for m in test_metrics_rows),
        "regime_breakdown": [item for m in test_metrics_rows for item in (m.get("regime_breakdown") or [])],
        "fold_count": len(folds),
        "all_folds_leakage_ok": all(bool(f.get("leakage_ok", False)) for f in folds),
    }
    aggregate["validation_report"] = format_validation_report(aggregate)
    return {"mode": mode, "purge": purge, "embargo": embargo, "folds": folds, "aggregate": aggregate, "rejection_reasons": rejection_reasons(aggregate), "train_artifacts": train_artifacts, "test_artifacts": test_artifacts}


def sensitivity_sweep(*, plans: Sequence[OrderPlan], fills: Sequence[FillOutcome], fee_grid: Iterable[float], slippage_grid: Iterable[float], total_symbols: int | None = None, bars_by_symbol: dict[str, list[HistoricalBar]] | None = None) -> list[SensitivityPoint]:
    base = compute_performance_metrics(plans=plans, fills=fills, bars_by_symbol=bars_by_symbol, total_symbols=total_symbols)
    out: list[SensitivityPoint] = []
    for fee_bps in fee_grid:
        for slippage_bps in slippage_grid:
            penalty = (float(fee_bps) + float(slippage_bps)) / 10000.0
            out.append(SensitivityPoint(fee_bps=float(fee_bps), slippage_bps=float(slippage_bps), expectancy=float(base["expectancy_after_cost"]) - penalty, hit_rate=float(base["hit_rate"]), coverage=float(base["coverage"]), no_trade_ratio=float(base["no_trade_ratio"])))
    return out

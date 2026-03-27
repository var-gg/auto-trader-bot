from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Mapping, Sequence

from .models import HistoricalBar

FEATURE_VERSION = "multiscale_manual_v2"
DEFAULT_SHAPE_HORIZONS = (1, 3, 5, 10, 20, 60)
CTX_SERIES = ("vix", "rate", "dollar", "oil", "breadth")


@dataclass(frozen=True)
class FeatureVector:
    shape_features: Dict[str, float]
    residual_features: Dict[str, float]
    context_features: Dict[str, float]
    shape_vector: List[float]
    ctx_vector: List[float]
    embedding: List[float]
    metadata: Dict[str, object]


@dataclass(frozen=True)
class FeatureScaler:
    means: Dict[str, float]
    stds: Dict[str, float]

    def transform(self, features: Mapping[str, float]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for key, value in features.items():
            mu = self.means.get(key, 0.0)
            sigma = self.stds.get(key, 1.0)
            out[key] = 0.0 if sigma <= 1e-12 else float((float(value) - mu) / sigma)
        return out


def fit_feature_scaler(rows: Sequence[Mapping[str, float]]) -> FeatureScaler:
    if not rows:
        return FeatureScaler(means={}, stds={})
    keys = sorted({k for row in rows for k in row.keys()})
    means: Dict[str, float] = {}
    stds: Dict[str, float] = {}
    for key in keys:
        values = [float(row.get(key, 0.0) or 0.0) for row in rows]
        means[key] = mean(values)
        stds[key] = max(1e-8, pstdev(values) if len(values) > 1 else 1.0)
    return FeatureScaler(means=means, stds=stds)


def compute_bar_features(bars: Iterable[HistoricalBar]) -> Dict[str, float]:
    bars = list(bars)
    if not bars:
        return {"return_1": 0.0, "range_pct": 0.0, "volume_mean": 0.0}
    first = bars[0]
    last = bars[-1]
    return_1 = (last.close - first.open) / first.open if first.open else 0.0
    highs = max(bar.high for bar in bars)
    lows = min(bar.low for bar in bars)
    closes = last.close or 0.0
    range_pct = ((highs - lows) / closes) if closes else 0.0
    volume_mean = sum(bar.volume for bar in bars) / len(bars)
    return {
        "return_1": float(return_1),
        "range_pct": float(range_pct),
        "volume_mean": float(volume_mean),
    }


def _safe_div(a: float, b: float) -> float:
    return float(a / b) if abs(b) > 1e-12 else 0.0


def _daily_returns(bars: Sequence[HistoricalBar]) -> List[float]:
    out: List[float] = []
    prev_close = None
    for bar in bars:
        if prev_close is not None and prev_close > 0:
            out.append((float(bar.close) / prev_close) - 1.0)
        prev_close = float(bar.close)
    return out


def _window_returns(bars: Sequence[HistoricalBar], horizon: int) -> float:
    if len(bars) < horizon + 1:
        return 0.0
    start = float(bars[-horizon - 1].close)
    end = float(bars[-1].close)
    return _safe_div(end - start, start)


def _realized_vol(returns: Sequence[float]) -> float:
    if len(returns) <= 1:
        return 0.0
    mu = mean(returns)
    var = sum((r - mu) ** 2 for r in returns) / len(returns)
    return sqrt(max(var, 0.0))


def _atr_pct(bars: Sequence[HistoricalBar], period: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    trs: List[float] = []
    prev_close = float(bars[0].close)
    for bar in bars[-period:]:
        high = float(bar.high)
        low = float(bar.low)
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = float(bar.close)
    close = float(bars[-1].close)
    return _safe_div(mean(trs) if trs else 0.0, close)


def _drawdown(bars: Sequence[HistoricalBar], period: int = 20) -> float:
    closes = [float(b.close) for b in bars[-period:]]
    if not closes:
        return 0.0
    peak = closes[0]
    dd = 0.0
    for close in closes:
        peak = max(peak, close)
        dd = min(dd, _safe_div(close - peak, peak))
    return dd


def _last_candle_features(bars: Sequence[HistoricalBar]) -> Dict[str, float]:
    if not bars:
        return {"gap_pct": 0.0, "close_location": 0.0, "body_pct": 0.0, "upper_wick_pct": 0.0, "lower_wick_pct": 0.0}
    bar = bars[-1]
    prev_close = float(bars[-2].close) if len(bars) > 1 else float(bar.open)
    high = float(bar.high)
    low = float(bar.low)
    open_ = float(bar.open)
    close = float(bar.close)
    rng = max(high - low, 1e-8)
    body = abs(close - open_)
    upper = high - max(open_, close)
    lower = min(open_, close) - low
    return {
        "gap_pct": _safe_div(open_ - prev_close, prev_close),
        "close_location": _safe_div(close - low, rng),
        "body_pct": _safe_div(body, rng),
        "upper_wick_pct": _safe_div(upper, rng),
        "lower_wick_pct": _safe_div(lower, rng),
    }


def _liquidity_features(bars: Sequence[HistoricalBar]) -> Dict[str, float]:
    if not bars:
        return {"relative_volume": 0.0, "adv_percentile": 0.0, "dollar_volume": 0.0}
    volumes = [float(b.volume or 0.0) for b in bars]
    dollar_volumes = [float(b.close) * float(b.volume or 0.0) for b in bars]
    current_volume = volumes[-1]
    trailing = volumes[:-1] or volumes
    current_dv = dollar_volumes[-1]
    dv_trailing = dollar_volumes[:-1] or dollar_volumes
    rank = sum(1 for value in dv_trailing if value <= current_dv)
    return {
        "relative_volume": _safe_div(current_volume, mean(trailing) if trailing else 0.0),
        "adv_percentile": _safe_div(rank, len(dv_trailing)),
        "dollar_volume": current_dv,
    }


def _context_series_features(series_name: str, history: Mapping[str, float]) -> Dict[str, float]:
    ordered = [float(v) for _k, v in sorted(history.items()) if v is not None]
    if not ordered:
        return {f"{series_name}_level": 0.0, f"{series_name}_change": 0.0, f"{series_name}_zscore": 0.0}
    level = ordered[-1]
    change = level - ordered[-2] if len(ordered) > 1 else 0.0
    std = pstdev(ordered) if len(ordered) > 1 else 0.0
    zscore = _safe_div(level - mean(ordered), std) if std > 1e-12 else 0.0
    return {f"{series_name}_level": level, f"{series_name}_change": change, f"{series_name}_zscore": zscore}


def _build_context_features(macro_history: Mapping[str, Mapping[str, float]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for name in CTX_SERIES:
        series_hist = {date: payload.get(name, 0.0) for date, payload in macro_history.items() if name in payload}
        out.update(_context_series_features(name, series_hist))
    return out


def compute_external_vector(payload: Dict[str, float]) -> List[float]:
    ordered_keys = sorted(payload.keys())
    return [float(payload[k]) for k in ordered_keys]


def build_multiscale_feature_vector(
    *,
    symbol: str,
    bars: Sequence[HistoricalBar],
    market_bars: Sequence[HistoricalBar] | None,
    sector_bars: Sequence[HistoricalBar] | None,
    macro_history: Mapping[str, Mapping[str, float]] | None,
    sector_code: str | None,
    scaler: FeatureScaler | None = None,
    shape_horizons: Sequence[int] | None = None,
) -> FeatureVector:
    bars = list(bars)
    market_bars = list(market_bars or [])
    sector_bars = list(sector_bars or [])
    macro_history = macro_history or {}
    own_rets = _daily_returns(bars)
    mkt_rets = _daily_returns(market_bars) if market_bars else []
    sec_rets = _daily_returns(sector_bars) if sector_bars else []
    resolved_shape_horizons = tuple(sorted({int(h) for h in (shape_horizons or DEFAULT_SHAPE_HORIZONS) if int(h) > 0})) or DEFAULT_SHAPE_HORIZONS

    shape: Dict[str, float] = {}
    for horizon in resolved_shape_horizons:
        shape[f"ret_{horizon}"] = _window_returns(bars, horizon)
    shape["realized_vol_20"] = _realized_vol(own_rets[-20:])
    shape["atr_pct_14"] = _atr_pct(bars, 14)
    shape["drawdown_20"] = _drawdown(bars, 20)
    shape.update(_last_candle_features(bars))
    shape.update(_liquidity_features(bars))

    residual: Dict[str, float] = {}
    for horizon in (1, 5, 20):
        own = shape.get(f"ret_{horizon}", 0.0)
        mkt = _window_returns(market_bars, horizon) if market_bars else 0.0
        sec = _window_returns(sector_bars, horizon) if sector_bars else mkt
        residual[f"mkt_rel_ret_{horizon}"] = own - mkt
        residual[f"sector_rel_ret_{horizon}"] = own - sec
    beta_num = sum(a * b for a, b in zip(own_rets[-20:], mkt_rets[-20:])) if mkt_rets else 0.0
    beta_den = sum(b * b for b in mkt_rets[-20:]) if mkt_rets else 0.0
    beta = _safe_div(beta_num, beta_den)
    residual["beta_20"] = beta
    residual["beta_residual_20"] = (own_rets[-1] if own_rets else 0.0) - beta * (mkt_rets[-1] if mkt_rets else 0.0)
    residual["vol_normalized_residual_20"] = _safe_div(residual["beta_residual_20"], shape["realized_vol_20"])

    context = _build_context_features(macro_history)
    raw_shape = {**shape, **residual}
    shape_scaled = scaler.transform(raw_shape) if scaler else raw_shape
    ctx_scaled = scaler.transform(context) if scaler else context
    shape_keys = sorted(shape_scaled.keys())
    ctx_keys = sorted(ctx_scaled.keys())
    shape_vector = [float(shape_scaled[k]) for k in shape_keys]
    ctx_vector = [float(ctx_scaled[k]) for k in ctx_keys]
    return FeatureVector(
        shape_features=shape_scaled,
        residual_features={k: shape_scaled[k] for k in sorted(residual.keys()) if k in shape_scaled},
        context_features=ctx_scaled,
        shape_vector=shape_vector,
        ctx_vector=ctx_vector,
        embedding=shape_vector + ctx_vector,
        metadata={
            "symbol": symbol,
            "sector_code": sector_code,
            "feature_version": FEATURE_VERSION,
            "vector_version": "research_similarity_v2_multiscale",
            "shape_dim": len(shape_vector),
            "ctx_dim": len(ctx_vector),
            "embedding_dim": len(shape_vector) + len(ctx_vector),
            "shape_keys": shape_keys,
            "ctx_keys": ctx_keys,
            "shape_horizons": list(resolved_shape_horizons),
        },
    )

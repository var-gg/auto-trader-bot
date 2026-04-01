from __future__ import annotations

from dataclasses import dataclass
from math import log10, sqrt
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Mapping, Sequence

from .models import HistoricalBar

FEATURE_VERSION = "multiscale_manual_v2"
FEATURE_TRANSFORM_VERSION = "feature_contract_v1"
DEFAULT_SHAPE_HORIZONS = (1, 3, 5, 10, 20, 60)
CTX_SERIES = ("vix", "rate", "dollar", "oil", "breadth")
SIMILARITY_CTX_SERIES = tuple(series for series in CTX_SERIES if series != "breadth")
DEFAULT_CONTEXT_LOOKBACKS = (5, 20)
REGIME_CONTEXT_PRIORITY_SUFFIXES = ("zscore_20", "pct_change_20", "pct_change_5", "slope_5", "percentile_20")


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


@dataclass(frozen=True)
class FeatureTransform:
    scaler: FeatureScaler
    feature_keys: List[str]
    version: str = FEATURE_TRANSFORM_VERSION

    def transform_raw_features(self, raw_features: Mapping[str, float]) -> Dict[str, float]:
        ordered_raw = {key: float(raw_features.get(key, 0.0) or 0.0) for key in self.feature_keys}
        return self.scaler.transform(ordered_raw)

    def embedding_from_transformed(self, transformed_features: Mapping[str, float]) -> List[float]:
        return [float(transformed_features.get(key, 0.0) or 0.0) for key in self.feature_keys]

    def apply(self, raw_features: Mapping[str, float]) -> tuple[Dict[str, float], List[float]]:
        transformed = self.transform_raw_features(raw_features)
        return transformed, self.embedding_from_transformed(transformed)


@dataclass(frozen=True)
class FeatureVector:
    raw_shape_features: Dict[str, float]
    raw_residual_features: Dict[str, float]
    raw_context_features: Dict[str, float]
    raw_regime_context_features: Dict[str, float]
    normalized_regime_context_features: Dict[str, float]
    shape_features: Dict[str, float]
    residual_features: Dict[str, float]
    context_features: Dict[str, float]
    regime_context_features: Dict[str, float]
    raw_features: Dict[str, float]
    transformed_features: Dict[str, float]
    shape_vector: List[float]
    ctx_vector: List[float]
    embedding: List[float]
    transform: FeatureTransform
    metadata: Dict[str, object]


@dataclass(frozen=True)
class RawFeaturePayload:
    shape_features: Dict[str, float]
    residual_features: Dict[str, float]
    context_features: Dict[str, float]
    regime_context_features: Dict[str, float]
    normalized_regime_context_features: Dict[str, float]
    raw_features: Dict[str, float]
    metadata: Dict[str, object]


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


def fit_feature_transform(rows: Sequence[Mapping[str, float]], *, version: str = FEATURE_TRANSFORM_VERSION) -> FeatureTransform:
    scaler = fit_feature_scaler(rows)
    feature_keys = sorted({k for row in rows for k in row.keys()})
    return FeatureTransform(scaler=scaler, feature_keys=feature_keys, version=version)


def identity_feature_transform(raw_features: Mapping[str, float], *, version: str = FEATURE_TRANSFORM_VERSION) -> FeatureTransform:
    keys = sorted(raw_features.keys())
    scaler = FeatureScaler(means={k: 0.0 for k in keys}, stds={k: 1.0 for k in keys})
    return FeatureTransform(scaler=scaler, feature_keys=keys, version=version)


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


def _liquidity_features(bars: Sequence[HistoricalBar], *, use_dollar_volume_absolute: bool = False) -> Dict[str, float]:
    if not bars:
        out = {"relative_volume": 0.0, "adv_percentile": 0.0, "log_dollar_volume": 0.0, "dollar_volume_percentile": 0.0}
        if use_dollar_volume_absolute:
            out["dollar_volume"] = 0.0
        return out
    volumes = [float(b.volume or 0.0) for b in bars]
    dollar_volumes = [float(b.close) * float(b.volume or 0.0) for b in bars]
    current_volume = volumes[-1]
    trailing = volumes[:-1] or volumes
    current_dv = dollar_volumes[-1]
    dv_trailing = dollar_volumes[:-1] or dollar_volumes
    rank = sum(1 for value in dv_trailing if value <= current_dv)
    out = {
        "relative_volume": _safe_div(current_volume, mean(trailing) if trailing else 0.0),
        "adv_percentile": _safe_div(rank, len(dv_trailing)),
        "log_dollar_volume": log10(max(current_dv, 1.0)),
        "dollar_volume_percentile": _safe_div(rank, len(dv_trailing)),
    }
    if use_dollar_volume_absolute:
        out["dollar_volume"] = current_dv
    return out


def _series_tail(history: Mapping[str, float]) -> List[float]:
    return [float(v) for _k, v in sorted(history.items()) if v is not None]


def _trailing_zscore(ordered: Sequence[float], lookback: int) -> float:
    window = list(ordered[-lookback:]) if lookback > 0 else list(ordered)
    if not window:
        return 0.0
    level = window[-1]
    std = pstdev(window) if len(window) > 1 else 0.0
    return _safe_div(level - mean(window), std) if std > 1e-12 else 0.0


def _change_over_window(ordered: Sequence[float], lookback: int) -> float:
    if len(ordered) <= lookback:
        return 0.0
    return ordered[-1] - ordered[-lookback - 1]


def _pct_change_over_window(ordered: Sequence[float], lookback: int) -> float:
    if len(ordered) <= lookback:
        return 0.0
    base = ordered[-lookback - 1]
    return _safe_div(ordered[-1] - base, base)


def _percentile_rank(ordered: Sequence[float]) -> float:
    if not ordered:
        return 0.0
    current = ordered[-1]
    return _safe_div(sum(1 for value in ordered if value <= current), len(ordered))


def _slope(ordered: Sequence[float], lookback: int) -> float:
    window = list(ordered[-lookback:]) if lookback > 0 else list(ordered)
    if len(window) <= 1:
        return 0.0
    return _safe_div(window[-1] - window[0], len(window) - 1)


def _context_series_features(series_name: str, history: Mapping[str, float], *, use_macro_level_in_similarity: bool = False) -> tuple[Dict[str, float], Dict[str, float]]:
    ordered = _series_tail(history)
    if not ordered:
        similarity = {
            f"{series_name}_zscore_20": 0.0,
            f"{series_name}_change_5": 0.0,
            f"{series_name}_change_20": 0.0,
            f"{series_name}_pct_change_5": 0.0,
            f"{series_name}_pct_change_20": 0.0,
            f"{series_name}_percentile_20": 0.0,
            f"{series_name}_slope_5": 0.0,
        }
        if use_macro_level_in_similarity:
            similarity[f"{series_name}_level"] = 0.0
        regime = {f"{series_name}_level": 0.0, f"{series_name}_change": 0.0, f"{series_name}_zscore": 0.0}
        return similarity, regime
    level = ordered[-1]
    change = ordered[-1] - ordered[-2] if len(ordered) > 1 else 0.0
    std = pstdev(ordered) if len(ordered) > 1 else 0.0
    zscore = _safe_div(level - mean(ordered), std) if std > 1e-12 else 0.0
    similarity = {
        f"{series_name}_zscore_20": _trailing_zscore(ordered, 20),
        f"{series_name}_change_5": _change_over_window(ordered, 5),
        f"{series_name}_change_20": _change_over_window(ordered, 20),
        f"{series_name}_pct_change_5": _pct_change_over_window(ordered, 5),
        f"{series_name}_pct_change_20": _pct_change_over_window(ordered, 20),
        f"{series_name}_percentile_20": _percentile_rank(ordered[-20:]),
        f"{series_name}_slope_5": _slope(ordered, 5),
    }
    if use_macro_level_in_similarity:
        similarity[f"{series_name}_level"] = level
    regime = {f"{series_name}_level": level, f"{series_name}_change": change, f"{series_name}_zscore": zscore}
    return similarity, regime


def _build_context_features(macro_history: Mapping[str, Mapping[str, float]], *, use_macro_level_in_similarity: bool = False) -> tuple[Dict[str, float], Dict[str, float]]:
    if not macro_history:
        return {}, {}
    similarity_out: Dict[str, float] = {}
    regime_out: Dict[str, float] = {}
    for name in SIMILARITY_CTX_SERIES:
        series_hist = {date: payload.get(name, 0.0) for date, payload in macro_history.items() if name in payload}
        if not series_hist:
            continue
        similarity_features, regime_features = _context_series_features(name, series_hist, use_macro_level_in_similarity=use_macro_level_in_similarity)
        similarity_out.update(similarity_features)
        regime_out.update(regime_features)
    return similarity_out, regime_out


def _normalized_regime_context_features(context_features: Mapping[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for series_name in CTX_SERIES:
        for suffix in REGIME_CONTEXT_PRIORITY_SUFFIXES:
            key = f"{series_name}_{suffix}"
            if key in context_features:
                out[key] = float(context_features[key])
                break
    return out


def _trim_proxy_diagnostics(
    proxy_diagnostics: Mapping[str, Mapping[str, object]] | None,
    bars: Sequence[HistoricalBar],
) -> Dict[str, object]:
    if not proxy_diagnostics:
        return {}
    valid_dates = {str(bar.timestamp)[:10] for bar in bars}
    out: Dict[str, object] = {}
    for scope, payload in dict(proxy_diagnostics).items():
        payload_dict = dict(payload or {})
        peer_count_by_date = {
            str(date_key): int(count)
            for date_key, count in dict(payload_dict.get("peer_count_by_date") or {}).items()
            if str(date_key) in valid_dates
        }
        contributing_symbols_by_date = {
            str(date_key): [str(symbol) for symbol in list(symbols or [])]
            for date_key, symbols in dict(payload_dict.get("contributing_symbols_by_date") or {}).items()
            if str(date_key) in valid_dates
        }
        out[str(scope)] = {
            "peer_count_by_date": peer_count_by_date,
            "contributing_symbols_by_date": contributing_symbols_by_date,
            "fallback_to_self": bool(payload_dict.get("fallback_to_self", False)),
        }
    return out


def compute_external_vector(payload: Dict[str, float]) -> List[float]:
    ordered_keys = sorted(payload.keys())
    return [float(payload[k]) for k in ordered_keys]


def build_raw_multiscale_feature_payload(
    *,
    symbol: str,
    bars: Sequence[HistoricalBar],
    market_bars: Sequence[HistoricalBar] | None,
    sector_bars: Sequence[HistoricalBar] | None,
    macro_history: Mapping[str, Mapping[str, float]] | None,
    sector_code: str | None,
    shape_horizons: Sequence[int] | None = None,
    use_macro_level_in_similarity: bool = False,
    use_dollar_volume_absolute: bool = False,
    proxy_diagnostics: Mapping[str, Mapping[str, object]] | None = None,
    macro_freshness_features: Mapping[str, float] | None = None,
    additional_metadata: Mapping[str, object] | None = None,
) -> RawFeaturePayload:
    bars = list(bars)
    market_bars = list(market_bars or [])
    sector_bars = list(sector_bars or [])
    macro_history = macro_history or {}
    own_rets = _daily_returns(bars)
    mkt_rets = _daily_returns(market_bars) if market_bars else []
    resolved_shape_horizons = tuple(sorted({int(h) for h in (shape_horizons or DEFAULT_SHAPE_HORIZONS) if int(h) > 0})) or DEFAULT_SHAPE_HORIZONS

    shape: Dict[str, float] = {}
    for horizon in resolved_shape_horizons:
        shape[f"ret_{horizon}"] = _window_returns(bars, horizon)
    shape["realized_vol_20"] = _realized_vol(own_rets[-20:])
    shape["atr_pct_14"] = _atr_pct(bars, 14)
    shape["drawdown_20"] = _drawdown(bars, 20)
    shape.update(_last_candle_features(bars))
    shape.update(_liquidity_features(bars, use_dollar_volume_absolute=use_dollar_volume_absolute))

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

    context, regime_context = _build_context_features(macro_history, use_macro_level_in_similarity=use_macro_level_in_similarity)
    freshness_features = {str(key): float(value or 0.0) for key, value in dict(macro_freshness_features or {}).items()}
    context.update(freshness_features)
    normalized_regime_context = _normalized_regime_context_features(context)
    raw_features = {**shape, **residual, **context}
    raw_zero_default_keys = sorted(key for key, value in raw_features.items() if abs(float(value)) <= 1e-12)
    return RawFeaturePayload(
        shape_features=shape,
        residual_features=residual,
        context_features=context,
        regime_context_features=regime_context,
        normalized_regime_context_features=normalized_regime_context,
        raw_features=raw_features,
        metadata={
            "symbol": symbol,
            "sector_code": sector_code,
            "feature_version": FEATURE_VERSION,
            "vector_version": "research_similarity_v2_multiscale",
            "shape_horizons": list(resolved_shape_horizons),
            "use_macro_level_in_similarity": use_macro_level_in_similarity,
            "use_dollar_volume_absolute": use_dollar_volume_absolute,
            "proxy_diagnostics": _trim_proxy_diagnostics(proxy_diagnostics, bars),
            "raw_zero_default_keys": raw_zero_default_keys,
            "macro_freshness_feature_keys": sorted(freshness_features.keys()),
            **dict(additional_metadata or {}),
        },
    )


def build_multiscale_feature_vector(
    *,
    symbol: str,
    bars: Sequence[HistoricalBar],
    market_bars: Sequence[HistoricalBar] | None,
    sector_bars: Sequence[HistoricalBar] | None,
    macro_history: Mapping[str, Mapping[str, float]] | None,
    sector_code: str | None,
    scaler: FeatureScaler | None = None,
    transform: FeatureTransform | None = None,
    shape_horizons: Sequence[int] | None = None,
    use_macro_level_in_similarity: bool = False,
    use_dollar_volume_absolute: bool = False,
    proxy_diagnostics: Mapping[str, Mapping[str, object]] | None = None,
    macro_freshness_features: Mapping[str, float] | None = None,
    additional_metadata: Mapping[str, object] | None = None,
) -> FeatureVector:
    raw_payload = build_raw_multiscale_feature_payload(
        symbol=symbol,
        bars=bars,
        market_bars=market_bars,
        sector_bars=sector_bars,
        macro_history=macro_history,
        sector_code=sector_code,
        shape_horizons=shape_horizons,
        use_macro_level_in_similarity=use_macro_level_in_similarity,
        use_dollar_volume_absolute=use_dollar_volume_absolute,
        proxy_diagnostics=proxy_diagnostics,
        macro_freshness_features=macro_freshness_features,
        additional_metadata=additional_metadata,
    )
    resolved_transform = transform
    if resolved_transform is None and scaler is not None:
        resolved_transform = FeatureTransform(scaler=scaler, feature_keys=sorted(raw_payload.raw_features.keys()))
    if resolved_transform is None:
        resolved_transform = identity_feature_transform(raw_payload.raw_features)

    transformed_features, embedding = resolved_transform.apply(raw_payload.raw_features)
    transform_missing_keys_filled_zero = sorted(key for key in resolved_transform.feature_keys if key not in raw_payload.raw_features)
    transformed_zero_feature_keys = sorted(key for key, value in transformed_features.items() if abs(float(value)) <= 1e-12)
    shape_keys = sorted([k for k in raw_payload.shape_features.keys() if k in transformed_features])
    residual_keys = sorted([k for k in raw_payload.residual_features.keys() if k in transformed_features])
    ctx_keys = sorted([k for k in raw_payload.context_features.keys() if k in transformed_features])
    shape_vector = [float(transformed_features[k]) for k in shape_keys + residual_keys]
    ctx_vector = [float(transformed_features[k]) for k in ctx_keys]
    return FeatureVector(
        raw_shape_features={k: float(raw_payload.shape_features[k]) for k in sorted(raw_payload.shape_features.keys())},
        raw_residual_features={k: float(raw_payload.residual_features[k]) for k in sorted(raw_payload.residual_features.keys())},
        raw_context_features={k: float(raw_payload.context_features[k]) for k in sorted(raw_payload.context_features.keys())},
        raw_regime_context_features={k: float(raw_payload.regime_context_features[k]) for k in sorted(raw_payload.regime_context_features.keys())},
        normalized_regime_context_features={k: float(raw_payload.normalized_regime_context_features[k]) for k in sorted(raw_payload.normalized_regime_context_features.keys())},
        shape_features={k: float(transformed_features[k]) for k in shape_keys},
        residual_features={k: float(transformed_features[k]) for k in residual_keys},
        context_features={k: float(transformed_features[k]) for k in ctx_keys},
        regime_context_features={k: float(raw_payload.regime_context_features[k]) for k in sorted(raw_payload.regime_context_features.keys())},
        raw_features={k: float(raw_payload.raw_features[k]) for k in sorted(raw_payload.raw_features.keys())},
        transformed_features={k: float(transformed_features[k]) for k in resolved_transform.feature_keys},
        shape_vector=shape_vector,
        ctx_vector=ctx_vector,
        embedding=embedding,
        transform=resolved_transform,
        metadata={
            **raw_payload.metadata,
            "transform_version": resolved_transform.version,
            "shape_dim": len(shape_vector),
            "ctx_dim": len(ctx_vector),
            "embedding_dim": len(embedding),
            "shape_keys": shape_keys + residual_keys,
            "ctx_keys": ctx_keys,
            "regime_ctx_keys": sorted(raw_payload.regime_context_features.keys()),
            "normalized_regime_ctx_keys": sorted(raw_payload.normalized_regime_context_features.keys()),
            "feature_keys": list(resolved_transform.feature_keys),
            "proxy_diagnostics": raw_payload.metadata.get("proxy_diagnostics", {}),
            "raw_zero_default_keys": list(raw_payload.metadata.get("raw_zero_default_keys", [])),
            "transform_missing_keys_filled_zero": transform_missing_keys_filled_zero,
            "transformed_zero_feature_keys": transformed_zero_feature_keys,
        },
    )

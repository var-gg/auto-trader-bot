from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from types import SimpleNamespace
from statistics import mean
from time import perf_counter
from typing import Any, Dict, List, Tuple

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.features import (
    CTX_SERIES,
    REGIME_CONTEXT_PRIORITY_SUFFIXES,
    SIMILARITY_CTX_SERIES,
    build_multiscale_feature_vector,
    build_raw_multiscale_feature_payload,
    compute_bar_features,
    fit_feature_scaler,
    fit_feature_transform,
)
from backtest_app.historical_data.models import HistoricalBar, SymbolSessionMetadata
from backtest_app.historical_data.session_alignment import derive_session_anchor_for_bar, derive_session_anchor_from_date, session_anchor_timestamp_utc, session_metadata_to_dict
from backtest_app.portfolio import PortfolioConfig, PortfolioState, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate

from .artifacts import JsonResearchArtifactStore
from .labeling import EventLabelingConfig, build_event_outcome_record, label_event_window
from .models import EventOutcomeRecord, ResearchAnchor
from .prototype import PrototypeConfig, build_state_prototypes_from_event_memory
from .repository import ExactCosineCandidateIndex, load_prototypes_asof
from .scoring import CalibrationModel, CandidateScore, EVConfig, ScoringConfig, build_decision_surface, estimate_expected_value, score_candidates_exact

DECISION_CONVENTION = "EOD_T_SIGNAL__T1_OPEN_EXECUTION"
REGIME_THRESHOLD = 0.2
REGIME_SOURCE_NORMALIZED = "normalized_regime_context"
REGIME_SOURCE_RAW = "raw_macro"
BREADTH_POLICY_DIAGNOSTICS_ONLY_V1 = "diagnostics_only_v1"
BREADTH_MISSING_REASON_CANONICAL_SOURCE_MISSING = "canonical_source_missing"
SIMILARITY_DISABLED_SERIES = {"breadth"}
STALE_DAYS_THRESHOLD = 3
STALE_BARS_THRESHOLD = 3


@dataclass(frozen=True)
class ProxySeriesResult:
    bars: List[HistoricalBar]
    peer_count_by_date: Dict[str, int]
    contributing_symbols_by_date: Dict[str, List[str]]
    fallback_to_self: bool = False
    proxy_mode: str = "date_aligned"
    same_exchange_peer_count: int = 0
    cross_exchange_proxy_used: bool = False


def _feature_flag(name: str, metadata: dict | None, default: bool = False) -> bool:
    meta = metadata or {}
    value = meta.get(name, default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"} if isinstance(value, str) else bool(value)


def _ev_config_from_metadata(metadata: dict | None = None, *, top_k: int = 3, abstain_margin: float | None = None) -> EVConfig:
    meta = metadata or {}
    resolved_abstain_margin = abstain_margin if abstain_margin is not None else meta.get("abstain_margin", 0.05)
    resolved_top_k = int(meta.get("top_k", top_k) or top_k)
    resolved_min_ess = float(meta.get("diagnostic_min_effective_sample_size_override", meta.get("quote_min_effective_sample_size", meta.get("min_effective_sample_size", 1.5))) or 1.5)
    return EVConfig(
        top_k=resolved_top_k,
        kernel_temperature=float(meta.get("kernel_temperature", 12.0) or 12.0),
        min_effective_sample_size=resolved_min_ess,
        max_uncertainty=float(meta.get("quote_uncertainty_cap", meta.get("max_uncertainty", 0.08)) or 0.08),
        min_expected_utility=float(meta.get("quote_ev_threshold", meta.get("min_expected_utility", 0.005)) or 0.005),
        min_regime_alignment=float(meta.get("quote_min_regime_alignment", meta.get("min_regime_alignment", 0.5)) or 0.5),
        use_kernel_weighting=str(meta.get("use_kernel_weighting", "true")).strip().lower() in {"1", "true", "yes", "on"},
        max_return_interval_width=float(meta.get("quote_max_return_interval_width", meta.get("max_return_interval_width", 0.08)) or 0.08),
        abstain_margin=float(resolved_abstain_margin or 0.0),
        diagnostic_disable_lower_bound_gate=str(meta.get("diagnostic_disable_lower_bound_gate", meta.get("disable_lower_bound_gate", "false"))).strip().lower() in {"1", "true", "yes", "on"},
        diagnostic_disable_ess_gate=str(meta.get("diagnostic_disable_ess_gate", "false")).strip().lower() in {"1", "true", "yes", "on"},
        diagnostic_lower_bound_formula=str(meta.get("diagnostic_lower_bound_formula", "lb_v1") or "lb_v1"),
        diagnostic_feasible_side_chooser=str(meta.get("diagnostic_feasible_side_chooser", "false")).strip().lower() in {"1", "true", "yes", "on"},
    )


def _default_spec(feature_window_bars: int = 60, horizon_days: int = 5) -> ResearchExperimentSpec:
    return ResearchExperimentSpec(feature_window_bars=feature_window_bars, horizon_days=horizon_days, lookback_horizons=[horizon_days])


def _similarity_macro_series() -> tuple[str, ...]:
    return tuple(SIMILARITY_CTX_SERIES)


def _count_present_similarity_macro_series(latest_macro_by_series: Dict[str, Dict[str, Any]]) -> int:
    return sum(1 for series_name in _similarity_macro_series() if latest_macro_by_series.get(series_name))


def _breadth_diagnostics_payload(latest_macro_by_series: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    breadth_present = bool(latest_macro_by_series.get("breadth"))
    return {
        "breadth_policy": BREADTH_POLICY_DIAGNOSTICS_ONLY_V1,
        "breadth_present": breadth_present,
        "breadth_missing_reason": None if breadth_present else BREADTH_MISSING_REASON_CANONICAL_SOURCE_MISSING,
    }


def _stale_similarity_macro(macro_freshness_summary: Dict[str, Dict[str, Any]] | None) -> bool:
    summary = macro_freshness_summary or {}
    return any(bool((summary.get(series_name) or {}).get("is_stale_flag", False)) for series_name in _similarity_macro_series())


def _regime_from_macro_raw(macro_payload: Dict[str, float]) -> str:
    if not macro_payload:
        return "NEUTRAL"
    avg = mean(float(v) for v in macro_payload.values())
    if avg >= 0.1:
        return "RISK_ON"
    if avg <= -0.1:
        return "RISK_OFF"
    return "NEUTRAL"


def _regime_from_macro(macro_payload: Dict[str, float]) -> str:
    # Deprecated compatibility wrapper. New runtime paths should prefer
    # _regime_from_context_features(normalized_regime_context_features).
    return _regime_from_macro_raw(macro_payload)


def _resolve_session_metadata(
    symbol: str,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> SymbolSessionMetadata | None:
    if not session_metadata_by_symbol:
        return None
    value = session_metadata_by_symbol.get(symbol)
    if value is None:
        return None
    if isinstance(value, SymbolSessionMetadata):
        return value
    if isinstance(value, dict):
        if not value.get("exchange_code") or not value.get("exchange_tz") or not value.get("session_close_local_time"):
            return None
        return SymbolSessionMetadata(
            symbol=str(value.get("symbol") or symbol),
            exchange_code=str(value.get("exchange_code")),
            country_code=(str(value.get("country_code")) if value.get("country_code") is not None else None),
            exchange_tz=str(value.get("exchange_tz")),
            session_close_local_time=str(value.get("session_close_local_time")),
        )
    return None


def _anchor_fields_for_symbol_date(
    *,
    symbol: str,
    session_date_local: str,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> Dict[str, Any]:
    if session_metadata_by_symbol is None:
        return {
            "exchange_code": None,
            "country_code": None,
            "exchange_tz": None,
            "session_date_local": session_date_local,
            "session_close_ts_local": None,
            "session_close_ts_utc": None,
            "feature_anchor_ts_utc": None,
            "missingness_family": None,
            "anchor_missing_reason": None,
        }
    session_metadata = _resolve_session_metadata(symbol, session_metadata_by_symbol)
    if session_metadata is None:
        return {
            "exchange_code": None,
            "country_code": None,
            "exchange_tz": None,
            "session_date_local": session_date_local,
            "session_close_ts_local": None,
            "session_close_ts_utc": None,
            "feature_anchor_ts_utc": None,
            "missingness_family": "data_quality_missing",
            "anchor_missing_reason": "unknown_exchange_session",
        }
    anchor = derive_session_anchor_from_date(
        session_date_local=session_date_local,
        session_metadata=session_metadata,
    )
    return {
        **anchor,
        "missingness_family": None,
        "anchor_missing_reason": None,
    }


def _bar_anchor_ts_utc(
    *,
    symbol: str,
    bar: HistoricalBar,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> datetime | None:
    session_metadata = _resolve_session_metadata(symbol, session_metadata_by_symbol)
    if session_metadata is None:
        return None
    return session_anchor_timestamp_utc(
        session_date_local=str(bar.timestamp)[:10],
        session_metadata=session_metadata,
    )


def _same_exchange_symbols(
    *,
    focus_symbol: str,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> Dict[str, List[HistoricalBar]]:
    if not session_metadata_by_symbol:
        return dict(bars_by_symbol)
    focus_meta = _resolve_session_metadata(focus_symbol, session_metadata_by_symbol)
    if focus_meta is None:
        return dict(bars_by_symbol)
    return {
        symbol: list(bars)
        for symbol, bars in bars_by_symbol.items()
        if (_resolve_session_metadata(symbol, session_metadata_by_symbol) or focus_meta).exchange_code == focus_meta.exchange_code
    }


def _derived_macro_rows_from_history(
    macro_history_by_date: Dict[str, Dict[str, float]] | None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for obs_date, payload in sorted((macro_history_by_date or {}).items()):
        for series_name, value in sorted((payload or {}).items()):
            if value is None:
                continue
            source_ts_utc = None
            if series_name != "breadth":
                source_ts_utc = datetime.fromisoformat(f"{str(obs_date)[:10]}T20:00:00+00:00").isoformat()
            out.append(
                {
                    "obs_date": str(obs_date)[:10],
                    "name": str(series_name),
                    "value": float(value),
                    "source_ts_utc": source_ts_utc,
                    "source_ts_is_derived": source_ts_utc is not None,
                }
            )
    return out


def _macro_observation_rows(
    *,
    macro_history_by_date: Dict[str, Dict[str, float]] | None,
    macro_series_history: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in list(macro_series_history or [])]
    if rows:
        rows.sort(key=lambda row: (str(row.get("source_ts_utc") or ""), str(row.get("obs_date") or ""), str(row.get("name") or "")))
        return rows
    return _derived_macro_rows_from_history(macro_history_by_date)


def _macro_history_until_anchor(
    *,
    macro_history_by_date: Dict[str, Dict[str, float]],
    macro_series_history: List[Dict[str, Any]] | None,
    feature_anchor_ts_utc: str | None,
    fallback_cutoff_date: str | None = None,
) -> tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, Any]], str | None]:
    rows = _macro_observation_rows(
        macro_history_by_date=macro_history_by_date,
        macro_series_history=macro_series_history,
    )
    if not feature_anchor_ts_utc:
        fallback_history = _macro_history_until(macro_history_by_date, fallback_cutoff_date or max(macro_history_by_date.keys(), default=""))
        latest_payload = _latest_macro_payload(fallback_history)
        latest_by_series = {
            series_name: {
                "name": series_name,
                "value": float(value),
                "source_ts_utc": None,
                "source_ts_is_derived": False,
            }
            for series_name, value in latest_payload.items()
            if series_name != "breadth"
        }
        return fallback_history, latest_by_series, None
    anchor_dt = datetime.fromisoformat(str(feature_anchor_ts_utc))
    snapshots: Dict[str, Dict[str, float]] = {}
    latest_by_series: Dict[str, Dict[str, Any]] = {}
    last_seen: Dict[str, float] = {}
    for row in rows:
        source_ts = row.get("source_ts_utc")
        if not source_ts:
            continue
        source_dt = datetime.fromisoformat(str(source_ts))
        if source_dt > anchor_dt:
            break
        obs_date = str(row.get("obs_date") or "")[:10]
        series_name = str(row.get("name") or "")
        if series_name == "breadth":
            continue
        snapshots.setdefault(obs_date, dict(last_seen))
        value = float(row.get("value") or 0.0)
        last_seen[series_name] = value
        snapshots[obs_date][series_name] = value
        latest_by_series[series_name] = dict(row)
    macro_asof_ts_utc = max((str(row.get("source_ts_utc")) for row in latest_by_series.values()), default=None)
    return snapshots, latest_by_series, macro_asof_ts_utc


def _macro_freshness_payload(
    *,
    symbol: str,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    latest_macro_by_series: Dict[str, Dict[str, Any]],
    feature_anchor_ts_utc: str | None,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> tuple[Dict[str, float], Dict[str, Dict[str, Any]]]:
    freshness_features: Dict[str, float] = {}
    freshness_summary: Dict[str, Dict[str, Any]] = {}
    anchor_dt = datetime.fromisoformat(str(feature_anchor_ts_utc)) if feature_anchor_ts_utc else None
    symbol_bars = list(bars_by_symbol.get(symbol, []))
    bar_anchor_dts = [
        _bar_anchor_ts_utc(symbol=symbol, bar=bar, session_metadata_by_symbol=session_metadata_by_symbol)
        for bar in symbol_bars
    ]
    for series_name in CTX_SERIES:
        if series_name in SIMILARITY_DISABLED_SERIES:
            freshness_summary[series_name] = {
                "present": False,
                "missing_reason": BREADTH_MISSING_REASON_CANONICAL_SOURCE_MISSING,
                "breadth_policy": BREADTH_POLICY_DIAGNOSTICS_ONLY_V1,
                "policy_disabled": True,
                "source_ts_utc": None,
                "source_ts_is_derived": False,
                "days_since_update": None,
                "bars_since_update": None,
                "is_stale_flag": False,
                "source_timestamp_age_bucket": "missing",
            }
            continue
        row = latest_macro_by_series.get(series_name)
        if not row or not row.get("source_ts_utc") or anchor_dt is None:
            freshness_summary[series_name] = {
                "present": False,
                "missing_reason": "not_published_yet",
                "source_ts_utc": None,
                "source_ts_is_derived": False,
                "days_since_update": None,
                "bars_since_update": None,
                "is_stale_flag": True,
                "source_timestamp_age_bucket": "missing",
            }
            freshness_features[f"{series_name}_is_stale"] = 1.0
            freshness_features[f"{series_name}_age_bucket"] = 3.0
            freshness_features[f"{series_name}_days_since_update"] = 999.0
            freshness_features[f"{series_name}_bars_since_update"] = 999.0
            continue
        source_dt = datetime.fromisoformat(str(row["source_ts_utc"]))
        days_since_update = max(0.0, (anchor_dt - source_dt).total_seconds() / 86400.0)
        bars_since_update = float(sum(1 for bar_dt in bar_anchor_dts if bar_dt and source_dt < bar_dt <= anchor_dt))
        is_stale_flag = days_since_update > STALE_DAYS_THRESHOLD or bars_since_update > STALE_BARS_THRESHOLD
        if is_stale_flag:
            age_bucket = 2 if days_since_update <= 7.0 else 3
        elif days_since_update > 1.0:
            age_bucket = 1
        else:
            age_bucket = 0
        freshness_features[f"{series_name}_days_since_update"] = float(days_since_update)
        freshness_features[f"{series_name}_bars_since_update"] = float(bars_since_update)
        freshness_features[f"{series_name}_is_stale"] = 1.0 if is_stale_flag else 0.0
        freshness_features[f"{series_name}_age_bucket"] = float(age_bucket)
        freshness_summary[series_name] = {
            "present": True,
            "missing_reason": None,
            "source_ts_utc": str(row["source_ts_utc"]),
            "source_ts_is_derived": bool(row.get("source_ts_is_derived", False)),
            "days_since_update": float(days_since_update),
            "bars_since_update": float(bars_since_update),
            "is_stale_flag": bool(is_stale_flag),
            "source_timestamp_age_bucket": int(age_bucket),
            "value": float(row.get("value") or 0.0),
        }
    return freshness_features, freshness_summary


def _missingness_family_for_reason(reason: str) -> str:
    if reason in {"insufficient_bars", "insufficient_query_history"}:
        return "structural_missing"
    if reason in {"unknown_exchange_session", "missing_session_metadata"}:
        return "data_quality_missing"
    return "data_quality_missing"


def _bars_until_date(bars: List[HistoricalBar], cutoff_date: str | None) -> List[HistoricalBar]:
    return [bar for bar in bars if not cutoff_date or str(bar.timestamp)[:10] <= cutoff_date]


def _trade_date(bar: HistoricalBar) -> str:
    return str(bar.timestamp)[:10]


def _latest_macro_payload(macro_history: Dict[str, Dict[str, float]] | None) -> Dict[str, float]:
    if not macro_history:
        return {}
    latest_date = max(macro_history.keys())
    return dict(macro_history.get(latest_date, {}) or {})


def _sign(value: float) -> int:
    if value > 1e-12:
        return 1
    if value < -1e-12:
        return -1
    return 0


def _regime_inputs_summary(normalized_regime_context_features: Dict[str, float]) -> Dict[str, Any]:
    if not normalized_regime_context_features:
        return {"aggregate_score": 0.0, "series_inputs": {}, "series_count": 0}
    series_inputs: Dict[str, Dict[str, float | int | str]] = {}
    signals: List[int] = []
    for key, value in sorted(normalized_regime_context_features.items()):
        if "_" not in key:
            continue
        series_name = key.split("_", 1)[0]
        signal_input = float(value)
        if key.endswith("percentile_20"):
            signal_input = float(value) - 0.5
        signal = _sign(signal_input)
        signals.append(signal)
        series_inputs[series_name] = {
            "key": key,
            "value": float(value),
            "signal_input": float(signal_input),
            "signal": signal,
        }
    aggregate_score = mean(signals) if signals else 0.0
    return {
        "aggregate_score": float(aggregate_score),
        "series_inputs": series_inputs,
        "series_count": len(series_inputs),
    }


def _regime_from_context_features(normalized_regime_context_features: Dict[str, float]) -> str:
    summary = _regime_inputs_summary(normalized_regime_context_features)
    score = float(summary.get("aggregate_score", 0.0) or 0.0)
    if score >= REGIME_THRESHOLD:
        return "RISK_ON"
    if score <= -REGIME_THRESHOLD:
        return "RISK_OFF"
    return "NEUTRAL"


def _market_proxy_series(
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    cutoff_date: str | None = None,
    *,
    proxy_symbol: str = "MKT",
    focus_symbol: str | None = None,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None,
    cutoff_anchor_ts_utc: str | None = None,
) -> ProxySeriesResult:
    source_bars = (
        _same_exchange_symbols(
            focus_symbol=focus_symbol,
            bars_by_symbol=bars_by_symbol,
            session_metadata_by_symbol=session_metadata_by_symbol,
        )
        if focus_symbol
        else dict(bars_by_symbol)
    )
    symbol_date_bars: Dict[str, Dict[str, HistoricalBar]] = {}
    for symbol, bars in source_bars.items():
        clipped = _bars_until_date(bars, cutoff_date)
        if cutoff_anchor_ts_utc:
            clipped = [
                bar
                for bar in clipped
                if (_bar_anchor_ts_utc(symbol=symbol, bar=bar, session_metadata_by_symbol=session_metadata_by_symbol) or datetime.min.replace(tzinfo=timezone.utc))
                <= datetime.fromisoformat(str(cutoff_anchor_ts_utc))
            ]
        if not clipped:
            continue
        symbol_date_bars[symbol] = {_trade_date(bar): bar for bar in clipped}
    if not symbol_date_bars:
        return ProxySeriesResult(
            bars=[],
            peer_count_by_date={},
            contributing_symbols_by_date={},
            fallback_to_self=False,
            proxy_mode="session_aware_same_exchange" if focus_symbol and session_metadata_by_symbol else "date_aligned",
            same_exchange_peer_count=max(0, len(source_bars)),
            cross_exchange_proxy_used=False,
        )
    all_dates = sorted({trade_date for date_map in symbol_date_bars.values() for trade_date in date_map.keys()})
    rows: List[HistoricalBar] = []
    peer_count_by_date: Dict[str, int] = {}
    contributing_symbols_by_date: Dict[str, List[str]] = {}
    for trade_date in all_dates:
        bucket = [(symbol, date_map[trade_date]) for symbol, date_map in symbol_date_bars.items() if trade_date in date_map]
        if not bucket:
            continue
        symbols = sorted(symbol for symbol, _bar in bucket)
        peer_count_by_date[trade_date] = len(symbols)
        contributing_symbols_by_date[trade_date] = symbols
        bars = [bar for _symbol, bar in bucket]
        rows.append(
            HistoricalBar(
                symbol=proxy_symbol,
                timestamp=trade_date,
                open=mean([float(bar.open) for bar in bars]),
                high=mean([float(bar.high) for bar in bars]),
                low=mean([float(bar.low) for bar in bars]),
                close=mean([float(bar.close) for bar in bars]),
                volume=mean([float(bar.volume) for bar in bars]),
            )
        )
    return ProxySeriesResult(
        bars=rows,
        peer_count_by_date=peer_count_by_date,
        contributing_symbols_by_date=contributing_symbols_by_date,
        fallback_to_self=False,
        proxy_mode="session_aware_same_exchange" if focus_symbol and session_metadata_by_symbol else "date_aligned",
        same_exchange_peer_count=max(0, len(symbol_date_bars)),
        cross_exchange_proxy_used=False,
    )


def _market_proxy_bars(bars_by_symbol: Dict[str, List[HistoricalBar]], cutoff_date: str | None = None) -> List[HistoricalBar]:
    return _market_proxy_series(bars_by_symbol, cutoff_date=cutoff_date).bars


def _sector_proxy_series(symbol: str, bars_by_symbol: Dict[str, List[HistoricalBar]], sector_map: Dict[str, str], cutoff_date: str | None = None, *, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, cutoff_anchor_ts_utc: str | None = None) -> ProxySeriesResult:
    sector = sector_map.get(symbol)
    focus_meta = _resolve_session_metadata(symbol, session_metadata_by_symbol)
    peers = {
        s: bars
        for s, bars in bars_by_symbol.items()
        if s != symbol
        and sector
        and sector_map.get(s) == sector
        and (
            focus_meta is None
            or session_metadata_by_symbol is None
            or (_resolve_session_metadata(s, session_metadata_by_symbol) and _resolve_session_metadata(s, session_metadata_by_symbol).exchange_code == focus_meta.exchange_code)
        )
    }
    fallback_to_self = not bool(peers)
    proxy_source = peers or {symbol: bars_by_symbol.get(symbol, [])}
    result = _market_proxy_series(
        proxy_source,
        cutoff_date=cutoff_date,
        proxy_symbol=f"SECTOR:{sector or symbol}",
        focus_symbol=symbol,
        session_metadata_by_symbol=session_metadata_by_symbol,
        cutoff_anchor_ts_utc=cutoff_anchor_ts_utc,
    )
    return ProxySeriesResult(
        bars=result.bars,
        peer_count_by_date=result.peer_count_by_date,
        contributing_symbols_by_date=result.contributing_symbols_by_date,
        fallback_to_self=fallback_to_self,
        proxy_mode=result.proxy_mode,
        same_exchange_peer_count=result.same_exchange_peer_count,
        cross_exchange_proxy_used=result.cross_exchange_proxy_used,
    )


def _sector_proxy_bars(symbol: str, bars_by_symbol: Dict[str, List[HistoricalBar]], sector_map: Dict[str, str], cutoff_date: str | None = None) -> List[HistoricalBar]:
    return _sector_proxy_series(symbol, bars_by_symbol, sector_map, cutoff_date=cutoff_date).bars


def _proxy_diagnostics_payload(market_proxy: ProxySeriesResult, sector_proxy: ProxySeriesResult) -> Dict[str, Any]:
    return {
        "market": {
            "peer_count_by_date": dict(market_proxy.peer_count_by_date),
            "contributing_symbols_by_date": dict(market_proxy.contributing_symbols_by_date),
            "fallback_to_self": False,
            "proxy_mode": market_proxy.proxy_mode,
            "same_exchange_peer_count": market_proxy.same_exchange_peer_count,
            "cross_exchange_proxy_used": market_proxy.cross_exchange_proxy_used,
        },
        "sector": {
            "peer_count_by_date": dict(sector_proxy.peer_count_by_date),
            "contributing_symbols_by_date": dict(sector_proxy.contributing_symbols_by_date),
            "fallback_to_self": bool(sector_proxy.fallback_to_self),
            "proxy_mode": sector_proxy.proxy_mode,
            "same_exchange_peer_count": sector_proxy.same_exchange_peer_count,
            "cross_exchange_proxy_used": sector_proxy.cross_exchange_proxy_used,
        },
    }


def _query_regime_code(query_meta: Dict[str, Any]) -> str:
    normalized = dict(query_meta.get("normalized_regime_context_features") or {})
    if normalized:
        return _regime_from_context_features(normalized)
    raw_regime = str(query_meta.get("regime_code_raw_macro") or "").strip()
    return raw_regime or "NEUTRAL"


def _assert_similarity_macro_history_contract(macro_history: Dict[str, Dict[str, float]] | None) -> None:
    if not macro_history:
        return
    populated_dates = [payload for payload in macro_history.values() if payload]
    if len(populated_dates) <= 1:
        raise AssertionError("single-day macro history collapses context semantics")


def build_query_embedding(*, symbol: str, bars: List[HistoricalBar], bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history: Dict[str, Dict[str, float]], sector_map: Dict[str, str], cutoff_date: str | None, spec: ResearchExperimentSpec | None = None, scaler=None, transform=None, use_macro_level_in_similarity: bool = False, use_dollar_volume_absolute: bool = False, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None) -> tuple[list[float], dict]:
    sector_code = sector_map.get(symbol)
    shape_horizons = list((spec.lookback_horizons if spec and spec.lookback_horizons else [spec.horizon_days] if spec else []) or [])
    resolved_scaler = scaler or (transform.scaler if transform is not None else None)
    session_date_local = cutoff_date or (str(bars[-1].timestamp)[:10] if bars else None)
    anchor_fields = _anchor_fields_for_symbol_date(
        symbol=symbol,
        session_date_local=str(session_date_local),
        session_metadata_by_symbol=session_metadata_by_symbol,
    ) if session_date_local else {}
    feature_anchor_ts_utc = anchor_fields.get("feature_anchor_ts_utc")
    market_proxy = _market_proxy_series(
        bars_by_symbol,
        cutoff_date=cutoff_date,
        focus_symbol=symbol,
        session_metadata_by_symbol=session_metadata_by_symbol,
        cutoff_anchor_ts_utc=feature_anchor_ts_utc,
    )
    sector_proxy = _sector_proxy_series(
        symbol,
        bars_by_symbol,
        sector_map,
        cutoff_date=cutoff_date,
        session_metadata_by_symbol=session_metadata_by_symbol,
        cutoff_anchor_ts_utc=feature_anchor_ts_utc,
    )
    macro_window, latest_macro_by_series, macro_asof_ts_utc = _macro_history_until_anchor(
        macro_history_by_date=macro_history,
        macro_series_history=macro_series_history,
        feature_anchor_ts_utc=feature_anchor_ts_utc,
        fallback_cutoff_date=cutoff_date,
    )
    _assert_similarity_macro_history_contract(macro_window)
    macro_freshness_features, macro_freshness_summary = _macro_freshness_payload(
        symbol=symbol,
        bars_by_symbol=bars_by_symbol,
        latest_macro_by_series=latest_macro_by_series,
        feature_anchor_ts_utc=feature_anchor_ts_utc,
        session_metadata_by_symbol=session_metadata_by_symbol,
    )
    breadth_diagnostics = _breadth_diagnostics_payload(latest_macro_by_series)
    fv = build_multiscale_feature_vector(
        symbol=symbol,
        bars=bars,
        market_bars=market_proxy.bars,
        sector_bars=sector_proxy.bars,
        macro_history=macro_window,
        sector_code=sector_code,
        scaler=resolved_scaler,
        transform=transform,
        shape_horizons=shape_horizons,
        use_macro_level_in_similarity=use_macro_level_in_similarity,
        use_dollar_volume_absolute=use_dollar_volume_absolute,
        proxy_diagnostics=_proxy_diagnostics_payload(market_proxy, sector_proxy),
        macro_freshness_features=macro_freshness_features,
        additional_metadata={
            "anchor_fields": dict(anchor_fields),
            "macro_asof_ts_utc": macro_asof_ts_utc,
            "macro_freshness_summary": macro_freshness_summary,
            **breadth_diagnostics,
        },
    )
    latest_macro_payload = _latest_macro_payload(macro_window)
    regime_inputs_summary = _regime_inputs_summary(fv.normalized_regime_context_features)
    regime_code = _regime_from_context_features(fv.normalized_regime_context_features)
    regime_code_raw_macro = _regime_from_macro_raw(latest_macro_payload)
    return fv.embedding, {
        "raw_shape_features": fv.raw_shape_features,
        "raw_residual_features": fv.raw_residual_features,
        "raw_context_features": fv.raw_context_features,
        "raw_regime_context_features": fv.raw_regime_context_features,
        "normalized_regime_context_features": fv.normalized_regime_context_features,
        "shape_features": fv.shape_features,
        "residual_features": fv.residual_features,
        "context_features": fv.context_features,
        "regime_context_features": fv.regime_context_features,
        "raw_features": fv.raw_features,
        "transformed_features": fv.transformed_features,
        "shape_vector": fv.shape_vector,
        "ctx_vector": fv.ctx_vector,
        "transform_version": fv.metadata.get("transform_version"),
        "regime_source": REGIME_SOURCE_NORMALIZED,
        "regime_inputs_summary": regime_inputs_summary,
        "regime_code": regime_code,
        "regime_code_raw_macro": regime_code_raw_macro,
        "macro_history_length": len(macro_window),
        "macro_series_present_count": _count_present_similarity_macro_series(latest_macro_by_series),
        "macro_asof_ts_utc": macro_asof_ts_utc,
        "macro_freshness_summary": macro_freshness_summary,
        **breadth_diagnostics,
        **anchor_fields,
        **fv.metadata,
    }


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


def _side_diag(ev, surface, side: str) -> dict:
    diagnostics = dict(getattr(ev, "diagnostics", {}) or {})
    utility = dict(diagnostics.get("ev_decomposition") or getattr(ev, "utility", {}) or {})
    interval = dict(diagnostics.get("interval") or {
        "q10": getattr(ev, "q10_return", 0.0),
        "q50": getattr(ev, "q50_return", 0.0),
        "q90": getattr(ev, "q90_return", 0.0),
    })
    top_matches = list(getattr(ev, "top_matches", []) or [])
    support_counts = [float(((m or {}).get("why") or {}).get("support", 0.0) or 0.0) for m in top_matches]
    summary = []
    for match in top_matches[:3]:
        why = dict((match or {}).get("why") or {})
        summary.append({
            "prototype_id": match.get("prototype_id"),
            "representative_symbol": match.get("representative_symbol"),
            "weight": match.get("weight"),
            "similarity": why.get("similarity"),
            "support": why.get("support"),
            "expected_return": match.get("expected_return"),
            "uncertainty": match.get("uncertainty"),
        })
    return {
        "side": side,
        "expected_net_return": getattr(ev, "expected_net_return", 0.0),
        "expected_mae": getattr(ev, "expected_mae", 0.0),
        "expected_mfe": getattr(ev, "expected_mfe", 0.0),
        "fallback_raw_ev": utility.get("fallback_raw_ev", getattr(ev, "expected_utility", 0.0)),
        "q10": interval.get("q10", 0.0),
        "q50": interval.get("q50", 0.0),
        "q90": interval.get("q90", 0.0),
        "uncertainty": getattr(ev, "uncertainty", 0.0),
        "regime_alignment": getattr(ev, "regime_alignment", 0.0),
        "lower_bound": interval.get("q10", 0.0) - float(getattr(ev, "uncertainty", 0.0) or 0.0),
        "interval_width": float(interval.get("q90", 0.0) or 0.0) - float(interval.get("q10", 0.0) or 0.0),
        "support_count": float(sum(support_counts)),
        "n_eff": getattr(ev, "effective_sample_size", 0.0),
        "p_target": utility.get("p_target_first", getattr(ev, "p_up_first", 0.0)),
        "p_stop": utility.get("p_stop_first", getattr(ev, "p_down_first", 0.0)),
        "p_flat": utility.get("p_flat", 0.0),
        "p_ambiguous": utility.get("p_ambiguous", 0.0),
        "p_no_trade": utility.get("p_no_trade", 0.0),
        "top_matches_summary": summary,
        "side_stats_summary": {
            "match_count": len(top_matches),
            "prototype_ids": [m.get("prototype_id") for m in top_matches[:3]],
            "representative_symbols": [m.get("representative_symbol") for m in top_matches[:3]],
            "mean_support": (sum(support_counts) / len(support_counts)) if support_counts else 0.0,
            "max_support": max(support_counts) if support_counts else 0.0,
            "abstain_reasons": list(getattr(ev, "abstain_reasons", []) or []),
            "decision_summary": (surface.diagnostics.get("decision_rule") or {}).get("why_summary"),
        },
    }


def _chosen_side_payload(*, surface, buy_side_diag: dict[str, Any], sell_side_diag: dict[str, Any]) -> dict[str, Any]:
    chosen_side = str(getattr(surface, "chosen_side", "ABSTAIN") or "ABSTAIN")
    chosen_diag = buy_side_diag if chosen_side == Side.BUY.value else sell_side_diag if chosen_side == Side.SELL.value else {}
    decision_rule = dict((getattr(surface, "diagnostics", {}) or {}).get("decision_rule") or {})
    return {
        "chosen_side": chosen_side,
        "available": bool(chosen_diag) and not bool(getattr(surface, "abstain", False)),
        "expected_net_return": chosen_diag.get("expected_net_return"),
        "q10_return": chosen_diag.get("q10"),
        "q50_return": chosen_diag.get("q50"),
        "q90_return": chosen_diag.get("q90"),
        "expected_mae": chosen_diag.get("expected_mae"),
        "expected_mfe": chosen_diag.get("expected_mfe"),
        "effective_sample_size": chosen_diag.get("n_eff"),
        "uncertainty": chosen_diag.get("uncertainty"),
        "regime_alignment": chosen_diag.get("regime_alignment"),
        "fill_probability_proxy": chosen_diag.get("p_target"),
        "lower_bound": decision_rule.get("chosen_lower_bound", chosen_diag.get("lower_bound")),
        "interval_width": decision_rule.get("chosen_interval_width", chosen_diag.get("interval_width")),
        "abstain_reasons": list(getattr(surface, "abstain_reasons", []) or []),
    }


def _label_cfg(spec: ResearchExperimentSpec) -> EventLabelingConfig:
    return EventLabelingConfig(target_return_pct=spec.target_return_pct, stop_return_pct=spec.stop_return_pct, horizon_days=spec.horizon_days, fee_bps=spec.fee_bps, slippage_bps=spec.slippage_bps, flat_return_band_pct=spec.flat_return_band_pct)


def _macro_history_until(macro_history_by_date: Dict[str, Dict[str, float]], feature_end_date: str) -> Dict[str, Dict[str, float]]:
    return {k: dict(v) for k, v in sorted(macro_history_by_date.items()) if k <= feature_end_date}


def build_event_memory_asof(*, decision_date: str, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, lookback_bars: int = 5, metadata: dict | None = None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None) -> dict:
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    label_cfg = _label_cfg(spec)
    use_macro_level_in_similarity = _feature_flag("use_macro_level_in_similarity", metadata, default=False)
    use_dollar_volume_absolute = _feature_flag("use_dollar_volume_absolute", metadata, default=False)
    event_records: List[EventOutcomeRecord] = []
    anchor_library: List[ResearchAnchor] = []
    raw_event_rows: List[dict] = []
    excluded_reasons: list[dict] = []
    pending_records: list[dict] = []
    for lib_symbol, lib_bars in bars_by_symbol.items():
        session_metadata = _resolve_session_metadata(lib_symbol, session_metadata_by_symbol)
        if session_metadata_by_symbol and session_metadata is None:
            excluded_reasons.append({"symbol": lib_symbol, "reason": "unknown_exchange_session", "missingness_family": "data_quality_missing"})
            continue
        if len(lib_bars) < min_required_bars + spec.horizon_days + 2:
            excluded_reasons.append({"symbol": lib_symbol, "reason": "insufficient_bars", "missingness_family": "structural_missing"})
            continue
        lib_sector = sector_map.get(lib_symbol)
        for j in range(min_required_bars - 1, len(lib_bars) - spec.horizon_days - 1):
            feature_end_date = str(lib_bars[j].timestamp)[:10]
            outcome_end_date = str(lib_bars[j + spec.horizon_days].timestamp)[:10]
            if feature_end_date > decision_date:
                break
            if outcome_end_date >= decision_date:
                break
            history_window = lib_bars[j - spec.feature_window_bars + 1 : j + 1]
            future_window = lib_bars[j + 1 : j + 1 + spec.horizon_days]
            anchor_fields = _anchor_fields_for_symbol_date(
                symbol=lib_symbol,
                session_date_local=feature_end_date,
                session_metadata_by_symbol=session_metadata_by_symbol,
            )
            macro_window, latest_macro_by_series, macro_asof_ts_utc = _macro_history_until_anchor(
                macro_history_by_date=macro_history_by_date,
                macro_series_history=macro_series_history,
                feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                fallback_cutoff_date=feature_end_date,
            )
            macro_payload = _latest_macro_payload(macro_window)
            event = build_event_outcome_record(future_window, label_cfg)
            market_proxy = _market_proxy_series(
                bars_by_symbol,
                cutoff_date=feature_end_date,
                focus_symbol=lib_symbol,
                session_metadata_by_symbol=session_metadata_by_symbol,
                cutoff_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
            )
            sector_proxy = _sector_proxy_series(
                lib_symbol,
                bars_by_symbol,
                sector_map,
                cutoff_date=feature_end_date,
                session_metadata_by_symbol=session_metadata_by_symbol,
                cutoff_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
            )
            macro_freshness_features, macro_freshness_summary = _macro_freshness_payload(
                symbol=lib_symbol,
                bars_by_symbol=bars_by_symbol,
                latest_macro_by_series=latest_macro_by_series,
                feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                session_metadata_by_symbol=session_metadata_by_symbol,
            )
            breadth_diagnostics = _breadth_diagnostics_payload(latest_macro_by_series)
            raw_payload = build_raw_multiscale_feature_payload(
                symbol=lib_symbol,
                bars=history_window,
                market_bars=market_proxy.bars,
                sector_bars=sector_proxy.bars,
                macro_history=macro_window,
                sector_code=lib_sector,
                shape_horizons=list((spec.lookback_horizons if spec.lookback_horizons else [spec.horizon_days]) or []),
                use_macro_level_in_similarity=use_macro_level_in_similarity,
                use_dollar_volume_absolute=use_dollar_volume_absolute,
                proxy_diagnostics=_proxy_diagnostics_payload(market_proxy, sector_proxy),
                macro_freshness_features=macro_freshness_features,
                additional_metadata={
                    "anchor_fields": dict(anchor_fields),
                    "macro_asof_ts_utc": macro_asof_ts_utc,
                    "macro_freshness_summary": macro_freshness_summary,
                    **breadth_diagnostics,
                },
            )
            regime_inputs_summary = _regime_inputs_summary(raw_payload.normalized_regime_context_features)
            regime_code = _regime_from_context_features(raw_payload.normalized_regime_context_features)
            regime_code_raw_macro = _regime_from_macro_raw(macro_payload)
            raw_event_rows.append(dict(raw_payload.raw_features))
            pending_records.append({
                "event": event,
                "symbol": lib_symbol,
                "feature_end_date": feature_end_date,
                "outcome_end_date": outcome_end_date,
                "lib_sector": lib_sector,
                "regime_code": regime_code,
                "regime_code_raw_macro": regime_code_raw_macro,
                "regime_inputs_summary": regime_inputs_summary,
                "history_window": history_window,
                "macro_history_length": len(macro_window),
                "macro_series_present_count": _count_present_similarity_macro_series(latest_macro_by_series),
                "raw_payload": raw_payload,
                "anchor_fields": anchor_fields,
                "macro_asof_ts_utc": macro_asof_ts_utc,
                "macro_freshness_summary": macro_freshness_summary,
                **breadth_diagnostics,
            })
    transform = fit_feature_transform(raw_event_rows)
    scaler = transform.scaler
    for pending in pending_records:
        raw_payload = pending["raw_payload"]
        transformed_features, embedding = transform.apply(raw_payload.raw_features)
        transform_missing_keys_filled_zero = sorted(key for key in transform.feature_keys if key not in raw_payload.raw_features)
        transformed_zero_feature_keys = sorted(key for key, value in transformed_features.items() if abs(float(value)) <= 1e-12)
        shape_keys = sorted(list(raw_payload.shape_features.keys()) + list(raw_payload.residual_features.keys()))
        ctx_keys = sorted(raw_payload.context_features.keys())
        shape_vector = [float(transformed_features.get(k, 0.0)) for k in shape_keys]
        ctx_vector = [float(transformed_features.get(k, 0.0)) for k in ctx_keys]
        event_records.append(EventOutcomeRecord(
            symbol=pending["symbol"],
            event_date=pending["feature_end_date"],
            outcome_end_date=pending["outcome_end_date"],
            schema_version=spec.label_version,
            exchange_code=pending["anchor_fields"].get("exchange_code"),
            country_code=pending["anchor_fields"].get("country_code"),
            exchange_tz=pending["anchor_fields"].get("exchange_tz"),
            session_date_local=pending["anchor_fields"].get("session_date_local"),
            session_close_ts_local=pending["anchor_fields"].get("session_close_ts_local"),
            session_close_ts_utc=pending["anchor_fields"].get("session_close_ts_utc"),
            feature_anchor_ts_utc=pending["anchor_fields"].get("feature_anchor_ts_utc"),
            macro_asof_ts_utc=pending["macro_asof_ts_utc"],
            path_summary={
                **pending["event"].path_summary,
                "path_label": pending["event"].path_label,
                "feature_end_date": pending["feature_end_date"],
                "embedding": embedding,
                "raw_features": dict(raw_payload.raw_features),
                "transformed_features": dict(transformed_features),
                "raw_regime_context_features": dict(raw_payload.regime_context_features),
                "normalized_regime_context_features": dict(raw_payload.normalized_regime_context_features),
                "transform_version": transform.version,
                "proxy_diagnostics": dict(raw_payload.metadata.get("proxy_diagnostics", {})),
                "raw_zero_default_keys": list(raw_payload.metadata.get("raw_zero_default_keys", [])),
                "transform_missing_keys_filled_zero": transform_missing_keys_filled_zero,
                "regime_source": REGIME_SOURCE_NORMALIZED,
                "regime_inputs_summary": dict(pending["regime_inputs_summary"]),
                "macro_freshness_summary": dict(pending["macro_freshness_summary"]),
                "macro_asof_ts_utc": pending["macro_asof_ts_utc"],
                "breadth_policy": pending["breadth_policy"],
                "breadth_present": pending["breadth_present"],
                "breadth_missing_reason": pending["breadth_missing_reason"],
                **pending["anchor_fields"],
            },
            side_outcomes=pending["event"].side_payload,
            diagnostics={
                **pending["event"].diagnostics,
                "decision_cutoff": decision_date,
                "feature_end_date": pending["feature_end_date"],
                "embedding": embedding,
                "raw_features": dict(raw_payload.raw_features),
                "transformed_features": dict(transformed_features),
                "shape_vector": shape_vector,
                "ctx_vector": ctx_vector,
                "raw_regime_context_features": dict(raw_payload.regime_context_features),
                "regime_context_features": dict(raw_payload.regime_context_features),
                "normalized_regime_context_features": dict(raw_payload.normalized_regime_context_features),
                "transform_version": transform.version,
                "regime_code": pending["regime_code"],
                "regime_code_raw_macro": pending["regime_code_raw_macro"],
                "regime_source": REGIME_SOURCE_NORMALIZED,
                "regime_inputs_summary": dict(pending["regime_inputs_summary"]),
                "sector_code": pending["lib_sector"],
                "proxy_diagnostics": dict(raw_payload.metadata.get("proxy_diagnostics", {})),
                "sector_proxy_fallback_to_self": bool((((raw_payload.metadata.get("proxy_diagnostics", {}) or {}).get("sector") or {}).get("fallback_to_self", False))),
                "raw_zero_default_keys": list(raw_payload.metadata.get("raw_zero_default_keys", [])),
                "transform_missing_keys_filled_zero": transform_missing_keys_filled_zero,
                "transformed_zero_feature_keys": transformed_zero_feature_keys,
                "macro_history_length": pending["macro_history_length"],
                "macro_series_present_count": pending["macro_series_present_count"],
                "macro_freshness_summary": dict(pending["macro_freshness_summary"]),
                "macro_asof_ts_utc": pending["macro_asof_ts_utc"],
                "breadth_policy": pending["breadth_policy"],
                "breadth_present": pending["breadth_present"],
                "breadth_missing_reason": pending["breadth_missing_reason"],
                "liquidity_score": max(0.0, min(1.0, compute_bar_features(pending["history_window"]).get("volume_mean", 0.0) / 1_000_000.0)),
                "quality_score": float(pending["event"].quality_score),
                **pending["anchor_fields"],
            },
        ))
    prototypes = build_state_prototypes_from_event_memory(event_records=event_records, as_of_date=decision_date, memory_version=spec.memory_version, spec_hash=spec.spec_hash(), config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version)) if event_records else []
    coverage = {"event_record_count": len(event_records), "anchor_count": len(anchor_library), "prototype_count": len(prototypes)}
    return {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "as_of_date": decision_date, "coverage": coverage, "excluded_reasons": excluded_reasons, "event_records": event_records, "anchor_library": anchor_library, "prototypes": prototypes, "scaler": scaler, "transform": transform}


def _build_query_panel(*, decision_dates: list[str], spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], scaler=None, transform=None, metadata: dict | None = None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None):
    out = {}
    excluded_reasons = []
    allowed = set(decision_dates)
    use_macro_level_in_similarity = _feature_flag("use_macro_level_in_similarity", metadata, default=False)
    use_dollar_volume_absolute = _feature_flag("use_dollar_volume_absolute", metadata, default=False)
    for decision_date in decision_dates:
        per_date = {}
        for symbol, bars in bars_by_symbol.items():
            if session_metadata_by_symbol and _resolve_session_metadata(symbol, session_metadata_by_symbol) is None:
                excluded_reasons.append({"symbol": symbol, "reason": "unknown_exchange_session", "decision_date": decision_date, "missingness_family": "data_quality_missing"})
                continue
            eligible = [i for i, bar in enumerate(bars) if str(bar.timestamp)[:10] == decision_date]
            if not eligible:
                continue
            idx = eligible[0]
            if idx < spec.feature_window_bars - 1 or idx + 1 >= len(bars):
                excluded_reasons.append({"symbol": symbol, "reason": "insufficient_query_history", "decision_date": decision_date, "missingness_family": "structural_missing"})
                continue
            query_window = bars[idx - spec.feature_window_bars + 1 : idx + 1]
            embedding, meta = build_query_embedding(symbol=symbol, bars=query_window, bars_by_symbol=bars_by_symbol, macro_history=macro_history_by_date, sector_map=sector_map, cutoff_date=decision_date, spec=spec, scaler=scaler, transform=transform, use_macro_level_in_similarity=use_macro_level_in_similarity, use_dollar_volume_absolute=use_dollar_volume_absolute, session_metadata_by_symbol=session_metadata_by_symbol, macro_series_history=macro_series_history)
            per_date[symbol] = {"idx": idx, "query_window": query_window, "embedding": embedding, "meta": meta, "execution_bar": bars[idx + 1]}
        if decision_date in allowed:
            out[decision_date] = per_date
    return out, excluded_reasons


def fit_train_artifacts(*, run_id: str, artifact_store: JsonResearchArtifactStore, train_end: str, test_start: str, purge: int, embargo: int, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, calibration_artifact: dict | None = None, quote_policy_calibration: dict | None = None, metadata: dict | None = None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None) -> dict:
    memory = build_event_memory_asof(decision_date=train_end, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market, metadata=metadata, session_metadata_by_symbol=session_metadata_by_symbol, macro_series_history=macro_series_history)
    max_train_date = max((r.event_date for r in memory["event_records"]), default=None)
    max_outcome_end = max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None)
    if max_outcome_end and max_outcome_end >= test_start:
        raise AssertionError("future event/outcome mixed into train artifact")
    snapshot_id = f"{run_id}:{train_end}:{spec.spec_hash()}"
    artifact_store.save_prototype_snapshot(run_id=run_id, as_of_date=train_end, memory_version=spec.memory_version, payload={"spec_hash": spec.spec_hash(), "snapshot_id": snapshot_id, "prototype_count": len(memory["prototypes"]), "prototypes": [p.__dict__ for p in memory["prototypes"]]})
    return {"run_id": run_id, "snapshot_id": snapshot_id, "spec_hash": spec.spec_hash(), "as_of_date": train_end, "train_end": train_end, "test_start": test_start, "purge": purge, "embargo": embargo, "memory_version": spec.memory_version, "prototype_snapshot_name": "prototype_snapshot", "max_train_date": max_train_date, "max_outcome_end_date": max_outcome_end, "prototypes": [p.__dict__ for p in memory["prototypes"]], "scaler": memory["transform"].scaler, "transform": memory["transform"], "calibration": dict(calibration_artifact or {"method": "logistic", "slope": 1.0, "intercept": 0.0, "ev_slope": 1.0, "ev_intercept": 0.0}), "quote_policy_calibration": dict(quote_policy_calibration or {"ev_threshold": 0.005, "uncertainty_cap": 0.12, "min_effective_sample_size": 1.5, "min_fill_probability": 0.1, "abstain_margin": 0.05}), "metadata": dict(metadata or {}), "session_metadata_by_symbol": {symbol: session_metadata_to_dict(meta) for symbol, meta in (session_metadata_by_symbol or {}).items()}, "macro_series_history": list(macro_series_history or []), "snapshot_ids": {"prototype_snapshot_id": snapshot_id}}


def run_test_with_frozen_artifacts(*, train_artifact: dict, artifact_store: JsonResearchArtifactStore, decision_dates: list[str], spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, top_k: int | None = None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None) -> dict:
    if not train_artifact:
        raise AssertionError("train artifact required")
    min_test_decision_date = min(decision_dates) if decision_dates else None
    if train_artifact.get("max_train_date") and min_test_decision_date and train_artifact["max_train_date"] >= min_test_decision_date:
        raise AssertionError("max_train_date must be < min_test_decision_date")
    if train_artifact.get("max_outcome_end_date") and min_test_decision_date and train_artifact["max_outcome_end_date"] >= min_test_decision_date:
        raise AssertionError("future event/outcome mixed into test runtime memory")
    prototype_pool = load_prototypes_asof(artifact_store=artifact_store, run_id=train_artifact["run_id"], name=train_artifact.get("prototype_snapshot_name", "prototype_snapshot"), as_of_date=train_artifact["as_of_date"], memory_version=train_artifact["memory_version"])
    if not prototype_pool and train_artifact.get("prototypes"):
        from .models import StatePrototype
        prototype_pool = [StatePrototype(**p) for p in train_artifact.get("prototypes") or []]
    query_panel, excluded = _build_query_panel(decision_dates=decision_dates, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, scaler=train_artifact.get("scaler"), transform=train_artifact.get("transform"), metadata=train_artifact.get("metadata"), session_metadata_by_symbol=session_metadata_by_symbol or train_artifact.get("session_metadata_by_symbol"), macro_series_history=macro_series_history or train_artifact.get("macro_series_history"))
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    qp = train_artifact.get("quote_policy_calibration") or {}
    metadata = train_artifact.get("metadata") or {}
    effective_top_k = int(top_k or metadata.get("portfolio_top_n", 3) or 3)
    ev_cfg = EVConfig(top_k=effective_top_k, min_effective_sample_size=float(qp.get("min_effective_sample_size", 1.5)), max_uncertainty=float(qp.get("uncertainty_cap", 0.12)), min_expected_utility=float(qp.get("ev_threshold", 0.005)), min_regime_alignment=float(qp.get("min_regime_alignment", metadata.get("quote_min_regime_alignment", 0.5)) or 0.5), max_return_interval_width=float(qp.get("max_return_interval_width", metadata.get("quote_max_return_interval_width", 0.08)) or 0.08), abstain_margin=float(qp.get("abstain_margin", metadata.get("abstain_margin", 0.05)) or 0.05))
    cal_payload = train_artifact.get("calibration") or {}
    calibration = CalibrationModel(method=str(cal_payload.get("method", "logistic")), slope=float(cal_payload.get("slope", 1.0)), intercept=float(cal_payload.get("intercept", 0.0)))
    ev_slope = float(cal_payload.get("ev_slope", 1.0))
    ev_intercept = float(cal_payload.get("ev_intercept", 0.0))
    panel_rows = []
    candidates = []
    broker = SimulatedBroker(rules=SimulationRules(slippage_bps=spec.slippage_bps, fee_bps=spec.fee_bps, allow_partial_fills=True))
    portfolio_cfg = PortfolioConfig(top_n=max(1, int(metadata.get("portfolio_top_n", effective_top_k) or effective_top_k)), risk_budget_fraction=float(metadata.get("portfolio_risk_budget_fraction", 0.95) or 0.95))
    tuning = {"MIN_TICK_GAP": 1, "ADAPTIVE_BASE_LEGS": 2, "ADAPTIVE_LEG_BOOST": 1.0, "MIN_TOTAL_SPREAD_PCT": 0.01, "ADAPTIVE_STRENGTH_SCALE": 0.1, "FIRST_LEG_BASE_PCT": 0.012, "FIRST_LEG_MIN_PCT": 0.006, "FIRST_LEG_MAX_PCT": 0.05, "FIRST_LEG_GAIN_WEIGHT": 0.6, "FIRST_LEG_ATR_WEIGHT": 0.5, "FIRST_LEG_REQ_FLOOR_PCT": 0.012, "MIN_FIRST_LEG_GAP_PCT": 0.03, "STRICT_MIN_FIRST_GAP": True, "ADAPTIVE_MAX_STEP_PCT": 0.06, "ADAPTIVE_FRAC_ALPHA": 1.25, "ADAPTIVE_GAIN_SCALE": 0.1, "MIN_LOT_QTY": 1}
    grouped_candidates = {}
    for decision_date, items in query_panel.items():
        batch = []
        for symbol, q in items.items():
            regime_code = _query_regime_code(q["meta"])
            sector_code = sector_map.get(symbol)
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            buy_side_diag = _side_diag(surface.buy, surface, Side.BUY.value)
            sell_side_diag = _side_diag(surface.sell, surface, Side.SELL.value)
            chosen_side_payload = _chosen_side_payload(surface=surface, buy_side_diag=buy_side_diag, sell_side_diag=sell_side_diag)
            long_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.BUY.value)
            short_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.SELL.value)
            panel_rows.append({"decision_date": decision_date, "symbol": symbol, "prototype_snapshot_id": train_artifact["snapshot_ids"]["prototype_snapshot_id"], "prototype_count": len(prototype_pool), "chosen_side": surface.chosen_side, "top_matches": {"long": _topk(long_scores, effective_top_k), "short": _topk(short_scores, effective_top_k)}})
            if surface.abstain:
                continue
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            raw_ev = float(surface.buy.expected_net_return if chosen_side == Side.BUY else surface.sell.expected_net_return)
            score = ev_slope * raw_ev + ev_intercept
            confidence = calibration.calibrate_prob(score)
            candidate = SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=score, confidence=confidence, anchor_date=date.fromisoformat(decision_date), reference_date=date.fromisoformat(decision_date), current_price=float(q["execution_bar"].open), atr_pct=float(max(0.01, compute_bar_features(q["query_window"]).get("range_pct", 0.02) / 3.0)), target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "execution_date": str(q["execution_bar"].timestamp)[:10], "spec_hash": spec.spec_hash(), "frozen_from_train_artifacts": True}, diagnostics={"query": {"regime_code": regime_code, "sector_code": sector_code}, "decision_surface": {"chosen_side": surface.chosen_side, "decision_rule": surface.diagnostics.get("decision_rule"), "chosen_payload": chosen_side_payload}, "chosen_side_payload": chosen_side_payload, "scorer_diagnostics": {"buy": buy_side_diag, "sell": sell_side_diag}, "ev": {"buy": {"expected_net_return": surface.buy.expected_net_return, "effective_sample_size": surface.buy.effective_sample_size, "uncertainty": surface.buy.uncertainty}, "sell": {"expected_net_return": surface.sell.expected_net_return, "effective_sample_size": surface.sell.effective_sample_size, "uncertainty": surface.sell.uncertainty}}, "top_matches": panel_rows[-1]["top_matches"]}, notes=["frozen_validation_path=true"])
            batch.append(candidate)
            candidates.append(candidate)
        grouped_candidates[decision_date] = batch
    from backtest_app.research_runtime.engine import execute_daily_execution_loop
    execution = execute_daily_execution_loop(trading_dates=decision_dates, grouped_candidates=grouped_candidates, bars_by_symbol=bars_by_symbol, config=SimpleNamespace(initial_capital=10000.0, fee_bps=spec.fee_bps, slippage_bps=spec.slippage_bps, allow_partial_fills=True), market=market, strategy_mode="research_similarity_v2", portfolio_cfg=portfolio_cfg, quote_policy_cfg=QuotePolicyConfig(ev_threshold=float(qp.get("ev_threshold", 0.005)), uncertainty_cap=float(qp.get("uncertainty_cap", 0.12)), min_effective_sample_size=float(qp.get("min_effective_sample_size", 1.5)), min_fill_probability=float(qp.get("min_fill_probability", 0.1))), tuning=tuning, broker=broker)
    decisions = [{"symbol": d.candidate.symbol, "decision_date": str(d.candidate.reference_date)[:10] if getattr(d.candidate, "reference_date", None) else str(d.candidate.anchor_date)[:10], "selected": d.selected} for d in execution["portfolio_decisions_all"]]
    return {"decision_dates": decision_dates, "panel_rows": panel_rows, "candidates": [c.to_dict() for c in candidates], "portfolio_decisions": decisions, "plans": [p.to_dict() for p in execution["plans"]], "fills": [f.to_dict() for f in execution["fills"]], "excluded_reasons": excluded, "frozen_snapshot_id": train_artifact["snapshot_ids"]["prototype_snapshot_id"], "test_executed_from_frozen_train_artifacts": True}


def generate_similarity_candidates(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_payload: Dict[str, float], sector_map: Dict[str, str] | None = None, top_k: int = 3, abstain_margin: float = 0.05, spec: ResearchExperimentSpec | None = None) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    spec = spec or _default_spec()
    macro_history = {str(bar.timestamp)[:10]: dict(macro_payload) for bars in bars_by_symbol.values() for bar in bars}
    candidates, diagnostics = generate_similarity_candidates_rolling(bars_by_symbol=bars_by_symbol, market=market, macro_history_by_date=macro_history, sector_map=sector_map, top_k=top_k, abstain_margin=abstain_margin, spec=spec)
    if not candidates:
        for symbol in bars_by_symbol.keys():
            diagnostics.setdefault(symbol, {"scores": {"abstained": True}, "strategy_mode": "research_similarity_v1"})
    return candidates, diagnostics


def generate_similarity_candidates_rolling(*, bars_by_symbol: Dict[str, List[HistoricalBar]], market: str, macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str] | None = None, lookback_bars: int = 5, feature_window_bars: int = 60, horizon_days: int = 5, top_k: int = 3, abstain_margin: float = 0.05, spec: ResearchExperimentSpec | None = None, metadata: dict | None = None, progress_callback=None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None) -> Tuple[List[SignalCandidate], Dict[str, dict]]:
    t0 = perf_counter()
    spec = spec or _default_spec(feature_window_bars=feature_window_bars, horizon_days=horizon_days)
    sector_map = sector_map or {}
    ev_cfg = _ev_config_from_metadata(metadata, top_k=top_k, abstain_margin=abstain_margin)
    diagnostics: Dict[str, dict] = {"pipeline": {"strategy_mode": "research_similarity_v2", "spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "lookback_bars": lookback_bars, "top_k": ev_cfg.top_k, "top_k_requested": top_k, "abstain_margin": abstain_margin, "feature_flags": {"use_macro_level_in_similarity": _feature_flag("use_macro_level_in_similarity", metadata, default=False), "use_dollar_volume_absolute": _feature_flag("use_dollar_volume_absolute", metadata, default=False)}, "session_alignment": {"mode": "session_aligned_v1", "symbols_with_session_metadata": sorted((session_metadata_by_symbol or {}).keys()), "missing_session_metadata_symbols": sorted(set(bars_by_symbol) - set((session_metadata_by_symbol or {}).keys()))}, "macro_join": {"mode": "anchor_asof_join", "series_rows": len(macro_series_history or []), "breadth_in_similarity": False, "breadth_policy": BREADTH_POLICY_DIAGNOSTICS_ONLY_V1}, "ev_config": {"kernel_temperature": ev_cfg.kernel_temperature, "use_kernel_weighting": ev_cfg.use_kernel_weighting, "min_effective_sample_size": ev_cfg.min_effective_sample_size, "max_uncertainty": ev_cfg.max_uncertainty, "max_return_interval_width": ev_cfg.max_return_interval_width, "min_regime_alignment": ev_cfg.min_regime_alignment, "min_expected_utility": ev_cfg.min_expected_utility, "diagnostic_disable_lower_bound_gate": ev_cfg.diagnostic_disable_lower_bound_gate, "diagnostic_disable_ess_gate": ev_cfg.diagnostic_disable_ess_gate}}}
    panel_rows: List[dict] = []
    out: List[SignalCandidate] = []
    scoring_cfg = ScoringConfig(min_liquidity_score=0.0)
    calibration = CalibrationModel(method="identity")
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    decision_dates = sorted({str(bars[i].timestamp)[:10] for bars in bars_by_symbol.values() if len(bars) >= min_required_bars + spec.horizon_days + 2 for i in range(min_required_bars - 1, len(bars) - spec.horizon_days - 1)})
    total_prototype_count = 0
    all_excluded_reasons: list[dict] = []
    event_record_batches: list[dict] = []
    progress_every = max(1, min(10, len(decision_dates) or 1))

    def _emit_progress(decision_dates_done: int, *, force: bool = False) -> None:
        if not progress_callback:
            return
        if not force and decision_dates_done not in {0, len(decision_dates)} and decision_dates_done % progress_every != 0:
            return
        progress_callback({
            "phase": "candidate_generation",
            "status": "running",
            "decision_dates_done": decision_dates_done,
            "decision_dates_total": len(decision_dates),
            "completed_trading_dates": decision_dates_done,
            "total_trading_dates": len(decision_dates),
            "event_records_built": sum(len((batch or {}).get("records") or []) for batch in event_record_batches),
            "prototype_batches_built": len(event_record_batches),
            "candidate_rows": len(out),
        })

    _emit_progress(0, force=True)
    for idx, decision_date in enumerate(decision_dates, start=1):
        memory = build_event_memory_asof(decision_date=decision_date, spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, market=market, lookback_bars=lookback_bars, metadata=metadata, session_metadata_by_symbol=session_metadata_by_symbol, macro_series_history=macro_series_history)
        query_panel, query_excluded = _build_query_panel(decision_dates=[decision_date], spec=spec, bars_by_symbol=bars_by_symbol, macro_history_by_date=macro_history_by_date, sector_map=sector_map, scaler=memory["scaler"], transform=memory["transform"], metadata=metadata, session_metadata_by_symbol=session_metadata_by_symbol, macro_series_history=macro_series_history)
        event_record_batches.append({"decision_date": decision_date, "records": [{"symbol": r.symbol, "event_date": r.event_date, "outcome_end_date": r.outcome_end_date, "side_outcomes": r.side_outcomes} for r in memory["event_records"]]})
        all_excluded_reasons.extend([{**r, "decision_date": decision_date} for r in memory["excluded_reasons"] + query_excluded])
        total_prototype_count += len(memory["prototypes"])
        prototype_pool = list(memory["prototypes"])
        for symbol, q in query_panel.get(decision_date, {}).items():
            query_meta = dict(q["meta"] or {})
            regime_code = _query_regime_code(query_meta)
            sector_code = sector_map.get(symbol)
            execution_bar = q["execution_bar"]
            execution_date = str(execution_bar.timestamp)[:10]
            long_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.BUY.value)
            short_scores = score_candidates_exact(query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, config=scoring_cfg, candidate_index=ExactCosineCandidateIndex(), side=Side.SELL.value)
            long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration)
            buy_side_diag = _side_diag(long_ev, surface, "BUY")
            sell_side_diag = _side_diag(short_ev, surface, "SELL")
            chosen_side_payload = _chosen_side_payload(surface=surface, buy_side_diag=buy_side_diag, sell_side_diag=sell_side_diag)
            row_diag = {
                "decision_date": decision_date,
                "symbol": symbol,
                "query": {
                    "regime_code": regime_code,
                    "regime_code_raw_macro": query_meta.get("regime_code_raw_macro"),
                    "regime_source": query_meta.get("regime_source", REGIME_SOURCE_NORMALIZED),
                    "regime_inputs_summary": query_meta.get("regime_inputs_summary", {}),
                    "sector_code": sector_code,
                    "decision_date": decision_date,
                    "execution_date": execution_date,
                    "decision_convention": DECISION_CONVENTION,
                    "price_reference_source": "next_open",
                    "feature_window_bars": spec.feature_window_bars,
                    "feature_coverage_bars": len(q["query_window"]),
                    "query_panel_count": len(query_panel.get(decision_date, {})),
                    "insufficient_history": False,
                    "shape_horizons": query_meta.get("shape_horizons", []),
                    "transform_version": query_meta.get("transform_version"),
                    "exchange_code": query_meta.get("exchange_code"),
                    "country_code": query_meta.get("country_code"),
                    "exchange_tz": query_meta.get("exchange_tz"),
                    "session_date_local": query_meta.get("session_date_local"),
                    "session_close_ts_local": query_meta.get("session_close_ts_local"),
                    "session_close_ts_utc": query_meta.get("session_close_ts_utc"),
                    "feature_anchor_ts_utc": query_meta.get("feature_anchor_ts_utc"),
                    "macro_asof_ts_utc": query_meta.get("macro_asof_ts_utc"),
                    "feature_flags": {
                        "use_macro_level_in_similarity": query_meta.get("use_macro_level_in_similarity", False),
                        "use_dollar_volume_absolute": query_meta.get("use_dollar_volume_absolute", False),
                    },
                    "regime_context_features": query_meta.get("regime_context_features", {}),
                    "normalized_regime_context_features": query_meta.get("normalized_regime_context_features", {}),
                    "proxy_diagnostics": query_meta.get("proxy_diagnostics", {}),
                    "proxy_mode": (((query_meta.get("proxy_diagnostics", {}) or {}).get("market") or {}).get("proxy_mode")),
                    "same_exchange_peer_count": (((query_meta.get("proxy_diagnostics", {}) or {}).get("market") or {}).get("same_exchange_peer_count")),
                    "cross_exchange_proxy_used": bool((((query_meta.get("proxy_diagnostics", {}) or {}).get("market") or {}).get("cross_exchange_proxy_used", False))),
                    "sector_proxy_fallback_to_self": bool((((query_meta.get("proxy_diagnostics", {}) or {}).get("sector") or {}).get("fallback_to_self", False))),
                    "raw_zero_default_keys": query_meta.get("raw_zero_default_keys", []),
                    "zero_imputed_feature_keys": query_meta.get("transform_missing_keys_filled_zero", []),
                    "zero_imputed_feature_count": len(query_meta.get("transform_missing_keys_filled_zero", [])),
                    "transform_missing_keys_filled_zero": query_meta.get("transform_missing_keys_filled_zero", []),
                    "transformed_zero_feature_keys": query_meta.get("transformed_zero_feature_keys", []),
                    "macro_history_length": query_meta.get("macro_history_length", 0),
                    "macro_series_present_count": query_meta.get("macro_series_present_count", 0),
                    "macro_freshness_summary": query_meta.get("macro_freshness_summary", {}),
                    "breadth_policy": query_meta.get("breadth_policy", BREADTH_POLICY_DIAGNOSTICS_ONLY_V1),
                    "breadth_present": query_meta.get("breadth_present", False),
                    "breadth_missing_reason": query_meta.get("breadth_missing_reason"),
                },
                "library": {
                    "event_record_count": len(memory["event_records"]),
                    "max_outcome_end_before_decision": max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None),
                },
                "decision_surface": {
                    "chosen_side": surface.chosen_side,
                    "abstain": surface.abstain,
                    "abstain_reasons": list(surface.abstain_reasons),
                    "prototype_pool_size": surface.diagnostics.get("prototype_pool_size"),
                    "chosen_lower_bound": (surface.diagnostics.get("decision_rule") or {}).get("chosen_lower_bound"),
                    "chosen_interval_width": (surface.diagnostics.get("decision_rule") or {}).get("chosen_interval_width"),
                    "chosen_effective_sample_size": (surface.diagnostics.get("decision_rule") or {}).get("chosen_effective_sample_size"),
                    "chosen_uncertainty": (surface.diagnostics.get("decision_rule") or {}).get("chosen_uncertainty"),
                    "gate_ablation": surface.diagnostics.get("gate_ablation"),
                    "decision_rule": surface.diagnostics.get("decision_rule"),
                    "chosen_payload": chosen_side_payload,
                },
                "chosen_side_payload": chosen_side_payload,
                "ev": {
                    "buy": {
                        "expected_utility": long_ev.expected_utility,
                        "expected_net_return": long_ev.expected_net_return,
                        "effective_sample_size": long_ev.effective_sample_size,
                        "uncertainty": long_ev.uncertainty,
                        "abstain_reasons": long_ev.abstain_reasons,
                    },
                    "sell": {
                        "expected_utility": short_ev.expected_utility,
                        "expected_net_return": short_ev.expected_net_return,
                        "effective_sample_size": short_ev.effective_sample_size,
                        "uncertainty": short_ev.uncertainty,
                        "abstain_reasons": short_ev.abstain_reasons,
                    },
                },
                "scorer_diagnostics": {"buy": buy_side_diag, "sell": sell_side_diag},
                "top_matches": {"long": surface.buy.top_matches, "short": surface.sell.top_matches},
            }
            row_diag["missingness"] = {
                "taxonomy": {
                    "structural_missing": False,
                    "stale_but_usable": _stale_similarity_macro(query_meta.get("macro_freshness_summary")),
                    "data_quality_missing": bool(query_meta.get("anchor_missing_reason")),
                },
                "breadth_policy": query_meta.get("breadth_policy", BREADTH_POLICY_DIAGNOSTICS_ONLY_V1),
                "breadth_present": query_meta.get("breadth_present", False),
                "breadth_missing_reason": query_meta.get("breadth_missing_reason"),
                "zero_imputed_feature_keys": list(query_meta.get("transform_missing_keys_filled_zero", []) or []),
                "zero_imputed_feature_count": len(list(query_meta.get("transform_missing_keys_filled_zero", []) or [])),
            }
            panel_rows.append(row_diag)
            diagnostics[f"{decision_date}:{symbol}"] = row_diag
            if surface.abstain:
                continue
            chosen_side = Side.BUY if surface.chosen_side == Side.BUY.value else Side.SELL
            chosen = long_scores[0] if chosen_side == Side.BUY and long_scores else short_scores[0] if short_scores else None
            out.append(SignalCandidate(symbol=symbol, ticker_id=None, market=MarketCode(market), side_bias=chosen_side, signal_strength=float((long_ev.calibrated_ev if chosen_side == Side.BUY else short_ev.calibrated_ev) if chosen else 0.0), confidence=float(long_ev.calibrated_win_prob if chosen_side == Side.BUY else short_ev.calibrated_win_prob), anchor_date=date.fromisoformat(decision_date), reference_date=date.fromisoformat(decision_date), current_price=float(execution_bar.open), atr_pct=float(max(0.01, compute_bar_features(q["query_window"]).get("range_pct", 0.02) / 3.0)), target_return_pct=spec.target_return_pct, max_reverse_pct=spec.stop_return_pct, expected_horizon_days=spec.horizon_days, outcome_label=OutcomeLabel.UNKNOWN, provenance={"strategy_mode": "research_similarity_v2", "decision_date": decision_date, "execution_date": execution_date, "spec_hash": spec.spec_hash(), "exchange_code": query_meta.get("exchange_code"), "feature_anchor_ts_utc": query_meta.get("feature_anchor_ts_utc"), "macro_asof_ts_utc": query_meta.get("macro_asof_ts_utc")}, diagnostics=row_diag, notes=[f"prototype_id={(chosen.prototype_id if chosen else '')}"]))
        _emit_progress(idx)
    _emit_progress(len(decision_dates), force=True)
    diagnostics["signal_panel"] = panel_rows
    diagnostics["signal_panel_jsonl"] = "\n".join(str(row) for row in panel_rows)
    diagnostics["cache_keys"] = {"library_cache_keys": [f"{d}:{spec.spec_hash()}" for d in decision_dates]}
    diagnostics["event_records"] = event_record_batches
    diagnostics["throughput"] = {"n_symbols": len(bars_by_symbol), "n_decision_dates": len(decision_dates), "prototype_count": total_prototype_count, "wall_clock_seconds": perf_counter() - t0}
    diagnostics["artifacts"] = {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "excluded_reasons": all_excluded_reasons, "excluded_reasons_histogram": dict(Counter(r.get("reason", "unknown") for r in all_excluded_reasons))}
    return out, diagnostics

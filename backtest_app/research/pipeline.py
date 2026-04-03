from __future__ import annotations

from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import json
import pickle
from types import SimpleNamespace
from statistics import mean, pstdev
from time import perf_counter
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.historical_data.features import (
    CTX_SERIES,
    FEATURE_TRANSFORM_VERSION,
    FeatureScaler,
    FeatureTransform,
    REGIME_CONTEXT_PRIORITY_SUFFIXES,
    SIMILARITY_CTX_SERIES,
    build_multiscale_feature_vector,
    build_raw_multiscale_feature_payload,
    compute_bar_features,
    fit_feature_transform,
    identity_feature_transform,
)
from backtest_app.historical_data.models import HistoricalBar, SymbolSessionMetadata
from backtest_app.historical_data.session_alignment import derive_session_anchor_for_bar, derive_session_anchor_from_date, session_anchor_timestamp_utc, session_metadata_to_dict
from backtest_app.portfolio import PortfolioConfig, PortfolioState, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate

from .artifacts import JsonResearchArtifactStore, _append_parquet_batches
from .labeling import EventLabelingConfig, build_event_outcome_record, label_event_window
from .models import EventOutcomeRecord, ResearchAnchor
from .prototype import PrototypeConfig, aggregate_prototype_compression_batches, build_prototype_compression_audit, build_state_prototypes_from_event_memory
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


@dataclass(frozen=True)
class EventRawCacheHandle:
    format_version: str
    manifest_path: str
    events_path: str
    raw_features_path: str
    prefix_event_ids_path: str
    prefix_sum_path: str
    prefix_sumsq_path: str
    prefix_present_count_path: str
    row_count: int
    feature_keys: Tuple[str, ...]
    spec_hash: str
    memory_version: str
    max_decision_date: str
    build_ms: int = 0

    @property
    def feature_index(self) -> dict[str, int]:
        return {key: idx for idx, key in enumerate(self.feature_keys)}

    def load_raw_features(self, *, mmap_mode: str = "r") -> np.ndarray:
        return np.load(self.raw_features_path, mmap_mode=mmap_mode)

    def load_prefix_event_ids(self, *, mmap_mode: str = "r") -> np.ndarray:
        return np.load(self.prefix_event_ids_path, mmap_mode=mmap_mode)

    def load_prefix_sum(self, *, mmap_mode: str = "r") -> np.ndarray:
        return np.load(self.prefix_sum_path, mmap_mode=mmap_mode)

    def load_prefix_sumsq(self, *, mmap_mode: str = "r") -> np.ndarray:
        return np.load(self.prefix_sumsq_path, mmap_mode=mmap_mode)

    def load_prefix_present_count(self, *, mmap_mode: str = "r") -> np.ndarray:
        return np.load(self.prefix_present_count_path, mmap_mode=mmap_mode)


def _feature_flag(name: str, metadata: dict | None, default: bool = False) -> bool:
    meta = metadata or {}
    value = meta.get(name, default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"} if isinstance(value, str) else bool(value)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _json_loads(raw: Any, default):
    if isinstance(raw, (dict, list)):
        return raw
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


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
    bar_anchor_dts_by_symbol: Dict[str, List[datetime]] | None = None,
) -> tuple[Dict[str, float], Dict[str, Dict[str, Any]]]:
    freshness_features: Dict[str, float] = {}
    freshness_summary: Dict[str, Dict[str, Any]] = {}
    anchor_dt = datetime.fromisoformat(str(feature_anchor_ts_utc)) if feature_anchor_ts_utc else None
    if bar_anchor_dts_by_symbol is not None:
        bar_anchor_dts = list(bar_anchor_dts_by_symbol.get(symbol) or [])
    else:
        symbol_bars = list(bars_by_symbol.get(symbol, []))
        bar_anchor_dts = [
            anchor_dt
            for anchor_dt in (
                _bar_anchor_ts_utc(symbol=symbol, bar=bar, session_metadata_by_symbol=session_metadata_by_symbol)
                for bar in symbol_bars
            )
            if anchor_dt is not None
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
        bars_since_update = float(
            bisect_right(bar_anchor_dts, anchor_dt) - bisect_right(bar_anchor_dts, source_dt)
        )
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


def _proxy_scope_key(
    symbol: str,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> str:
    session_metadata = _resolve_session_metadata(symbol, session_metadata_by_symbol)
    exchange_code = getattr(session_metadata, "exchange_code", None) if session_metadata is not None else None
    return str(exchange_code or "__ALL__")


def _proxy_aggregate_add(
    aggregate_by_date: Dict[str, Dict[str, Any]],
    trade_date: str,
    symbol: str,
    bar: HistoricalBar,
) -> None:
    bucket = aggregate_by_date.setdefault(
        trade_date,
        {
            "count": 0,
            "open_sum": 0.0,
            "high_sum": 0.0,
            "low_sum": 0.0,
            "close_sum": 0.0,
            "volume_sum": 0.0,
            "symbols": [],
        },
    )
    bucket["count"] += 1
    bucket["open_sum"] += float(bar.open)
    bucket["high_sum"] += float(bar.high)
    bucket["low_sum"] += float(bar.low)
    bucket["close_sum"] += float(bar.close)
    bucket["volume_sum"] += float(bar.volume or 0.0)
    bucket["symbols"].append(str(symbol))


def _proxy_bar_from_bucket(
    *,
    proxy_symbol: str,
    trade_date: str,
    bucket: Mapping[str, Any],
    subtract_bar: HistoricalBar | None = None,
) -> tuple[HistoricalBar | None, int]:
    count = int(bucket.get("count") or 0)
    open_sum = float(bucket.get("open_sum") or 0.0)
    high_sum = float(bucket.get("high_sum") or 0.0)
    low_sum = float(bucket.get("low_sum") or 0.0)
    close_sum = float(bucket.get("close_sum") or 0.0)
    volume_sum = float(bucket.get("volume_sum") or 0.0)
    if subtract_bar is not None:
        count -= 1
        open_sum -= float(subtract_bar.open)
        high_sum -= float(subtract_bar.high)
        low_sum -= float(subtract_bar.low)
        close_sum -= float(subtract_bar.close)
        volume_sum -= float(subtract_bar.volume or 0.0)
    if count <= 0:
        return None, 0
    return (
        HistoricalBar(
            symbol=proxy_symbol,
            timestamp=trade_date,
            open=open_sum / count,
            high=high_sum / count,
            low=low_sum / count,
            close=close_sum / count,
            volume=volume_sum / count,
        ),
        count,
    )


def _active_scope_count(first_dates: Sequence[str], cutoff_date: str | None, fallback_count: int) -> int:
    if not first_dates:
        return int(fallback_count)
    if not cutoff_date:
        return len(first_dates)
    active = bisect_right(list(first_dates), str(cutoff_date))
    return active if active > 0 else int(fallback_count)


def _build_proxy_aggregate_cache(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    sector_map: Dict[str, str],
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> Dict[str, Any]:
    symbol_bar_by_date: Dict[str, Dict[str, HistoricalBar]] = {}
    symbol_first_date: Dict[str, str] = {}
    market_scope_symbols: Dict[str, List[str]] = defaultdict(list)
    sector_scope_symbols: Dict[tuple[str, str], List[str]] = defaultdict(list)
    market_aggregate_by_scope: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    sector_aggregate_by_scope: Dict[tuple[str, str], Dict[str, Dict[str, Any]]] = defaultdict(dict)

    for symbol, bars in bars_by_symbol.items():
        exchange_scope = _proxy_scope_key(symbol, session_metadata_by_symbol)
        market_scope_symbols[exchange_scope].append(symbol)
        sector_code = str(sector_map.get(symbol) or "").strip()
        if sector_code:
            sector_scope_symbols[(sector_code, exchange_scope)].append(symbol)
        date_map: Dict[str, HistoricalBar] = {}
        first_trade_date: str | None = None
        for bar in bars:
            trade_date = _trade_date(bar)
            date_map[trade_date] = bar
            if first_trade_date is None or trade_date < first_trade_date:
                first_trade_date = trade_date
            _proxy_aggregate_add(market_aggregate_by_scope[exchange_scope], trade_date, symbol, bar)
            if sector_code:
                _proxy_aggregate_add(sector_aggregate_by_scope[(sector_code, exchange_scope)], trade_date, symbol, bar)
        symbol_bar_by_date[symbol] = date_map
        if first_trade_date is not None:
            symbol_first_date[symbol] = first_trade_date

    for aggregate_by_scope in list(market_aggregate_by_scope.values()) + list(sector_aggregate_by_scope.values()):
        for bucket in aggregate_by_scope.values():
            bucket["symbols"] = sorted(str(item) for item in list(bucket.get("symbols") or []))

    return {
        "symbol_bar_by_date": symbol_bar_by_date,
        "symbol_first_date": symbol_first_date,
        "market_scope_symbols": {key: sorted(values) for key, values in market_scope_symbols.items()},
        "sector_scope_symbols": {key: sorted(values) for key, values in sector_scope_symbols.items()},
        "market_aggregate_by_scope": dict(market_aggregate_by_scope),
        "sector_aggregate_by_scope": dict(sector_aggregate_by_scope),
        "market_dates_by_scope": {key: sorted(value.keys()) for key, value in market_aggregate_by_scope.items()},
        "sector_dates_by_scope": {key: sorted(value.keys()) for key, value in sector_aggregate_by_scope.items()},
        "market_first_dates_by_scope": {
            key: sorted(symbol_first_date[symbol] for symbol in values if symbol in symbol_first_date)
            for key, values in market_scope_symbols.items()
        },
        "sector_first_dates_by_scope": {
            key: sorted(symbol_first_date[symbol] for symbol in values if symbol in symbol_first_date)
            for key, values in sector_scope_symbols.items()
        },
    }


def _cached_market_proxy_series(
    *,
    symbol: str,
    history_window: Sequence[HistoricalBar],
    cutoff_date: str,
    proxy_cache: Mapping[str, Any],
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> ProxySeriesResult:
    history_window_dates = [str(bar.timestamp)[:10] for bar in history_window]
    scope_key = _proxy_scope_key(symbol, session_metadata_by_symbol)
    aggregate_by_date = dict((proxy_cache.get("market_aggregate_by_scope") or {}).get(scope_key) or {})
    scope_dates = list((proxy_cache.get("market_dates_by_scope") or {}).get(scope_key) or [])
    scope_symbols = list((proxy_cache.get("market_scope_symbols") or {}).get(scope_key) or [])
    first_dates = list((proxy_cache.get("market_first_dates_by_scope") or {}).get(scope_key) or [])
    if not aggregate_by_date:
        return ProxySeriesResult(
            bars=[],
            peer_count_by_date={},
            contributing_symbols_by_date={},
            fallback_to_self=False,
            proxy_mode="session_aware_same_exchange" if session_metadata_by_symbol else "date_aligned",
            same_exchange_peer_count=max(0, len(scope_symbols)),
            cross_exchange_proxy_used=False,
        )
    cutoff_index = bisect_right(scope_dates, str(cutoff_date))
    selected_dates = scope_dates[:cutoff_index][-21:]
    bars: List[HistoricalBar] = []
    for trade_date in selected_dates:
        proxy_bar, _count = _proxy_bar_from_bucket(
            proxy_symbol="MKT",
            trade_date=trade_date,
            bucket=aggregate_by_date[trade_date],
        )
        if proxy_bar is not None:
            bars.append(proxy_bar)
    peer_count_by_date: Dict[str, int] = {}
    contributing_symbols_by_date: Dict[str, List[str]] = {}
    for trade_date in history_window_dates:
        bucket = aggregate_by_date.get(trade_date)
        if not bucket:
            continue
        peer_count_by_date[trade_date] = int(bucket.get("count") or 0)
        contributing_symbols_by_date[trade_date] = list(bucket.get("symbols") or [])
    return ProxySeriesResult(
        bars=bars,
        peer_count_by_date=peer_count_by_date,
        contributing_symbols_by_date=contributing_symbols_by_date,
        fallback_to_self=False,
        proxy_mode="session_aware_same_exchange" if session_metadata_by_symbol else "date_aligned",
        same_exchange_peer_count=_active_scope_count(first_dates, cutoff_date, len(scope_symbols)),
        cross_exchange_proxy_used=False,
    )


def _cached_sector_proxy_series(
    *,
    symbol: str,
    history_window: Sequence[HistoricalBar],
    cutoff_date: str,
    sector_map: Dict[str, str],
    proxy_cache: Mapping[str, Any],
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> ProxySeriesResult:
    history_window_dates = [str(bar.timestamp)[:10] for bar in history_window]
    exchange_scope = _proxy_scope_key(symbol, session_metadata_by_symbol)
    sector_code = str(sector_map.get(symbol) or "").strip()
    symbol_bar_by_date = dict((proxy_cache.get("symbol_bar_by_date") or {}).get(symbol) or {})
    symbol_history_dates = sorted(symbol_bar_by_date.keys())
    if not sector_code:
        fallback_bars = [
            symbol_bar_by_date[trade_date]
            for trade_date in symbol_history_dates[: bisect_right(symbol_history_dates, str(cutoff_date))][-21:]
            if trade_date in symbol_bar_by_date
        ]
        return ProxySeriesResult(
            bars=fallback_bars,
            peer_count_by_date={trade_date: 1 for trade_date in history_window_dates if trade_date in symbol_bar_by_date},
            contributing_symbols_by_date={trade_date: [symbol] for trade_date in history_window_dates if trade_date in symbol_bar_by_date},
            fallback_to_self=True,
            proxy_mode="session_aware_same_exchange" if session_metadata_by_symbol else "date_aligned",
            same_exchange_peer_count=1 if fallback_bars else 0,
            cross_exchange_proxy_used=False,
        )
    scope_key = (sector_code, exchange_scope)
    scope_symbols = [peer for peer in list((proxy_cache.get("sector_scope_symbols") or {}).get(scope_key) or []) if peer != symbol]
    aggregate_by_date = dict((proxy_cache.get("sector_aggregate_by_scope") or {}).get(scope_key) or {})
    scope_dates = list((proxy_cache.get("sector_dates_by_scope") or {}).get(scope_key) or [])
    first_dates = sorted(
        str((proxy_cache.get("symbol_first_date") or {}).get(peer))
        for peer in scope_symbols
        if (proxy_cache.get("symbol_first_date") or {}).get(peer)
    )
    if not scope_symbols:
        fallback_bars = [
            symbol_bar_by_date[trade_date]
            for trade_date in symbol_history_dates[: bisect_right(symbol_history_dates, str(cutoff_date))][-21:]
            if trade_date in symbol_bar_by_date
        ]
        return ProxySeriesResult(
            bars=fallback_bars,
            peer_count_by_date={trade_date: 1 for trade_date in history_window_dates if trade_date in symbol_bar_by_date},
            contributing_symbols_by_date={trade_date: [symbol] for trade_date in history_window_dates if trade_date in symbol_bar_by_date},
            fallback_to_self=True,
            proxy_mode="session_aware_same_exchange" if session_metadata_by_symbol else "date_aligned",
            same_exchange_peer_count=1 if fallback_bars else 0,
            cross_exchange_proxy_used=False,
        )
    cutoff_index = bisect_right(scope_dates, str(cutoff_date))
    bars: List[HistoricalBar] = []
    for trade_date in reversed(scope_dates[:cutoff_index]):
        proxy_bar, peer_count = _proxy_bar_from_bucket(
            proxy_symbol=f"SECTOR:{sector_code}",
            trade_date=trade_date,
            bucket=aggregate_by_date[trade_date],
            subtract_bar=symbol_bar_by_date.get(trade_date),
        )
        if proxy_bar is None or peer_count <= 0:
            continue
        bars.append(proxy_bar)
        if len(bars) >= 21:
            break
    bars.reverse()
    peer_count_by_date: Dict[str, int] = {}
    contributing_symbols_by_date: Dict[str, List[str]] = {}
    for trade_date in history_window_dates:
        bucket = aggregate_by_date.get(trade_date)
        if not bucket:
            continue
        peer_count = int(bucket.get("count") or 0) - (1 if trade_date in symbol_bar_by_date else 0)
        if peer_count <= 0:
            continue
        peer_count_by_date[trade_date] = peer_count
        contributing_symbols_by_date[trade_date] = [peer for peer in list(bucket.get("symbols") or []) if peer != symbol]
    return ProxySeriesResult(
        bars=bars,
        peer_count_by_date=peer_count_by_date,
        contributing_symbols_by_date=contributing_symbols_by_date,
        fallback_to_self=False,
        proxy_mode="session_aware_same_exchange" if session_metadata_by_symbol else "date_aligned",
        same_exchange_peer_count=_active_scope_count(first_dates, cutoff_date, len(scope_symbols)),
        cross_exchange_proxy_used=False,
    )


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


def build_query_feature_payload_asof(*, symbol: str, bars: List[HistoricalBar], bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history: Dict[str, Dict[str, float]], sector_map: Dict[str, str], cutoff_date: str | None, spec: ResearchExperimentSpec | None = None, use_macro_level_in_similarity: bool = False, use_dollar_volume_absolute: bool = False, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None, market_proxy_override: ProxySeriesResult | None = None, sector_proxy_override: ProxySeriesResult | None = None) -> dict:
    sector_code = sector_map.get(symbol)
    shape_horizons = list((spec.lookback_horizons if spec and spec.lookback_horizons else [spec.horizon_days] if spec else []) or [])
    session_date_local = cutoff_date or (str(bars[-1].timestamp)[:10] if bars else None)
    anchor_fields = _anchor_fields_for_symbol_date(
        symbol=symbol,
        session_date_local=str(session_date_local),
        session_metadata_by_symbol=session_metadata_by_symbol,
    ) if session_date_local else {}
    feature_anchor_ts_utc = anchor_fields.get("feature_anchor_ts_utc")
    market_proxy = market_proxy_override or _market_proxy_series(
        bars_by_symbol,
        cutoff_date=cutoff_date,
        focus_symbol=symbol,
        session_metadata_by_symbol=session_metadata_by_symbol,
        cutoff_anchor_ts_utc=feature_anchor_ts_utc,
    )
    sector_proxy = sector_proxy_override or _sector_proxy_series(
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
    raw_payload = build_raw_multiscale_feature_payload(
        symbol=symbol,
        bars=bars,
        market_bars=market_proxy.bars,
        sector_bars=sector_proxy.bars,
        macro_history=macro_window,
        sector_code=sector_code,
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
    regime_inputs_summary = _regime_inputs_summary(raw_payload.normalized_regime_context_features)
    regime_code = _regime_from_context_features(raw_payload.normalized_regime_context_features)
    regime_code_raw_macro = _regime_from_macro_raw(latest_macro_payload)
    meta = {
        "raw_shape_features": raw_payload.shape_features,
        "raw_residual_features": raw_payload.residual_features,
        "raw_context_features": raw_payload.context_features,
        "raw_regime_context_features": raw_payload.regime_context_features,
        "normalized_regime_context_features": raw_payload.normalized_regime_context_features,
        "raw_features": raw_payload.raw_features,
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
        **raw_payload.metadata,
    }
    return {
        "raw_payload": raw_payload,
        "meta": meta,
    }


def build_query_embedding(*, symbol: str, bars: List[HistoricalBar], bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history: Dict[str, Dict[str, float]], sector_map: Dict[str, str], cutoff_date: str | None, spec: ResearchExperimentSpec | None = None, scaler=None, transform=None, use_macro_level_in_similarity: bool = False, use_dollar_volume_absolute: bool = False, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None) -> tuple[list[float], dict]:
    payload = build_query_feature_payload_asof(
        symbol=symbol,
        bars=bars,
        bars_by_symbol=bars_by_symbol,
        macro_history=macro_history,
        sector_map=sector_map,
        cutoff_date=cutoff_date,
        spec=spec,
        use_macro_level_in_similarity=use_macro_level_in_similarity,
        use_dollar_volume_absolute=use_dollar_volume_absolute,
        session_metadata_by_symbol=session_metadata_by_symbol,
        macro_series_history=macro_series_history,
    )
    raw_payload = payload["raw_payload"]
    meta = dict(payload["meta"])
    resolved_transform = transform
    resolved_scaler = scaler or (transform.scaler if transform is not None else None)
    if resolved_transform is None and resolved_scaler is not None:
        resolved_transform = FeatureTransform(scaler=resolved_scaler, feature_keys=sorted(raw_payload.raw_features.keys()))
    if resolved_transform is None:
        resolved_transform = identity_feature_transform(raw_payload.raw_features)
    transformed_features, embedding = resolved_transform.apply(raw_payload.raw_features)
    shape_keys = sorted([k for k in raw_payload.shape_features.keys() if k in transformed_features])
    residual_keys = sorted([k for k in raw_payload.residual_features.keys() if k in transformed_features])
    ctx_keys = sorted([k for k in raw_payload.context_features.keys() if k in transformed_features])
    meta.update(
        {
            "shape_features": {k: float(transformed_features[k]) for k in shape_keys},
            "residual_features": {k: float(transformed_features[k]) for k in residual_keys},
            "context_features": {k: float(transformed_features[k]) for k in ctx_keys},
            "regime_context_features": {k: float(raw_payload.regime_context_features[k]) for k in sorted(raw_payload.regime_context_features.keys())},
            "raw_features": {k: float(raw_payload.raw_features[k]) for k in sorted(raw_payload.raw_features.keys())},
            "transformed_features": {k: float(transformed_features[k]) for k in resolved_transform.feature_keys},
            "shape_vector": [float(transformed_features[k]) for k in shape_keys + residual_keys],
            "ctx_vector": [float(transformed_features[k]) for k in ctx_keys],
            "transform_version": resolved_transform.version,
            "feature_keys": list(resolved_transform.feature_keys),
            "transform_missing_keys_filled_zero": sorted(key for key in resolved_transform.feature_keys if key not in raw_payload.raw_features),
            "transformed_zero_feature_keys": sorted(key for key, value in transformed_features.items() if abs(float(value)) <= 1e-12),
        }
    )
    return embedding, meta


def _topk(scores: List[CandidateScore], k: int) -> List[dict]:
    return [asdict(s) for s in scores[:k]]


def _side_diag(ev, surface, side: str) -> dict:
    diagnostics = dict(getattr(ev, "diagnostics", {}) or {})
    utility = dict(diagnostics.get("ev_decomposition") or getattr(ev, "utility", {}) or {})
    telemetry = dict(diagnostics.get("telemetry") or {})
    interval = dict(diagnostics.get("interval") or {
        "q10": getattr(ev, "q10_return", 0.0),
        "q50": getattr(ev, "q50_return", 0.0),
        "q90": getattr(ev, "q90_return", 0.0),
    })
    top_matches = list(getattr(ev, "top_matches", []) or [])
    member_top_matches = list(diagnostics.get("member_top_matches") or utility.get("member_top_matches") or [])
    support_counts = [float(((m or {}).get("why") or {}).get("support", 0.0) or 0.0) for m in top_matches]
    prototype_summary = []
    for match in top_matches[:3]:
        why = dict((match or {}).get("why") or {})
        prototype_summary.append({
            "prototype_id": match.get("prototype_id"),
            "representative_hash": match.get("representative_hash"),
            "representative_symbol": match.get("representative_symbol"),
            "weight": match.get("weight"),
            "weight_share": match.get("weight_share"),
            "similarity": why.get("similarity"),
            "support": why.get("support"),
            "expected_return": match.get("expected_return"),
            "uncertainty": match.get("uncertainty"),
        })
    member_summary = []
    for match in member_top_matches[:3]:
        member_summary.append({
            "member_key": match.get("member_key"),
            "prototype_id": match.get("prototype_id"),
            "representative_hash": match.get("representative_hash"),
            "symbol": match.get("symbol"),
            "event_date": match.get("event_date"),
            "weight": match.get("weight"),
            "weight_share": match.get("weight_share"),
            "similarity": match.get("similarity"),
            "support": match.get("support"),
            "expected_return": match.get("expected_return"),
            "q50_d2_return": match.get("q50_d2_return"),
            "q50_d3_return": match.get("q50_d3_return"),
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
        "q50_d2_return": utility.get("q50_d2_return", 0.0),
        "q50_d3_return": utility.get("q50_d3_return", 0.0),
        "p_resolved_by_d2": utility.get("p_resolved_by_d2", 0.0),
        "p_resolved_by_d3": utility.get("p_resolved_by_d3", 0.0),
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
        "prototype_pool_size": telemetry.get("prototype_pool_size", (surface.diagnostics.get("prototype_pool_size") if hasattr(surface, "diagnostics") else None)),
        "ranked_candidate_count": telemetry.get("ranked_candidate_count", 0),
        "positive_weight_candidate_count": telemetry.get("positive_weight_candidate_count", 0),
        "pre_truncation_candidate_count": telemetry.get("pre_truncation_candidate_count", 0),
        "top1_weight_share": telemetry.get("top1_weight_share", 0.0),
        "cumulative_weight_top3": telemetry.get("cumulative_weight_top3", 0.0),
        "mixture_ess": telemetry.get("mixture_ess", getattr(ev, "effective_sample_size", 0.0)),
        "member_support_sum": telemetry.get("member_support_sum", float(sum(support_counts))),
        "consensus_signature": telemetry.get("consensus_signature", ""),
        "member_candidate_count": telemetry.get("member_candidate_count", 0),
        "member_pre_truncation_count": telemetry.get("member_pre_truncation_count", 0),
        "positive_weight_member_count": telemetry.get("positive_weight_member_count", 0),
        "member_top1_weight_share": telemetry.get("member_top1_weight_share", 0.0),
        "member_cumulative_weight_top3": telemetry.get("member_cumulative_weight_top3", 0.0),
        "member_mixture_ess": telemetry.get("member_mixture_ess", getattr(ev, "effective_sample_size", 0.0)),
        "member_consensus_signature": telemetry.get("member_consensus_signature", ""),
        "top_matches_summary": prototype_summary,
        "member_top_matches_summary": member_summary,
        "side_stats_summary": {
            "match_count": len(top_matches),
            "prototype_ids": [m.get("prototype_id") for m in top_matches[:3]],
            "representative_hashes": [m.get("representative_hash") for m in top_matches[:3]],
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
        "q50_d2_return": chosen_diag.get("q50_d2_return"),
        "q50_d3_return": chosen_diag.get("q50_d3_return"),
        "p_resolved_by_d2": chosen_diag.get("p_resolved_by_d2"),
        "p_resolved_by_d3": chosen_diag.get("p_resolved_by_d3"),
        "expected_mae": chosen_diag.get("expected_mae"),
        "expected_mfe": chosen_diag.get("expected_mfe"),
        "effective_sample_size": chosen_diag.get("n_eff"),
        "uncertainty": chosen_diag.get("uncertainty"),
        "regime_alignment": chosen_diag.get("regime_alignment"),
        "fill_probability_proxy": chosen_diag.get("p_target"),
        "lower_bound": decision_rule.get("chosen_lower_bound", chosen_diag.get("lower_bound")),
        "interval_width": decision_rule.get("chosen_interval_width", chosen_diag.get("interval_width")),
        "prototype_pool_size": chosen_diag.get("prototype_pool_size"),
        "ranked_candidate_count": chosen_diag.get("ranked_candidate_count"),
        "positive_weight_candidate_count": chosen_diag.get("positive_weight_candidate_count"),
        "pre_truncation_candidate_count": chosen_diag.get("pre_truncation_candidate_count"),
        "top1_weight_share": chosen_diag.get("top1_weight_share"),
        "cumulative_weight_top3": chosen_diag.get("cumulative_weight_top3"),
        "mixture_ess": chosen_diag.get("mixture_ess"),
        "member_support_sum": chosen_diag.get("member_support_sum"),
        "consensus_signature": chosen_diag.get("consensus_signature"),
        "member_candidate_count": chosen_diag.get("member_candidate_count"),
        "member_pre_truncation_count": chosen_diag.get("member_pre_truncation_count"),
        "positive_weight_member_count": chosen_diag.get("positive_weight_member_count"),
        "member_top1_weight_share": chosen_diag.get("member_top1_weight_share"),
        "member_cumulative_weight_top3": chosen_diag.get("member_cumulative_weight_top3"),
        "member_mixture_ess": chosen_diag.get("member_mixture_ess"),
        "member_consensus_signature": chosen_diag.get("member_consensus_signature"),
        "abstain_reasons": list(getattr(surface, "abstain_reasons", []) or []),
    }


def _label_cfg(spec: ResearchExperimentSpec) -> EventLabelingConfig:
    return EventLabelingConfig(target_return_pct=spec.target_return_pct, stop_return_pct=spec.stop_return_pct, horizon_days=spec.horizon_days, fee_bps=spec.fee_bps, slippage_bps=spec.slippage_bps, flat_return_band_pct=spec.flat_return_band_pct)


def _macro_history_until(macro_history_by_date: Dict[str, Dict[str, float]], feature_end_date: str) -> Dict[str, Dict[str, float]]:
    return {k: dict(v) for k, v in sorted(macro_history_by_date.items()) if k <= feature_end_date}


def _write_pickle_atomic(path: str | Path, payload: Any) -> str:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = resolved.with_suffix(resolved.suffix + ".tmp")
    with tmp_path.open("wb") as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    tmp_path.replace(resolved)
    return str(resolved)


def _load_pickle_payload(path: str | Path) -> Any:
    with Path(path).open("rb") as handle:
        return pickle.load(handle)


def _prototype_input_payload(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    event_records: Sequence[EventOutcomeRecord],
    excluded_reasons: Sequence[Mapping[str, Any]],
    anchor_count: int,
    transform: FeatureTransform,
    phase_timings_ms: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "as_of_date": str(decision_date),
        "spec_hash": spec.spec_hash(),
        "memory_version": spec.memory_version,
        "event_records": [asdict(record) for record in event_records],
        "excluded_reasons": [dict(row) for row in excluded_reasons],
        "anchor_count": int(anchor_count),
        "transform": transform.to_payload(),
        "phase_timings_ms": {
            "event_memory": int((phase_timings_ms or {}).get("event_memory") or 0),
            "transform": int((phase_timings_ms or {}).get("transform") or 0),
        },
    }


def _load_prototype_input_payload(*, path: str | Path, decision_date: str, spec: ResearchExperimentSpec) -> dict[str, Any] | None:
    resolved = Path(path)
    if not resolved.exists():
        return None
    payload = _load_pickle_payload(resolved)
    if str(payload.get("as_of_date") or "") != str(decision_date):
        return None
    if str(payload.get("spec_hash") or "") != spec.spec_hash():
        return None
    if str(payload.get("memory_version") or "") != str(spec.memory_version):
        return None
    event_records = [EventOutcomeRecord(**dict(row)) for row in list(payload.get("event_records") or [])]
    transform = FeatureTransform.from_payload(payload.get("transform") or {})
    return {
        "event_records": event_records,
        "excluded_reasons": [dict(row) for row in list(payload.get("excluded_reasons") or [])],
        "anchor_count": int(payload.get("anchor_count") or 0),
        "transform": transform,
        "scaler": transform.scaler,
        "phase_timings_ms": {
            "event_memory": int(((payload.get("phase_timings_ms") or {}).get("event_memory")) or 0),
            "transform": int(((payload.get("phase_timings_ms") or {}).get("transform")) or 0),
        },
    }


def _prototype_resume_metadata_path(checkpoint_path: str | Path) -> Path:
    return Path(checkpoint_path).with_name("prototype_resume_metadata.json")


def _prototype_resume_metadata_payload(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    excluded_reasons: Sequence[Mapping[str, Any]],
    anchor_count: int,
    transform: FeatureTransform,
    phase_timings_ms: Mapping[str, Any],
    coverage: Mapping[str, Any],
    compression_audit: Mapping[str, Any],
    max_train_date: str | None,
    max_outcome_end_date: str | None,
) -> dict[str, Any]:
    return {
        "as_of_date": str(decision_date),
        "spec_hash": spec.spec_hash(),
        "memory_version": spec.memory_version,
        "excluded_reasons": [dict(row) for row in excluded_reasons],
        "anchor_count": int(anchor_count),
        "transform": transform.to_payload(),
        "phase_timings_ms": {
            "event_memory": int((phase_timings_ms or {}).get("event_memory") or 0),
            "transform": int((phase_timings_ms or {}).get("transform") or 0),
            "prototype": int((phase_timings_ms or {}).get("prototype") or 0),
            "artifact_write": int((phase_timings_ms or {}).get("artifact_write") or 0),
        },
        "coverage": dict(coverage or {}),
        "compression_audit": dict(compression_audit or {}),
        "max_train_date": max_train_date,
        "max_outcome_end_date": max_outcome_end_date,
    }


def _load_prototype_resume_metadata(*, checkpoint_path: str | Path, decision_date: str, spec: ResearchExperimentSpec) -> dict[str, Any] | None:
    resolved = _prototype_resume_metadata_path(checkpoint_path)
    if not resolved.exists():
        return None
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if str(payload.get("as_of_date") or "") != str(decision_date):
        return None
    if str(payload.get("spec_hash") or "") != spec.spec_hash():
        return None
    if str(payload.get("memory_version") or "") != str(spec.memory_version):
        return None
    transform = FeatureTransform.from_payload(payload.get("transform") or {})
    return {
        "excluded_reasons": [dict(row) for row in list(payload.get("excluded_reasons") or [])],
        "anchor_count": int(payload.get("anchor_count") or 0),
        "transform": transform,
        "scaler": transform.scaler,
        "phase_timings_ms": {
            "event_memory": int(((payload.get("phase_timings_ms") or {}).get("event_memory")) or 0),
            "transform": int(((payload.get("phase_timings_ms") or {}).get("transform")) or 0),
            "prototype": int(((payload.get("phase_timings_ms") or {}).get("prototype")) or 0),
            "artifact_write": int(((payload.get("phase_timings_ms") or {}).get("artifact_write")) or 0),
        },
        "coverage": dict(payload.get("coverage") or {}),
        "compression_audit": dict(payload.get("compression_audit") or {}),
        "max_train_date": payload.get("max_train_date"),
        "max_outcome_end_date": payload.get("max_outcome_end_date"),
    }


def _event_input_payload(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    candidate_refs: Sequence[Mapping[str, Any]],
    excluded_reasons: Sequence[Mapping[str, Any]],
    phase_timings_ms: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "as_of_date": str(decision_date),
        "spec_hash": spec.spec_hash(),
        "memory_version": spec.memory_version,
        "candidate_refs": [dict(row) for row in candidate_refs],
        "excluded_reasons": [dict(row) for row in excluded_reasons],
        "phase_timings_ms": {
            "event_candidate_prep": int((phase_timings_ms or {}).get("event_candidate_prep") or 0),
            "event_payload_build": int((phase_timings_ms or {}).get("event_payload_build") or 0),
            "event_memory": int((phase_timings_ms or {}).get("event_memory") or 0),
        },
    }


def _load_event_input_payload(*, path: str | Path, decision_date: str, spec: ResearchExperimentSpec) -> dict[str, Any] | None:
    resolved = Path(path)
    if not resolved.exists():
        return None
    payload = _load_pickle_payload(resolved)
    if str(payload.get("as_of_date") or "") != str(decision_date):
        return None
    if str(payload.get("spec_hash") or "") != spec.spec_hash():
        return None
    if str(payload.get("memory_version") or "") != str(spec.memory_version):
        return None
    return {
        "candidate_refs": [dict(row) for row in list(payload.get("candidate_refs") or [])],
        "excluded_reasons": [dict(row) for row in list(payload.get("excluded_reasons") or [])],
        "phase_timings_ms": {
            "event_candidate_prep": int(((payload.get("phase_timings_ms") or {}).get("event_candidate_prep")) or 0),
            "event_payload_build": int(((payload.get("phase_timings_ms") or {}).get("event_payload_build")) or 0),
            "event_memory": int(((payload.get("phase_timings_ms") or {}).get("event_memory")) or 0),
        },
    }


def _event_batch_dir_from_checkpoint_path(event_checkpoint_path: str | Path | None) -> Path | None:
    if not event_checkpoint_path:
        return None
    return Path(event_checkpoint_path).with_name("event_memory_batches")


def _event_batch_file(batch_dir: Path, batch_index: int) -> Path:
    return batch_dir / f"batch_{int(batch_index):06d}.pkl"


def _write_event_batch(*, batch_dir: Path, batch_index: int, pending_records: Sequence[Mapping[str, Any]]) -> str:
    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_path = _event_batch_file(batch_dir, batch_index)
    _write_pickle_atomic(batch_path, [dict(record) for record in pending_records])
    return str(batch_path)


def _load_event_batch(path: str | Path) -> list[dict[str, Any]]:
    payload = _load_pickle_payload(path)
    return [dict(row) for row in list(payload or [])]


def _event_checkpoint_payload(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    next_candidate_index: int,
    batch_paths: Sequence[str],
    phase_timings_ms: Mapping[str, Any],
    raw_event_row_count: int,
    pending_record_count: int,
) -> dict[str, Any]:
    return {
        "payload_version": "event_memory_checkpoint_v3",
        "as_of_date": str(decision_date),
        "spec_hash": spec.spec_hash(),
        "memory_version": spec.memory_version,
        "next_candidate_index": int(next_candidate_index),
        "batch_paths": [str(path) for path in batch_paths],
        "raw_event_row_count": int(raw_event_row_count),
        "pending_record_count": int(pending_record_count),
        "phase_timings_ms": {
            "event_candidate_prep": int((phase_timings_ms or {}).get("event_candidate_prep") or 0),
            "event_payload_build": int((phase_timings_ms or {}).get("event_payload_build") or 0),
            "event_memory": int((phase_timings_ms or {}).get("event_memory") or 0),
        },
    }


def _load_event_checkpoint_payload(*, path: str | Path, decision_date: str, spec: ResearchExperimentSpec) -> dict[str, Any] | None:
    resolved = Path(path)
    if not resolved.exists():
        return None
    payload = _load_pickle_payload(resolved)
    if str(payload.get("payload_version") or "") != "event_memory_checkpoint_v3":
        return None
    if str(payload.get("as_of_date") or "") != str(decision_date):
        return None
    if str(payload.get("spec_hash") or "") != spec.spec_hash():
        return None
    if str(payload.get("memory_version") or "") != str(spec.memory_version):
        return None
    return {
        "next_candidate_index": int(payload.get("next_candidate_index") or 0),
        "batch_paths": [str(path) for path in list(payload.get("batch_paths") or [])],
        "raw_event_row_count": int(payload.get("raw_event_row_count") or 0),
        "pending_record_count": int(payload.get("pending_record_count") or 0),
        "phase_timings_ms": {
            "event_candidate_prep": int(((payload.get("phase_timings_ms") or {}).get("event_candidate_prep")) or 0),
            "event_payload_build": int(((payload.get("phase_timings_ms") or {}).get("event_payload_build")) or 0),
            "event_memory": int(((payload.get("phase_timings_ms") or {}).get("event_memory")) or 0),
        },
    }


def _write_numpy_atomic(path: str | Path, payload: np.ndarray) -> str:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = resolved.with_suffix(resolved.suffix + ".tmp")
    with tmp_path.open("wb") as handle:
        np.save(handle, np.asarray(payload))
    tmp_path.replace(resolved)
    return str(resolved)


def _open_numpy_memmap_atomic(
    path: str | Path,
    *,
    dtype: Any,
    shape: tuple[int, ...],
) -> tuple[np.memmap, Path, Path]:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = resolved.with_suffix(resolved.suffix + ".tmp")
    memmap = np.lib.format.open_memmap(tmp_path, mode="w+", dtype=dtype, shape=shape)
    return memmap, tmp_path, resolved


def _finalize_numpy_memmap_atomic(memmap: np.memmap | None, tmp_path: Path, resolved: Path) -> str:
    if memmap is not None:
        memmap.flush()
        backing_mmap = getattr(memmap, "_mmap", None)
        if backing_mmap is not None:
            backing_mmap.close()
        del memmap
    tmp_path.replace(resolved)
    return str(resolved)


def _event_raw_cache_dir(*, output_dir: str, run_id: str) -> Path:
    return Path(output_dir) / run_id / "event_raw_cache_v2"


def _event_raw_cache_manifest_path(*, output_dir: str, run_id: str) -> Path:
    return _event_raw_cache_dir(output_dir=output_dir, run_id=run_id) / "manifest.json"


def _event_cache_storage_row(
    *,
    event_ordinal: int,
    symbol: str,
    feature_end_date: str,
    outcome_end_date: str,
    lib_sector: str | None,
    regime_code: str,
    regime_code_raw_macro: str,
    regime_inputs_summary: Mapping[str, Any],
    macro_history_length: int,
    macro_series_present_count: int,
    present_feature_keys: Sequence[str],
    shape_keys: Sequence[str],
    ctx_keys: Sequence[str],
    raw_regime_context_features: Mapping[str, float],
    normalized_regime_context_features: Mapping[str, float],
    proxy_diagnostics: Mapping[str, Any],
    raw_zero_default_keys: Sequence[str],
    liquidity_score: float,
    anchor_fields: Mapping[str, Any],
    macro_asof_ts_utc: str | None,
    macro_freshness_summary: Mapping[str, Any],
    breadth_policy: str | None,
    breadth_present: bool,
    breadth_missing_reason: str | None,
    event_path_summary: Mapping[str, Any],
    event_path_label: str,
    event_side_payload: Mapping[str, Any],
    event_diagnostics: Mapping[str, Any],
    event_quality_score: float,
) -> dict[str, Any]:
    return {
        "event_ordinal": int(event_ordinal),
        "symbol": str(symbol),
        "feature_end_date": str(feature_end_date),
        "outcome_end_date": str(outcome_end_date),
        "lib_sector": str(lib_sector or ""),
        "regime_code": str(regime_code),
        "regime_code_raw_macro": str(regime_code_raw_macro),
        "regime_inputs_summary_json": _json_dumps(dict(regime_inputs_summary or {})),
        "macro_history_length": int(macro_history_length or 0),
        "macro_series_present_count": int(macro_series_present_count or 0),
        "present_feature_keys_json": _json_dumps(list(present_feature_keys or [])),
        "shape_keys_json": _json_dumps(list(shape_keys or [])),
        "ctx_keys_json": _json_dumps(list(ctx_keys or [])),
        "raw_regime_context_features_json": _json_dumps(dict(raw_regime_context_features or {})),
        "normalized_regime_context_features_json": _json_dumps(dict(normalized_regime_context_features or {})),
        "proxy_diagnostics_json": _json_dumps(dict(proxy_diagnostics or {})),
        "raw_zero_default_keys_json": _json_dumps(list(raw_zero_default_keys or [])),
        "liquidity_score": float(liquidity_score or 0.0),
        "anchor_fields_json": _json_dumps(dict(anchor_fields or {})),
        "macro_asof_ts_utc": macro_asof_ts_utc,
        "macro_freshness_summary_json": _json_dumps(dict(macro_freshness_summary or {})),
        "breadth_policy": str(breadth_policy or ""),
        "breadth_present": bool(breadth_present),
        "breadth_missing_reason": str(breadth_missing_reason or ""),
        "event_path_summary_json": _json_dumps(dict(event_path_summary or {})),
        "event_path_label": str(event_path_label or ""),
        "event_side_payload_json": _json_dumps(dict(event_side_payload or {})),
        "event_diagnostics_json": _json_dumps(dict(event_diagnostics or {})),
        "event_quality_score": float(event_quality_score or 0.0),
    }


def _event_cache_stage_row(
    *,
    raw_features: Mapping[str, float],
    **kwargs: Any,
) -> dict[str, Any]:
    payload = _event_cache_storage_row(
        present_feature_keys=sorted(str(key) for key in dict(raw_features or {}).keys()),
        **kwargs,
    )
    payload["raw_features_json"] = _json_dumps(dict(raw_features or {}))
    return payload


def _event_cache_row_from_storage(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(raw)
    payload["event_ordinal"] = int(payload.get("event_ordinal") or 0)
    payload["macro_history_length"] = int(payload.get("macro_history_length") or 0)
    payload["macro_series_present_count"] = int(payload.get("macro_series_present_count") or 0)
    payload["liquidity_score"] = float(payload.get("liquidity_score") or 0.0)
    payload["event_quality_score"] = float(payload.get("event_quality_score") or 0.0)
    payload["regime_inputs_summary"] = _json_loads(payload.get("regime_inputs_summary_json"), {})
    payload["present_feature_keys"] = _json_loads(payload.get("present_feature_keys_json"), [])
    payload["shape_keys"] = _json_loads(payload.get("shape_keys_json"), [])
    payload["ctx_keys"] = _json_loads(payload.get("ctx_keys_json"), [])
    payload["raw_regime_context_features"] = _json_loads(payload.get("raw_regime_context_features_json"), {})
    payload["normalized_regime_context_features"] = _json_loads(payload.get("normalized_regime_context_features_json"), {})
    payload["proxy_diagnostics"] = _json_loads(payload.get("proxy_diagnostics_json"), {})
    payload["raw_zero_default_keys"] = _json_loads(payload.get("raw_zero_default_keys_json"), [])
    payload["anchor_fields"] = _json_loads(payload.get("anchor_fields_json"), {})
    payload["macro_freshness_summary"] = _json_loads(payload.get("macro_freshness_summary_json"), {})
    payload["event_path_summary"] = _json_loads(payload.get("event_path_summary_json"), {})
    payload["event_side_payload"] = _json_loads(payload.get("event_side_payload_json"), {})
    payload["event_diagnostics"] = _json_loads(payload.get("event_diagnostics_json"), {})
    return payload


def build_event_raw_cache(
    *,
    output_dir: str,
    run_id: str,
    decision_date: str,
    spec: ResearchExperimentSpec,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    macro_history_by_date: Dict[str, Dict[str, float]],
    sector_map: Dict[str, str],
    metadata: dict | None = None,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None,
    macro_series_history: List[Dict[str, Any]] | None = None,
    lookback_bars: int = 5,
    use_proxy_aggregate_cache: bool = True,
    progress_callback=None,
) -> EventRawCacheHandle:
    manifest_path = _event_raw_cache_manifest_path(output_dir=output_dir, run_id=run_id)
    existing = load_event_raw_cache(manifest_path=str(manifest_path), spec=spec, expected_max_decision_date=decision_date)
    if existing is not None:
        return existing
    cache_dir = _event_raw_cache_dir(output_dir=output_dir, run_id=run_id)
    if cache_dir.exists():
        for child in cache_dir.rglob("*"):
            if child.is_file():
                child.unlink()
        for child in sorted(cache_dir.rglob("*"), reverse=True):
            if child.is_dir():
                child.rmdir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_started = perf_counter()
    prep_result = _prepare_event_candidate_refs(
        decision_date=decision_date,
        spec=spec,
        bars_by_symbol=bars_by_symbol,
        sector_map=sector_map,
        lookback_bars=lookback_bars,
        session_metadata_by_symbol=session_metadata_by_symbol,
        progress_callback=progress_callback,
    )
    candidate_refs = list(prep_result["candidate_refs"])
    row_count = len(candidate_refs)
    use_macro_level_in_similarity = _feature_flag("use_macro_level_in_similarity", metadata, default=False)
    use_dollar_volume_absolute = _feature_flag("use_dollar_volume_absolute", metadata, default=False)
    bar_anchor_dts_by_symbol = _build_bar_anchor_timestamp_cache(
        bars_by_symbol=bars_by_symbol,
        session_metadata_by_symbol=session_metadata_by_symbol,
    )
    proxy_cache = (
        _build_proxy_aggregate_cache(
            bars_by_symbol=bars_by_symbol,
            sector_map=sector_map,
            session_metadata_by_symbol=session_metadata_by_symbol,
        )
        if use_proxy_aggregate_cache
        else None
    )
    macro_history_cache: Dict[str, tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, Any]], str | None]] = {}
    macro_freshness_cache: Dict[tuple[str, str], tuple[Dict[str, float], Dict[str, Dict[str, Any]]]] = {}
    market_proxy_cache: Dict[tuple[str, str], ProxySeriesResult] = {}
    sector_proxy_cache: Dict[tuple[str, str], ProxySeriesResult] = {}
    feature_key_set: set[str] = set()
    outcome_end_dates: list[str] = [""] * row_count
    progress_every = max(1, min(1000, row_count or 1))
    last_progress_at = perf_counter()

    def _emit_cache_progress(*, done: int, current_symbol: str = "", current_event_date: str = "", status: str = "running") -> None:
        if progress_callback is None:
            return
        progress_callback(
            {
                "phase": "event_cache_build",
                "status": status,
                "symbols_done": 0,
                "symbols_total": len(bars_by_symbol),
                "current_symbol": current_symbol,
                "current_event_date": current_event_date,
                "event_candidate_total": row_count,
                "event_candidate_done": int(done),
                "raw_event_row_count": int(done),
                "pending_record_count": int(done),
            }
        )

    def _iter_event_rows() -> Iterable[dict[str, Any]]:
        nonlocal last_progress_at
        for event_ordinal, candidate in enumerate(candidate_refs):
            symbol = str(candidate["symbol"])
            feature_end_date = str(candidate["feature_end_date"])
            history_start_idx = int(candidate["history_start_idx"])
            feature_end_idx = int(candidate["feature_end_idx"])
            history_window = bars_by_symbol[symbol][history_start_idx : feature_end_idx + 1]
            anchor_fields = dict(candidate.get("anchor_fields") or {})
            macro_window, latest_macro_by_series, macro_asof_ts_utc = _resolve_cached_macro_history_until_anchor(
                macro_history_cache=macro_history_cache,
                macro_history_by_date=macro_history_by_date,
                macro_series_history=macro_series_history,
                feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                fallback_cutoff_date=feature_end_date,
            )
            macro_payload = _latest_macro_payload(macro_window)
            proxy_key = (symbol, feature_end_date)
            if proxy_cache is not None:
                market_proxy = market_proxy_cache.get(proxy_key)
                if market_proxy is None:
                    market_proxy = _cached_market_proxy_series(
                        symbol=symbol,
                        history_window=history_window,
                        cutoff_date=feature_end_date,
                        proxy_cache=proxy_cache,
                        session_metadata_by_symbol=session_metadata_by_symbol,
                    )
                    market_proxy_cache[proxy_key] = market_proxy
                sector_proxy = sector_proxy_cache.get(proxy_key)
                if sector_proxy is None:
                    sector_proxy = _cached_sector_proxy_series(
                        symbol=symbol,
                        history_window=history_window,
                        cutoff_date=feature_end_date,
                        sector_map=sector_map,
                        proxy_cache=proxy_cache,
                        session_metadata_by_symbol=session_metadata_by_symbol,
                    )
                    sector_proxy_cache[proxy_key] = sector_proxy
            else:
                market_proxy = _market_proxy_series(
                    bars_by_symbol,
                    cutoff_date=feature_end_date,
                    focus_symbol=symbol,
                    session_metadata_by_symbol=session_metadata_by_symbol,
                    cutoff_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                )
                sector_proxy = _sector_proxy_series(
                    symbol,
                    bars_by_symbol,
                    sector_map,
                    cutoff_date=feature_end_date,
                    session_metadata_by_symbol=session_metadata_by_symbol,
                    cutoff_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                )
            freshness_key = (
                symbol,
                str(
                    candidate.get("macro_key")
                    or _macro_cache_key(
                        feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                        fallback_cutoff_date=feature_end_date,
                    )
                ),
            )
            macro_freshness = macro_freshness_cache.get(freshness_key)
            if macro_freshness is None:
                macro_freshness = _macro_freshness_payload(
                    symbol=symbol,
                    bars_by_symbol=bars_by_symbol,
                    latest_macro_by_series=latest_macro_by_series,
                    feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                    session_metadata_by_symbol=session_metadata_by_symbol,
                    bar_anchor_dts_by_symbol=bar_anchor_dts_by_symbol,
                )
                macro_freshness_cache[freshness_key] = macro_freshness
            macro_freshness_features, macro_freshness_summary = macro_freshness
            breadth_diagnostics = _breadth_diagnostics_payload(latest_macro_by_series)
            raw_payload = build_raw_multiscale_feature_payload(
                symbol=symbol,
                bars=history_window,
                market_bars=market_proxy.bars,
                sector_bars=sector_proxy.bars,
                macro_history=macro_window,
                sector_code=candidate.get("lib_sector"),
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
            feature_key_set.update(str(key) for key in raw_payload.raw_features.keys())
            outcome_end_dates[event_ordinal] = str(candidate["outcome_end_date"])
            now = perf_counter()
            done = event_ordinal + 1
            if done == 1 or done == row_count or done % progress_every == 0 or (now - last_progress_at) >= 10.0:
                _emit_cache_progress(
                    done=done,
                    current_symbol=symbol,
                    current_event_date=feature_end_date,
                    status="running",
                )
                last_progress_at = now
            yield _event_cache_stage_row(
                event_ordinal=event_ordinal,
                symbol=symbol,
                feature_end_date=feature_end_date,
                outcome_end_date=str(candidate["outcome_end_date"]),
                lib_sector=candidate.get("lib_sector"),
                regime_code=regime_code,
                regime_code_raw_macro=regime_code_raw_macro,
                regime_inputs_summary=regime_inputs_summary,
                macro_history_length=len(macro_window),
                macro_series_present_count=_count_present_similarity_macro_series(latest_macro_by_series),
                raw_features=raw_payload.raw_features,
                shape_keys=sorted(list(raw_payload.shape_features.keys()) + list(raw_payload.residual_features.keys())),
                ctx_keys=sorted(raw_payload.context_features.keys()),
                raw_regime_context_features=raw_payload.regime_context_features,
                normalized_regime_context_features=raw_payload.normalized_regime_context_features,
                proxy_diagnostics=raw_payload.metadata.get("proxy_diagnostics", {}),
                raw_zero_default_keys=raw_payload.metadata.get("raw_zero_default_keys", []),
                liquidity_score=max(
                    0.0,
                    min(1.0, compute_bar_features(history_window).get("volume_mean", 0.0) / 1_000_000.0),
                ),
                anchor_fields=anchor_fields,
                macro_asof_ts_utc=macro_asof_ts_utc,
                macro_freshness_summary=macro_freshness_summary,
                breadth_policy=breadth_diagnostics.get("breadth_policy"),
                breadth_present=bool(breadth_diagnostics.get("breadth_present")),
                breadth_missing_reason=breadth_diagnostics.get("breadth_missing_reason"),
                event_path_summary=dict(candidate["event"].path_summary),
                event_path_label=str(candidate["event"].path_label),
                event_side_payload=dict(candidate["event"].side_payload),
                event_diagnostics=dict(candidate["event"].diagnostics),
                event_quality_score=float(candidate["event"].quality_score),
            )

    stage_events_path = cache_dir / "events_stage.parquet"
    event_rows_written, _ = _append_parquet_batches(stage_events_path, _iter_event_rows(), batch_size=500)
    feature_keys = tuple(sorted(feature_key_set))
    feature_index = {key: idx for idx, key in enumerate(feature_keys)}
    raw_features_path = cache_dir / "raw_features.f64.npy"
    raw_matrix, raw_tmp_path, raw_final_path = _open_numpy_memmap_atomic(
        raw_features_path,
        dtype=np.float64,
        shape=(row_count, len(feature_keys)),
    )
    present_matrix_path = cache_dir / "present_matrix.i1.npy"
    present_matrix, present_tmp_path, present_final_path = _open_numpy_memmap_atomic(
        present_matrix_path,
        dtype=np.int8,
        shape=(row_count, len(feature_keys)),
    )
    stage_event_file = pq.ParquetFile(stage_events_path)
    rows_filled = 0
    last_matrix_progress_at = perf_counter()
    for batch in stage_event_file.iter_batches(columns=["event_ordinal", "raw_features_json"], batch_size=1000):
        batch_payload = batch.to_pydict()
        ordinals = list(batch_payload.get("event_ordinal") or [])
        raw_json_rows = list(batch_payload.get("raw_features_json") or [])
        for raw_event_ordinal, raw_json in zip(ordinals, raw_json_rows):
            row_index = int(raw_event_ordinal)
            raw_features = dict(_json_loads(raw_json, {}))
            for key, value in raw_features.items():
                feature_idx = feature_index[str(key)]
                raw_matrix[row_index, feature_idx] = float(value or 0.0)
                present_matrix[row_index, feature_idx] = 1
            rows_filled += 1
        now = perf_counter()
        if rows_filled == row_count or rows_filled % 5000 == 0 or (now - last_matrix_progress_at) >= 10.0:
            _emit_cache_progress(done=rows_filled, status="running")
            last_matrix_progress_at = now
    del stage_event_file
    events_path = cache_dir / "events.parquet"

    def _iter_final_event_rows() -> Iterable[dict[str, Any]]:
        final_file = pq.ParquetFile(stage_events_path)
        try:
            for batch in final_file.iter_batches(batch_size=1000):
                batch_payload = batch.to_pydict()
                row_count_in_batch = len(list(batch_payload.get("event_ordinal") or []))
                for row_index in range(row_count_in_batch):
                    row_payload = {
                        key: values[row_index]
                        for key, values in batch_payload.items()
                        if key != "raw_features_json"
                    }
                    yield row_payload
        finally:
            del final_file

    _, _ = _append_parquet_batches(events_path, _iter_final_event_rows(), batch_size=1000)
    raw_matrix_path_str = _finalize_numpy_memmap_atomic(raw_matrix, raw_tmp_path, raw_final_path)
    present_matrix_path_str = _finalize_numpy_memmap_atomic(present_matrix, present_tmp_path, present_final_path)
    raw_matrix = np.load(raw_matrix_path_str, mmap_mode="r")
    present_matrix = np.load(present_matrix_path_str, mmap_mode="r")
    outcome_order = sorted(range(row_count), key=lambda idx: (str(outcome_end_dates[idx] or ""), idx))
    prefix_event_ids_path = cache_dir / "prefix_event_ids.npy"
    _write_numpy_atomic(prefix_event_ids_path, np.asarray(outcome_order, dtype=np.int64))
    prefix_sum_path = cache_dir / "prefix_sum.f64.npy"
    prefix_sumsq_path = cache_dir / "prefix_sumsq.f64.npy"
    prefix_present_count_path = cache_dir / "prefix_present_count.i64.npy"
    prefix_sum, prefix_sum_tmp_path, prefix_sum_final_path = _open_numpy_memmap_atomic(
        prefix_sum_path,
        dtype=np.float64,
        shape=(row_count, len(feature_keys)),
    )
    prefix_sumsq, prefix_sumsq_tmp_path, prefix_sumsq_final_path = _open_numpy_memmap_atomic(
        prefix_sumsq_path,
        dtype=np.float64,
        shape=(row_count, len(feature_keys)),
    )
    prefix_present_count, prefix_present_tmp_path, prefix_present_final_path = _open_numpy_memmap_atomic(
        prefix_present_count_path,
        dtype=np.int64,
        shape=(row_count, len(feature_keys)),
    )
    running_sum = np.zeros((len(feature_keys),), dtype=np.float64)
    running_sumsq = np.zeros((len(feature_keys),), dtype=np.float64)
    running_present = np.zeros((len(feature_keys),), dtype=np.int64)
    last_prefix_progress_at = perf_counter()
    for prefix_index, row_index in enumerate(outcome_order, start=1):
        row_values = np.asarray(raw_matrix[row_index], dtype=np.float64)
        present_values = np.asarray(present_matrix[row_index], dtype=np.int64)
        running_sum += row_values
        running_sumsq += np.square(row_values)
        running_present += present_values
        prefix_sum[prefix_index - 1] = running_sum
        prefix_sumsq[prefix_index - 1] = running_sumsq
        prefix_present_count[prefix_index - 1] = running_present
        now = perf_counter()
        if prefix_index == row_count or prefix_index % 5000 == 0 or (now - last_prefix_progress_at) >= 10.0:
            _emit_cache_progress(done=prefix_index, status="running")
            last_prefix_progress_at = now
    prefix_sum_path_str = _finalize_numpy_memmap_atomic(prefix_sum, prefix_sum_tmp_path, prefix_sum_final_path)
    prefix_sumsq_path_str = _finalize_numpy_memmap_atomic(prefix_sumsq, prefix_sumsq_tmp_path, prefix_sumsq_final_path)
    prefix_present_count_path_str = _finalize_numpy_memmap_atomic(
        prefix_present_count,
        prefix_present_tmp_path,
        prefix_present_final_path,
    )
    del raw_matrix
    del present_matrix
    Path(present_matrix_path_str).unlink(missing_ok=True)
    stage_events_path.unlink(missing_ok=True)
    manifest = {
        "format_version": "event_raw_cache_v2",
        "schema_version": "event_raw_cache_row_v2",
        "spec_hash": spec.spec_hash(),
        "memory_version": str(spec.memory_version),
        "max_decision_date": str(decision_date),
        "row_count": int(event_rows_written),
        "feature_keys": list(feature_keys),
        "events_file": "events.parquet",
        "raw_features_file": "raw_features.f64.npy",
        "prefix_event_ids_file": "prefix_event_ids.npy",
        "prefix_sum_file": "prefix_sum.f64.npy",
        "prefix_sumsq_file": "prefix_sumsq.f64.npy",
        "prefix_present_count_file": "prefix_present_count.i64.npy",
        "build_ms": int((perf_counter() - cache_started) * 1000),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _emit_cache_progress(done=int(event_rows_written), status="ok")
    return EventRawCacheHandle(
        format_version="event_raw_cache_v2",
        manifest_path=str(manifest_path),
        events_path=str(events_path),
        raw_features_path=str(raw_matrix_path_str),
        prefix_event_ids_path=str(prefix_event_ids_path),
        prefix_sum_path=str(prefix_sum_path_str),
        prefix_sumsq_path=str(prefix_sumsq_path_str),
        prefix_present_count_path=str(prefix_present_count_path_str),
        row_count=int(event_rows_written),
        feature_keys=feature_keys,
        spec_hash=spec.spec_hash(),
        memory_version=str(spec.memory_version),
        max_decision_date=str(decision_date),
        build_ms=int(manifest.get("build_ms") or 0),
    )


def load_event_raw_cache(
    *,
    manifest_path: str,
    spec: ResearchExperimentSpec,
    expected_max_decision_date: str = "",
) -> EventRawCacheHandle | None:
    resolved = Path(str(manifest_path or ""))
    if not resolved.exists():
        return None
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if str(payload.get("format_version") or "") != "event_raw_cache_v2":
        return None
    if str(payload.get("spec_hash") or "") != spec.spec_hash():
        return None
    if str(payload.get("memory_version") or "") != str(spec.memory_version):
        return None
    if expected_max_decision_date and str(payload.get("max_decision_date") or "") != str(expected_max_decision_date):
        return None
    cache_dir = resolved.parent
    return EventRawCacheHandle(
        format_version="event_raw_cache_v2",
        manifest_path=str(resolved),
        events_path=str(cache_dir / str(payload.get("events_file") or "events.parquet")),
        raw_features_path=str(cache_dir / str(payload.get("raw_features_file") or "raw_features.f64.npy")),
        prefix_event_ids_path=str(cache_dir / str(payload.get("prefix_event_ids_file") or "prefix_event_ids.npy")),
        prefix_sum_path=str(cache_dir / str(payload.get("prefix_sum_file") or "prefix_sum.f64.npy")),
        prefix_sumsq_path=str(cache_dir / str(payload.get("prefix_sumsq_file") or "prefix_sumsq.f64.npy")),
        prefix_present_count_path=str(cache_dir / str(payload.get("prefix_present_count_file") or "prefix_present_count.i64.npy")),
        row_count=int(payload.get("row_count") or 0),
        feature_keys=tuple(str(item) for item in list(payload.get("feature_keys") or [])),
        spec_hash=str(payload.get("spec_hash") or ""),
        memory_version=str(payload.get("memory_version") or ""),
        max_decision_date=str(payload.get("max_decision_date") or ""),
        build_ms=int(payload.get("build_ms") or 0),
    )


def _feature_transform_from_prefix_stats(
    *,
    event_cache_handle: EventRawCacheHandle,
    eligible_event_count: int,
) -> FeatureTransform:
    count = int(eligible_event_count or 0)
    if count <= 0:
        return FeatureTransform(
            scaler=FeatureScaler(means={}, stds={}),
            feature_keys=[],
            version=FEATURE_TRANSFORM_VERSION,
        )
    prefix_present_count = event_cache_handle.load_prefix_present_count(mmap_mode="r")
    prefix_event_ids = event_cache_handle.load_prefix_event_ids(mmap_mode="r")
    raw_features = event_cache_handle.load_raw_features(mmap_mode="r")
    present = np.asarray(prefix_present_count[count - 1], dtype=np.int64)
    eligible_row_ids = np.asarray(prefix_event_ids[:count], dtype=np.int64)
    resolved_feature_keys = [
        str(event_cache_handle.feature_keys[idx])
        for idx in range(len(event_cache_handle.feature_keys))
        if int(present[idx]) > 0
    ]
    means: dict[str, float] = {}
    stds: dict[str, float] = {}
    for key in resolved_feature_keys:
        feature_idx = event_cache_handle.feature_index[key]
        values = [float(value) for value in np.asarray(raw_features[eligible_row_ids, feature_idx], dtype=np.float64)]
        means[key] = mean(values) if values else 0.0
        stds[key] = max(1e-8, pstdev(values) if len(values) > 1 else 1.0)
    return FeatureTransform(
        scaler=FeatureScaler(means=means, stds=stds),
        feature_keys=resolved_feature_keys,
        version=FEATURE_TRANSFORM_VERSION,
    )


def _build_bar_anchor_timestamp_cache(
    *,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
) -> Dict[str, List[datetime]]:
    cache: Dict[str, List[datetime]] = {}
    for symbol, bars in bars_by_symbol.items():
        anchor_dts: List[datetime] = []
        for bar in bars:
            anchor_dt = _bar_anchor_ts_utc(
                symbol=symbol,
                bar=bar,
                session_metadata_by_symbol=session_metadata_by_symbol,
            )
            if anchor_dt is not None:
                anchor_dts.append(anchor_dt)
        cache[symbol] = anchor_dts
    return cache


def _macro_cache_key(*, feature_anchor_ts_utc: str | None, fallback_cutoff_date: str) -> str:
    if feature_anchor_ts_utc:
        return f"anchor:{feature_anchor_ts_utc}"
    return f"date:{str(fallback_cutoff_date or '')[:10]}"


def _resolve_cached_macro_history_until_anchor(
    *,
    macro_history_cache: Dict[str, tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, Any]], str | None]],
    macro_history_by_date: Dict[str, Dict[str, float]],
    macro_series_history: List[Dict[str, Any]] | None,
    feature_anchor_ts_utc: str | None,
    fallback_cutoff_date: str,
) -> tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, Any]], str | None]:
    cache_key = _macro_cache_key(
        feature_anchor_ts_utc=feature_anchor_ts_utc,
        fallback_cutoff_date=fallback_cutoff_date,
    )
    cached = macro_history_cache.get(cache_key)
    if cached is None:
        cached = _macro_history_until_anchor(
            macro_history_by_date=macro_history_by_date,
            macro_series_history=macro_series_history,
            feature_anchor_ts_utc=feature_anchor_ts_utc,
            fallback_cutoff_date=fallback_cutoff_date,
        )
        macro_history_cache[cache_key] = cached
    return cached


def _build_event_memory_pending_records_legacy(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    macro_history_by_date: Dict[str, Dict[str, float]],
    sector_map: Dict[str, str],
    lookback_bars: int,
    metadata: dict | None,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
    macro_series_history: List[Dict[str, Any]] | None,
    progress_callback,
    use_proxy_aggregate_cache: bool,
) -> dict[str, Any]:
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    label_cfg = _label_cfg(spec)
    use_macro_level_in_similarity = _feature_flag("use_macro_level_in_similarity", metadata, default=False)
    use_dollar_volume_absolute = _feature_flag("use_dollar_volume_absolute", metadata, default=False)
    raw_event_rows: List[dict] = []
    excluded_reasons: list[dict] = []
    pending_records: list[dict] = []
    symbol_count = len(bars_by_symbol)
    symbol_progress_every = max(1, min(25, symbol_count or 1))
    last_event_progress_at = perf_counter()
    proxy_cache = (
        _build_proxy_aggregate_cache(
            bars_by_symbol=bars_by_symbol,
            sector_map=sector_map,
            session_metadata_by_symbol=session_metadata_by_symbol,
        )
        if use_proxy_aggregate_cache
        else None
    )

    def _emit_progress(payload: dict[str, Any]) -> None:
        if progress_callback is not None:
            progress_callback(dict(payload))

    event_memory_started = perf_counter()
    _emit_progress(
        {
            "phase": "event_memory",
            "status": "running",
            "symbols_done": 0,
            "symbols_total": symbol_count,
            "current_symbol": "",
            "raw_event_row_count": 0,
            "pending_record_count": 0,
        }
    )
    for lib_idx, (lib_symbol, lib_bars) in enumerate(bars_by_symbol.items(), start=1):
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
            if proxy_cache is not None:
                market_proxy = _cached_market_proxy_series(
                    symbol=lib_symbol,
                    history_window=history_window,
                    cutoff_date=feature_end_date,
                    proxy_cache=proxy_cache,
                    session_metadata_by_symbol=session_metadata_by_symbol,
                )
                sector_proxy = _cached_sector_proxy_series(
                    symbol=lib_symbol,
                    history_window=history_window,
                    cutoff_date=feature_end_date,
                    sector_map=sector_map,
                    proxy_cache=proxy_cache,
                    session_metadata_by_symbol=session_metadata_by_symbol,
                )
            else:
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
            now = perf_counter()
            if (now - last_event_progress_at) >= 10.0:
                _emit_progress(
                    {
                        "phase": "event_memory",
                        "status": "running",
                        "symbols_done": max(0, lib_idx - 1),
                        "symbols_total": symbol_count,
                        "current_symbol": lib_symbol,
                        "raw_event_row_count": len(raw_event_rows),
                        "pending_record_count": len(pending_records),
                    }
                )
                last_event_progress_at = now
        now = perf_counter()
        if (
            lib_idx == symbol_count
            or lib_idx == 1
            or lib_idx % symbol_progress_every == 0
            or (now - last_event_progress_at) >= 10.0
        ):
            _emit_progress(
                {
                    "phase": "event_memory",
                    "status": "running",
                    "symbols_done": lib_idx,
                    "symbols_total": symbol_count,
                    "current_symbol": lib_symbol,
                    "raw_event_row_count": len(raw_event_rows),
                    "pending_record_count": len(pending_records),
                }
            )
            last_event_progress_at = now
    event_memory_ms = int((perf_counter() - event_memory_started) * 1000)
    _emit_progress(
        {
            "phase": "event_memory",
            "status": "ok",
            "symbols_done": symbol_count,
            "symbols_total": symbol_count,
            "current_symbol": "",
            "raw_event_row_count": len(raw_event_rows),
            "pending_record_count": len(pending_records),
            "event_memory_ms": event_memory_ms,
        }
    )
    return {
        "excluded_reasons": excluded_reasons,
        "raw_event_rows": raw_event_rows,
        "pending_records": pending_records,
        "anchor_count": 0,
        "phase_timings_ms": {
            "event_memory": event_memory_ms,
        },
    }


def _prepare_event_candidate_refs(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    sector_map: Dict[str, str],
    lookback_bars: int,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
    progress_callback,
) -> dict[str, Any]:
    min_required_bars = max(lookback_bars, spec.feature_window_bars)
    label_cfg = _label_cfg(spec)
    excluded_reasons: list[dict] = []
    candidate_refs: list[dict[str, Any]] = []
    symbol_count = len(bars_by_symbol)
    symbol_progress_every = max(1, min(25, symbol_count or 1))
    last_progress_at = perf_counter()

    def _emit_progress(payload: dict[str, Any]) -> None:
        if progress_callback is not None:
            progress_callback(dict(payload))

    prep_started = perf_counter()
    _emit_progress(
        {
            "phase": "event_candidate_prep",
            "status": "running",
            "symbols_done": 0,
            "symbols_total": symbol_count,
            "current_symbol": "",
            "current_event_date": "",
            "event_candidate_total": 0,
            "event_candidate_done": 0,
            "raw_event_row_count": 0,
            "pending_record_count": 0,
        }
    )
    for lib_idx, (lib_symbol, lib_bars) in enumerate(bars_by_symbol.items(), start=1):
        session_metadata = _resolve_session_metadata(lib_symbol, session_metadata_by_symbol)
        if session_metadata_by_symbol and session_metadata is None:
            excluded_reasons.append(
                {"symbol": lib_symbol, "reason": "unknown_exchange_session", "missingness_family": "data_quality_missing"}
            )
            continue
        if len(lib_bars) < min_required_bars + spec.horizon_days + 2:
            excluded_reasons.append(
                {"symbol": lib_symbol, "reason": "insufficient_bars", "missingness_family": "structural_missing"}
            )
            continue
        lib_sector = sector_map.get(lib_symbol)
        for j in range(min_required_bars - 1, len(lib_bars) - spec.horizon_days - 1):
            feature_end_date = str(lib_bars[j].timestamp)[:10]
            outcome_end_date = str(lib_bars[j + spec.horizon_days].timestamp)[:10]
            if feature_end_date > decision_date:
                break
            if outcome_end_date >= decision_date:
                break
            future_window = lib_bars[j + 1 : j + 1 + spec.horizon_days]
            anchor_fields = _anchor_fields_for_symbol_date(
                symbol=lib_symbol,
                session_date_local=feature_end_date,
                session_metadata_by_symbol=session_metadata_by_symbol,
            )
            event = build_event_outcome_record(future_window, label_cfg)
            candidate_refs.append(
                {
                    "symbol": lib_symbol,
                    "feature_end_idx": int(j),
                    "history_start_idx": int(j - spec.feature_window_bars + 1),
                    "feature_end_date": feature_end_date,
                    "outcome_end_date": outcome_end_date,
                    "lib_sector": lib_sector,
                    "event": event,
                    "anchor_fields": dict(anchor_fields),
                    "macro_key": _macro_cache_key(
                        feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                        fallback_cutoff_date=feature_end_date,
                    ),
                }
            )
            now = perf_counter()
            if (now - last_progress_at) >= 10.0:
                _emit_progress(
                    {
                        "phase": "event_candidate_prep",
                        "status": "running",
                        "symbols_done": max(0, lib_idx - 1),
                        "symbols_total": symbol_count,
                        "current_symbol": lib_symbol,
                        "current_event_date": feature_end_date,
                        "event_candidate_total": len(candidate_refs),
                        "event_candidate_done": len(candidate_refs),
                        "raw_event_row_count": 0,
                        "pending_record_count": 0,
                    }
                )
                last_progress_at = now
        now = perf_counter()
        if (
            lib_idx == symbol_count
            or lib_idx == 1
            or lib_idx % symbol_progress_every == 0
            or (now - last_progress_at) >= 10.0
        ):
            _emit_progress(
                {
                    "phase": "event_candidate_prep",
                    "status": "running",
                    "symbols_done": lib_idx,
                    "symbols_total": symbol_count,
                    "current_symbol": lib_symbol,
                    "current_event_date": "",
                    "event_candidate_total": len(candidate_refs),
                    "event_candidate_done": len(candidate_refs),
                    "raw_event_row_count": 0,
                    "pending_record_count": 0,
                }
            )
            last_progress_at = now
    event_candidate_prep_ms = int((perf_counter() - prep_started) * 1000)
    _emit_progress(
        {
            "phase": "event_candidate_prep",
            "status": "ok",
            "symbols_done": symbol_count,
            "symbols_total": symbol_count,
            "current_symbol": "",
            "current_event_date": "",
            "event_candidate_total": len(candidate_refs),
            "event_candidate_done": len(candidate_refs),
            "raw_event_row_count": 0,
            "pending_record_count": 0,
            "event_memory_ms": event_candidate_prep_ms,
        }
    )
    return {
        "candidate_refs": candidate_refs,
        "excluded_reasons": excluded_reasons,
        "phase_timings_ms": {
            "event_candidate_prep": event_candidate_prep_ms,
            "event_payload_build": 0,
            "event_memory": event_candidate_prep_ms,
        },
    }


def _build_event_memory_pending_records_fast(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    bars_by_symbol: Dict[str, List[HistoricalBar]],
    macro_history_by_date: Dict[str, Dict[str, float]],
    sector_map: Dict[str, str],
    metadata: dict | None,
    session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None,
    macro_series_history: List[Dict[str, Any]] | None,
    progress_callback,
    use_proxy_aggregate_cache: bool,
    event_input_path: str | None,
    event_checkpoint_path: str | None,
    resume_event_memory_from_checkpoint: bool,
    lookback_bars: int,
) -> dict[str, Any]:
    use_macro_level_in_similarity = _feature_flag("use_macro_level_in_similarity", metadata, default=False)
    use_dollar_volume_absolute = _feature_flag("use_dollar_volume_absolute", metadata, default=False)

    def _emit_progress(payload: dict[str, Any]) -> None:
        if progress_callback is not None:
            progress_callback(dict(payload))

    input_payload = (
        _load_event_input_payload(path=event_input_path, decision_date=decision_date, spec=spec)
        if event_input_path and resume_event_memory_from_checkpoint
        else None
    )
    if input_payload is None:
        prep_result = _prepare_event_candidate_refs(
            decision_date=decision_date,
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            sector_map=sector_map,
            lookback_bars=lookback_bars,
            session_metadata_by_symbol=session_metadata_by_symbol,
            progress_callback=progress_callback,
        )
        candidate_refs = list(prep_result["candidate_refs"])
        excluded_reasons = list(prep_result["excluded_reasons"])
        phase_timings_ms = dict(prep_result["phase_timings_ms"])
        if event_input_path:
            _write_pickle_atomic(
                event_input_path,
                _event_input_payload(
                    decision_date=decision_date,
                    spec=spec,
                    candidate_refs=candidate_refs,
                    excluded_reasons=excluded_reasons,
                    phase_timings_ms=phase_timings_ms,
                ),
            )
    else:
        candidate_refs = list(input_payload["candidate_refs"])
        excluded_reasons = list(input_payload["excluded_reasons"])
        phase_timings_ms = dict(input_payload.get("phase_timings_ms") or {})
        _emit_progress(
            {
                "phase": "event_candidate_prep",
                "status": "ok",
                "symbols_done": len(bars_by_symbol),
                "symbols_total": len(bars_by_symbol),
                "current_symbol": "",
                "current_event_date": "",
                "event_candidate_total": len(candidate_refs),
                "event_candidate_done": len(candidate_refs),
                "raw_event_row_count": 0,
                "pending_record_count": 0,
                "event_memory_ms": int(phase_timings_ms.get("event_candidate_prep") or 0),
            }
        )

    checkpoint_payload = (
        _load_event_checkpoint_payload(path=event_checkpoint_path, decision_date=decision_date, spec=spec)
        if event_checkpoint_path and resume_event_memory_from_checkpoint
        else None
    )
    batch_paths = [str(path) for path in list((checkpoint_payload or {}).get("batch_paths") or [])]
    pending_records: list[dict[str, Any]] = []
    raw_event_row_count = int((checkpoint_payload or {}).get("raw_event_row_count") or len(pending_records))
    pending_record_count = int((checkpoint_payload or {}).get("pending_record_count") or len(pending_records))
    next_candidate_index = int((checkpoint_payload or {}).get("next_candidate_index") or 0)
    checkpoint_phase_timings = dict((checkpoint_payload or {}).get("phase_timings_ms") or {})
    if checkpoint_phase_timings:
        phase_timings_ms.update(checkpoint_phase_timings)
    event_batch_dir = _event_batch_dir_from_checkpoint_path(event_checkpoint_path)
    batch_size = 500
    next_batch_index = len(batch_paths)

    bar_anchor_dts_by_symbol = _build_bar_anchor_timestamp_cache(
        bars_by_symbol=bars_by_symbol,
        session_metadata_by_symbol=session_metadata_by_symbol,
    )
    proxy_cache = (
        _build_proxy_aggregate_cache(
            bars_by_symbol=bars_by_symbol,
            sector_map=sector_map,
            session_metadata_by_symbol=session_metadata_by_symbol,
        )
        if use_proxy_aggregate_cache
        else None
    )
    macro_history_cache: Dict[str, tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, Any]], str | None]] = {}
    macro_freshness_cache: Dict[tuple[str, str], tuple[Dict[str, float], Dict[str, Dict[str, Any]]]] = {}
    market_proxy_cache: Dict[tuple[str, str], ProxySeriesResult] = {}
    sector_proxy_cache: Dict[tuple[str, str], ProxySeriesResult] = {}

    payload_started = perf_counter()
    last_progress_at = perf_counter()
    last_checkpoint_at_perf = perf_counter()
    total_candidates = len(candidate_refs)
    _emit_progress(
        {
            "phase": "event_payload_build",
            "status": "running",
            "current_symbol": str(candidate_refs[next_candidate_index]["symbol"]) if next_candidate_index < total_candidates else "",
            "current_event_date": str(candidate_refs[next_candidate_index]["feature_end_date"]) if next_candidate_index < total_candidates else "",
            "event_candidate_total": total_candidates,
            "event_candidate_done": next_candidate_index,
            "raw_event_row_count": raw_event_row_count,
            "pending_record_count": pending_record_count,
        }
    )
    last_checkpoint_at_iso = ""
    checkpoint_due_every = 1000

    def _flush_pending_batch() -> None:
        nonlocal pending_records, next_batch_index
        if not pending_records or event_batch_dir is None:
            return
        batch_path = _write_event_batch(
            batch_dir=event_batch_dir,
            batch_index=next_batch_index,
            pending_records=pending_records,
        )
        batch_paths.append(batch_path)
        next_batch_index += 1
        pending_records = []

    for candidate_idx in range(next_candidate_index, total_candidates):
        candidate = candidate_refs[candidate_idx]
        symbol = str(candidate["symbol"])
        feature_end_date = str(candidate["feature_end_date"])
        history_start_idx = int(candidate["history_start_idx"])
        feature_end_idx = int(candidate["feature_end_idx"])
        history_window = bars_by_symbol[symbol][history_start_idx : feature_end_idx + 1]
        anchor_fields = dict(candidate.get("anchor_fields") or {})
        macro_window, latest_macro_by_series, macro_asof_ts_utc = _resolve_cached_macro_history_until_anchor(
            macro_history_cache=macro_history_cache,
            macro_history_by_date=macro_history_by_date,
            macro_series_history=macro_series_history,
            feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
            fallback_cutoff_date=feature_end_date,
        )
        macro_payload = _latest_macro_payload(macro_window)
        proxy_key = (symbol, feature_end_date)
        if proxy_cache is not None:
            market_proxy = market_proxy_cache.get(proxy_key)
            if market_proxy is None:
                market_proxy = _cached_market_proxy_series(
                    symbol=symbol,
                    history_window=history_window,
                    cutoff_date=feature_end_date,
                    proxy_cache=proxy_cache,
                    session_metadata_by_symbol=session_metadata_by_symbol,
                )
                market_proxy_cache[proxy_key] = market_proxy
            sector_proxy = sector_proxy_cache.get(proxy_key)
            if sector_proxy is None:
                sector_proxy = _cached_sector_proxy_series(
                    symbol=symbol,
                    history_window=history_window,
                    cutoff_date=feature_end_date,
                    sector_map=sector_map,
                    proxy_cache=proxy_cache,
                    session_metadata_by_symbol=session_metadata_by_symbol,
                )
                sector_proxy_cache[proxy_key] = sector_proxy
        else:
            market_proxy = _market_proxy_series(
                bars_by_symbol,
                cutoff_date=feature_end_date,
                focus_symbol=symbol,
                session_metadata_by_symbol=session_metadata_by_symbol,
                cutoff_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
            )
            sector_proxy = _sector_proxy_series(
                symbol,
                bars_by_symbol,
                sector_map,
                cutoff_date=feature_end_date,
                session_metadata_by_symbol=session_metadata_by_symbol,
                cutoff_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
            )
        freshness_key = (symbol, candidate.get("macro_key") or _macro_cache_key(
            feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
            fallback_cutoff_date=feature_end_date,
        ))
        macro_freshness = macro_freshness_cache.get(freshness_key)
        if macro_freshness is None:
            macro_freshness = _macro_freshness_payload(
                symbol=symbol,
                bars_by_symbol=bars_by_symbol,
                latest_macro_by_series=latest_macro_by_series,
                feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                session_metadata_by_symbol=session_metadata_by_symbol,
                bar_anchor_dts_by_symbol=bar_anchor_dts_by_symbol,
            )
            macro_freshness_cache[freshness_key] = macro_freshness
        macro_freshness_features, macro_freshness_summary = macro_freshness
        breadth_diagnostics = _breadth_diagnostics_payload(latest_macro_by_series)
        raw_payload = build_raw_multiscale_feature_payload(
            symbol=symbol,
            bars=history_window,
            market_bars=market_proxy.bars,
            sector_bars=sector_proxy.bars,
            macro_history=macro_window,
            sector_code=candidate.get("lib_sector"),
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
        pending_records.append(
            {
                "event_path_summary": dict(candidate["event"].path_summary),
                "event_path_label": str(candidate["event"].path_label),
                "event_side_payload": dict(candidate["event"].side_payload),
                "event_diagnostics": dict(candidate["event"].diagnostics),
                "event_quality_score": float(candidate["event"].quality_score),
                "symbol": symbol,
                "feature_end_date": feature_end_date,
                "outcome_end_date": str(candidate["outcome_end_date"]),
                "lib_sector": candidate.get("lib_sector"),
                "regime_code": regime_code,
                "regime_code_raw_macro": regime_code_raw_macro,
                "regime_inputs_summary": regime_inputs_summary,
                "macro_history_length": len(macro_window),
                "macro_series_present_count": _count_present_similarity_macro_series(latest_macro_by_series),
                "raw_features": dict(raw_payload.raw_features),
                "shape_keys": sorted(list(raw_payload.shape_features.keys()) + list(raw_payload.residual_features.keys())),
                "ctx_keys": sorted(raw_payload.context_features.keys()),
                "raw_regime_context_features": dict(raw_payload.regime_context_features),
                "normalized_regime_context_features": dict(raw_payload.normalized_regime_context_features),
                "proxy_diagnostics": dict(raw_payload.metadata.get("proxy_diagnostics", {})),
                "raw_zero_default_keys": list(raw_payload.metadata.get("raw_zero_default_keys", [])),
                "liquidity_score": max(
                    0.0,
                    min(1.0, compute_bar_features(history_window).get("volume_mean", 0.0) / 1_000_000.0),
                ),
                "anchor_fields": anchor_fields,
                "macro_asof_ts_utc": macro_asof_ts_utc,
                "macro_freshness_summary": macro_freshness_summary,
                **breadth_diagnostics,
            }
        )
        raw_event_row_count += 1
        pending_record_count += 1
        if len(pending_records) >= batch_size:
            _flush_pending_batch()
        done = candidate_idx + 1
        phase_timings_ms["event_payload_build"] = int((perf_counter() - payload_started) * 1000) + int(
            checkpoint_phase_timings.get("event_payload_build") or 0
        )
        phase_timings_ms["event_memory"] = int(phase_timings_ms.get("event_candidate_prep") or 0) + int(
            phase_timings_ms.get("event_payload_build") or 0
        )
        now = perf_counter()
        checkpoint_written = False
        if event_checkpoint_path and (
            done == total_candidates
            or done % checkpoint_due_every == 0
            or (now - last_checkpoint_at_perf) >= 30.0
        ):
            _flush_pending_batch()
            last_checkpoint_at_iso = datetime.now(timezone.utc).isoformat()
            _write_pickle_atomic(
                event_checkpoint_path,
                _event_checkpoint_payload(
                    decision_date=decision_date,
                    spec=spec,
                    next_candidate_index=done,
                    batch_paths=batch_paths,
                    phase_timings_ms=phase_timings_ms,
                    raw_event_row_count=raw_event_row_count,
                    pending_record_count=pending_record_count,
                ),
            )
            checkpoint_written = True
            last_checkpoint_at_perf = now
        if (
            done == total_candidates
            or done == 1
            or checkpoint_written
            or (now - last_progress_at) >= 10.0
        ):
            _emit_progress(
                {
                    "phase": "event_payload_build",
                    "status": "running" if done < total_candidates else "ok",
                    "current_symbol": symbol if done < total_candidates else "",
                    "current_event_date": feature_end_date if done < total_candidates else "",
                    "event_candidate_total": total_candidates,
                    "event_candidate_done": done,
                    "raw_event_row_count": raw_event_row_count,
                    "pending_record_count": pending_record_count,
                    "event_memory_ms": int(phase_timings_ms.get("event_memory") or 0),
                    "last_event_checkpoint_at": last_checkpoint_at_iso if checkpoint_written else None,
                    "event_checkpoint_path": str(event_checkpoint_path or "") if checkpoint_written else None,
                }
            )
            last_progress_at = now
    phase_timings_ms["event_memory"] = int(phase_timings_ms.get("event_candidate_prep") or 0) + int(
        phase_timings_ms.get("event_payload_build") or 0
    )
    if total_candidates == 0:
        _emit_progress(
            {
                "phase": "event_payload_build",
                "status": "ok",
                "current_symbol": "",
                "current_event_date": "",
                "event_candidate_total": 0,
                "event_candidate_done": 0,
                "raw_event_row_count": 0,
                "pending_record_count": 0,
                "event_memory_ms": int(phase_timings_ms.get("event_memory") or 0),
            }
        )
    return {
        "excluded_reasons": excluded_reasons,
        "raw_event_row_count": raw_event_row_count,
        "pending_records": pending_records,
        "pending_record_count": pending_record_count,
        "pending_record_batch_paths": batch_paths,
        "anchor_count": 0,
        "phase_timings_ms": phase_timings_ms,
    }


def _finalize_event_memory_from_pending_records(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    pending_bundle: Mapping[str, Any],
    progress_callback,
    prototype_input_path: str | None,
    prototype_checkpoint_path: str | None,
    resume_prototype_from_checkpoint: bool,
    comparison_block_size: int,
) -> dict:
    event_records: List[EventOutcomeRecord] = []
    anchor_library: List[ResearchAnchor] = []
    raw_event_rows = list(pending_bundle.get("raw_event_rows") or [])
    excluded_reasons = list(pending_bundle.get("excluded_reasons") or [])
    pending_records = list(pending_bundle.get("pending_records") or [])
    pending_record_batch_paths = [str(path) for path in list(pending_bundle.get("pending_record_batch_paths") or [])]
    if not raw_event_rows and pending_records:
        raw_event_rows = [dict(pending.get("raw_features") or {}) for pending in pending_records]
    if not raw_event_rows and pending_record_batch_paths:
        for batch_path in pending_record_batch_paths:
            for pending in _load_event_batch(batch_path):
                raw_event_rows.append(dict(pending.get("raw_features") or {}))
    raw_event_row_count = int(
        pending_bundle.get("raw_event_row_count")
        or len(raw_event_rows)
        or len(pending_records)
    )
    pending_record_count = int(
        pending_bundle.get("pending_record_count")
        or len(pending_records)
        or raw_event_row_count
    )
    phase_timings_ms = {
        "event_memory": int(((pending_bundle.get("phase_timings_ms") or {}).get("event_memory")) or 0),
        "transform": int(((pending_bundle.get("phase_timings_ms") or {}).get("transform")) or 0),
        "prototype": int(((pending_bundle.get("phase_timings_ms") or {}).get("prototype")) or 0),
    }
    pending_progress_every = max(1, min(250, pending_record_count or 1))
    last_transform_progress_at = perf_counter()

    def _emit_progress(payload: dict[str, Any]) -> None:
        if progress_callback is not None:
            progress_callback(dict(payload))

    _emit_progress(
        {
            "phase": "transform",
            "status": "running",
            "raw_event_row_count": raw_event_row_count,
            "pending_record_count": pending_record_count,
            "event_record_count": 0,
        }
    )
    transform_started = perf_counter()
    transform = fit_feature_transform(raw_event_rows)
    raw_event_rows = []
    scaler = transform.scaler
    pending_progress_every = max(1, min(250, pending_record_count or 1))

    def _pending_record_iter() -> Sequence[dict[str, Any]] | Any:
        if pending_record_batch_paths:
            for batch_path in pending_record_batch_paths:
                for batch_record in _load_event_batch(batch_path):
                    yield batch_record
        for batch_record in pending_records:
            if batch_record is not None:
                yield batch_record

    for pending_idx, pending in enumerate(_pending_record_iter(), start=1):
        raw_payload = pending.get("raw_payload")
        raw_features = dict(
            pending.get("raw_features")
            or (
                dict(raw_payload.raw_features)
                if raw_payload is not None
                else {}
            )
        )
        transformed_features, embedding = transform.apply(raw_features)
        transform_missing_keys_filled_zero = sorted(key for key in transform.feature_keys if key not in raw_features)
        transformed_zero_feature_keys = sorted(key for key, value in transformed_features.items() if abs(float(value)) <= 1e-12)
        shape_keys = list(
            pending.get("shape_keys")
            or (
                sorted(list(raw_payload.shape_features.keys()) + list(raw_payload.residual_features.keys()))
                if raw_payload is not None
                else []
            )
        )
        ctx_keys = list(
            pending.get("ctx_keys")
            or (
                sorted(raw_payload.context_features.keys())
                if raw_payload is not None
                else []
            )
        )
        raw_regime_context_features = dict(
            pending.get("raw_regime_context_features")
            or (
                dict(raw_payload.regime_context_features)
                if raw_payload is not None
                else {}
            )
        )
        normalized_regime_context_features = dict(
            pending.get("normalized_regime_context_features")
            or (
                dict(raw_payload.normalized_regime_context_features)
                if raw_payload is not None
                else {}
            )
        )
        proxy_diagnostics = dict(
            pending.get("proxy_diagnostics")
            or (
                dict(raw_payload.metadata.get("proxy_diagnostics", {}))
                if raw_payload is not None
                else {}
            )
        )
        raw_zero_default_keys = list(
            pending.get("raw_zero_default_keys")
            or (
                list(raw_payload.metadata.get("raw_zero_default_keys", []))
                if raw_payload is not None
                else []
            )
        )
        event_path_summary = dict(
            pending.get("event_path_summary")
            or (
                dict(pending["event"].path_summary)
                if pending.get("event") is not None
                else {}
            )
        )
        event_path_label = str(
            pending.get("event_path_label")
            or (
                pending["event"].path_label
                if pending.get("event") is not None
                else ""
            )
        )
        event_side_payload = dict(
            pending.get("event_side_payload")
            or (
                dict(pending["event"].side_payload)
                if pending.get("event") is not None
                else {}
            )
        )
        event_diagnostics = dict(
            pending.get("event_diagnostics")
            or (
                dict(pending["event"].diagnostics)
                if pending.get("event") is not None
                else {}
            )
        )
        event_quality_score = float(
            pending.get("event_quality_score")
            if pending.get("event_quality_score") is not None
            else (
                float(pending["event"].quality_score)
                if pending.get("event") is not None
                else 0.0
            )
        )
        liquidity_score = float(
            pending.get("liquidity_score")
            if pending.get("liquidity_score") is not None
            else max(
                0.0,
                min(
                    1.0,
                    compute_bar_features(pending.get("history_window") or []).get("volume_mean", 0.0) / 1_000_000.0,
                ),
            )
        )
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
                **event_path_summary,
                "path_label": event_path_label,
                "feature_end_date": pending["feature_end_date"],
                "embedding": embedding,
                "raw_features": dict(raw_features),
                "transformed_features": dict(transformed_features),
                "raw_regime_context_features": raw_regime_context_features,
                "normalized_regime_context_features": normalized_regime_context_features,
                "transform_version": transform.version,
                "proxy_diagnostics": proxy_diagnostics,
                "raw_zero_default_keys": raw_zero_default_keys,
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
            side_outcomes=event_side_payload,
            diagnostics={
                **event_diagnostics,
                "decision_cutoff": decision_date,
                "feature_end_date": pending["feature_end_date"],
                "embedding": embedding,
                "raw_features": dict(raw_features),
                "transformed_features": dict(transformed_features),
                "shape_vector": shape_vector,
                "ctx_vector": ctx_vector,
                "raw_regime_context_features": raw_regime_context_features,
                "regime_context_features": raw_regime_context_features,
                "normalized_regime_context_features": normalized_regime_context_features,
                "transform_version": transform.version,
                "regime_code": pending["regime_code"],
                "regime_code_raw_macro": pending["regime_code_raw_macro"],
                "regime_source": REGIME_SOURCE_NORMALIZED,
                "regime_inputs_summary": dict(pending["regime_inputs_summary"]),
                "sector_code": pending["lib_sector"],
                "proxy_diagnostics": proxy_diagnostics,
                "sector_proxy_fallback_to_self": bool((((proxy_diagnostics or {}).get("sector") or {}).get("fallback_to_self", False))),
                "raw_zero_default_keys": raw_zero_default_keys,
                "transform_missing_keys_filled_zero": transform_missing_keys_filled_zero,
                "transformed_zero_feature_keys": transformed_zero_feature_keys,
                "macro_history_length": pending["macro_history_length"],
                "macro_series_present_count": pending["macro_series_present_count"],
                "macro_freshness_summary": dict(pending["macro_freshness_summary"]),
                "macro_asof_ts_utc": pending["macro_asof_ts_utc"],
                "breadth_policy": pending["breadth_policy"],
                "breadth_present": pending["breadth_present"],
                "breadth_missing_reason": pending["breadth_missing_reason"],
                "liquidity_score": liquidity_score,
                "quality_score": event_quality_score,
                **pending["anchor_fields"],
            },
        ))
        now = perf_counter()
        if (
            pending_idx == pending_record_count
            or pending_idx == 1
            or pending_idx % pending_progress_every == 0
            or (now - last_transform_progress_at) >= 10.0
        ):
            _emit_progress(
                {
                    "phase": "transform",
                    "status": "running",
                    "raw_event_row_count": raw_event_row_count,
                    "pending_record_count": pending_record_count,
                    "event_record_count": len(event_records),
                }
            )
            last_transform_progress_at = now
    phase_timings_ms["transform"] = int((perf_counter() - transform_started) * 1000)
    _emit_progress(
        {
            "phase": "transform",
            "status": "ok",
            "raw_event_row_count": raw_event_row_count,
            "pending_record_count": pending_record_count,
            "event_record_count": len(event_records),
            "transform_ms": phase_timings_ms["transform"],
        }
    )
    if prototype_input_path:
        _write_pickle_atomic(
            prototype_input_path,
            _prototype_input_payload(
                decision_date=decision_date,
                spec=spec,
                event_records=event_records,
                excluded_reasons=excluded_reasons,
                anchor_count=len(anchor_library),
                transform=transform,
                phase_timings_ms=phase_timings_ms,
            ),
        )
    prototype_started = perf_counter()
    _emit_progress(
        {
            "phase": "prototype",
            "status": "running",
            "event_record_count": len(event_records),
            "prototype_count": 0,
        }
    )
    prototypes = (
        build_state_prototypes_from_event_memory(
            event_records=event_records,
            as_of_date=decision_date,
            memory_version=spec.memory_version,
            spec_hash=spec.spec_hash(),
            config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version),
            progress_callback=_emit_progress,
            checkpoint_path=prototype_checkpoint_path,
            resume_from_checkpoint=resume_prototype_from_checkpoint,
            comparison_block_size=comparison_block_size,
        )
        if event_records
        else []
    )
    compression_audit = build_prototype_compression_audit(event_records=event_records, prototypes=prototypes, as_of_date=decision_date)
    phase_timings_ms["prototype"] = int((perf_counter() - prototype_started) * 1000)
    _emit_progress(
        {
            "phase": "prototype",
            "status": "ok",
            "event_record_count": len(event_records),
            "prototype_count": len(prototypes),
            "prototype_ms": phase_timings_ms["prototype"],
        }
    )
    coverage = {"event_record_count": len(event_records), "anchor_count": len(anchor_library), "prototype_count": len(prototypes)}
    return {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "as_of_date": decision_date, "coverage": coverage, "excluded_reasons": excluded_reasons, "event_records": event_records, "anchor_library": anchor_library, "prototypes": prototypes, "scaler": scaler, "transform": transform, "compression_audit": compression_audit, "phase_timings_ms": phase_timings_ms}


def build_event_memory_asof(*, decision_date: str, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, lookback_bars: int = 5, metadata: dict | None = None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None, progress_callback=None, use_proxy_aggregate_cache: bool = False, use_event_memory_fast_path: bool = False, event_input_path: str | None = None, event_checkpoint_path: str | None = None, resume_event_memory_from_checkpoint: bool = False, prototype_input_path: str | None = None, prototype_checkpoint_path: str | None = None, resume_prototype_from_checkpoint: bool = False, comparison_block_size: int = 2048) -> dict:
    pending_bundle = (
        _build_event_memory_pending_records_fast(
            decision_date=decision_date,
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            metadata=metadata,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
            progress_callback=progress_callback,
            use_proxy_aggregate_cache=use_proxy_aggregate_cache,
            event_input_path=event_input_path,
            event_checkpoint_path=event_checkpoint_path,
            resume_event_memory_from_checkpoint=resume_event_memory_from_checkpoint,
            lookback_bars=lookback_bars,
        )
        if use_event_memory_fast_path
        else _build_event_memory_pending_records_legacy(
            decision_date=decision_date,
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            lookback_bars=lookback_bars,
            metadata=metadata,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
            progress_callback=progress_callback,
            use_proxy_aggregate_cache=use_proxy_aggregate_cache,
        )
    )
    return _finalize_event_memory_from_pending_records(
        decision_date=decision_date,
        spec=spec,
        pending_bundle=pending_bundle,
        progress_callback=progress_callback,
        prototype_input_path=prototype_input_path,
        prototype_checkpoint_path=prototype_checkpoint_path,
        resume_prototype_from_checkpoint=resume_prototype_from_checkpoint,
        comparison_block_size=comparison_block_size,
    )


def _event_memory_from_raw_cache(
    *,
    decision_date: str,
    spec: ResearchExperimentSpec,
    event_cache_handle: EventRawCacheHandle,
    progress_callback,
    prototype_checkpoint_path: str | None,
    resume_prototype_from_checkpoint: bool,
    comparison_block_size: int,
) -> dict[str, Any]:
    import duckdb

    def _emit_progress(payload: dict[str, Any]) -> None:
        if progress_callback is not None:
            progress_callback(dict(payload))

    event_memory_started = perf_counter()
    _emit_progress(
        {
            "phase": "event_memory",
            "status": "running",
            "symbols_done": 0,
            "symbols_total": 0,
            "current_symbol": "",
            "raw_event_row_count": 0,
            "pending_record_count": 0,
            "event_candidate_total": 0,
            "event_candidate_done": 0,
            "event_cache_build_ms": int(event_cache_handle.build_ms),
        }
    )
    con = duckdb.connect()
    try:
        event_frame = con.execute(
            "SELECT * FROM read_parquet(?) WHERE outcome_end_date < ? ORDER BY event_ordinal",
            [event_cache_handle.events_path, str(decision_date)],
        ).fetch_df()
    finally:
        con.close()
    eligible_event_rows = [_event_cache_row_from_storage(row) for row in event_frame.to_dict(orient="records")]
    eligible_event_count = len(eligible_event_rows)
    raw_feature_matrix = event_cache_handle.load_raw_features(mmap_mode="r")
    feature_keys = list(event_cache_handle.feature_keys)
    feature_index = event_cache_handle.feature_index
    scaler_started = perf_counter()
    transform = _feature_transform_from_prefix_stats(
        event_cache_handle=event_cache_handle,
        eligible_event_count=eligible_event_count,
    )
    scaler_reconstruct_ms = int((perf_counter() - scaler_started) * 1000)
    _emit_progress(
        {
            "phase": "event_memory",
            "status": "ok",
            "symbols_done": 0,
            "symbols_total": 0,
            "current_symbol": "",
            "raw_event_row_count": eligible_event_count,
            "pending_record_count": eligible_event_count,
            "event_candidate_total": eligible_event_count,
            "event_candidate_done": eligible_event_count,
            "event_memory_ms": int((perf_counter() - event_memory_started) * 1000),
            "event_cache_build_ms": int(event_cache_handle.build_ms),
            "eligible_event_count": eligible_event_count,
            "scaler_reconstruct_ms": scaler_reconstruct_ms,
        }
    )
    transform_started = perf_counter()
    _emit_progress(
        {
            "phase": "transform",
            "status": "running",
            "raw_event_row_count": eligible_event_count,
            "pending_record_count": eligible_event_count,
            "event_record_count": 0,
        }
    )
    event_records: list[EventOutcomeRecord] = []
    progress_every = max(1, min(250, eligible_event_count or 1))
    last_progress_at = perf_counter()
    for row_index, cached_row in enumerate(eligible_event_rows, start=1):
        event_ordinal = int(cached_row.get("event_ordinal") or 0)
        present_feature_keys = [str(item) for item in list(cached_row.get("present_feature_keys") or [])]
        raw_row = np.asarray(raw_feature_matrix[event_ordinal], dtype=np.float64) if feature_keys else np.zeros((0,), dtype=np.float64)
        raw_features = {
            key: float(raw_row[feature_index[key]])
            for key in present_feature_keys
            if key in feature_index
        }
        transformed_features, embedding = transform.apply(raw_features)
        transform_missing_keys_filled_zero = sorted(key for key in transform.feature_keys if key not in raw_features)
        transformed_zero_feature_keys = sorted(
            key for key, value in transformed_features.items() if abs(float(value)) <= 1e-12
        )
        shape_keys = [str(item) for item in list(cached_row.get("shape_keys") or [])]
        ctx_keys = [str(item) for item in list(cached_row.get("ctx_keys") or [])]
        shape_vector = [float(transformed_features.get(key, 0.0)) for key in shape_keys]
        ctx_vector = [float(transformed_features.get(key, 0.0)) for key in ctx_keys]
        anchor_fields = dict(cached_row.get("anchor_fields") or {})
        event_path_summary = dict(cached_row.get("event_path_summary") or {})
        event_diagnostics = dict(cached_row.get("event_diagnostics") or {})
        proxy_diagnostics = dict(cached_row.get("proxy_diagnostics") or {})
        raw_regime_context_features = dict(cached_row.get("raw_regime_context_features") or {})
        normalized_regime_context_features = dict(cached_row.get("normalized_regime_context_features") or {})
        raw_zero_default_keys = list(cached_row.get("raw_zero_default_keys") or [])
        event_records.append(
            EventOutcomeRecord(
                symbol=str(cached_row["symbol"]),
                event_date=str(cached_row["feature_end_date"]),
                outcome_end_date=str(cached_row["outcome_end_date"]),
                schema_version=spec.label_version,
                exchange_code=anchor_fields.get("exchange_code"),
                country_code=anchor_fields.get("country_code"),
                exchange_tz=anchor_fields.get("exchange_tz"),
                session_date_local=anchor_fields.get("session_date_local"),
                session_close_ts_local=anchor_fields.get("session_close_ts_local"),
                session_close_ts_utc=anchor_fields.get("session_close_ts_utc"),
                feature_anchor_ts_utc=anchor_fields.get("feature_anchor_ts_utc"),
                macro_asof_ts_utc=cached_row.get("macro_asof_ts_utc"),
                path_summary={
                    **event_path_summary,
                    "path_label": str(cached_row.get("event_path_label") or ""),
                    "feature_end_date": str(cached_row["feature_end_date"]),
                    "embedding": embedding,
                    "raw_features": dict(raw_features),
                    "transformed_features": dict(transformed_features),
                    "raw_regime_context_features": raw_regime_context_features,
                    "normalized_regime_context_features": normalized_regime_context_features,
                    "transform_version": transform.version,
                    "proxy_diagnostics": proxy_diagnostics,
                    "raw_zero_default_keys": raw_zero_default_keys,
                    "transform_missing_keys_filled_zero": transform_missing_keys_filled_zero,
                    "regime_source": REGIME_SOURCE_NORMALIZED,
                    "regime_inputs_summary": dict(cached_row.get("regime_inputs_summary") or {}),
                    "macro_freshness_summary": dict(cached_row.get("macro_freshness_summary") or {}),
                    "macro_asof_ts_utc": cached_row.get("macro_asof_ts_utc"),
                    "breadth_policy": cached_row.get("breadth_policy"),
                    "breadth_present": bool(cached_row.get("breadth_present")),
                    "breadth_missing_reason": cached_row.get("breadth_missing_reason"),
                    **anchor_fields,
                },
                side_outcomes=dict(cached_row.get("event_side_payload") or {}),
                diagnostics={
                    **event_diagnostics,
                    "decision_cutoff": decision_date,
                    "feature_end_date": str(cached_row["feature_end_date"]),
                    "embedding": embedding,
                    "raw_features": dict(raw_features),
                    "transformed_features": dict(transformed_features),
                    "shape_vector": shape_vector,
                    "ctx_vector": ctx_vector,
                    "raw_regime_context_features": raw_regime_context_features,
                    "regime_context_features": raw_regime_context_features,
                    "normalized_regime_context_features": normalized_regime_context_features,
                    "transform_version": transform.version,
                    "regime_code": cached_row.get("regime_code"),
                    "regime_code_raw_macro": cached_row.get("regime_code_raw_macro"),
                    "regime_source": REGIME_SOURCE_NORMALIZED,
                    "regime_inputs_summary": dict(cached_row.get("regime_inputs_summary") or {}),
                    "sector_code": cached_row.get("lib_sector"),
                    "proxy_diagnostics": proxy_diagnostics,
                    "sector_proxy_fallback_to_self": bool((((proxy_diagnostics or {}).get("sector") or {}).get("fallback_to_self", False))),
                    "raw_zero_default_keys": raw_zero_default_keys,
                    "transform_missing_keys_filled_zero": transform_missing_keys_filled_zero,
                    "transformed_zero_feature_keys": transformed_zero_feature_keys,
                    "macro_history_length": int(cached_row.get("macro_history_length") or 0),
                    "macro_series_present_count": int(cached_row.get("macro_series_present_count") or 0),
                    "macro_freshness_summary": dict(cached_row.get("macro_freshness_summary") or {}),
                    "macro_asof_ts_utc": cached_row.get("macro_asof_ts_utc"),
                    "breadth_policy": cached_row.get("breadth_policy"),
                    "breadth_present": bool(cached_row.get("breadth_present")),
                    "breadth_missing_reason": cached_row.get("breadth_missing_reason"),
                    "liquidity_score": float(cached_row.get("liquidity_score") or 0.0),
                    "quality_score": float(cached_row.get("event_quality_score") or 0.0),
                    **anchor_fields,
                },
            )
        )
        now = perf_counter()
        if row_index == eligible_event_count or row_index == 1 or row_index % progress_every == 0 or (now - last_progress_at) >= 10.0:
            _emit_progress(
                {
                    "phase": "transform",
                    "status": "running",
                    "raw_event_row_count": eligible_event_count,
                    "pending_record_count": eligible_event_count,
                    "event_record_count": len(event_records),
                }
            )
            last_progress_at = now
    transform_ms = int((perf_counter() - transform_started) * 1000)
    _emit_progress(
        {
            "phase": "transform",
            "status": "ok",
            "raw_event_row_count": eligible_event_count,
            "pending_record_count": eligible_event_count,
            "event_record_count": len(event_records),
            "transform_ms": transform_ms,
        }
    )
    prototype_started = perf_counter()
    _emit_progress(
        {
            "phase": "prototype",
            "status": "running",
            "event_record_count": len(event_records),
            "prototype_count": 0,
        }
    )
    prototypes = (
        build_state_prototypes_from_event_memory(
            event_records=event_records,
            as_of_date=decision_date,
            memory_version=spec.memory_version,
            spec_hash=spec.spec_hash(),
            config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version),
            progress_callback=_emit_progress,
            checkpoint_path=prototype_checkpoint_path,
            resume_from_checkpoint=resume_prototype_from_checkpoint,
            comparison_block_size=comparison_block_size,
        )
        if event_records
        else []
    )
    prototype_ms = int((perf_counter() - prototype_started) * 1000)
    _emit_progress(
        {
            "phase": "prototype",
            "status": "ok",
            "event_record_count": len(event_records),
            "prototype_count": len(prototypes),
            "prototype_ms": prototype_ms,
        }
    )
    return {
        "spec": spec.to_dict(),
        "spec_hash": spec.spec_hash(),
        "as_of_date": decision_date,
        "coverage": {"event_record_count": len(event_records), "anchor_count": 0, "prototype_count": len(prototypes)},
        "excluded_reasons": [],
        "event_records": event_records,
        "anchor_library": [],
        "prototypes": prototypes,
        "scaler": transform.scaler,
        "transform": transform,
        "compression_audit": build_prototype_compression_audit(event_records=event_records, prototypes=prototypes, as_of_date=decision_date),
        "phase_timings_ms": {
            "event_memory": int((perf_counter() - event_memory_started) * 1000),
            "transform": transform_ms,
            "prototype": prototype_ms,
            "event_cache_build": int(event_cache_handle.build_ms),
            "scaler_reconstruct": scaler_reconstruct_ms,
        },
        "event_cache_build_ms": int(event_cache_handle.build_ms),
        "eligible_event_count": eligible_event_count,
        "scaler_reconstruct_ms": scaler_reconstruct_ms,
    }


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


def fit_train_artifacts(*, run_id: str, artifact_store: JsonResearchArtifactStore, train_end: str, test_start: str, purge: int, embargo: int, spec: ResearchExperimentSpec, bars_by_symbol: Dict[str, List[HistoricalBar]], macro_history_by_date: Dict[str, Dict[str, float]], sector_map: Dict[str, str], market: str, calibration_artifact: dict | None = None, quote_policy_calibration: dict | None = None, metadata: dict | None = None, session_metadata_by_symbol: Dict[str, SymbolSessionMetadata] | None = None, macro_series_history: List[Dict[str, Any]] | None = None, progress_callback=None, use_proxy_aggregate_cache: bool = False, use_event_memory_fast_path: bool = False, event_input_path: str | None = None, event_checkpoint_path: str | None = None, resume_event_memory_from_checkpoint: bool = False, prototype_input_path: str | None = None, prototype_checkpoint_path: str | None = None, resume_prototype_from_checkpoint: bool = False, comparison_block_size: int = 2048, event_cache_handle: EventRawCacheHandle | None = None) -> dict:
    resumed_preprototype = (
        _load_prototype_input_payload(path=prototype_input_path, decision_date=train_end, spec=spec)
        if prototype_input_path and prototype_checkpoint_path and resume_prototype_from_checkpoint
        else None
    )
    resumed_prototype_metadata = (
        _load_prototype_resume_metadata(checkpoint_path=prototype_checkpoint_path, decision_date=train_end, spec=spec)
        if prototype_checkpoint_path and resume_prototype_from_checkpoint
        else None
    )
    if resumed_prototype_metadata is not None:
        transform = resumed_prototype_metadata["transform"]
        phase_timings_ms = dict(resumed_prototype_metadata.get("phase_timings_ms") or {})
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "event_memory",
                    "status": "ok",
                    "symbols_done": len(bars_by_symbol),
                    "symbols_total": len(bars_by_symbol),
                    "current_symbol": "",
                    "raw_event_row_count": int((resumed_prototype_metadata.get("coverage") or {}).get("event_record_count") or 0),
                    "pending_record_count": int((resumed_prototype_metadata.get("coverage") or {}).get("event_record_count") or 0),
                    "event_memory_ms": int(phase_timings_ms.get("event_memory") or 0),
                }
            )
            progress_callback(
                {
                    "phase": "transform",
                    "status": "ok",
                    "raw_event_row_count": int((resumed_prototype_metadata.get("coverage") or {}).get("event_record_count") or 0),
                    "pending_record_count": int((resumed_prototype_metadata.get("coverage") or {}).get("event_record_count") or 0),
                    "event_record_count": int((resumed_prototype_metadata.get("coverage") or {}).get("event_record_count") or 0),
                    "transform_ms": int(phase_timings_ms.get("transform") or 0),
                }
            )
            progress_callback(
                {
                    "phase": "prototype",
                    "status": "running",
                    "event_record_count": int((resumed_prototype_metadata.get("coverage") or {}).get("event_record_count") or 0),
                    "prototype_count": 0,
                }
            )
        prototype_started = perf_counter()
        prototypes = build_state_prototypes_from_event_memory(
            event_records=[],
            as_of_date=train_end,
            memory_version=spec.memory_version,
            spec_hash=spec.spec_hash(),
            config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version),
            progress_callback=progress_callback,
            checkpoint_path=prototype_checkpoint_path,
            resume_from_checkpoint=True,
            comparison_block_size=comparison_block_size,
        )
        phase_timings_ms["prototype"] = int((perf_counter() - prototype_started) * 1000)
        memory = {
            "event_records": [],
            "prototypes": prototypes,
            "transform": transform,
            "scaler": transform.scaler,
            "excluded_reasons": list(resumed_prototype_metadata.get("excluded_reasons") or []),
            "anchor_library": [],
            "coverage": dict(resumed_prototype_metadata.get("coverage") or {}),
            "compression_audit": dict(resumed_prototype_metadata.get("compression_audit") or {}),
            "phase_timings_ms": phase_timings_ms,
        }
        max_train_date = resumed_prototype_metadata.get("max_train_date")
        max_outcome_end = resumed_prototype_metadata.get("max_outcome_end_date")
    elif event_cache_handle is not None:
        memory = _event_memory_from_raw_cache(
            decision_date=train_end,
            spec=spec,
            event_cache_handle=event_cache_handle,
            progress_callback=progress_callback,
            prototype_checkpoint_path=prototype_checkpoint_path,
            resume_prototype_from_checkpoint=resume_prototype_from_checkpoint,
            comparison_block_size=comparison_block_size,
        )
    elif resumed_preprototype is None:
        memory = build_event_memory_asof(
            decision_date=train_end,
            spec=spec,
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            market=market,
            metadata=metadata,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
            progress_callback=progress_callback,
            use_proxy_aggregate_cache=use_proxy_aggregate_cache,
            use_event_memory_fast_path=use_event_memory_fast_path,
            event_input_path=event_input_path,
            event_checkpoint_path=event_checkpoint_path,
            resume_event_memory_from_checkpoint=resume_event_memory_from_checkpoint,
            prototype_input_path=prototype_input_path,
            prototype_checkpoint_path=prototype_checkpoint_path,
            resume_prototype_from_checkpoint=False,
            comparison_block_size=comparison_block_size,
        )
    else:
        event_records = list(resumed_preprototype["event_records"])
        transform = resumed_preprototype["transform"]
        phase_timings_ms = dict(resumed_preprototype.get("phase_timings_ms") or {})
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "event_memory",
                    "status": "ok",
                    "symbols_done": len(bars_by_symbol),
                    "symbols_total": len(bars_by_symbol),
                    "current_symbol": "",
                    "raw_event_row_count": 0,
                    "pending_record_count": len(event_records),
                    "event_memory_ms": int(phase_timings_ms.get("event_memory") or 0),
                }
            )
            progress_callback(
                {
                    "phase": "transform",
                    "status": "ok",
                    "raw_event_row_count": 0,
                    "pending_record_count": len(event_records),
                    "event_record_count": len(event_records),
                    "transform_ms": int(phase_timings_ms.get("transform") or 0),
                }
            )
            progress_callback(
                {
                    "phase": "prototype",
                    "status": "running",
                    "event_record_count": len(event_records),
                    "prototype_count": 0,
                }
            )
        prototype_started = perf_counter()
        prototypes = build_state_prototypes_from_event_memory(
            event_records=event_records,
            as_of_date=train_end,
            memory_version=spec.memory_version,
            spec_hash=spec.spec_hash(),
            config=PrototypeConfig(dedup_similarity_threshold=0.985, memory_version=spec.memory_version),
            progress_callback=progress_callback,
            checkpoint_path=prototype_checkpoint_path,
            resume_from_checkpoint=True,
            comparison_block_size=comparison_block_size,
        )
        phase_timings_ms["prototype"] = int((perf_counter() - prototype_started) * 1000)
        compression_audit = build_prototype_compression_audit(event_records=event_records, prototypes=prototypes, as_of_date=train_end)
        memory = {
            "event_records": event_records,
            "prototypes": prototypes,
            "transform": transform,
            "scaler": transform.scaler,
            "excluded_reasons": list(resumed_preprototype.get("excluded_reasons") or []),
            "anchor_library": [],
            "coverage": {
                "event_record_count": len(event_records),
                "anchor_count": int(resumed_preprototype.get("anchor_count") or 0),
                "prototype_count": len(prototypes),
            },
            "compression_audit": compression_audit,
            "phase_timings_ms": phase_timings_ms,
        }
        max_train_date = max((r.event_date for r in memory["event_records"]), default=None)
        max_outcome_end = max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None)
    if resumed_prototype_metadata is None:
        max_train_date = max((r.event_date for r in memory["event_records"]), default=None)
        max_outcome_end = max((r.outcome_end_date for r in memory["event_records"] if r.outcome_end_date), default=None)
    if max_outcome_end and max_outcome_end >= test_start:
        raise AssertionError("future event/outcome mixed into train artifact")
    snapshot_id = f"{run_id}:{train_end}:{spec.spec_hash()}"
    phase_timings_ms = dict(memory.get("phase_timings_ms") or {})
    prototype_payload = {
        "spec_hash": spec.spec_hash(),
        "snapshot_id": snapshot_id,
        "prototype_count": len(memory["prototypes"]),
        "prototypes": memory["prototypes"],
    }
    if resumed_prototype_metadata is None and prototype_checkpoint_path:
        resume_metadata = _prototype_resume_metadata_payload(
            decision_date=train_end,
            spec=spec,
            excluded_reasons=list(memory.get("excluded_reasons") or []),
            anchor_count=int((memory.get("coverage") or {}).get("anchor_count") or 0),
            transform=memory["transform"],
            phase_timings_ms=phase_timings_ms,
            coverage=dict(memory.get("coverage") or {}),
            compression_audit=dict(memory.get("compression_audit") or {}),
            max_train_date=max_train_date,
            max_outcome_end_date=max_outcome_end,
        )
        _prototype_resume_metadata_path(prototype_checkpoint_path).write_text(
            json.dumps(resume_metadata, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    prototype_manifest_path = ""
    prototype_snapshot_name = f"prototype_snapshot_{train_end.replace('-', '')}"
    prototype_write_started = perf_counter()
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "artifact_write",
                "status": "running",
                "event_record_count": int((memory.get("coverage") or {}).get("event_record_count") or len(memory["event_records"])),
                "prototype_count": len(memory["prototypes"]),
                "artifact_rows_total": len(memory["prototypes"]),
                "artifact_rows_done": 0,
                "artifact_part_count": 0,
                "artifact_bytes_written": 0,
            }
        )
    prototype_manifest_path = artifact_store.save_prototype_snapshot(
        run_id=run_id,
        name=prototype_snapshot_name,
        as_of_date=train_end,
        memory_version=spec.memory_version,
        payload=prototype_payload,
        progress_callback=progress_callback,
    )
    phase_timings_ms["artifact_write"] = int(
        phase_timings_ms.get("artifact_write", 0) + ((perf_counter() - prototype_write_started) * 1000)
    )
    prototype_part_count = len(list((Path(prototype_manifest_path).parent / "parts").glob("*.parquet")))
    prototype_bytes_written = sum(
        path.stat().st_size for path in [Path(prototype_manifest_path), *(Path(prototype_manifest_path).parent / "parts").glob("*.parquet")] if path.exists()
    )
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "prototype",
                "status": "ok",
                "event_record_count": int((memory.get("coverage") or {}).get("event_record_count") or len(memory["event_records"])),
                "prototype_count": len(memory["prototypes"]),
                "prototype_ms": int(phase_timings_ms.get("prototype") or 0),
            }
        )
        progress_callback(
            {
                "phase": "artifact_write",
                "status": "running",
                "event_record_count": int((memory.get("coverage") or {}).get("event_record_count") or len(memory["event_records"])),
                "prototype_count": len(memory["prototypes"]),
                "artifact_rows_total": len(memory["prototypes"]),
                "artifact_rows_done": len(memory["prototypes"]),
                "artifact_part_count": prototype_part_count,
                "artifact_bytes_written": int(prototype_bytes_written),
            }
        )
    return {
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "spec_hash": spec.spec_hash(),
        "as_of_date": train_end,
        "train_end": train_end,
        "test_start": test_start,
        "purge": purge,
        "embargo": embargo,
        "memory_version": spec.memory_version,
        "prototype_snapshot_name": prototype_snapshot_name,
        "prototype_snapshot_format": "prototype_snapshot_v4",
        "prototype_snapshot_manifest_path": prototype_manifest_path,
        "max_train_date": max_train_date,
        "max_outcome_end_date": max_outcome_end,
        "event_record_count": int((memory.get("coverage") or {}).get("event_record_count") or len(memory["event_records"])),
        "prototype_count": len(memory["prototypes"]),
        "prototypes": [p.__dict__ for p in memory["prototypes"]],
        "scaler": memory["transform"].scaler,
        "transform": memory["transform"],
        "calibration": dict(
            calibration_artifact
            or {"method": "logistic", "slope": 1.0, "intercept": 0.0, "ev_slope": 1.0, "ev_intercept": 0.0}
        ),
        "quote_policy_calibration": dict(
            quote_policy_calibration
            or {
                "ev_threshold": 0.005,
                "uncertainty_cap": 0.12,
                "min_effective_sample_size": 1.5,
                "min_fill_probability": 0.1,
                "abstain_margin": 0.05,
            }
        ),
        "metadata": dict(metadata or {}),
        "session_metadata_by_symbol": {
            symbol: session_metadata_to_dict(meta) for symbol, meta in (session_metadata_by_symbol or {}).items()
        },
        "macro_series_history": list(macro_series_history or []),
        "snapshot_ids": {"prototype_snapshot_id": snapshot_id},
        "phase_timings_ms": phase_timings_ms,
        "event_cache_format": str(getattr(event_cache_handle, "format_version", "") or ""),
        "event_cache_manifest_path": str(getattr(event_cache_handle, "manifest_path", "") or ""),
        "event_cache_build_ms": int(memory.get("event_cache_build_ms") or 0),
        "eligible_event_count": int(memory.get("eligible_event_count") or 0),
        "scaler_reconstruct_ms": int(memory.get("scaler_reconstruct_ms") or 0),
    }


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
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration, query_date=decision_date)
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
    compression_batches: list[dict] = []
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
        compression_batches.append(dict(memory.get("compression_audit") or {"as_of_date": decision_date}))
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
            long_ev = estimate_expected_value(side=Side.BUY.value, query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration, query_date=decision_date)
            short_ev = estimate_expected_value(side=Side.SELL.value, query_embedding=q["embedding"], candidates=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration, query_date=decision_date)
            surface = build_decision_surface(query_embedding=q["embedding"], prototype_pool=prototype_pool, regime_code=regime_code, sector_code=sector_code, ev_config=ev_cfg, candidate_index=ExactCosineCandidateIndex(), calibration=calibration, query_date=decision_date)
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
    diagnostics["prototype_compression_batches"] = compression_batches
    diagnostics["prototype_compression_audit"] = aggregate_prototype_compression_batches(compression_batches)
    diagnostics["throughput"] = {"n_symbols": len(bars_by_symbol), "n_decision_dates": len(decision_dates), "prototype_count": total_prototype_count, "wall_clock_seconds": perf_counter() - t0}
    diagnostics["artifacts"] = {"spec": spec.to_dict(), "spec_hash": spec.spec_hash(), "excluded_reasons": all_excluded_reasons, "excluded_reasons_histogram": dict(Counter(r.get("reason", "unknown") for r in all_excluded_reasons))}
    return out, diagnostics

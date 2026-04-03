from __future__ import annotations

from bisect import bisect_right
from dataclasses import replace
import json
import math
import os
import shutil
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from unittest.mock import patch

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.db.local_session import create_backtest_session_factory
from backtest_app.db.local_write_session import create_backtest_write_session_factory
from backtest_app.historical_data.features import FeatureScaler, FeatureTransform
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.artifacts import (
    PROTOTYPE_CORE_RETRIEVAL_COLUMNS,
    JsonResearchArtifactStore,
    PrototypeSnapshotHandle,
    load_prototype_subset,
)
from backtest_app.research.models import DecisionSurface, DistributionEstimate, StatePrototype
from backtest_app.research.pipeline import (
    ProxySeriesResult,
    _chosen_side_payload,
    _ev_config_from_metadata,
    _side_diag,
    build_event_raw_cache,
    build_query_feature_payload_asof,
    build_event_memory_asof,
    fit_train_artifacts,
    generate_similarity_candidates_rolling,
)
from backtest_app.research.pre_optuna import build_pre_optuna_evidence
from backtest_app.research.scoring import (
    CalibrationModel,
    build_decision_surface_from_ranked_candidates,
    exact_block_prototype_topk,
)
from backtest_app.research_runtime import engine as research_engine
from backtest_app.research_runtime.frozen_seed import (
    CALIBRATION_UNIVERSE_SEED_PROFILE,
    build_optuna_replay_seed,
    write_calibration_bundle_artifacts,
    write_study_cache_from_rows,
)

BUNDLE_STATUSES = {"pending", "running", "ok", "partial", "failed"}
CHUNK_STATUSES = {"pending", "running", "reused", "ok", "failed"}
SNAPSHOT_STATUSES = {"pending", "running", "ok", "failed"}
QUERY_CHUNK_STATUSES = {"pending", "running", "reused", "ok", "failed"}
DAILY_REUSE_MODEL_VERSION = "daily_reuse_v1"
MONTHLY_SNAPSHOT_MODEL_VERSION = "monthly_snapshot_v1"
TRAIN_SNAPSHOT_ARTIFACT_KIND = "train_snapshot_v4"
STALE_SNAPSHOT_CONTRACT_ERROR = "stale_artifact_contract_v4"
STALE_BUNDLE_CONTRACT_ERROR = "stale_bundle_contract_v3"
DEFAULT_MODEL_VERSION_BY_CADENCE = {
    "daily": DAILY_REUSE_MODEL_VERSION,
    "monthly": MONTHLY_SNAPSHOT_MODEL_VERSION,
}


class ForbiddenCalibrationBundleCall(RuntimeError):
    def __init__(self, call_name: str):
        super().__init__(f"forbidden calibration bundle call: {call_name}")
        self.call_name = call_name


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_iso() -> str:
    return _utcnow().isoformat()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _calendar_date(text_value: str) -> datetime.date:
    return datetime.fromisoformat(str(text_value)[:10]).date()


def _date_iso(value: datetime.date) -> str:
    return value.isoformat()


def _to_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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


def _resolve_model_version(snapshot_cadence: str, model_version: str = "") -> str:
    cadence = str(snapshot_cadence or "daily").strip().lower() or "daily"
    if model_version:
        return str(model_version)
    return DEFAULT_MODEL_VERSION_BY_CADENCE.get(cadence, DAILY_REUSE_MODEL_VERSION)


def _spec_or_default(spec: ResearchExperimentSpec | None) -> ResearchExperimentSpec:
    return spec or ResearchExperimentSpec()


def _decision_date(bar: HistoricalBar) -> str:
    return str(bar.timestamp)[:10]


def _session_exchange_code(
    symbol: str,
    session_metadata_by_symbol: Mapping[str, Any] | None,
) -> str | None:
    if not session_metadata_by_symbol:
        return None
    payload = session_metadata_by_symbol.get(symbol)
    if payload is None:
        return None
    if hasattr(payload, "exchange_code"):
        value = getattr(payload, "exchange_code", None)
    elif isinstance(payload, Mapping):
        value = payload.get("exchange_code")
    else:
        value = None
    text = str(value or "").strip()
    return text or None


def _aggregate_scope_key(exchange_code: str | None) -> str:
    return str(exchange_code or "__ALL__")


def _aggregate_add(
    aggregate_by_date: dict[str, dict[str, Any]],
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
    open_sum = _to_float(bucket.get("open_sum"))
    high_sum = _to_float(bucket.get("high_sum"))
    low_sum = _to_float(bucket.get("low_sum"))
    close_sum = _to_float(bucket.get("close_sum"))
    volume_sum = _to_float(bucket.get("volume_sum"))
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


def _build_query_proxy_aggregate_cache(
    *,
    symbols: Sequence[str],
    bars_by_symbol: Mapping[str, Sequence[HistoricalBar]],
    sector_map: Mapping[str, str],
    session_metadata_by_symbol: Mapping[str, Any] | None,
) -> dict[str, Any]:
    symbol_bar_by_date: dict[str, dict[str, HistoricalBar]] = {}
    symbol_first_date: dict[str, str] = {}
    market_scope_symbols: dict[str, list[str]] = defaultdict(list)
    sector_scope_symbols: dict[tuple[str, str], list[str]] = defaultdict(list)
    market_aggregate_by_scope: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    sector_aggregate_by_scope: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)

    for raw_symbol in [str(item) for item in symbols if item]:
        exchange_scope = _aggregate_scope_key(_session_exchange_code(raw_symbol, session_metadata_by_symbol))
        market_scope_symbols[exchange_scope].append(raw_symbol)
        sector_code = str(sector_map.get(raw_symbol) or "").strip()
        if sector_code:
            sector_scope_symbols[(sector_code, exchange_scope)].append(raw_symbol)
        bars = list(bars_by_symbol.get(raw_symbol) or [])
        date_map: dict[str, HistoricalBar] = {}
        first_trade_date: str | None = None
        for bar in bars:
            trade_date = _decision_date(bar)
            date_map[trade_date] = bar
            if first_trade_date is None or trade_date < first_trade_date:
                first_trade_date = trade_date
            _aggregate_add(market_aggregate_by_scope[exchange_scope], trade_date, raw_symbol, bar)
            if sector_code:
                _aggregate_add(sector_aggregate_by_scope[(sector_code, exchange_scope)], trade_date, raw_symbol, bar)
        symbol_bar_by_date[raw_symbol] = date_map
        if first_trade_date is not None:
            symbol_first_date[raw_symbol] = first_trade_date

    for aggregate_by_scope in list(market_aggregate_by_scope.values()) + list(sector_aggregate_by_scope.values()):
        for bucket in aggregate_by_scope.values():
            bucket["symbols"] = sorted(str(symbol) for symbol in list(bucket.get("symbols") or []))

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


def _build_cached_market_proxy(
    *,
    symbol: str,
    query_window: Sequence[HistoricalBar],
    cutoff_date: str,
    proxy_cache: Mapping[str, Any],
    session_metadata_by_symbol: Mapping[str, Any] | None,
) -> Any:
    query_window_dates = [str(bar.timestamp)[:10] for bar in query_window]
    scope_key = _aggregate_scope_key(_session_exchange_code(symbol, session_metadata_by_symbol))
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
    bars = []
    for trade_date in selected_dates:
        proxy_bar, _count = _proxy_bar_from_bucket(
            proxy_symbol="MKT",
            trade_date=trade_date,
            bucket=aggregate_by_date[trade_date],
        )
        if proxy_bar is not None:
            bars.append(proxy_bar)
    peer_count_by_date = {}
    contributing_symbols_by_date = {}
    for trade_date in query_window_dates:
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


def _build_cached_sector_proxy(
    *,
    symbol: str,
    query_window: Sequence[HistoricalBar],
    cutoff_date: str,
    sector_map: Mapping[str, str],
    proxy_cache: Mapping[str, Any],
    session_metadata_by_symbol: Mapping[str, Any] | None,
) -> Any:
    query_window_dates = [str(bar.timestamp)[:10] for bar in query_window]
    exchange_scope = _aggregate_scope_key(_session_exchange_code(symbol, session_metadata_by_symbol))
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
            peer_count_by_date={trade_date: 1 for trade_date in query_window_dates if trade_date in symbol_bar_by_date},
            contributing_symbols_by_date={trade_date: [symbol] for trade_date in query_window_dates if trade_date in symbol_bar_by_date},
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
            peer_count_by_date={trade_date: 1 for trade_date in query_window_dates if trade_date in symbol_bar_by_date},
            contributing_symbols_by_date={trade_date: [symbol] for trade_date in query_window_dates if trade_date in symbol_bar_by_date},
            fallback_to_self=True,
            proxy_mode="session_aware_same_exchange" if session_metadata_by_symbol else "date_aligned",
            same_exchange_peer_count=1 if fallback_bars else 0,
            cross_exchange_proxy_used=False,
        )
    cutoff_index = bisect_right(scope_dates, str(cutoff_date))
    bars = []
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
    peer_count_by_date = {}
    contributing_symbols_by_date = {}
    for trade_date in query_window_dates:
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


def _json_safe_snapshot_payload(train_artifact: Mapping[str, Any]) -> dict[str, Any]:
    scaler = train_artifact.get("scaler")
    transform = train_artifact.get("transform")
    if isinstance(scaler, FeatureScaler):
        scaler_payload = scaler.to_payload()
    else:
        scaler_payload = FeatureScaler.from_payload(scaler if isinstance(scaler, Mapping) else {}).to_payload()
    if isinstance(transform, FeatureTransform):
        transform_payload = transform.to_payload()
    else:
        transform_payload = FeatureTransform.from_payload(transform if isinstance(transform, Mapping) else {}).to_payload()
    return {
        "run_id": str(train_artifact.get("run_id") or ""),
        "snapshot_id": str(train_artifact.get("snapshot_id") or ""),
        "spec_hash": str(train_artifact.get("spec_hash") or ""),
        "as_of_date": str(train_artifact.get("as_of_date") or ""),
        "train_end": str(train_artifact.get("train_end") or ""),
        "test_start": str(train_artifact.get("test_start") or ""),
        "purge": _to_int(train_artifact.get("purge")),
        "embargo": _to_int(train_artifact.get("embargo")),
        "memory_version": str(train_artifact.get("memory_version") or ""),
        "prototype_snapshot_name": str(train_artifact.get("prototype_snapshot_name") or "prototype_snapshot"),
        "prototype_snapshot_format": str(train_artifact.get("prototype_snapshot_format") or "prototype_snapshot_v4"),
        "prototype_snapshot_manifest_path": str(train_artifact.get("prototype_snapshot_manifest_path") or ""),
        "max_train_date": train_artifact.get("max_train_date"),
        "max_outcome_end_date": train_artifact.get("max_outcome_end_date"),
        "event_record_count": _to_int(train_artifact.get("event_record_count")),
        "prototype_count": _to_int(train_artifact.get("prototype_count"), len(list(train_artifact.get("prototypes") or []))),
        "scaler": scaler_payload,
        "transform": transform_payload,
        "calibration": dict(train_artifact.get("calibration") or {}),
        "quote_policy_calibration": dict(train_artifact.get("quote_policy_calibration") or {}),
        "metadata": dict(train_artifact.get("metadata") or {}),
        "session_metadata_by_symbol": dict(train_artifact.get("session_metadata_by_symbol") or {}),
        "event_cache_format": str(train_artifact.get("event_cache_format") or ""),
        "event_cache_manifest_path": str(train_artifact.get("event_cache_manifest_path") or ""),
        "snapshot_ids": dict(train_artifact.get("snapshot_ids") or {}),
        "artifact_kind": TRAIN_SNAPSHOT_ARTIFACT_KIND,
    }


def _load_train_snapshot_payload(path: str, *, hydrate_prototypes: bool = True) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    out = {
        **payload,
        "scaler": FeatureScaler.from_payload(payload.get("scaler") if isinstance(payload.get("scaler"), Mapping) else {}),
        "transform": FeatureTransform.from_payload(payload.get("transform") if isinstance(payload.get("transform"), Mapping) else {}),
    }
    if not hydrate_prototypes:
        out["prototypes"] = []
        return out
    prototypes: list[StatePrototype] = []
    if payload.get("prototypes"):
        prototypes = [StatePrototype(**prototype) for prototype in list(payload.get("prototypes") or [])]
    else:
        train_snapshot_path = Path(path)
        run_id = train_snapshot_path.parent.name
        artifact_store = JsonResearchArtifactStore(str(train_snapshot_path.parent.parent))
        prototype_snapshot = artifact_store.load_prototype_snapshot(
            run_id=run_id,
            name=str(payload.get("prototype_snapshot_name") or "prototype_snapshot"),
        ) or {}
        prototypes = [StatePrototype(**prototype) for prototype in list(prototype_snapshot.get("prototypes") or [])]
    out["prototypes"] = prototypes
    return out


def _snapshot_artifact_store(path: str) -> tuple[JsonResearchArtifactStore, str]:
    train_snapshot_path = Path(path)
    return JsonResearchArtifactStore(str(train_snapshot_path.parent.parent)), train_snapshot_path.parent.name


def _snapshot_core_rows_from_handle(handle: PrototypeSnapshotHandle) -> list[dict[str, Any]]:
    frame = handle.load_core_frame(columns=PROTOTYPE_CORE_RETRIEVAL_COLUMNS)
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for raw in frame.to_dict(orient="records"):
        rows.append(
            {
                "prototype_row_index": _to_int(raw.get("prototype_row_index")),
                "prototype_id": str(raw.get("prototype_id") or ""),
                "anchor_code": str(raw.get("anchor_code") or ""),
                "member_count": _to_int(raw.get("member_count")),
                "representative_symbol": raw.get("representative_symbol"),
                "representative_date": raw.get("representative_date"),
                "representative_hash": raw.get("representative_hash"),
                "vector_version": raw.get("vector_version"),
                "feature_version": raw.get("feature_version"),
                "embedding_model": raw.get("embedding_model"),
                "vector_dim": _to_int(raw.get("vector_dim")),
                "anchor_quality": _to_float(raw.get("anchor_quality")),
                "regime_code": raw.get("regime_code"),
                "sector_code": raw.get("sector_code"),
                "liquidity_score": _to_float(raw.get("liquidity_score")),
                "support_count": _to_int(raw.get("support_count")),
                "decayed_support": _to_float(raw.get("decayed_support")),
                "freshness_days": _to_float(raw.get("freshness_days")),
                "exchange_code": raw.get("exchange_code"),
                "country_code": raw.get("country_code"),
                "exchange_tz": raw.get("exchange_tz"),
                "session_date_local": raw.get("session_date_local"),
                "session_close_ts_utc": raw.get("session_close_ts_utc"),
                "feature_anchor_ts_utc": raw.get("feature_anchor_ts_utc"),
                "side_stats": dict(_json_loads(raw.get("side_stats"), {})),
            }
        )
    return rows


def _snapshot_runtime_state(row: Mapping[str, Any]) -> dict[str, Any]:
    artifact_path = str(row.get("artifact_path") or "")
    if not artifact_path:
        return {}
    started = time.perf_counter()
    payload = _load_train_snapshot_payload(artifact_path, hydrate_prototypes=False)
    store, run_id = _snapshot_artifact_store(artifact_path)
    prototype_name = str(payload.get("prototype_snapshot_name") or "prototype_snapshot")
    prototype_manifest_path = str(payload.get("prototype_snapshot_manifest_path") or "")
    handle = store.open_prototype_snapshot_handle(
        run_id=run_id,
        name=prototype_name,
        manifest_path=prototype_manifest_path,
    )
    if handle is None or handle.format_version not in {"prototype_snapshot_v3", "prototype_snapshot_v4"}:
        legacy_payload = _load_train_snapshot_payload(artifact_path, hydrate_prototypes=True)
        prototypes = list(legacy_payload.get("prototypes") or [])
        core_rows = [
            {
                "prototype_id": prototype.prototype_id,
                "regime_code": prototype.regime_code,
                "sector_code": prototype.sector_code,
                "side_stats": dict(prototype.side_stats or {}),
                "decayed_support": _to_float(prototype.decayed_support),
                "freshness_days": _to_float(prototype.freshness_days),
                "embedding": list(prototype.embedding or []),
            }
            for prototype in prototypes
        ]
        core_embeddings = np.asarray([list(prototype.embedding or []) for prototype in prototypes], dtype=np.float64)
        if core_embeddings.size:
            norms = np.linalg.norm(core_embeddings, axis=1, keepdims=True)
            norms = np.where(norms <= 1e-12, 1.0, norms)
            core_embeddings = core_embeddings / norms
        return {
            "payload": payload,
            "artifact_store": store,
            "run_id": run_id,
            "prototype_snapshot_name": prototype_name,
            "prototype_snapshot_manifest_path": prototype_manifest_path,
            "handle": None,
            "core_rows": core_rows,
            "core_embeddings": core_embeddings,
            "legacy_prototypes": prototypes,
            "snapshot_core_load_ms": int((time.perf_counter() - started) * 1000),
        }
    core_rows = _snapshot_core_rows_from_handle(handle)
    return {
        "payload": payload,
        "artifact_store": store,
        "run_id": run_id,
        "prototype_snapshot_name": prototype_name,
        "prototype_snapshot_manifest_path": prototype_manifest_path,
        "handle": handle,
        "core_rows": core_rows,
        "core_embeddings": handle.load_core_embeddings(mmap_mode="r"),
        "legacy_prototypes": [],
        "snapshot_core_load_ms": int((time.perf_counter() - started) * 1000),
    }


def _train_snapshot_artifact_is_reusable(path: str) -> bool:
    artifact_path = Path(str(path or ""))
    if not artifact_path.exists():
        return False
    try:
        with artifact_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return False
    artifact_kind = str(payload.get("artifact_kind") or "train_snapshot_v1")
    if artifact_kind != TRAIN_SNAPSHOT_ARTIFACT_KIND:
        return False
    prototype_manifest_path = str(payload.get("prototype_snapshot_manifest_path") or "").strip()
    prototype_snapshot_name = str(payload.get("prototype_snapshot_name") or "").strip()
    if prototype_manifest_path and Path(prototype_manifest_path).exists():
        return True
    if payload.get("prototypes"):
        return True
    # Legacy snapshots that all pointed at the shared "prototype_snapshot" name
    # are not safe to reuse because later snapshots overwrite the same artifact.
    if prototype_snapshot_name == "prototype_snapshot":
        return False
    return False


def _clear_materialized_stage_dirs(*, output_dir: str) -> None:
    root = Path(output_dir)
    run_root = root.parent if root.name == "train_snapshots" else root
    for target in (run_root / "train_snapshots", run_root / "bundle", run_root / "study_cache"):
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


def _invalidate_materialized_contracts_if_needed(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    output_dir: str,
) -> bool:
    with session_factory() as session:
        snapshot_result = session.execute(
            text(
                """
                SELECT artifact_path
                  FROM bt_result.calibration_snapshot_run
                 WHERE bundle_run_id = :bundle_run_id
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings()
        if hasattr(snapshot_result, "all"):
            snapshot_rows = [dict(row) for row in snapshot_result.all()]
        else:
            first_row = snapshot_result.first()
            snapshot_rows = [] if first_row is None else [dict(first_row)]
        needs_invalidation = any(
            str(row.get("artifact_path") or "").strip()
            and not _train_snapshot_artifact_is_reusable(str(row.get("artifact_path") or ""))
            for row in snapshot_rows
        )
        if not needs_invalidation:
            return False
        session.execute(
            text(
                """
                UPDATE bt_result.calibration_snapshot_run
                   SET status = 'failed',
                       finished_at = COALESCE(finished_at, NOW()),
                       last_error = :last_error
                 WHERE bundle_run_id = :bundle_run_id
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "last_error": STALE_SNAPSHOT_CONTRACT_ERROR,
            },
        )
        session.execute(
            text(
                """
                UPDATE bt_result.calibration_chunk_run
                   SET status = 'failed',
                       finished_at = COALESCE(finished_at, NOW()),
                       last_error = :last_error
                 WHERE bundle_run_id = :bundle_run_id
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "last_error": STALE_BUNDLE_CONTRACT_ERROR,
            },
        )
        session.commit()
    _clear_materialized_stage_dirs(output_dir=output_dir)
    return True


def _eligible_query_rows_for_symbol(
    *,
    symbol: str,
    bars: Sequence[HistoricalBar],
    bars_by_symbol: Mapping[str, Sequence[HistoricalBar]],
    macro_history_by_date: Mapping[str, Mapping[str, float]],
    sector_map: Mapping[str, str],
    session_metadata_by_symbol: Mapping[str, Any] | None,
    macro_series_history: Sequence[Mapping[str, Any]] | None,
    spec: ResearchExperimentSpec,
    start_date: str,
    end_date: str,
    metadata: Mapping[str, Any] | None,
    proxy_cache: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    query_rows: list[dict[str, Any]] = []
    replay_rows: list[dict[str, Any]] = []
    if not bars:
        return query_rows, replay_rows
    use_macro_level_in_similarity = _to_bool((metadata or {}).get("use_macro_level_in_similarity"))
    use_dollar_volume_absolute = _to_bool((metadata or {}).get("use_dollar_volume_absolute"))
    for idx in range(len(bars)):
        if idx < spec.feature_window_bars - 1 or idx + 1 >= len(bars):
            continue
        decision_date = _decision_date(bars[idx])
        if decision_date < start_date or decision_date > end_date:
            continue
        query_window = list(bars[idx - spec.feature_window_bars + 1 : idx + 1])
        market_proxy_override = None
        sector_proxy_override = None
        if proxy_cache:
            market_proxy_override = _build_cached_market_proxy(
                symbol=symbol,
                query_window=query_window,
                cutoff_date=decision_date,
                proxy_cache=proxy_cache,
                session_metadata_by_symbol=session_metadata_by_symbol,
            )
            sector_proxy_override = _build_cached_sector_proxy(
                symbol=symbol,
                query_window=query_window,
                cutoff_date=decision_date,
                sector_map=sector_map,
                proxy_cache=proxy_cache,
                session_metadata_by_symbol=session_metadata_by_symbol,
            )
        payload = build_query_feature_payload_asof(
            symbol=symbol,
            bars=query_window,
            bars_by_symbol=bars_by_symbol,
            macro_history=macro_history_by_date,
            sector_map=sector_map,
            cutoff_date=decision_date,
            spec=spec,
            use_macro_level_in_similarity=use_macro_level_in_similarity,
            use_dollar_volume_absolute=use_dollar_volume_absolute,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
            market_proxy_override=market_proxy_override,
            sector_proxy_override=sector_proxy_override,
        )
        meta = dict(payload.get("meta") or {})
        execution_bar = bars[idx + 1]
        query_rows.append(
            {
                "decision_date": decision_date,
                "symbol": symbol,
                "execution_date": _decision_date(execution_bar),
                "t1_open": float(execution_bar.open),
                "regime_code": str(meta.get("regime_code") or "UNKNOWN"),
                "sector_code": str(sector_map.get(symbol) or "UNKNOWN"),
                "feature_anchor_ts_utc": meta.get("feature_anchor_ts_utc"),
                "macro_asof_ts_utc": meta.get("macro_asof_ts_utc"),
                "raw_features_json": _json_dumps(dict(meta.get("raw_features") or {})),
                "transformed_features_json": _json_dumps({}),
                "embedding_json": _json_dumps([]),
                "query_meta_json": _json_dumps({**dict(meta), "sector_code": str(sector_map.get(symbol) or "UNKNOWN")}),
            }
        )
        path = list(bars[idx + 1 : idx + 6])
        for side in ("BUY", "SELL"):
            for bar_n, bar in enumerate(path, start=1):
                replay_rows.append(
                    {
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "side": side,
                        "bar_n": bar_n,
                        "session_date": _decision_date(bar),
                        "open": float(bar.open),
                        "high": float(bar.high),
                        "low": float(bar.low),
                        "close": float(bar.close),
                    }
                )
    return query_rows, replay_rows


def build_query_feature_cache_rows(
    *,
    symbols: Sequence[str],
    bars_by_symbol: Mapping[str, Sequence[HistoricalBar]],
    macro_history_by_date: Mapping[str, Mapping[str, float]],
    sector_map: Mapping[str, str],
    session_metadata_by_symbol: Mapping[str, Any] | None,
    macro_series_history: Sequence[Mapping[str, Any]] | None,
    spec: ResearchExperimentSpec,
    start_date: str,
    end_date: str,
    metadata: Mapping[str, Any] | None = None,
    use_proxy_aggregate_cache: bool = True,
) -> dict[str, Any]:
    query_rows: list[dict[str, Any]] = []
    replay_rows: list[dict[str, Any]] = []
    decision_dates: set[str] = set()
    proxy_cache = (
        _build_query_proxy_aggregate_cache(
            symbols=list(dict.fromkeys([str(item) for item in list(bars_by_symbol.keys()) + list(symbols) if item])),
            bars_by_symbol=bars_by_symbol,
            sector_map=sector_map,
            session_metadata_by_symbol=session_metadata_by_symbol,
        )
        if use_proxy_aggregate_cache
        else None
    )
    for symbol in [str(item) for item in symbols if item]:
        rows_for_symbol, replay_for_symbol = _eligible_query_rows_for_symbol(
            symbol=symbol,
            bars=list(bars_by_symbol.get(symbol) or []),
            bars_by_symbol=bars_by_symbol,
            macro_history_by_date=macro_history_by_date,
            sector_map=sector_map,
            session_metadata_by_symbol=session_metadata_by_symbol,
            macro_series_history=macro_series_history,
            spec=spec,
            start_date=start_date,
            end_date=end_date,
            metadata=metadata,
            proxy_cache=proxy_cache,
        )
        query_rows.extend(rows_for_symbol)
        replay_rows.extend(replay_for_symbol)
        decision_dates.update(str(row["decision_date"]) for row in rows_for_symbol)
    query_rows.sort(key=lambda row: (str(row["decision_date"]), str(row["symbol"])))
    replay_rows.sort(key=lambda row: (str(row["decision_date"]), str(row["symbol"]), str(row["side"]), int(row["bar_n"])))
    return {
        "query_rows": query_rows,
        "replay_rows": replay_rows,
        "decision_date_count": len(decision_dates),
        "query_row_count": len(query_rows),
    }


def _month_windows(*, start_date: str, end_date: str, window_months: int) -> list[tuple[str, str]]:
    start_day = _calendar_date(start_date)
    end_day = _calendar_date(end_date)
    cursor = start_day.replace(day=1)
    windows: list[tuple[str, str]] = []
    step_months = max(1, int(window_months or 1))
    while cursor <= end_day:
        year = cursor.year
        month = cursor.month + step_months
        while month > 12:
            month -= 12
            year += 1
        next_cursor = cursor.replace(year=year, month=month, day=1)
        window_start = max(start_day, cursor)
        window_end = min(end_day, next_cursor - timedelta(days=1))
        windows.append((_date_iso(window_start), _date_iso(window_end)))
        cursor = next_cursor
    return windows


def _date_subwindows(*, start_date: str, end_date: str, window_days: int) -> list[tuple[str, str]]:
    start_day = _calendar_date(start_date)
    end_day = _calendar_date(end_date)
    step_days = max(1, int(window_days or 1))
    cursor = start_day
    windows: list[tuple[str, str]] = []
    while cursor <= end_day:
        window_end = min(end_day, cursor + timedelta(days=step_days - 1))
        windows.append((_date_iso(cursor), _date_iso(window_end)))
        cursor = window_end + timedelta(days=1)
    return windows


def _query_chunk_rows(
    *,
    bundle_run_id: int,
    chunk_id: int,
    status: str,
    window_start: str,
    window_end: str,
    symbols: Sequence[str],
    started_at: bool = False,
    finished: bool = False,
    elapsed_ms: int | None = None,
    load_ms: int | None = None,
    feature_build_ms: int | None = None,
    db_write_ms: int | None = None,
    decision_date_count: int | None = None,
    query_row_count: int | None = None,
    replay_bar_count: int | None = None,
    last_error: str | None = None,
) -> tuple[str, dict[str, Any]]:
    statements = [
        "status = :status",
        "window_start = CAST(:window_start AS date)",
        "window_end = CAST(:window_end AS date)",
        "symbols_json = :symbols_json",
        "symbol_count = :symbol_count",
        "last_heartbeat_at = NOW()",
    ]
    params: dict[str, Any] = {
        "bundle_run_id": bundle_run_id,
        "chunk_id": chunk_id,
        "status": status,
        "window_start": window_start,
        "window_end": window_end,
        "symbols_json": _json_dumps([str(symbol) for symbol in symbols]),
        "symbol_count": len(list(symbols)),
    }
    if started_at:
        statements.append("started_at = NOW()")
        statements.append("finished_at = NULL")
    if finished:
        statements.append("finished_at = NOW()")
    if elapsed_ms is not None:
        statements.append("elapsed_ms = :elapsed_ms")
        params["elapsed_ms"] = int(elapsed_ms)
    if load_ms is not None:
        statements.append("load_ms = :load_ms")
        params["load_ms"] = int(load_ms)
    if feature_build_ms is not None:
        statements.append("feature_build_ms = :feature_build_ms")
        params["feature_build_ms"] = int(feature_build_ms)
    if db_write_ms is not None:
        statements.append("db_write_ms = :db_write_ms")
        params["db_write_ms"] = int(db_write_ms)
    if decision_date_count is not None:
        statements.append("decision_date_count = :decision_date_count")
        params["decision_date_count"] = int(decision_date_count)
    if query_row_count is not None:
        statements.append("query_row_count = :query_row_count")
        params["query_row_count"] = int(query_row_count)
    if replay_bar_count is not None:
        statements.append("replay_bar_count = :replay_bar_count")
        params["replay_bar_count"] = int(replay_bar_count)
    if last_error is not None:
        statements.append("last_error = :last_error")
        params["last_error"] = str(last_error)
    sql = f"""
        INSERT INTO bt_result.calibration_query_chunk_run(
            bundle_run_id, chunk_id, window_start, window_end, status, symbols_json, symbol_count,
            started_at, last_heartbeat_at
        )
        VALUES (
            :bundle_run_id, :chunk_id, CAST(:window_start AS date), CAST(:window_end AS date), :status, :symbols_json, :symbol_count,
            NOW(), NOW()
        )
        ON CONFLICT (bundle_run_id, chunk_id) DO UPDATE
            SET {", ".join(statements)}
    """
    return sql, params


def _query_chunk_range(
    *,
    bundle_start: str,
    bundle_end: str,
    window_start: str,
    window_end: str,
    lookback_days: int,
    forward_days: int,
) -> tuple[str, str]:
    start_day = _calendar_date(bundle_start)
    end_day = _calendar_date(bundle_end)
    window_start_day = _calendar_date(window_start)
    window_end_day = _calendar_date(window_end)
    load_start = max(start_day, window_start_day - timedelta(days=max(0, int(lookback_days))))
    load_end = min(end_day, window_end_day + timedelta(days=max(0, int(forward_days))))
    return _date_iso(load_start), _date_iso(load_end)


def _decision_dates_from_query_rows(query_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({str(row.get("decision_date") or "") for row in query_rows if row.get("decision_date")})


def _snapshot_dates(decision_dates: Sequence[str], snapshot_cadence: str) -> list[str]:
    cadence = str(snapshot_cadence or "daily").strip().lower() or "daily"
    normalized = [str(value) for value in decision_dates if value]
    if cadence == "daily":
        return normalized
    if cadence == "monthly":
        per_month: dict[str, str] = {}
        for decision_date in normalized:
            per_month.setdefault(str(decision_date)[:7], decision_date)
        return [per_month[key] for key in sorted(per_month)]
    raise ValueError(f"unsupported snapshot_cadence: {snapshot_cadence}")


def _query_feature_row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "decision_date": str(row.get("decision_date") or ""),
        "symbol": str(row.get("symbol") or ""),
        "execution_date": str(row.get("execution_date") or "") or None,
        "t1_open": _to_float(row.get("t1_open")),
        "regime_code": str(row.get("regime_code") or "UNKNOWN"),
        "sector_code": str(row.get("sector_code") or "UNKNOWN"),
        "feature_anchor_ts_utc": row.get("feature_anchor_ts_utc"),
        "macro_asof_ts_utc": row.get("macro_asof_ts_utc"),
        "raw_features_json": str(row.get("raw_features_json") or "{}"),
        "transformed_features_json": str(row.get("transformed_features_json") or "{}"),
        "embedding_json": str(row.get("embedding_json") or "[]"),
        "query_meta_json": str(row.get("query_meta_json") or "{}"),
    }


def _query_feature_rows_by_key(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(row.get("decision_date") or ""), str(row.get("symbol") or "")): _query_feature_row_dict(row) for row in rows}


def _replay_bars_by_key(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        key = (str(row.get("decision_date") or ""), str(row.get("symbol") or ""))
        grouped[key].append(
            {
                "session_date": str(row.get("session_date") or ""),
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": _to_float(row.get("close")),
            }
        )
    for key in grouped:
        grouped[key].sort(key=lambda item: str(item.get("session_date") or ""))
    return grouped


def _snapshot_metadata_rows(*, session_factory: sessionmaker[Session], bundle_run_id: int) -> list[dict[str, Any]]:
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_snapshot_run
                 WHERE bundle_run_id = :bundle_run_id
                   AND status = 'ok'
                 ORDER BY snapshot_date, id
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings().all()
    return [dict(row) for row in rows]


def _load_snapshot_payload_for_row(row: Mapping[str, Any]) -> dict[str, Any]:
    artifact_path = str(row.get("artifact_path") or "")
    if not artifact_path:
        return {}
    return _load_train_snapshot_payload(artifact_path, hydrate_prototypes=False)


def _select_snapshot_row(snapshot_rows: Sequence[Mapping[str, Any]], decision_date: str) -> dict[str, Any] | None:
    eligible = [dict(row) for row in snapshot_rows if str(row.get("snapshot_date") or "") <= decision_date]
    if not eligible:
        return None
    eligible.sort(key=lambda row: (str(row.get("snapshot_date") or ""), int(row.get("id") or 0)))
    return eligible[-1]


def _query_feature_block(
    rows: Sequence[Mapping[str, Any]],
    *,
    transform: FeatureTransform,
) -> dict[str, Any]:
    feature_keys = list(transform.feature_keys or [])
    feature_index = {key: idx for idx, key in enumerate(feature_keys)}
    raw_rows: list[dict[str, Any]] = []
    query_metas: list[dict[str, Any]] = []
    matrix = np.zeros((len(rows), len(feature_keys)), dtype=np.float64)
    parse_started = time.perf_counter()
    for row_index, row in enumerate(rows):
        raw_features = dict(_json_loads(row.get("raw_features_json"), {}))
        query_meta = dict(_json_loads(row.get("query_meta_json"), {}))
        raw_rows.append(raw_features)
        query_metas.append(query_meta)
        for key, value in raw_features.items():
            feature_idx = feature_index.get(str(key))
            if feature_idx is None:
                continue
            matrix[row_index, feature_idx] = _to_float(value)
    query_parse_ms = int((time.perf_counter() - parse_started) * 1000)
    means = np.asarray([_to_float(transform.scaler.means.get(key), 0.0) for key in feature_keys], dtype=np.float64)
    stds = np.asarray([_to_float(transform.scaler.stds.get(key), 1.0) for key in feature_keys], dtype=np.float64)
    stds = np.where(np.abs(stds) <= 1e-12, 1.0, stds)
    transform_started = time.perf_counter()
    transformed = (matrix - means) / stds if len(feature_keys) else matrix
    query_transform_ms = int((time.perf_counter() - transform_started) * 1000)
    return {
        "raw_rows": raw_rows,
        "query_metas": query_metas,
        "transformed_matrix": transformed,
        "query_parse_ms": query_parse_ms,
        "query_transform_ms": query_transform_ms,
        "feature_keys": feature_keys,
    }


def _ordered_union_prototype_indices(*, similarities: np.ndarray, buy_indices: Sequence[int], sell_indices: Sequence[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for idx in list(buy_indices) + list(sell_indices):
        raw_idx = int(idx)
        if raw_idx in seen:
            continue
        seen.add(raw_idx)
        out.append(raw_idx)
    out.sort(key=lambda raw_idx: (-float(similarities[raw_idx]), raw_idx))
    return out


def _patched_distribution(
    dist: DistributionEstimate,
    *,
    prototype_pool_size: int,
    pre_truncation_candidate_count: int,
    positive_weight_candidate_count: int,
) -> DistributionEstimate:
    if not isinstance(dist, DistributionEstimate):
        return dist
    utility = dict(dist.utility or {})
    utility.update(
        {
            "prototype_pool_size": int(prototype_pool_size),
            "pre_truncation_candidate_count": int(pre_truncation_candidate_count),
            "positive_weight_candidate_count": int(positive_weight_candidate_count),
        }
    )
    return replace(
        dist,
        prototype_pool_size=int(prototype_pool_size),
        pre_truncation_candidate_count=int(pre_truncation_candidate_count),
        positive_weight_candidate_count=int(positive_weight_candidate_count),
        utility=utility,
    )


def _patched_surface(
    surface: DecisionSurface,
    *,
    buy_result: Mapping[str, Any],
    sell_result: Mapping[str, Any],
    prototype_pool_size: int,
) -> DecisionSurface:
    if not isinstance(surface, DecisionSurface):
        return surface
    buy = _patched_distribution(
        surface.buy,
        prototype_pool_size=prototype_pool_size,
        pre_truncation_candidate_count=_to_int(buy_result.get("pre_truncation_candidate_count")),
        positive_weight_candidate_count=_to_int(buy_result.get("positive_weight_candidate_count")),
    )
    sell = _patched_distribution(
        surface.sell,
        prototype_pool_size=prototype_pool_size,
        pre_truncation_candidate_count=_to_int(sell_result.get("pre_truncation_candidate_count")),
        positive_weight_candidate_count=_to_int(sell_result.get("positive_weight_candidate_count")),
    )
    diagnostics = dict(surface.diagnostics or {})
    diagnostics["prototype_pool_size"] = int(prototype_pool_size)
    return replace(surface, buy=buy, sell=sell, diagnostics=diagnostics)


def _load_subset_prototype_pool(
    *,
    runtime_state: Mapping[str, Any],
    prototype_indices: Sequence[int],
) -> list[StatePrototype]:
    if not prototype_indices:
        return []
    legacy_prototypes = list(runtime_state.get("legacy_prototypes") or [])
    if legacy_prototypes:
        return [legacy_prototypes[idx] for idx in prototype_indices if 0 <= int(idx) < len(legacy_prototypes)]
    core_rows = list(runtime_state.get("core_rows") or [])
    prototype_ids = [
        str(core_rows[idx].get("prototype_id") or "")
        for idx in prototype_indices
        if 0 <= int(idx) < len(core_rows)
    ]
    if not prototype_ids:
        return []
    subset_payloads = load_prototype_subset(
        artifact_store=runtime_state["artifact_store"],
        run_id=str(runtime_state.get("run_id") or ""),
        name=str(runtime_state.get("prototype_snapshot_name") or "prototype_snapshot"),
        prototype_ids=prototype_ids,
        manifest_path=str(runtime_state.get("prototype_snapshot_manifest_path") or ""),
    )
    return [StatePrototype(**payload) for payload in subset_payloads]


def _signal_panel_rows_from_cache(
    *,
    query_rows: Sequence[Mapping[str, Any]],
    snapshot_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in query_rows:
        by_date[str(row.get("decision_date") or "")].append(_query_feature_row_dict(row))
    panel_rows: list[dict[str, Any]] = []
    raw_event_row_count = 0
    prototype_count = 0
    snapshot_load_ms = 0
    snapshot_core_load_ms = 0
    query_parse_ms = 0
    query_transform_ms = 0
    prototype_score_ms = 0
    member_lazy_load_ms = 0
    query_block_count = 0
    loaded_snapshot_id = ""
    loaded_snapshot_state: dict[str, Any] = {}
    for decision_date in sorted(by_date):
        snapshot_row = _select_snapshot_row(snapshot_rows, decision_date)
        if not snapshot_row:
            continue
        snapshot_id = str(snapshot_row.get("snapshot_id") or "")
        if snapshot_id != loaded_snapshot_id:
            snapshot_load_started = time.perf_counter()
            loaded_snapshot_state = _snapshot_runtime_state(snapshot_row)
            snapshot_load_ms += int((time.perf_counter() - snapshot_load_started) * 1000)
            snapshot_core_load_ms += int(loaded_snapshot_state.get("snapshot_core_load_ms") or 0)
            loaded_snapshot_id = snapshot_id if loaded_snapshot_state else ""
        snapshot_state = dict(loaded_snapshot_state or {})
        snapshot_payload = dict(snapshot_state.get("payload") or {})
        if not snapshot_payload:
            continue
        transform = snapshot_payload.get("transform")
        if not isinstance(transform, FeatureTransform):
            continue
        core_rows = list(snapshot_state.get("core_rows") or [])
        core_embedding_payload = snapshot_state.get("core_embeddings")
        core_embeddings = np.asarray(
            core_embedding_payload if core_embedding_payload is not None else np.zeros((0, 0), dtype=np.float64),
            dtype=np.float64,
        )
        if not core_rows:
            continue
        metadata = dict(snapshot_payload.get("metadata") or {})
        quote_policy_calibration = dict(snapshot_payload.get("quote_policy_calibration") or {})
        ev_cfg = _ev_config_from_metadata(
            metadata,
            top_k=int(metadata.get("portfolio_top_n", 3) or 3),
            abstain_margin=float(quote_policy_calibration.get("abstain_margin", metadata.get("abstain_margin", 0.05)) or 0.05),
        )
        calibration_payload = dict(snapshot_payload.get("calibration") or {})
        calibration = CalibrationModel(
            method=str(calibration_payload.get("method", "logistic")),
            slope=float(calibration_payload.get("slope", 1.0)),
            intercept=float(calibration_payload.get("intercept", 0.0)),
        )
        raw_event_row_count += _to_int(snapshot_payload.get("event_record_count"))
        prototype_count += len(core_rows)
        prototype_regime_codes = [str(row.get("regime_code") or "") for row in core_rows]
        prototype_sector_codes = [str(row.get("sector_code") or "") for row in core_rows]
        prototype_side_stats = [dict(row.get("side_stats") or {}) for row in core_rows]
        prototype_decayed_support = [_to_float(row.get("decayed_support")) for row in core_rows]
        prototype_freshness_days = [_to_float(row.get("freshness_days")) for row in core_rows]
        subset_cache: dict[tuple[int, ...], list[StatePrototype]] = {}
        date_rows = list(by_date[decision_date])
        block_size = 256
        for offset in range(0, len(date_rows), block_size):
            block_rows = date_rows[offset : offset + block_size]
            query_block_count += 1
            query_block = _query_feature_block(block_rows, transform=transform)
            query_parse_ms += int(query_block["query_parse_ms"])
            query_transform_ms += int(query_block["query_transform_ms"])
            transformed_matrix = np.asarray(query_block["transformed_matrix"], dtype=np.float64)
            query_metas = list(query_block["query_metas"])
            raw_rows = list(query_block["raw_rows"])
            score_started = time.perf_counter()
            topk_results = exact_block_prototype_topk(
                query_embeddings=transformed_matrix,
                prototype_embeddings=core_embeddings,
                query_regime_codes=[
                    str((meta.get("regime_code") if isinstance(meta, Mapping) else None) or row.get("regime_code") or "UNKNOWN")
                    for meta, row in zip(query_metas, block_rows)
                ],
                query_sector_codes=[
                    str((meta.get("sector_code") if isinstance(meta, Mapping) else None) or row.get("sector_code") or "UNKNOWN")
                    for meta, row in zip(query_metas, block_rows)
                ],
                prototype_regime_codes=prototype_regime_codes,
                prototype_sector_codes=prototype_sector_codes,
                prototype_side_stats=prototype_side_stats,
                prototype_decayed_support=prototype_decayed_support,
                prototype_freshness_days=prototype_freshness_days,
                cfg=ev_cfg,
            )
            prototype_score_ms += int((time.perf_counter() - score_started) * 1000)
            for row_index, query_row in enumerate(block_rows):
                query_meta = dict(query_metas[row_index] or {})
                raw_features = dict(raw_rows[row_index] or {})
                embedding = [float(value) for value in transformed_matrix[row_index].tolist()]
                transformed_features = {
                    key: float(transformed_matrix[row_index, feature_idx])
                    for feature_idx, key in enumerate(query_block["feature_keys"])
                }
                regime_code = str(query_meta.get("regime_code") or query_row.get("regime_code") or "UNKNOWN")
                sector_code = str(query_meta.get("sector_code") or query_row.get("sector_code") or "UNKNOWN")
                topk = dict(topk_results[row_index] or {})
                similarity_payload = topk.get("similarities")
                similarities = np.asarray(
                    similarity_payload if similarity_payload is not None else np.zeros((0,), dtype=float),
                    dtype=float,
                )
                union_indices = _ordered_union_prototype_indices(
                    similarities=similarities,
                    buy_indices=list((topk.get("BUY") or {}).get("top_indices") or []),
                    sell_indices=list((topk.get("SELL") or {}).get("top_indices") or []),
                )
                subset_key = tuple(union_indices)
                prototype_pool = subset_cache.get(subset_key)
                if prototype_pool is None:
                    member_load_started = time.perf_counter()
                    prototype_pool = _load_subset_prototype_pool(
                        runtime_state=snapshot_state,
                        prototype_indices=union_indices,
                    )
                    member_lazy_load_ms += int((time.perf_counter() - member_load_started) * 1000)
                    subset_cache[subset_key] = prototype_pool
                prototype_by_id = {
                    str(prototype.prototype_id): prototype
                    for prototype in prototype_pool
                }
                buy_candidates = [
                    prototype_by_id[prototype_id]
                    for prototype_id in (
                        str(core_rows[idx].get("prototype_id") or "")
                        for idx in list((topk.get("BUY") or {}).get("top_indices") or [])
                        if 0 <= int(idx) < len(core_rows)
                    )
                    if prototype_id in prototype_by_id
                ]
                sell_candidates = [
                    prototype_by_id[prototype_id]
                    for prototype_id in (
                        str(core_rows[idx].get("prototype_id") or "")
                        for idx in list((topk.get("SELL") or {}).get("top_indices") or [])
                        if 0 <= int(idx) < len(core_rows)
                    )
                    if prototype_id in prototype_by_id
                ]
                surface = build_decision_surface_from_ranked_candidates(
                    query_embedding=embedding,
                    buy_candidates=buy_candidates,
                    sell_candidates=sell_candidates,
                    regime_code=regime_code,
                    sector_code=sector_code,
                    ev_config=ev_cfg,
                    calibration=calibration,
                    query_date=decision_date,
                    prototype_pool_size=len(core_rows),
                    buy_pre_truncation_candidate_count=_to_int((topk.get("BUY") or {}).get("pre_truncation_candidate_count")),
                    sell_pre_truncation_candidate_count=_to_int((topk.get("SELL") or {}).get("pre_truncation_candidate_count")),
                    buy_positive_weight_candidate_count=_to_int((topk.get("BUY") or {}).get("positive_weight_candidate_count")),
                    sell_positive_weight_candidate_count=_to_int((topk.get("SELL") or {}).get("positive_weight_candidate_count")),
                )
                buy_side_diag = _side_diag(surface.buy, surface, "BUY")
                sell_side_diag = _side_diag(surface.sell, surface, "SELL")
                chosen_payload = _chosen_side_payload(
                    surface=surface,
                    buy_side_diag=buy_side_diag,
                    sell_side_diag=sell_side_diag,
                )
                panel_rows.append(
                    {
                        "decision_date": decision_date,
                        "symbol": query_row.get("symbol"),
                        "query": {
                            "regime_code": regime_code,
                            "sector_code": sector_code,
                            "exchange_code": query_meta.get("exchange_code"),
                            "country_code": query_meta.get("country_code"),
                            "exchange_tz": query_meta.get("exchange_tz"),
                            "session_date_local": query_meta.get("session_date_local"),
                            "session_close_ts_utc": query_meta.get("session_close_ts_utc"),
                            "feature_anchor_ts_utc": query_meta.get("feature_anchor_ts_utc"),
                            "macro_asof_ts_utc": query_meta.get("macro_asof_ts_utc"),
                        },
                        "decision_surface": {
                            "chosen_side": surface.chosen_side,
                            "abstain": surface.abstain,
                            "abstain_reasons": list(surface.abstain_reasons),
                            "chosen_lower_bound": (surface.diagnostics.get("decision_rule") or {}).get("chosen_lower_bound"),
                            "chosen_interval_width": (surface.diagnostics.get("decision_rule") or {}).get("chosen_interval_width"),
                            "chosen_effective_sample_size": (surface.diagnostics.get("decision_rule") or {}).get("chosen_effective_sample_size"),
                            "chosen_uncertainty": (surface.diagnostics.get("decision_rule") or {}).get("chosen_uncertainty"),
                            "decision_rule": surface.diagnostics.get("decision_rule"),
                            "chosen_payload": chosen_payload,
                        },
                        "chosen_side_payload": chosen_payload,
                        "scorer_diagnostics": {"buy": buy_side_diag, "sell": sell_side_diag},
                        "ev": {
                            "buy": {
                                "regime_alignment": buy_side_diag.get("regime_alignment"),
                                "abstain_reasons": list(buy_side_diag.get("abstain_reasons") or []),
                            },
                            "sell": {
                                "regime_alignment": sell_side_diag.get("regime_alignment"),
                                "abstain_reasons": list(sell_side_diag.get("abstain_reasons") or []),
                            },
                        },
                        "missingness": {},
                        "_snapshot_id": snapshot_id,
                        "_raw_features": raw_features,
                        "_transformed_features": transformed_features,
                        "_embedding": embedding,
                    }
                )
    return panel_rows, {
        "raw_event_row_count": raw_event_row_count,
        "prototype_count": prototype_count,
        "snapshot_load_ms": snapshot_load_ms,
        "snapshot_core_load_ms": snapshot_core_load_ms,
        "query_parse_ms": query_parse_ms,
        "query_transform_ms": query_transform_ms,
        "prototype_score_ms": prototype_score_ms,
        "member_lazy_load_ms": member_lazy_load_ms,
        "query_block_count": query_block_count,
    }


def _augment_forecast_rows_with_replay_path(
    *,
    forecast_rows: Sequence[Mapping[str, Any]],
    replay_bars: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in forecast_rows:
        row = dict(raw)
        key = (str(row.get("decision_date") or ""), str(row.get("symbol") or ""))
        path = [dict(item) for item in list(replay_bars.get(key) or [])]
        first = path[0] if path else {}
        row.update(
            {
                "execution_date": first.get("session_date"),
                "t1_open": first.get("open"),
                "d1_open": first.get("open"),
                "d1_high": first.get("high"),
                "d1_low": first.get("low"),
                "d1_close": first.get("close"),
                "bar_path_d1_to_d5": _json_dumps(path),
                "path_length": len(path),
                "last_path_close": path[-1]["close"] if path else None,
            }
        )
        out.append(row)
    return out


def build_calibration_seed_payloads_from_cache(
    *,
    query_rows: Sequence[Mapping[str, Any]],
    replay_rows: Sequence[Mapping[str, Any]],
    snapshot_rows: Sequence[Mapping[str, Any]],
    scenario_id: str,
    policy_scope: str,
) -> dict[str, Any]:
    panel_rows, telemetry = _signal_panel_rows_from_cache(
        query_rows=query_rows,
        snapshot_rows=snapshot_rows,
    )
    forecast_rows = research_engine._forecast_rows(panel_rows)
    forecast_rows = _augment_forecast_rows_with_replay_path(
        forecast_rows=forecast_rows,
        replay_bars=_replay_bars_by_key(replay_rows),
    )
    analysis = build_pre_optuna_evidence(forecast_rows)
    replay_seed = build_optuna_replay_seed(
        forecast_rows=list(analysis.get("forecast_rows") or forecast_rows),
        bars_by_symbol={},
        run_label=scenario_id,
        policy_scope=policy_scope,
    )
    seed_payloads: list[dict[str, Any]] = []
    for row in list(replay_seed.get("seed_rows") or []):
        seed_payloads.append(
            {
                "decision_date": str(row.get("decision_date") or ""),
                "symbol": str(row.get("symbol") or ""),
                "side": str(row.get("side") or ""),
                "market": str(row.get("market") or "US"),
                "policy_family": str(row.get("policy_family") or "echo_or_collapse"),
                "pattern_key": str(row.get("pattern_key") or ""),
                "lower_bound": _to_float(row.get("lower_bound")),
                "q10_return": _to_float(row.get("q10_return")),
                "q50_return": _to_float(row.get("q50_return")),
                "q90_return": _to_float(row.get("q90_return")),
                "interval_width": _to_float(row.get("interval_width")),
                "uncertainty": _to_float(row.get("uncertainty")),
                "member_mixture_ess": _to_float(row.get("member_mixture_ess")),
                "member_top1_weight_share": _to_float(row.get("member_top1_weight_share")),
                "member_pre_truncation_count": _to_int(row.get("member_pre_truncation_count")),
                "forecast_selected": _to_bool(row.get("forecast_selected")),
                "optuna_eligible": _to_bool(row.get("optuna_eligible")),
                "recurring_family": _to_bool(row.get("recurring_family")),
                "single_prototype_collapse": _to_bool(row.get("single_prototype_collapse")),
                "regime_code": str(row.get("regime_code") or "UNKNOWN"),
                "sector_code": str(row.get("sector_code") or "UNKNOWN"),
                "member_consensus_signature": str(row.get("member_consensus_signature") or ""),
                "q50_d2_return": _to_float(row.get("q50_d2_return")),
                "q50_d3_return": _to_float(row.get("q50_d3_return")),
                "p_resolved_by_d2": _to_float(row.get("p_resolved_by_d2")),
                "p_resolved_by_d3": _to_float(row.get("p_resolved_by_d3")),
                "t1_open": _to_float(row.get("t1_open")),
            }
        )
    return {
        "analysis": analysis,
        "replay_seed": replay_seed,
        "seed_payloads": seed_payloads,
        "telemetry": {
            **telemetry,
            "signal_panel_row_count": len(panel_rows),
            "forecast_row_count": len(forecast_rows),
        },
    }


@contextmanager
def _forbidden_bundle_calls_guard():
    def _raise(name: str):
        def _inner(*args, **kwargs):
            raise ForbiddenCalibrationBundleCall(name)

        return _inner

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "backtest_app.historical_data.local_postgres_loader.LocalPostgresLoader.load_for_scenario",
                side_effect=_raise("LocalPostgresLoader.load_for_scenario"),
            )
        )
        stack.enter_context(
            patch(
                "backtest_app.research.pipeline.generate_similarity_candidates_rolling",
                side_effect=_raise("generate_similarity_candidates_rolling"),
            )
        )
        stack.enter_context(
            patch(
                "backtest_app.research.pipeline.build_event_memory_asof",
                side_effect=_raise("build_event_memory_asof"),
            )
        )
        yield


def create_or_resume_bundle_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_key: str,
    market: str,
    strategy_mode: str,
    policy_scope: str,
    seed_profile: str,
    proof_reference_run: str,
    start_date: str,
    end_date: str,
    chunk_size: int,
    worker_count: int,
    universe_symbol_count: int,
    snapshot_cadence: str = "daily",
    model_version: str = "",
) -> dict[str, Any]:
    resolved_model_version = _resolve_model_version(snapshot_cadence, model_version)
    with session_factory() as session:
        row = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_bundle_run
                 WHERE bundle_key = :bundle_key
                """
            ),
            {"bundle_key": bundle_key},
        ).mappings().first()
        if row:
            session.execute(
                text(
                    """
                    UPDATE bt_result.calibration_bundle_run
                       SET status = 'running',
                            started_at = COALESCE(started_at, NOW()),
                            finished_at = NULL,
                            worker_count = :worker_count,
                            chunk_size = :chunk_size,
                            universe_symbol_count = :universe_symbol_count,
                            snapshot_cadence = :snapshot_cadence,
                            model_version = :model_version,
                            current_step = NULL,
                            last_heartbeat_at = NOW(),
                            last_pid = NULL,
                            last_error = NULL
                     WHERE id = :bundle_run_id
                    """
                ),
                {
                    "bundle_run_id": int(row["id"]),
                    "worker_count": worker_count,
                    "chunk_size": chunk_size,
                    "universe_symbol_count": universe_symbol_count,
                    "snapshot_cadence": snapshot_cadence,
                    "model_version": resolved_model_version,
                },
            )
            session.commit()
            return {"bundle_run_id": int(row["id"]), "bundle_key": bundle_key, "status": "running"}
        inserted = session.execute(
            text(
                """
                INSERT INTO bt_result.calibration_bundle_run(
                    bundle_key, market, strategy_mode, policy_scope, seed_profile,
                    proof_reference_run, status, start_date, end_date, chunk_size,
                    worker_count, universe_symbol_count, snapshot_cadence, model_version, started_at,
                    current_step, last_heartbeat_at
                )
                VALUES (
                    :bundle_key, :market, :strategy_mode, :policy_scope, :seed_profile,
                    :proof_reference_run, 'running', :start_date, :end_date, :chunk_size,
                    :worker_count, :universe_symbol_count, :snapshot_cadence, :model_version, NOW(),
                    NULL, NOW()
                )
                RETURNING id
                """
            ),
            {
                "bundle_key": bundle_key,
                "market": market,
                "strategy_mode": strategy_mode,
                "policy_scope": policy_scope,
                "seed_profile": seed_profile,
                "proof_reference_run": proof_reference_run,
                "start_date": start_date,
                "end_date": end_date,
                "chunk_size": chunk_size,
                "worker_count": worker_count,
                "universe_symbol_count": universe_symbol_count,
                "snapshot_cadence": snapshot_cadence,
                "model_version": resolved_model_version,
            },
        ).first()
        session.commit()
        return {"bundle_run_id": int(inserted[0]), "bundle_key": bundle_key, "status": "running"}


def resolve_bundle_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int | None = None,
    bundle_key: str = "",
) -> dict[str, Any]:
    if not bundle_run_id and not bundle_key:
        raise ValueError("bundle_run_id or bundle_key is required")
    with session_factory() as session:
        row = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_bundle_run
                 WHERE (:bundle_run_id > 0 AND id = :bundle_run_id)
                    OR (:bundle_run_id <= 0 AND bundle_key = :bundle_key)
                 ORDER BY id DESC
                 LIMIT 1
                """
            ),
            {"bundle_run_id": int(bundle_run_id or 0), "bundle_key": bundle_key},
        ).mappings().first()
        if not row:
            raise KeyError(f"calibration bundle not found: id={bundle_run_id} key={bundle_key}")
        return dict(row)


def touch_bundle_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    current_step: str = "",
    pid: int = 0,
    status: str = "",
    last_error: str | None = None,
) -> dict[str, Any]:
    update_parts = [
        "last_heartbeat_at = NOW()",
        "last_pid = :pid",
    ]
    params: dict[str, Any] = {
        "bundle_run_id": bundle_run_id,
        "pid": int(pid or 0) or None,
    }
    if current_step:
        update_parts.append("current_step = :current_step")
        params["current_step"] = str(current_step)
    if status:
        update_parts.append("status = :status")
        params["status"] = str(status)
    if last_error is not None:
        update_parts.append("last_error = :last_error")
        params["last_error"] = str(last_error)
    with session_factory() as session:
        session.execute(
            text(
                f"""
                UPDATE bt_result.calibration_bundle_run
                   SET {", ".join(update_parts)}
                 WHERE id = :bundle_run_id
                """
            ),
            params,
        )
        session.commit()
    return resolve_bundle_run(session_factory=session_factory, bundle_run_id=bundle_run_id)


def mark_bundle_run_failed(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    last_error: str,
) -> dict[str, Any]:
    with session_factory() as session:
        session.execute(
            text(
                """
                UPDATE bt_result.calibration_bundle_run
                   SET status = 'failed',
                       finished_at = NOW(),
                       last_error = :last_error,
                       last_heartbeat_at = NOW()
                 WHERE id = :bundle_run_id
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "last_error": str(last_error),
            },
        )
        session.commit()
    return resolve_bundle_run(session_factory=session_factory, bundle_run_id=bundle_run_id)


def _pid_is_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    except Exception:
        return False
    return True


def mark_stale_bundle_run_if_dead(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    stale_after_seconds: int = 120,
    pid_alive_fn=None,
) -> dict[str, Any]:
    bundle = resolve_bundle_run(session_factory=session_factory, bundle_run_id=bundle_run_id)
    if str(bundle.get("status") or "") != "running":
        return bundle
    heartbeat_at = _parse_iso_datetime(bundle.get("last_heartbeat_at"))
    if heartbeat_at is None:
        return bundle
    if (_utcnow() - heartbeat_at).total_seconds() < max(1, int(stale_after_seconds)):
        return bundle
    pid = _to_int(bundle.get("last_pid"))
    alive = pid_alive_fn(pid) if pid_alive_fn is not None else _pid_is_alive(pid)
    if alive:
        return bundle
    return mark_bundle_run_failed(
        session_factory=session_factory,
        bundle_run_id=bundle_run_id,
        last_error="stale_orchestrator_no_heartbeat",
    )


def list_chunk_runs(*, session_factory: sessionmaker[Session], bundle_run_id: int) -> list[dict[str, Any]]:
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_chunk_run
                 WHERE bundle_run_id = :bundle_run_id
                 ORDER BY chunk_id
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings().all()
    return [dict(row) for row in rows]


def list_query_chunk_runs(*, session_factory: sessionmaker[Session], bundle_run_id: int) -> list[dict[str, Any]]:
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_query_chunk_run
                 WHERE bundle_run_id = :bundle_run_id
                 ORDER BY chunk_id
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings().all()
    return [dict(row) for row in rows]


def list_snapshot_runs(*, session_factory: sessionmaker[Session], bundle_run_id: int) -> list[dict[str, Any]]:
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_snapshot_run
                 WHERE bundle_run_id = :bundle_run_id
                 ORDER BY snapshot_date, id
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings().all()
    return [dict(row) for row in rows]


def _snapshot_checkpoint_paths(*, output_dir: str, run_id: str, snapshot_date: str) -> dict[str, str]:
    checkpoint_root = Path(output_dir) / run_id / "prototype_checkpoints" / snapshot_date.replace("-", "")
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    return {
        "root": str(checkpoint_root),
        "input_path": str(checkpoint_root / "prototype_input.pkl"),
        "checkpoint_path": str(checkpoint_root / "prototype_checkpoint.pkl"),
        "prototype_input_path": str(checkpoint_root / "prototype_input.pkl"),
        "prototype_checkpoint_path": str(checkpoint_root / "prototype_checkpoint.pkl"),
        "prototype_resume_meta_path": str(checkpoint_root / "prototype_resume_metadata.json"),
        "prototype_rows_dir": str(checkpoint_root / "prototype_rows"),
        "prototype_norms_path": str(checkpoint_root / "prototype_norms.npy"),
        "prototype_representatives_path": str(checkpoint_root / "prototype_representatives.npy"),
        "event_input_path": str(checkpoint_root / "event_memory_input.pkl"),
        "event_checkpoint_path": str(checkpoint_root / "event_memory_checkpoint.pkl"),
        "event_batch_dir": str(checkpoint_root / "event_memory_batches"),
    }


def _clear_snapshot_checkpoint_files(*, checkpoint_paths: Mapping[str, Any]) -> None:
    for key in (
        "input_path",
        "checkpoint_path",
        "prototype_input_path",
        "prototype_checkpoint_path",
        "prototype_resume_meta_path",
        "prototype_norms_path",
        "prototype_representatives_path",
        "event_input_path",
        "event_checkpoint_path",
    ):
        raw_path = str(checkpoint_paths.get(key) or "").strip()
        if not raw_path:
            continue
        path = Path(raw_path)
        if path.exists():
            path.unlink()
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        if tmp_path.exists():
            tmp_path.unlink()
    root = Path(str(checkpoint_paths.get("root") or ""))
    prototype_rows_dir = Path(str(checkpoint_paths.get("prototype_rows_dir") or ""))
    event_batch_dir = Path(str(checkpoint_paths.get("event_batch_dir") or ""))
    if str(checkpoint_paths.get("prototype_rows_dir") or "").strip() and prototype_rows_dir.exists():
        for child in prototype_rows_dir.glob("*"):
            if child.is_file():
                child.unlink()
        try:
            prototype_rows_dir.rmdir()
        except OSError:
            pass
    if str(checkpoint_paths.get("event_batch_dir") or "").strip() and event_batch_dir.exists():
        for child in event_batch_dir.glob("*"):
            if child.is_file():
                child.unlink()
        try:
            event_batch_dir.rmdir()
        except OSError:
            pass
    if root.exists():
        try:
            root.rmdir()
        except OSError:
            pass


def _estimate_remaining_snapshot_eta_seconds(
    *,
    remaining_snapshot_count: int,
    created_snapshot_seconds: float,
    created_event_candidate_rows: int,
    recent_snapshot_rows: Sequence[dict[str, Any]],
) -> int:
    if remaining_snapshot_count <= 0:
        return 0
    recent_seconds = [
        max(
            1.0,
            (
                _to_float(row.get("event_memory_ms"))
                + _to_float(row.get("transform_ms"))
                + _to_float(row.get("prototype_ms"))
                + _to_float(row.get("artifact_write_ms"))
            )
            / 1000.0,
        )
        for row in recent_snapshot_rows
    ]
    recent_rows = [
        max(
            0,
            _to_int(
                row.get("event_candidate_total")
                or row.get("prototype_rows_total")
                or row.get("event_record_count")
            ),
        )
        for row in recent_snapshot_rows
    ]
    avg_snapshot_seconds = float(sum(recent_seconds) / len(recent_seconds)) if recent_seconds else float(created_snapshot_seconds)
    avg_event_candidate_rows = float(sum(recent_rows) / len(recent_rows)) if recent_rows else float(created_event_candidate_rows)
    baseline_seconds = max(float(created_snapshot_seconds), avg_snapshot_seconds)
    eta_from_snapshot_seconds = baseline_seconds * remaining_snapshot_count
    if created_snapshot_seconds > 0 and created_event_candidate_rows > 0:
        rows_per_second = created_event_candidate_rows / created_snapshot_seconds
        eta_from_rows_seconds = (max(avg_event_candidate_rows, float(created_event_candidate_rows)) * remaining_snapshot_count) / max(rows_per_second, 1e-9)
    else:
        eta_from_rows_seconds = eta_from_snapshot_seconds
    return int(max(eta_from_snapshot_seconds, eta_from_rows_seconds))


def begin_snapshot_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    snapshot_id: str,
    snapshot_date: str,
    train_start: str,
    train_end: str,
    spec_hash: str,
    memory_version: str,
    model_version: str,
    snapshot_cadence: str,
    current_phase: str = "event_memory",
    checkpoint_path: str = "",
    event_checkpoint_path: str = "",
) -> None:
    with session_factory() as session:
        session.execute(
            text(
                """
                INSERT INTO bt_result.calibration_snapshot_run(
                    bundle_run_id, snapshot_id, snapshot_date, train_start, train_end, spec_hash,
                    memory_version, model_version, snapshot_cadence, status, current_phase,
                    started_at, last_heartbeat_at, checkpoint_path, event_checkpoint_path
                )
                VALUES (
                    :bundle_run_id, :snapshot_id, CAST(:snapshot_date AS date), CAST(:train_start AS date), CAST(:train_end AS date), :spec_hash,
                    :memory_version, :model_version, :snapshot_cadence, 'running', :current_phase,
                    NOW(), NOW(), NULLIF(:checkpoint_path, ''), NULLIF(:event_checkpoint_path, '')
                )
                ON CONFLICT (bundle_run_id, snapshot_id) DO UPDATE
                    SET status = 'running',
                        current_phase = :current_phase,
                        started_at = NOW(),
                        finished_at = NULL,
                        last_error = NULL,
                        artifact_path = NULL,
                        last_heartbeat_at = NOW(),
                        current_symbol = NULL,
                        symbols_done = 0,
                        symbols_total = 0,
                        raw_event_row_count = 0,
                        pending_record_count = 0,
                        event_candidate_total = 0,
                        event_candidate_done = 0,
                        current_event_date = NULL,
                        event_checkpoint_path = NULLIF(:event_checkpoint_path, ''),
                        last_event_checkpoint_at = NULL,
                        prototype_rows_total = 0,
                        prototype_rows_done = 0,
                        cluster_count = 0,
                        checkpoint_path = NULLIF(:checkpoint_path, ''),
                        last_checkpoint_at = NULL,
                        event_record_count = 0,
                        prototype_count = 0,
                        event_memory_ms = 0,
                        event_cache_build_ms = 0,
                        eligible_event_count = 0,
                        scaler_reconstruct_ms = 0,
                        transform_ms = 0,
                        prototype_prepare_ms = 0,
                        prototype_ms = 0,
                        artifact_write_ms = 0,
                        artifact_rows_total = 0,
                        artifact_rows_done = 0,
                        artifact_part_count = 0,
                        artifact_bytes_written = 0
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "snapshot_id": snapshot_id,
                "snapshot_date": snapshot_date,
                "train_start": train_start,
                "train_end": train_end,
                "spec_hash": spec_hash,
                "memory_version": memory_version,
                "model_version": model_version,
                "snapshot_cadence": snapshot_cadence,
                "current_phase": str(current_phase or "event_memory"),
                "checkpoint_path": str(checkpoint_path or ""),
                "event_checkpoint_path": str(event_checkpoint_path or ""),
            },
        )
        session.commit()


def touch_snapshot_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    snapshot_id: str,
    current_phase: str = "",
    status: str = "",
    event_record_count: int | None = None,
    prototype_count: int | None = None,
    event_memory_ms: int | None = None,
    event_cache_build_ms: int | None = None,
    eligible_event_count: int | None = None,
    scaler_reconstruct_ms: int | None = None,
    transform_ms: int | None = None,
    prototype_prepare_ms: int | None = None,
    prototype_ms: int | None = None,
    artifact_write_ms: int | None = None,
    artifact_rows_total: int | None = None,
    artifact_rows_done: int | None = None,
    artifact_part_count: int | None = None,
    artifact_bytes_written: int | None = None,
    current_symbol: str | None = None,
    current_event_date: str | None = None,
    symbols_done: int | None = None,
    symbols_total: int | None = None,
    raw_event_row_count: int | None = None,
    pending_record_count: int | None = None,
    event_candidate_total: int | None = None,
    event_candidate_done: int | None = None,
    last_event_checkpoint_at: str | None = None,
    event_checkpoint_path: str | None = None,
    prototype_rows_total: int | None = None,
    prototype_rows_done: int | None = None,
    cluster_count: int | None = None,
    last_checkpoint_at: str | None = None,
    checkpoint_path: str | None = None,
    last_error: str | None = None,
) -> None:
    update_parts = ["last_heartbeat_at = NOW()"]
    params: dict[str, Any] = {
        "bundle_run_id": int(bundle_run_id or 0),
        "snapshot_id": str(snapshot_id),
    }
    if current_phase:
        update_parts.append("current_phase = :current_phase")
        params["current_phase"] = str(current_phase)
    if status:
        if status not in SNAPSHOT_STATUSES:
            raise ValueError(f"unsupported snapshot status: {status}")
        update_parts.append("status = :status")
        params["status"] = str(status)
    if event_record_count is not None:
        update_parts.append("event_record_count = :event_record_count")
        params["event_record_count"] = int(event_record_count)
    if prototype_count is not None:
        update_parts.append("prototype_count = :prototype_count")
        params["prototype_count"] = int(prototype_count)
    if event_memory_ms is not None:
        update_parts.append("event_memory_ms = :event_memory_ms")
        params["event_memory_ms"] = int(event_memory_ms)
    if event_cache_build_ms is not None:
        update_parts.append("event_cache_build_ms = :event_cache_build_ms")
        params["event_cache_build_ms"] = int(event_cache_build_ms)
    if eligible_event_count is not None:
        update_parts.append("eligible_event_count = :eligible_event_count")
        params["eligible_event_count"] = int(eligible_event_count)
    if scaler_reconstruct_ms is not None:
        update_parts.append("scaler_reconstruct_ms = :scaler_reconstruct_ms")
        params["scaler_reconstruct_ms"] = int(scaler_reconstruct_ms)
    if transform_ms is not None:
        update_parts.append("transform_ms = :transform_ms")
        params["transform_ms"] = int(transform_ms)
    if prototype_prepare_ms is not None:
        update_parts.append("prototype_prepare_ms = :prototype_prepare_ms")
        params["prototype_prepare_ms"] = int(prototype_prepare_ms)
    if prototype_ms is not None:
        update_parts.append("prototype_ms = :prototype_ms")
        params["prototype_ms"] = int(prototype_ms)
    if artifact_write_ms is not None:
        update_parts.append("artifact_write_ms = :artifact_write_ms")
        params["artifact_write_ms"] = int(artifact_write_ms)
    if artifact_rows_total is not None:
        update_parts.append("artifact_rows_total = :artifact_rows_total")
        params["artifact_rows_total"] = int(artifact_rows_total)
    if artifact_rows_done is not None:
        update_parts.append("artifact_rows_done = :artifact_rows_done")
        params["artifact_rows_done"] = int(artifact_rows_done)
    if artifact_part_count is not None:
        update_parts.append("artifact_part_count = :artifact_part_count")
        params["artifact_part_count"] = int(artifact_part_count)
    if artifact_bytes_written is not None:
        update_parts.append("artifact_bytes_written = :artifact_bytes_written")
        params["artifact_bytes_written"] = int(artifact_bytes_written)
    if current_symbol is not None:
        update_parts.append("current_symbol = NULLIF(:current_symbol, '')")
        params["current_symbol"] = str(current_symbol)
    if current_event_date is not None:
        update_parts.append("current_event_date = NULLIF(:current_event_date, '')::date")
        params["current_event_date"] = str(current_event_date)
    if symbols_done is not None:
        update_parts.append("symbols_done = :symbols_done")
        params["symbols_done"] = int(symbols_done)
    if symbols_total is not None:
        update_parts.append("symbols_total = :symbols_total")
        params["symbols_total"] = int(symbols_total)
    if raw_event_row_count is not None:
        update_parts.append("raw_event_row_count = :raw_event_row_count")
        params["raw_event_row_count"] = int(raw_event_row_count)
    if pending_record_count is not None:
        update_parts.append("pending_record_count = :pending_record_count")
        params["pending_record_count"] = int(pending_record_count)
    if event_candidate_total is not None:
        update_parts.append("event_candidate_total = :event_candidate_total")
        params["event_candidate_total"] = int(event_candidate_total)
    if event_candidate_done is not None:
        update_parts.append("event_candidate_done = :event_candidate_done")
        params["event_candidate_done"] = int(event_candidate_done)
    if last_event_checkpoint_at is not None:
        update_parts.append("last_event_checkpoint_at = CAST(:last_event_checkpoint_at AS timestamptz)")
        params["last_event_checkpoint_at"] = str(last_event_checkpoint_at)
    if event_checkpoint_path is not None:
        update_parts.append("event_checkpoint_path = NULLIF(:event_checkpoint_path, '')")
        params["event_checkpoint_path"] = str(event_checkpoint_path)
    if prototype_rows_total is not None:
        update_parts.append("prototype_rows_total = :prototype_rows_total")
        params["prototype_rows_total"] = int(prototype_rows_total)
    if prototype_rows_done is not None:
        update_parts.append("prototype_rows_done = :prototype_rows_done")
        params["prototype_rows_done"] = int(prototype_rows_done)
    if cluster_count is not None:
        update_parts.append("cluster_count = :cluster_count")
        params["cluster_count"] = int(cluster_count)
    if last_checkpoint_at is not None:
        update_parts.append("last_checkpoint_at = CAST(:last_checkpoint_at AS timestamptz)")
        params["last_checkpoint_at"] = str(last_checkpoint_at)
    if checkpoint_path is not None:
        update_parts.append("checkpoint_path = NULLIF(:checkpoint_path, '')")
        params["checkpoint_path"] = str(checkpoint_path)
    if last_error is not None:
        update_parts.append("last_error = :last_error")
        params["last_error"] = str(last_error)
    with session_factory() as session:
        session.execute(
            text(
                f"""
                UPDATE bt_result.calibration_snapshot_run
                   SET {", ".join(update_parts)}
                 WHERE bundle_run_id = :bundle_run_id
                   AND snapshot_id = :snapshot_id
                """
            ),
            params,
        )
        session.commit()


def complete_snapshot_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    snapshot_id: str,
    artifact_path: str,
    event_record_count: int,
    prototype_count: int,
    event_memory_ms: int = 0,
    event_cache_build_ms: int = 0,
    eligible_event_count: int = 0,
    scaler_reconstruct_ms: int = 0,
    transform_ms: int = 0,
    prototype_prepare_ms: int = 0,
    prototype_ms: int = 0,
    artifact_write_ms: int = 0,
    artifact_rows_total: int = 0,
    artifact_rows_done: int = 0,
    artifact_part_count: int = 0,
    artifact_bytes_written: int = 0,
    event_candidate_total: int = 0,
    event_candidate_done: int = 0,
    prototype_rows_total: int = 0,
    prototype_rows_done: int = 0,
    cluster_count: int = 0,
    checkpoint_path: str = "",
    event_checkpoint_path: str = "",
) -> None:
    with session_factory() as session:
        session.execute(
            text(
                """
                UPDATE bt_result.calibration_snapshot_run
                   SET status = 'ok',
                       current_phase = 'complete',
                       artifact_path = :artifact_path,
                       event_record_count = :event_record_count,
                       prototype_count = :prototype_count,
                       event_candidate_total = :event_candidate_total,
                       event_candidate_done = :event_candidate_done,
                       prototype_rows_total = :prototype_rows_total,
                       prototype_rows_done = :prototype_rows_done,
                       cluster_count = :cluster_count,
                       event_memory_ms = :event_memory_ms,
                       event_cache_build_ms = :event_cache_build_ms,
                       eligible_event_count = :eligible_event_count,
                       scaler_reconstruct_ms = :scaler_reconstruct_ms,
                       transform_ms = :transform_ms,
                       prototype_prepare_ms = :prototype_prepare_ms,
                       prototype_ms = :prototype_ms,
                       artifact_write_ms = :artifact_write_ms,
                       artifact_rows_total = :artifact_rows_total,
                       artifact_rows_done = :artifact_rows_done,
                       artifact_part_count = :artifact_part_count,
                       artifact_bytes_written = :artifact_bytes_written,
                       checkpoint_path = NULLIF(:checkpoint_path, ''),
                       event_checkpoint_path = NULLIF(:event_checkpoint_path, ''),
                       last_checkpoint_at = NOW(),
                       finished_at = NOW(),
                       last_error = NULL,
                       last_heartbeat_at = NOW()
                 WHERE bundle_run_id = :bundle_run_id
                   AND snapshot_id = :snapshot_id
                """
            ),
            {
                "bundle_run_id": int(bundle_run_id or 0),
                "snapshot_id": str(snapshot_id),
                "artifact_path": str(artifact_path),
                "event_record_count": int(event_record_count),
                "prototype_count": int(prototype_count),
                "event_candidate_total": int(event_candidate_total),
                "event_candidate_done": int(event_candidate_done),
                "prototype_rows_total": int(prototype_rows_total),
                "prototype_rows_done": int(prototype_rows_done),
                "cluster_count": int(cluster_count),
                "event_memory_ms": int(event_memory_ms),
                "event_cache_build_ms": int(event_cache_build_ms),
                "eligible_event_count": int(eligible_event_count),
                "scaler_reconstruct_ms": int(scaler_reconstruct_ms),
                "transform_ms": int(transform_ms),
                "prototype_prepare_ms": int(prototype_prepare_ms),
                "prototype_ms": int(prototype_ms),
                "artifact_write_ms": int(artifact_write_ms),
                "artifact_rows_total": int(artifact_rows_total),
                "artifact_rows_done": int(artifact_rows_done),
                "artifact_part_count": int(artifact_part_count),
                "artifact_bytes_written": int(artifact_bytes_written),
                "checkpoint_path": str(checkpoint_path or ""),
                "event_checkpoint_path": str(event_checkpoint_path or ""),
            },
        )
        session.commit()


def fail_snapshot_run(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    snapshot_id: str,
    last_error: str,
    current_phase: str = "",
    checkpoint_path: str = "",
    event_checkpoint_path: str = "",
) -> None:
    with session_factory() as session:
        session.execute(
            text(
                """
                UPDATE bt_result.calibration_snapshot_run
                   SET status = 'failed',
                       current_phase = COALESCE(NULLIF(:current_phase, ''), current_phase),
                       finished_at = NOW(),
                       checkpoint_path = COALESCE(NULLIF(:checkpoint_path, ''), checkpoint_path),
                       event_checkpoint_path = COALESCE(NULLIF(:event_checkpoint_path, ''), event_checkpoint_path),
                       last_error = :last_error,
                       last_heartbeat_at = NOW()
                 WHERE bundle_run_id = :bundle_run_id
                   AND snapshot_id = :snapshot_id
                """
            ),
            {
                "bundle_run_id": int(bundle_run_id or 0),
                "snapshot_id": str(snapshot_id),
                "current_phase": str(current_phase or ""),
                "checkpoint_path": str(checkpoint_path or ""),
                "event_checkpoint_path": str(event_checkpoint_path or ""),
                "last_error": str(last_error),
            },
        )
        session.commit()


def load_materialized_seed_rows(*, session_factory: sessionmaker[Session], bundle_run_id: int, policy_scope: str) -> list[dict[str, Any]]:
    with session_factory() as session:
        seed_rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_seed_row
                 WHERE bundle_run_id = :bundle_run_id
                 ORDER BY decision_date, symbol, side
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings().all()
        replay_rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_replay_bar
                 WHERE bundle_run_id = :bundle_run_id
                 ORDER BY decision_date, symbol, side, bar_n
                """
            ),
            {"bundle_run_id": bundle_run_id},
        ).mappings().all()
    bars_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in replay_rows:
        key = (str(row["decision_date"]), str(row["symbol"]), str(row["side"]))
        bars_by_key[key].append(
            {
                "session_date": str(row["session_date"]),
                "open": _to_float(row["open"]),
                "high": _to_float(row["high"]),
                "low": _to_float(row["low"]),
                "close": _to_float(row["close"]),
            }
        )
    normalized: list[dict[str, Any]] = []
    for row in seed_rows:
        key = (str(row["decision_date"]), str(row["symbol"]), str(row["side"]))
        path = bars_by_key.get(key, [])
        first = path[0] if path else {}
        normalized.append(
            {
                "decision_date": str(row["decision_date"]),
                "execution_date": first.get("session_date"),
                "symbol": str(row["symbol"]),
                "side": str(row["side"]),
                "run_label": "calibration_bundle",
                "policy_scope": policy_scope,
                "pattern_key": str(row.get("pattern_key") or ""),
                "policy_family": str(row.get("policy_family") or "echo_or_collapse"),
                "optuna_eligible": _to_bool(row.get("optuna_eligible")),
                "forecast_selected": _to_bool(row.get("forecast_selected")),
                "chosen_side_before_deploy": None,
                "abstain": False,
                "single_prototype_collapse": _to_bool(row.get("single_prototype_collapse")),
                "policy_edge_score": None,
                "q10_return": _to_float(row.get("q10_return")),
                "q50_return": _to_float(row.get("q50_return")),
                "q90_return": _to_float(row.get("q90_return")),
                "lower_bound": _to_float(row.get("lower_bound")),
                "interval_width": _to_float(row.get("interval_width")),
                "uncertainty": _to_float(row.get("uncertainty")),
                "member_mixture_ess": _to_float(row.get("member_mixture_ess")),
                "member_top1_weight_share": _to_float(row.get("member_top1_weight_share")),
                "member_pre_truncation_count": _to_int(row.get("member_pre_truncation_count")),
                "member_support_sum": 0.0,
                "member_consensus_signature": str(row.get("member_consensus_signature") or ""),
                "member_candidate_count": _to_int(row.get("member_pre_truncation_count")),
                "positive_weight_member_count": _to_int(row.get("member_pre_truncation_count")),
                "q50_d2_return": _to_float(row.get("q50_d2_return")),
                "q50_d3_return": _to_float(row.get("q50_d3_return")),
                "p_resolved_by_d2": _to_float(row.get("p_resolved_by_d2")),
                "p_resolved_by_d3": _to_float(row.get("p_resolved_by_d3")),
                "regime_code": str(row.get("regime_code") or "UNKNOWN"),
                "sector_code": str(row.get("sector_code") or "UNKNOWN"),
                "country_code": "US" if str(row.get("market") or "US").upper() == "US" else "KR",
                "exchange_code": "NMS",
                "exchange_tz": "America/New_York",
                "shape_bucket": "wide",
                "market": str(row.get("market") or "US"),
                "t1_open": _to_float(row.get("t1_open")),
                "d1_open": first.get("open"),
                "d1_high": first.get("high"),
                "d1_low": first.get("low"),
                "d1_close": first.get("close"),
                "bar_path_d1_to_d5": _json_dumps(path),
                "path_length": len(path),
                "last_path_close": path[-1]["close"] if path else None,
                "recurring_family": _to_bool(row.get("recurring_family")),
            }
        )
    return normalized


def materialize_query_feature_cache(
    *,
    write_session_factory: sessionmaker[Session],
    bundle_run_id: int,
    market: str,
    start_date: str,
    end_date: str,
    symbols: Sequence[str],
    research_spec: ResearchExperimentSpec | None = None,
    metadata: Mapping[str, Any] | None = None,
    symbol_chunk_size: int = 10,
    date_window_months: int = 1,
    subwindow_days: int = 0,
    worker_count: int = 4,
    lookback_buffer_days: int = 220,
    forward_buffer_days: int = 10,
    progress_path: str = "",
) -> dict[str, Any]:
    spec = _spec_or_default(research_spec)
    del market
    symbol_list = [str(symbol) for symbol in symbols if symbol]
    chunk_size = max(1, int(symbol_chunk_size or 10))
    subwindow_size_days = max(0, int(subwindow_days or 0))
    month_windows = _month_windows(start_date=start_date, end_date=end_date, window_months=max(1, int(date_window_months or 1)))
    progress_target = Path(progress_path) if str(progress_path or "").strip() else None
    existing_chunks = {int(row.get("chunk_id") or 0): row for row in list_query_chunk_runs(session_factory=write_session_factory, bundle_run_id=bundle_run_id)}
    progress_lock = threading.Lock()
    total_query_rows = 0
    total_replay_rows = 0
    total_decision_dates: set[str] = set()
    load_total_ms = 0
    feature_total_ms = 0
    db_write_total_ms = 0
    chunk_jobs: list[dict[str, Any]] = []
    chunk_id = 0
    for symbol_start in range(0, len(symbol_list), chunk_size):
        chunk_symbols = symbol_list[symbol_start : symbol_start + chunk_size]
        for window_start, window_end in month_windows:
            chunk_id += 1
            chunk_jobs.append(
                {
                    "chunk_id": chunk_id,
                    "symbols": list(chunk_symbols),
                    "window_start": window_start,
                    "window_end": window_end,
                }
            )

    def _write_progress(*, status: str, current_chunk_id: int = 0, current_symbols: Sequence[str] | None = None, current_window_start: str = "", current_window_end: str = "", last_error: str = "") -> None:
        if progress_target is None:
            return
        with progress_lock:
            rows = list_query_chunk_runs(session_factory=write_session_factory, bundle_run_id=bundle_run_id)
            payload = {
                "status": status,
                "bundle_run_id": bundle_run_id,
                "current_step": "build-query-feature-cache",
                "updated_at": _utcnow_iso(),
                "total_chunks": len(chunk_jobs),
                "completed_chunks": sum(1 for row in rows if str(row.get("status") or "") in {"ok", "reused"}),
                "failed_chunks": sum(1 for row in rows if str(row.get("status") or "") == "failed"),
                "query_row_count": sum(
                    _to_int(row.get("query_row_count"))
                    for row in rows
                    if str(row.get("status") or "") in {"ok", "reused", "running"}
                ),
                "replay_bar_count": sum(
                    _to_int(row.get("replay_bar_count"))
                    for row in rows
                    if str(row.get("status") or "") in {"ok", "reused", "running"}
                ),
                "current_chunk_id": current_chunk_id,
                "current_symbols": list(current_symbols or []),
                "current_window_start": current_window_start,
                "current_window_end": current_window_end,
                "last_error": last_error,
            }
            progress_target.parent.mkdir(parents=True, exist_ok=True)
            progress_target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    touch_bundle_run(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        current_step="build-query-feature-cache",
        pid=os.getpid(),
        status="running",
        last_error="",
    )
    _write_progress(status="running")

    def _run_query_chunk(job: Mapping[str, Any]) -> dict[str, Any]:
        local_session_factory = create_backtest_write_session_factory()
        loader = LocalPostgresLoader(create_backtest_session_factory())
        local_chunk_id = int(job["chunk_id"])
        local_symbols = list(job["symbols"])
        window_start = str(job["window_start"])
        window_end = str(job["window_end"])
        existing = existing_chunks.get(local_chunk_id) or {}
        if str(existing.get("status") or "") in {"ok", "reused"}:
            with local_session_factory() as session:
                sql, params = _query_chunk_rows(
                    bundle_run_id=bundle_run_id,
                    chunk_id=local_chunk_id,
                    status="reused",
                    window_start=window_start,
                    window_end=window_end,
                    symbols=local_symbols,
                    elapsed_ms=_to_int(existing.get("elapsed_ms")),
                    load_ms=_to_int(existing.get("load_ms")),
                    feature_build_ms=_to_int(existing.get("feature_build_ms")),
                    db_write_ms=_to_int(existing.get("db_write_ms")),
                    decision_date_count=_to_int(existing.get("decision_date_count")),
                    query_row_count=_to_int(existing.get("query_row_count")),
                    replay_bar_count=_to_int(existing.get("replay_bar_count")),
                    last_error=None,
                )
                session.execute(text(sql), params)
                session.commit()
            return {
                "status": "reused",
                "chunk_id": local_chunk_id,
                "window_start": window_start,
                "window_end": window_end,
                "symbols": local_symbols,
                "decision_date_count": _to_int(existing.get("decision_date_count")),
                "query_row_count": _to_int(existing.get("query_row_count")),
                "replay_bar_count": _to_int(existing.get("replay_bar_count")),
                "load_ms": _to_int(existing.get("load_ms")),
                "feature_build_ms": _to_int(existing.get("feature_build_ms")),
                "db_write_ms": _to_int(existing.get("db_write_ms")),
            }
        with local_session_factory() as session:
            sql, params = _query_chunk_rows(
                bundle_run_id=bundle_run_id,
                chunk_id=local_chunk_id,
                status="running",
                window_start=window_start,
                window_end=window_end,
                symbols=local_symbols,
                started_at=True,
                last_error=None,
            )
            session.execute(text(sql), params)
            session.commit()
        touch_bundle_run(
            session_factory=local_session_factory,
            bundle_run_id=bundle_run_id,
            current_step="build-query-feature-cache",
            pid=os.getpid(),
            status="running",
            last_error="",
        )
        chunk_started = time.perf_counter()
        load_start_date, load_end_date = _query_chunk_range(
            bundle_start=start_date,
            bundle_end=end_date,
            window_start=window_start,
            window_end=window_end,
            lookback_days=lookback_buffer_days,
            forward_days=forward_buffer_days,
        )
        load_started = time.perf_counter()
        def _load_progress(stage: str, details: Mapping[str, Any] | None = None) -> None:
            current_load_ms = int((time.perf_counter() - load_started) * 1000)
            with local_session_factory() as session:
                sql, params = _query_chunk_rows(
                    bundle_run_id=bundle_run_id,
                    chunk_id=local_chunk_id,
                    status="running",
                    window_start=window_start,
                    window_end=window_end,
                    symbols=local_symbols,
                    elapsed_ms=int((time.perf_counter() - chunk_started) * 1000),
                    load_ms=current_load_ms,
                    decision_date_count=0,
                    query_row_count=0,
                    replay_bar_count=0,
                    last_error=None,
                )
                session.execute(text(sql), params)
                session.commit()
            touch_bundle_run(
                session_factory=local_session_factory,
                bundle_run_id=bundle_run_id,
                current_step="build-query-feature-cache",
                pid=os.getpid(),
                status="running",
                last_error="",
            )
            _write_progress(
                status="running",
                current_chunk_id=local_chunk_id,
                current_symbols=local_symbols,
                current_window_start=window_start,
                current_window_end=window_end,
                last_error=f"{stage} {dict(details or {})}".strip(),
            )
        context = loader.load_research_context(
            start_date=load_start_date,
            end_date=load_end_date,
            symbols=local_symbols,
            research_spec=spec,
            progress_callback=_load_progress,
        )
        load_ms = int((time.perf_counter() - load_started) * 1000)
        subwindows = _date_subwindows(
            start_date=window_start,
            end_date=window_end,
            window_days=subwindow_size_days or ((_calendar_date(window_end) - _calendar_date(window_start)).days + 1),
        )
        feature_build_ms = 0
        db_write_ms = 0
        total_query_rows_local = 0
        total_replay_rows_local = 0
        decision_dates: set[str] = set()
        with local_session_factory() as session:
            session.execute(
                text(
                    """
                    DELETE FROM bt_result.calibration_query_feature_row
                     WHERE bundle_run_id = :bundle_run_id
                       AND symbol = ANY(:symbols)
                       AND decision_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "symbols": local_symbols,
                    "start_date": window_start,
                    "end_date": window_end,
                },
            )
            session.execute(
                text(
                    """
                    DELETE FROM bt_result.calibration_replay_bar
                     WHERE bundle_run_id = :bundle_run_id
                       AND symbol = ANY(:symbols)
                       AND decision_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "symbols": local_symbols,
                    "start_date": window_start,
                    "end_date": window_end,
                },
            )
            session.commit()
        for subwindow_start, subwindow_end in subwindows:
            feature_started = time.perf_counter()
            payload = build_query_feature_cache_rows(
                symbols=local_symbols,
                bars_by_symbol=context["bars_by_symbol"],
                macro_history_by_date=context["macro_history_by_date"],
                sector_map=context["sector_map"],
                session_metadata_by_symbol=context["session_metadata_by_symbol"],
                macro_series_history=context["macro_series_history"],
                spec=spec,
                start_date=subwindow_start,
                end_date=subwindow_end,
                metadata=metadata,
            )
            feature_build_ms += int((time.perf_counter() - feature_started) * 1000)
            query_rows = list(payload["query_rows"])
            replay_rows = list(payload["replay_rows"])
            total_query_rows_local += len(query_rows)
            total_replay_rows_local += len(replay_rows)
            decision_dates.update(str(row["decision_date"]) for row in query_rows)
            db_write_started = time.perf_counter()
            with local_session_factory() as session:
                if query_rows:
                    session.execute(
                        text(
                            """
                            INSERT INTO bt_result.calibration_query_feature_row(
                                bundle_run_id, decision_date, symbol, execution_date, t1_open,
                                regime_code, sector_code, feature_anchor_ts_utc, macro_asof_ts_utc,
                                raw_features_json, transformed_features_json, embedding_json, query_meta_json
                            )
                            VALUES (
                                :bundle_run_id, CAST(:decision_date AS date), :symbol, CAST(:execution_date AS date), :t1_open,
                                :regime_code, :sector_code, CAST(:feature_anchor_ts_utc AS timestamptz), CAST(:macro_asof_ts_utc AS timestamptz),
                                :raw_features_json, :transformed_features_json, :embedding_json, :query_meta_json
                            )
                            """
                        ),
                        [{"bundle_run_id": bundle_run_id, **row} for row in query_rows],
                    )
                if replay_rows:
                    session.execute(
                        text(
                            """
                            INSERT INTO bt_result.calibration_replay_bar(
                                bundle_run_id, decision_date, symbol, side, bar_n, session_date, open, high, low, close
                            )
                            VALUES (
                                :bundle_run_id, CAST(:decision_date AS date), :symbol, :side, :bar_n, CAST(:session_date AS date), :open, :high, :low, :close
                            )
                            ON CONFLICT (bundle_run_id, decision_date, symbol, side, bar_n) DO UPDATE
                                SET session_date = EXCLUDED.session_date,
                                    open = EXCLUDED.open,
                                    high = EXCLUDED.high,
                                    low = EXCLUDED.low,
                                    close = EXCLUDED.close
                            """
                        ),
                        [{"bundle_run_id": bundle_run_id, **row} for row in replay_rows],
                    )
                db_write_ms += int((time.perf_counter() - db_write_started) * 1000)
                sql, params = _query_chunk_rows(
                    bundle_run_id=bundle_run_id,
                    chunk_id=local_chunk_id,
                    status="running",
                    window_start=window_start,
                    window_end=window_end,
                    symbols=local_symbols,
                    elapsed_ms=int((time.perf_counter() - chunk_started) * 1000),
                    load_ms=load_ms,
                    feature_build_ms=feature_build_ms,
                    db_write_ms=db_write_ms,
                    decision_date_count=len(decision_dates),
                    query_row_count=total_query_rows_local,
                    replay_bar_count=total_replay_rows_local,
                    last_error=None,
                )
                session.execute(text(sql), params)
                session.commit()
            touch_bundle_run(
                session_factory=local_session_factory,
                bundle_run_id=bundle_run_id,
                current_step="build-query-feature-cache",
                pid=os.getpid(),
                status="running",
                last_error="",
            )
            _write_progress(
                status="running",
                current_chunk_id=local_chunk_id,
                current_symbols=local_symbols,
                current_window_start=subwindow_start,
                current_window_end=subwindow_end,
            )
        with local_session_factory() as session:
            sql, params = _query_chunk_rows(
                bundle_run_id=bundle_run_id,
                chunk_id=local_chunk_id,
                status="ok",
                window_start=window_start,
                window_end=window_end,
                symbols=local_symbols,
                finished=True,
                elapsed_ms=int((time.perf_counter() - chunk_started) * 1000),
                load_ms=load_ms,
                feature_build_ms=feature_build_ms,
                db_write_ms=db_write_ms,
                decision_date_count=len(decision_dates),
                query_row_count=total_query_rows_local,
                replay_bar_count=total_replay_rows_local,
                last_error=None,
            )
            session.execute(text(sql), params)
            session.commit()
        return {
            "status": "ok",
            "chunk_id": local_chunk_id,
            "window_start": window_start,
            "window_end": window_end,
            "symbols": local_symbols,
            "decision_date_count": len(decision_dates),
            "query_row_count": total_query_rows_local,
            "replay_bar_count": total_replay_rows_local,
            "load_ms": load_ms,
            "feature_build_ms": feature_build_ms,
            "db_write_ms": db_write_ms,
        }

    try:
        with ThreadPoolExecutor(max_workers=max(1, int(worker_count or 4))) as executor:
            futures = {executor.submit(_run_query_chunk, job): job for job in chunk_jobs}
            for future in as_completed(futures):
                job = futures[future]
                try:
                    chunk_result = future.result()
                except Exception as exc:
                    with write_session_factory() as session:
                        sql, params = _query_chunk_rows(
                            bundle_run_id=bundle_run_id,
                            chunk_id=int(job["chunk_id"]),
                            status="failed",
                            window_start=str(job["window_start"]),
                            window_end=str(job["window_end"]),
                            symbols=list(job["symbols"]),
                            finished=True,
                            last_error=str(exc),
                        )
                        session.execute(text(sql), params)
                        session.commit()
                    touch_bundle_run(
                        session_factory=write_session_factory,
                        bundle_run_id=bundle_run_id,
                        current_step="build-query-feature-cache",
                        pid=os.getpid(),
                        status="running",
                        last_error=str(exc),
                    )
                    _write_progress(
                        status="running",
                        current_chunk_id=int(job["chunk_id"]),
                        current_symbols=list(job["symbols"]),
                        current_window_start=str(job["window_start"]),
                        current_window_end=str(job["window_end"]),
                        last_error=str(exc),
                    )
                    raise
                total_query_rows += int(chunk_result["query_row_count"])
                total_replay_rows += int(chunk_result["replay_bar_count"])
                total_decision_dates.update(
                    row["decision_date"]
                    for row in _load_cached_query_rows(
                        session_factory=write_session_factory,
                        bundle_run_id=bundle_run_id,
                        symbols=chunk_result["symbols"],
                        start_date=chunk_result["window_start"],
                        end_date=chunk_result["window_end"],
                    )[0]
                )
                load_total_ms += int(chunk_result.get("load_ms") or 0)
                feature_total_ms += int(chunk_result.get("feature_build_ms") or 0)
                db_write_total_ms += int(chunk_result.get("db_write_ms") or 0)
                touch_bundle_run(
                    session_factory=write_session_factory,
                    bundle_run_id=bundle_run_id,
                    current_step="build-query-feature-cache",
                    pid=os.getpid(),
                    status="running",
                    last_error="",
                )
                _write_progress(
                    status="running",
                    current_chunk_id=int(chunk_result["chunk_id"]),
                    current_symbols=list(chunk_result["symbols"]),
                    current_window_start=str(chunk_result["window_start"]),
                    current_window_end=str(chunk_result["window_end"]),
                )
    except Exception:
        mark_bundle_run_failed(
            session_factory=write_session_factory,
            bundle_run_id=bundle_run_id,
            last_error="build-query-feature-cache failed",
        )
        _write_progress(status="failed", last_error="build-query-feature-cache failed")
        raise

    touch_bundle_run(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        current_step="build-query-feature-cache",
        pid=os.getpid(),
        status="running",
        last_error="",
    )
    _write_progress(status="ok")
    return {
        "status": "ok",
        "bundle_run_id": bundle_run_id,
        "symbol_count": len(symbol_list),
        "decision_date_count": len(total_decision_dates),
        "query_row_count": total_query_rows,
        "replay_bar_count": total_replay_rows,
        "load_ms": load_total_ms,
        "query_feature_ms": feature_total_ms,
        "db_write_ms": db_write_total_ms,
        "query_chunk_count": len(chunk_jobs),
    }


def materialize_train_snapshots(
    *,
    write_session_factory: sessionmaker[Session],
    bundle_run_id: int,
    bundle_key: str,
    market: str,
    start_date: str,
    end_date: str,
    symbols: Sequence[str],
    research_spec: ResearchExperimentSpec | None = None,
    metadata: Mapping[str, Any] | None = None,
    output_dir: str,
    snapshot_cadence: str = "daily",
    model_version: str = "",
) -> dict[str, Any]:
    spec = _spec_or_default(research_spec)
    cadence = str(snapshot_cadence or "daily").strip().lower() or "daily"
    resolved_model_version = _resolve_model_version(cadence, model_version)
    _invalidate_materialized_contracts_if_needed(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        output_dir=output_dir,
    )
    touch_bundle_run(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        current_step="build-train-snapshots",
        pid=os.getpid(),
        status="running",
        last_error="",
    )
    decision_dates = _load_cached_decision_dates(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    if not decision_dates:
        raise RuntimeError(
            "build-train-snapshots requires pre-materialized query feature cache rows; "
            "run build-query-feature-cache first"
        )
    loader = LocalPostgresLoader(create_backtest_session_factory())
    def _context_progress(phase: str, payload: Mapping[str, Any]) -> None:
        touch_bundle_run(
            session_factory=write_session_factory,
            bundle_run_id=bundle_run_id,
            current_step="build-train-snapshots",
            pid=os.getpid(),
            status="running",
            last_error="",
        )
    context = loader.load_research_context(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        research_spec=spec,
        progress_callback=_context_progress,
    )
    snapshot_dates = _snapshot_dates(decision_dates, cadence)
    artifact_store = JsonResearchArtifactStore(output_dir)
    run_id = f"{bundle_key or f'bundle-{bundle_run_id}'}__snapshots"
    event_cache_handle = None
    if snapshot_dates:
        def _event_cache_progress(_payload: Mapping[str, Any]) -> None:
            touch_bundle_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                current_step="build-train-snapshots",
                pid=os.getpid(),
                status="running",
                last_error="",
            )

        event_cache_handle = build_event_raw_cache(
            output_dir=output_dir,
            run_id=run_id,
            decision_date=str(snapshot_dates[-1]),
            spec=spec,
            bars_by_symbol=context["bars_by_symbol"],
            macro_history_by_date=context["macro_history_by_date"],
            sector_map=context["sector_map"],
            metadata=dict(metadata or {}),
            session_metadata_by_symbol=context["session_metadata_by_symbol"],
            macro_series_history=context["macro_series_history"],
            use_proxy_aggregate_cache=True,
            progress_callback=_event_cache_progress,
        )
    created = 0
    reused = 0
    fast_path_recent_snapshots: list[dict[str, Any]] = []
    for snapshot_date in snapshot_dates:
        snapshot_id = f"{run_id}:{snapshot_date}:{spec.spec_hash()}"
        snapshot_name = f"train_snapshot_{snapshot_date.replace('-', '')}"
        checkpoint_paths = _snapshot_checkpoint_paths(output_dir=output_dir, run_id=run_id, snapshot_date=snapshot_date)
        with write_session_factory() as session:
            existing = session.execute(
                text(
                    """
                    SELECT *
                      FROM bt_result.calibration_snapshot_run
                     WHERE bundle_run_id = :bundle_run_id
                       AND snapshot_id = :snapshot_id
                    """
                ),
                {"bundle_run_id": bundle_run_id, "snapshot_id": snapshot_id},
            ).mappings().first()
            if existing and str(existing.get("status") or "") == "ok" and str(existing.get("artifact_path") or "").strip():
                if _train_snapshot_artifact_is_reusable(str(existing["artifact_path"])):
                    reused += 1
                    touch_bundle_run(
                        session_factory=write_session_factory,
                        bundle_run_id=bundle_run_id,
                        current_step="build-train-snapshots",
                        pid=os.getpid(),
                        status="running",
                        last_error="",
                    )
                    continue
            resume_event_memory_from_checkpoint = bool(
                Path(checkpoint_paths["event_input_path"]).exists()
            )
            resume_prototype_from_checkpoint = bool(
                Path(checkpoint_paths["prototype_checkpoint_path"]).exists()
            )
            begin_snapshot_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                snapshot_id=snapshot_id,
                snapshot_date=snapshot_date,
                train_start=start_date,
                train_end=snapshot_date,
                spec_hash=spec.spec_hash(),
                memory_version=spec.memory_version,
                model_version=resolved_model_version,
                snapshot_cadence=cadence,
                current_phase="event_candidate_prep",
                checkpoint_path=checkpoint_paths["prototype_checkpoint_path"],
                event_checkpoint_path=checkpoint_paths["event_checkpoint_path"],
            )
        last_snapshot_phase = "event_candidate_prep"

        def _snapshot_progress(payload: Mapping[str, Any]) -> None:
            nonlocal last_snapshot_phase
            current_phase = str(payload.get("phase") or last_snapshot_phase or "event_memory")
            last_snapshot_phase = current_phase
            touch_snapshot_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                snapshot_id=snapshot_id,
                current_phase=current_phase,
                status="running",
                event_record_count=(
                    _to_int(payload.get("event_record_count"))
                    if payload.get("event_record_count") is not None
                    else None
                ),
                prototype_count=(
                    _to_int(payload.get("prototype_count"))
                    if payload.get("prototype_count") is not None
                    else None
                ),
                event_memory_ms=(
                    _to_int(payload.get("event_memory_ms"))
                    if payload.get("event_memory_ms") is not None
                    else None
                ),
                event_cache_build_ms=(
                    _to_int(payload.get("event_cache_build_ms"))
                    if payload.get("event_cache_build_ms") is not None
                    else None
                ),
                eligible_event_count=(
                    _to_int(payload.get("eligible_event_count"))
                    if payload.get("eligible_event_count") is not None
                    else None
                ),
                scaler_reconstruct_ms=(
                    _to_int(payload.get("scaler_reconstruct_ms"))
                    if payload.get("scaler_reconstruct_ms") is not None
                    else None
                ),
                transform_ms=(
                    _to_int(payload.get("transform_ms"))
                    if payload.get("transform_ms") is not None
                    else None
                ),
                prototype_prepare_ms=(
                    _to_int(payload.get("prototype_prepare_ms"))
                    if payload.get("prototype_prepare_ms") is not None
                    else None
                ),
                prototype_ms=(
                    _to_int(payload.get("prototype_ms"))
                    if payload.get("prototype_ms") is not None
                    else None
                ),
                artifact_write_ms=(
                    _to_int(payload.get("artifact_write_ms"))
                    if payload.get("artifact_write_ms") is not None
                    else None
                ),
                artifact_rows_total=(
                    _to_int(payload.get("artifact_rows_total"))
                    if payload.get("artifact_rows_total") is not None
                    else None
                ),
                artifact_rows_done=(
                    _to_int(payload.get("artifact_rows_done"))
                    if payload.get("artifact_rows_done") is not None
                    else None
                ),
                artifact_part_count=(
                    _to_int(payload.get("artifact_part_count"))
                    if payload.get("artifact_part_count") is not None
                    else None
                ),
                artifact_bytes_written=(
                    _to_int(payload.get("artifact_bytes_written"))
                    if payload.get("artifact_bytes_written") is not None
                    else None
                ),
                current_symbol=(
                    str(payload.get("current_symbol") or "")
                    if payload.get("current_symbol") is not None
                    else None
                ),
                current_event_date=(
                    str(payload.get("current_event_date") or "")
                    if payload.get("current_event_date") is not None
                    else None
                ),
                symbols_done=(
                    _to_int(payload.get("symbols_done"))
                    if payload.get("symbols_done") is not None
                    else None
                ),
                symbols_total=(
                    _to_int(payload.get("symbols_total"))
                    if payload.get("symbols_total") is not None
                    else None
                ),
                raw_event_row_count=(
                    _to_int(payload.get("raw_event_row_count"))
                    if payload.get("raw_event_row_count") is not None
                    else None
                ),
                pending_record_count=(
                    _to_int(payload.get("pending_record_count"))
                    if payload.get("pending_record_count") is not None
                    else None
                ),
                event_candidate_total=(
                    _to_int(payload.get("event_candidate_total"))
                    if payload.get("event_candidate_total") is not None
                    else None
                ),
                event_candidate_done=(
                    _to_int(payload.get("event_candidate_done"))
                    if payload.get("event_candidate_done") is not None
                    else None
                ),
                last_event_checkpoint_at=(
                    str(payload.get("last_event_checkpoint_at") or "")
                    if payload.get("last_event_checkpoint_at") is not None and str(payload.get("last_event_checkpoint_at") or "").strip()
                    else None
                ),
                event_checkpoint_path=(
                    str(payload.get("event_checkpoint_path") or "")
                    if payload.get("event_checkpoint_path") is not None
                    else None
                ),
                prototype_rows_total=(
                    _to_int(payload.get("prototype_rows_total"))
                    if payload.get("prototype_rows_total") is not None
                    else None
                ),
                prototype_rows_done=(
                    _to_int(payload.get("prototype_rows_done"))
                    if payload.get("prototype_rows_done") is not None
                    else None
                ),
                cluster_count=(
                    _to_int(payload.get("cluster_count"))
                    if payload.get("cluster_count") is not None
                    else None
                ),
                last_checkpoint_at=(
                    str(payload.get("last_checkpoint_at") or "")
                    if payload.get("last_checkpoint_at") is not None and str(payload.get("last_checkpoint_at") or "").strip()
                    else None
                ),
                checkpoint_path=(
                    str(payload.get("checkpoint_path") or "")
                    if payload.get("checkpoint_path") is not None
                    else None
                ),
            )
            touch_bundle_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                current_step="build-train-snapshots",
                pid=os.getpid(),
                status="running",
                last_error="",
            )
        try:
            train_artifact = fit_train_artifacts(
                run_id=run_id,
                artifact_store=artifact_store,
                train_end=snapshot_date,
                test_start=snapshot_date,
                purge=0,
                embargo=0,
                spec=spec,
                bars_by_symbol=context["bars_by_symbol"],
                macro_history_by_date=context["macro_history_by_date"],
                sector_map=context["sector_map"],
                market=market,
                calibration_artifact=None,
                quote_policy_calibration=None,
                metadata=dict(metadata or {}),
                session_metadata_by_symbol=context["session_metadata_by_symbol"],
                macro_series_history=context["macro_series_history"],
                progress_callback=_snapshot_progress,
                use_proxy_aggregate_cache=True,
                use_event_memory_fast_path=True,
                event_input_path=checkpoint_paths["event_input_path"],
                event_checkpoint_path=checkpoint_paths["event_checkpoint_path"],
                resume_event_memory_from_checkpoint=resume_event_memory_from_checkpoint,
                prototype_input_path=None,
                prototype_checkpoint_path=checkpoint_paths["prototype_checkpoint_path"],
                resume_prototype_from_checkpoint=resume_prototype_from_checkpoint,
                comparison_block_size=2048,
                event_cache_handle=event_cache_handle,
            )
            snapshot_payload = _json_safe_snapshot_payload(train_artifact)
            phase_timings_ms = dict(train_artifact.get("phase_timings_ms") or {})
            _snapshot_progress(
                {
                    "phase": "artifact_write",
                    "status": "running",
                    "event_record_count": snapshot_payload.get("event_record_count"),
                    "prototype_count": snapshot_payload.get("prototype_count"),
                    "event_memory_ms": phase_timings_ms.get("event_memory"),
                    "transform_ms": phase_timings_ms.get("transform"),
                    "prototype_ms": phase_timings_ms.get("prototype"),
                    "artifact_write_ms": phase_timings_ms.get("artifact_write"),
                    "artifact_rows_total": snapshot_payload.get("prototype_count"),
                    "artifact_rows_done": snapshot_payload.get("prototype_count"),
                    "artifact_part_count": 0,
                    "artifact_bytes_written": 0,
                }
            )
            artifact_write_started = time.perf_counter()
            artifact_path = artifact_store.save_train_snapshot(
                run_id=run_id,
                name=snapshot_name,
                as_of_date=snapshot_date,
                memory_version=spec.memory_version,
                payload=snapshot_payload,
            )
            train_snapshot_artifact_write_ms = int((time.perf_counter() - artifact_write_started) * 1000)
            artifact_write_ms = int(_to_int(phase_timings_ms.get("artifact_write")) + train_snapshot_artifact_write_ms)
            prototype_manifest_path = Path(str(train_artifact.get("prototype_snapshot_manifest_path") or ""))
            prototype_parts_dir = prototype_manifest_path.parent / "parts" if prototype_manifest_path.exists() else None
            artifact_part_count = len(list(prototype_parts_dir.glob("*.parquet"))) if prototype_parts_dir and prototype_parts_dir.exists() else 0
            artifact_bytes_written = int(
                sum(
                    path.stat().st_size
                    for path in [prototype_manifest_path, artifact_path and Path(artifact_path)]
                    if path and Path(path).exists()
                )
                + (
                    sum(path.stat().st_size for path in prototype_parts_dir.glob("*.parquet"))
                    if prototype_parts_dir and prototype_parts_dir.exists()
                    else 0
                )
            )
            complete_snapshot_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                snapshot_id=snapshot_id,
                artifact_path=artifact_path,
                event_record_count=_to_int(snapshot_payload.get("event_record_count")),
                prototype_count=_to_int(snapshot_payload.get("prototype_count")),
                event_memory_ms=_to_int(phase_timings_ms.get("event_memory")),
                event_cache_build_ms=_to_int(train_artifact.get("event_cache_build_ms")),
                eligible_event_count=_to_int(train_artifact.get("eligible_event_count")),
                scaler_reconstruct_ms=_to_int(train_artifact.get("scaler_reconstruct_ms")),
                transform_ms=_to_int(phase_timings_ms.get("transform")),
                prototype_prepare_ms=_to_int(phase_timings_ms.get("prototype_prepare")),
                prototype_ms=_to_int(phase_timings_ms.get("prototype")),
                artifact_write_ms=artifact_write_ms,
                artifact_rows_total=_to_int(snapshot_payload.get("prototype_count")),
                artifact_rows_done=_to_int(snapshot_payload.get("prototype_count")),
                artifact_part_count=artifact_part_count,
                artifact_bytes_written=artifact_bytes_written,
                event_candidate_total=_to_int(snapshot_payload.get("event_record_count")),
                event_candidate_done=_to_int(snapshot_payload.get("event_record_count")),
                prototype_rows_total=_to_int(snapshot_payload.get("event_record_count")),
                prototype_rows_done=_to_int(snapshot_payload.get("event_record_count")),
                cluster_count=_to_int(snapshot_payload.get("prototype_count")),
                checkpoint_path="",
                event_checkpoint_path="",
            )
            _clear_snapshot_checkpoint_files(checkpoint_paths=checkpoint_paths)
            touch_bundle_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                current_step="build-train-snapshots",
                pid=os.getpid(),
                status="running",
                last_error="",
            )
            created += 1
            snapshot_elapsed_seconds = max(
                1.0,
                (
                    _to_float(phase_timings_ms.get("event_memory"))
                    + _to_float(phase_timings_ms.get("transform"))
                    + _to_float(phase_timings_ms.get("prototype"))
                    + float(artifact_write_ms)
                )
                / 1000.0,
            )
            fast_path_recent_snapshots.append(
                {
                    "event_memory_ms": _to_int(phase_timings_ms.get("event_memory")),
                    "transform_ms": _to_int(phase_timings_ms.get("transform")),
                    "prototype_ms": _to_int(phase_timings_ms.get("prototype")),
                    "artifact_write_ms": int(artifact_write_ms),
                    "event_candidate_total": _to_int(snapshot_payload.get("event_record_count")),
                }
            )
            if created == 1:
                remaining_snapshot_count = max(0, len(snapshot_dates) - reused - created)
                remaining_eta_seconds = _estimate_remaining_snapshot_eta_seconds(
                    remaining_snapshot_count=remaining_snapshot_count,
                    created_snapshot_seconds=snapshot_elapsed_seconds,
                    created_event_candidate_rows=_to_int(snapshot_payload.get("event_record_count")),
                    recent_snapshot_rows=fast_path_recent_snapshots[-2:],
                )
                if remaining_eta_seconds > 8 * 60 * 60:
                    last_error = "eta_gate_exceeded_after_event_memory_fast_path"
                    mark_bundle_run_failed(
                        session_factory=write_session_factory,
                        bundle_run_id=bundle_run_id,
                        last_error=last_error,
                    )
                    raise RuntimeError(last_error)
        except Exception as exc:
            if str(exc) == "eta_gate_exceeded_after_event_memory_fast_path":
                raise
            fail_snapshot_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                snapshot_id=snapshot_id,
                last_error=str(exc),
                current_phase=last_snapshot_phase,
                checkpoint_path=checkpoint_paths["prototype_checkpoint_path"],
                event_checkpoint_path=checkpoint_paths["event_checkpoint_path"],
            )
            touch_bundle_run(
                session_factory=write_session_factory,
                bundle_run_id=bundle_run_id,
                current_step="build-train-snapshots",
                pid=os.getpid(),
                status="running",
                last_error=str(exc),
            )
            raise
    touch_bundle_run(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        current_step="build-train-snapshots",
        pid=os.getpid(),
        status="running",
        last_error="",
    )
    return {
        "status": "ok",
        "bundle_run_id": bundle_run_id,
        "snapshot_cadence": cadence,
        "model_version": resolved_model_version,
        "snapshot_count": len(snapshot_dates),
        "created_snapshot_count": created,
        "reused_snapshot_count": reused,
    }


def _load_cached_query_rows(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    symbols: Sequence[str],
    start_date: str,
    end_date: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    symbol_list = [str(symbol) for symbol in symbols if symbol]
    with session_factory() as session:
        query_rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_query_feature_row
                 WHERE bundle_run_id = :bundle_run_id
                   AND symbol = ANY(:symbols)
                   AND decision_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                 ORDER BY decision_date, symbol
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "symbols": symbol_list,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings().all()
        replay_rows = session.execute(
            text(
                """
                SELECT *
                  FROM bt_result.calibration_replay_bar
                 WHERE bundle_run_id = :bundle_run_id
                   AND symbol = ANY(:symbols)
                   AND decision_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                 ORDER BY decision_date, symbol, side, bar_n
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "symbols": symbol_list,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings().all()
    return [dict(row) for row in query_rows], [dict(row) for row in replay_rows]


def _load_cached_decision_dates(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    symbols: Sequence[str],
    start_date: str,
    end_date: str,
) -> list[str]:
    symbol_list = [str(symbol) for symbol in symbols if symbol]
    with session_factory() as session:
        rows = session.execute(
            text(
                """
                SELECT DISTINCT decision_date
                  FROM bt_result.calibration_query_feature_row
                 WHERE bundle_run_id = :bundle_run_id
                   AND symbol = ANY(:symbols)
                   AND decision_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                 ORDER BY decision_date
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "symbols": symbol_list,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).all()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def materialize_calibration_chunk(
    *,
    write_session_factory: sessionmaker[Session],
    bundle_run_id: int,
    chunk_id: int,
    market: str,
    scenario_id: str,
    start_date: str,
    end_date: str,
    symbols: Sequence[str],
    strategy_mode: str = "research_similarity_v2",
    policy_scope: str = "directional_wide_only",
    research_spec: ResearchExperimentSpec | None = None,
    metadata: Mapping[str, Any] | None = None,
    chunk_output_dir: str = "",
) -> dict[str, Any]:
    del market, strategy_mode, research_spec, metadata, chunk_output_dir
    started = time.perf_counter()
    symbols = [str(symbol) for symbol in symbols if symbol]
    with write_session_factory() as session:
        session.execute(
            text(
                """
                INSERT INTO bt_result.calibration_chunk_run(
                    bundle_run_id, chunk_id, status, symbols_json, symbol_count, started_at
                )
                VALUES (
                    :bundle_run_id, :chunk_id, 'running', :symbols_json, :symbol_count, NOW()
                )
                ON CONFLICT (bundle_run_id, chunk_id) DO UPDATE
                    SET status = 'running',
                        symbols_json = EXCLUDED.symbols_json,
                        symbol_count = EXCLUDED.symbol_count,
                        started_at = NOW(),
                        finished_at = NULL,
                        elapsed_ms = NULL,
                        soft_timeout_exceeded = FALSE,
                        last_error = NULL,
                        load_ms = NULL,
                        query_feature_ms = NULL,
                        snapshot_load_ms = NULL,
                        snapshot_core_load_ms = NULL,
                        query_parse_ms = NULL,
                        query_transform_ms = NULL,
                        prototype_score_ms = NULL,
                        member_lazy_load_ms = NULL,
                        query_block_count = 0,
                        score_ms = NULL,
                        db_write_ms = NULL,
                        decision_date_count = 0,
                        query_row_count = 0,
                        seed_row_count = 0,
                        replay_bar_count = 0,
                        raw_event_row_count = 0,
                        prototype_count = 0,
                        forbidden_call_violation = FALSE,
                        forbidden_call_name = NULL
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "chunk_id": chunk_id,
                "symbols_json": _json_dumps(symbols),
                "symbol_count": len(symbols),
            },
        )
        session.commit()
    load_started = time.perf_counter()
    query_rows, replay_rows = _load_cached_query_rows(
        session_factory=write_session_factory,
        bundle_run_id=bundle_run_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    snapshot_rows = _snapshot_metadata_rows(session_factory=write_session_factory, bundle_run_id=bundle_run_id)
    load_ms = int((time.perf_counter() - load_started) * 1000)
    if not snapshot_rows:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        with write_session_factory() as session:
            session.execute(
                text(
                    """
                    UPDATE bt_result.calibration_chunk_run
                       SET status = 'failed',
                           finished_at = NOW(),
                           elapsed_ms = :elapsed_ms,
                           load_ms = :load_ms,
                           query_row_count = :query_row_count,
                           replay_bar_count = :replay_bar_count,
                           last_error = :last_error
                     WHERE bundle_run_id = :bundle_run_id
                       AND chunk_id = :chunk_id
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "chunk_id": chunk_id,
                    "elapsed_ms": elapsed_ms,
                    "load_ms": load_ms,
                    "query_row_count": len(query_rows),
                    "replay_bar_count": len(replay_rows),
                    "last_error": "missing_train_snapshot_cache",
                },
            )
            session.commit()
        raise RuntimeError("build-calibration-chunk requires pre-materialized train snapshots")
    if not query_rows:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        with write_session_factory() as session:
            session.execute(
                text(
                    """
                    UPDATE bt_result.calibration_chunk_run
                       SET status = 'ok',
                           finished_at = NOW(),
                           elapsed_ms = :elapsed_ms,
                               load_ms = :load_ms,
                               query_feature_ms = 0,
                               snapshot_load_ms = 0,
                               snapshot_core_load_ms = 0,
                               query_parse_ms = 0,
                               query_transform_ms = 0,
                               prototype_score_ms = 0,
                               member_lazy_load_ms = 0,
                               query_block_count = 0,
                               score_ms = 0,
                               db_write_ms = 0,
                               decision_date_count = 0,
                           query_row_count = :query_row_count,
                           seed_row_count = 0,
                           replay_bar_count = :replay_bar_count,
                           raw_event_row_count = 0,
                           prototype_count = 0
                     WHERE bundle_run_id = :bundle_run_id
                       AND chunk_id = :chunk_id
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "chunk_id": chunk_id,
                    "elapsed_ms": elapsed_ms,
                    "load_ms": load_ms,
                    "query_row_count": len(query_rows),
                    "replay_bar_count": len(replay_rows),
                },
            )
            session.commit()
        return {
            "status": "ok",
            "bundle_run_id": bundle_run_id,
            "chunk_id": chunk_id,
            "symbol_count": len(symbols),
            "seed_row_count": 0,
            "replay_bar_count": len(replay_rows),
            "decision_date_count": 0,
        }
    try:
        with _forbidden_bundle_calls_guard():
            query_feature_started = time.perf_counter()
            _query_feature_rows_by_key(query_rows)
            query_feature_ms = int((time.perf_counter() - query_feature_started) * 1000)
            score_started = time.perf_counter()
            seed_payloads_result = build_calibration_seed_payloads_from_cache(
                query_rows=query_rows,
                replay_rows=replay_rows,
                snapshot_rows=snapshot_rows,
                scenario_id=scenario_id,
                policy_scope=policy_scope,
            )
            snapshot_load_ms = _to_int((seed_payloads_result.get("telemetry") or {}).get("snapshot_load_ms"))
            score_ms = int((time.perf_counter() - score_started) * 1000)
            seed_payloads = list(seed_payloads_result["seed_payloads"])
            db_write_started = time.perf_counter()
            with write_session_factory() as session:
                session.execute(
                    text(
                        """
                        DELETE FROM bt_result.calibration_seed_row
                         WHERE bundle_run_id = :bundle_run_id
                           AND symbol = ANY(:symbols)
                           AND decision_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date)
                        """
                    ),
                    {
                        "bundle_run_id": bundle_run_id,
                        "symbols": symbols,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                )
                if seed_payloads:
                    session.execute(
                        text(
                            """
                            INSERT INTO bt_result.calibration_seed_row(
                                bundle_run_id, decision_date, symbol, side, market, policy_family, pattern_key,
                                lower_bound, q10_return, q50_return, q90_return, interval_width, uncertainty,
                                member_mixture_ess, member_top1_weight_share, member_pre_truncation_count,
                                forecast_selected, optuna_eligible, recurring_family, single_prototype_collapse,
                                regime_code, sector_code, member_consensus_signature,
                                q50_d2_return, q50_d3_return, p_resolved_by_d2, p_resolved_by_d3, t1_open
                            )
                            VALUES (
                                :bundle_run_id, CAST(:decision_date AS date), :symbol, :side, :market, :policy_family, :pattern_key,
                                :lower_bound, :q10_return, :q50_return, :q90_return, :interval_width, :uncertainty,
                                :member_mixture_ess, :member_top1_weight_share, :member_pre_truncation_count,
                                :forecast_selected, :optuna_eligible, :recurring_family, :single_prototype_collapse,
                                :regime_code, :sector_code, :member_consensus_signature,
                                :q50_d2_return, :q50_d3_return, :p_resolved_by_d2, :p_resolved_by_d3, :t1_open
                            )
                            """
                        ),
                        [{"bundle_run_id": bundle_run_id, **row} for row in seed_payloads],
                    )
                telemetry = dict(seed_payloads_result.get("telemetry") or {})
                db_write_ms = int((time.perf_counter() - db_write_started) * 1000)
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                session.execute(
                    text(
                        """
                        UPDATE bt_result.calibration_chunk_run
                           SET status = 'ok',
                               finished_at = NOW(),
                               elapsed_ms = :elapsed_ms,
                               load_ms = :load_ms,
                               query_feature_ms = :query_feature_ms,
                               snapshot_load_ms = :snapshot_load_ms,
                               snapshot_core_load_ms = :snapshot_core_load_ms,
                               query_parse_ms = :query_parse_ms,
                               query_transform_ms = :query_transform_ms,
                               prototype_score_ms = :prototype_score_ms,
                               member_lazy_load_ms = :member_lazy_load_ms,
                               query_block_count = :query_block_count,
                               score_ms = :score_ms,
                               db_write_ms = :db_write_ms,
                               decision_date_count = :decision_date_count,
                               query_row_count = :query_row_count,
                               seed_row_count = :seed_row_count,
                               replay_bar_count = :replay_bar_count,
                               raw_event_row_count = :raw_event_row_count,
                               prototype_count = :prototype_count
                         WHERE bundle_run_id = :bundle_run_id
                           AND chunk_id = :chunk_id
                        """
                    ),
                    {
                        "bundle_run_id": bundle_run_id,
                        "chunk_id": chunk_id,
                        "elapsed_ms": elapsed_ms,
                        "load_ms": load_ms,
                        "query_feature_ms": query_feature_ms,
                        "snapshot_load_ms": snapshot_load_ms,
                        "snapshot_core_load_ms": _to_int(telemetry.get("snapshot_core_load_ms")),
                        "query_parse_ms": _to_int(telemetry.get("query_parse_ms")),
                        "query_transform_ms": _to_int(telemetry.get("query_transform_ms")),
                        "prototype_score_ms": _to_int(telemetry.get("prototype_score_ms")),
                        "member_lazy_load_ms": _to_int(telemetry.get("member_lazy_load_ms")),
                        "query_block_count": _to_int(telemetry.get("query_block_count")),
                        "score_ms": score_ms,
                        "db_write_ms": db_write_ms,
                        "decision_date_count": len({str(row.get("decision_date") or "") for row in query_rows}),
                        "query_row_count": len(query_rows),
                        "seed_row_count": len(seed_payloads),
                        "replay_bar_count": len(replay_rows),
                        "raw_event_row_count": _to_int(telemetry.get("raw_event_row_count")),
                        "prototype_count": _to_int(telemetry.get("prototype_count")),
                    },
                )
                session.commit()
            return {
                "status": "ok",
                "bundle_run_id": bundle_run_id,
                "chunk_id": chunk_id,
                "symbol_count": len(symbols),
                "seed_row_count": len(seed_payloads),
                "replay_bar_count": len(replay_rows),
                "decision_date_count": len({str(row.get("decision_date") or "") for row in query_rows}),
                "query_row_count": len(query_rows),
            }
    except ForbiddenCalibrationBundleCall as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        with write_session_factory() as session:
            session.execute(
                text(
                    """
                    UPDATE bt_result.calibration_chunk_run
                       SET status = 'failed',
                           finished_at = NOW(),
                           elapsed_ms = :elapsed_ms,
                           last_error = :last_error,
                           forbidden_call_violation = TRUE,
                           forbidden_call_name = :forbidden_call_name
                     WHERE bundle_run_id = :bundle_run_id
                       AND chunk_id = :chunk_id
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "chunk_id": chunk_id,
                    "elapsed_ms": elapsed_ms,
                    "last_error": str(exc),
                    "forbidden_call_name": exc.call_name,
                },
            )
            session.commit()
        raise
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        with write_session_factory() as session:
            session.execute(
                text(
                    """
                    UPDATE bt_result.calibration_chunk_run
                       SET status = 'failed',
                           finished_at = NOW(),
                           elapsed_ms = :elapsed_ms,
                           last_error = :last_error
                     WHERE bundle_run_id = :bundle_run_id
                       AND chunk_id = :chunk_id
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "chunk_id": chunk_id,
                    "elapsed_ms": elapsed_ms,
                    "last_error": str(exc),
                },
            )
            session.commit()
        raise


def export_materialized_bundle_artifacts(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    output_dir: str,
    policy_scope: str,
) -> dict[str, Any]:
    bundle = resolve_bundle_run(session_factory=session_factory, bundle_run_id=bundle_run_id)
    seed_rows = load_materialized_seed_rows(session_factory=session_factory, bundle_run_id=bundle_run_id, policy_scope=policy_scope)
    source_chunks = list_chunk_runs(session_factory=session_factory, bundle_run_id=bundle_run_id)
    artifacts = write_calibration_bundle_artifacts(
        output_dir=Path(output_dir),
        seed_rows=seed_rows,
        source_chunks=source_chunks,
        policy_scope=policy_scope,
        proof_reference_run=str(bundle.get("proof_reference_run") or ""),
        universe_symbol_count=int(bundle.get("universe_symbol_count") or 0),
    )
    coverage = dict(artifacts.get("coverage_summary") or {})
    failed_chunk_count = int(coverage.get("failed_chunk_count") or 0)
    status = "ok" if seed_rows and failed_chunk_count == 0 else ("partial" if seed_rows else "failed")
    with session_factory() as session:
        session.execute(
            text(
                """
                UPDATE bt_result.calibration_bundle_run
                   SET status = :status,
                       buy_candidate_count = :buy_candidate_count,
                       sell_replay_row_count = :sell_replay_row_count,
                       source_chunk_count = :source_chunk_count,
                       failed_chunk_count = :failed_chunk_count,
                       finished_at = NOW()
                 WHERE id = :bundle_run_id
                """
            ),
            {
                "bundle_run_id": bundle_run_id,
                "status": status,
                "buy_candidate_count": int(coverage.get("buy_candidate_count") or 0),
                "sell_replay_row_count": int(coverage.get("sell_replay_row_count") or 0),
                "source_chunk_count": int(coverage.get("source_chunk_count") or 0),
                "failed_chunk_count": failed_chunk_count,
            },
        )
        session.commit()
    refreshed_bundle = resolve_bundle_run(session_factory=session_factory, bundle_run_id=bundle_run_id)
    return {"status": status, "bundle_run": refreshed_bundle, **artifacts}


def build_study_cache_from_materialized_bundle(
    *,
    session_factory: sessionmaker[Session],
    output_dir: str,
    policy_scope: str,
    seed_profile: str = CALIBRATION_UNIVERSE_SEED_PROFILE,
    bundle_run_id: int | None = None,
    bundle_key: str = "",
) -> dict[str, Any]:
    bundle = resolve_bundle_run(session_factory=session_factory, bundle_run_id=bundle_run_id, bundle_key=bundle_key)
    seed_rows = load_materialized_seed_rows(session_factory=session_factory, bundle_run_id=int(bundle["id"]), policy_scope=policy_scope)
    source_summary = {
        "proof_reference_run": str(bundle.get("proof_reference_run") or ""),
        "source_chunk_count": int(bundle.get("source_chunk_count") or 0),
        "failed_chunk_count": int(bundle.get("failed_chunk_count") or 0),
        "universe_symbol_count": int(bundle.get("universe_symbol_count") or 0),
        "bundle_key": str(bundle.get("bundle_key") or ""),
        "snapshot_cadence": str(bundle.get("snapshot_cadence") or "daily"),
        "model_version": str(bundle.get("model_version") or DAILY_REUSE_MODEL_VERSION),
    }
    return write_study_cache_from_rows(
        seed_rows=seed_rows,
        output_dir=output_dir,
        policy_scope=policy_scope,
        seed_profile=seed_profile,
        source_seed_root=f"db://bundle/{bundle.get('id')}",
        source_seed_summary=source_summary,
    )


def derive_chunk_timeouts(
    *,
    session_factory: sessionmaker[Session],
    bundle_run_id: int,
    fallback_soft_timeout_seconds: int,
    fallback_hard_timeout_seconds: int,
) -> dict[str, int]:
    rows = [
        row
        for row in list_chunk_runs(session_factory=session_factory, bundle_run_id=bundle_run_id)
        if str(row.get("status") or "") in {"ok", "reused"} and _to_int(row.get("elapsed_ms")) > 0
    ]
    if len(rows) < 8:
        return {
            "soft_timeout_seconds": int(fallback_soft_timeout_seconds),
            "hard_timeout_seconds": int(fallback_hard_timeout_seconds),
        }
    elapsed_seconds = sorted(max(1, math.ceil(_to_int(row.get("elapsed_ms")) / 1000.0)) for row in rows)
    index = min(len(elapsed_seconds) - 1, max(0, math.ceil(0.95 * len(elapsed_seconds)) - 1))
    p95_seconds = elapsed_seconds[index]
    hard_timeout_seconds = max(10 * 60, int(math.ceil(1.5 * p95_seconds)))
    soft_timeout_seconds = int(math.ceil(0.75 * hard_timeout_seconds))
    return {
        "soft_timeout_seconds": soft_timeout_seconds,
        "hard_timeout_seconds": hard_timeout_seconds,
    }

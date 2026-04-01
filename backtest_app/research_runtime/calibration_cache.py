from __future__ import annotations

import json
import math
import time
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.db.local_session import create_backtest_session_factory
from backtest_app.historical_data.features import FeatureScaler, FeatureTransform
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.historical_data.models import HistoricalBar
from backtest_app.research.artifacts import JsonResearchArtifactStore
from backtest_app.research.models import StatePrototype
from backtest_app.research.pipeline import (
    _chosen_side_payload,
    _ev_config_from_metadata,
    _side_diag,
    build_decision_surface,
    build_event_memory_asof,
    build_query_embedding,
    fit_train_artifacts,
    generate_similarity_candidates_rolling,
)
from backtest_app.research.pre_optuna import build_pre_optuna_evidence
from backtest_app.research.scoring import CalibrationModel
from backtest_app.research.repository import ExactCosineCandidateIndex
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
DAILY_REUSE_MODEL_VERSION = "daily_reuse_v1"
MONTHLY_SNAPSHOT_MODEL_VERSION = "monthly_snapshot_v1"
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
        "max_train_date": train_artifact.get("max_train_date"),
        "max_outcome_end_date": train_artifact.get("max_outcome_end_date"),
        "event_record_count": _to_int(train_artifact.get("event_record_count")),
        "prototype_count": _to_int(train_artifact.get("prototype_count"), len(list(train_artifact.get("prototypes") or []))),
        "prototypes": list(train_artifact.get("prototypes") or []),
        "scaler": scaler_payload,
        "transform": transform_payload,
        "calibration": dict(train_artifact.get("calibration") or {}),
        "quote_policy_calibration": dict(train_artifact.get("quote_policy_calibration") or {}),
        "metadata": dict(train_artifact.get("metadata") or {}),
        "session_metadata_by_symbol": dict(train_artifact.get("session_metadata_by_symbol") or {}),
        "macro_series_history": list(train_artifact.get("macro_series_history") or []),
        "snapshot_ids": dict(train_artifact.get("snapshot_ids") or {}),
        "artifact_kind": "train_snapshot_v1",
    }


def _load_train_snapshot_payload(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return {
        **payload,
        "scaler": FeatureScaler.from_payload(payload.get("scaler") if isinstance(payload.get("scaler"), Mapping) else {}),
        "transform": FeatureTransform.from_payload(payload.get("transform") if isinstance(payload.get("transform"), Mapping) else {}),
        "prototypes": [StatePrototype(**prototype) for prototype in list(payload.get("prototypes") or [])],
    }


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
        embedding, meta = build_query_embedding(
            symbol=symbol,
            bars=query_window,
            bars_by_symbol={key: list(value) for key, value in bars_by_symbol.items()},
            macro_history={str(key): dict(value) for key, value in macro_history_by_date.items()},
            sector_map=dict(sector_map),
            cutoff_date=decision_date,
            spec=spec,
            scaler=None,
            transform=None,
            use_macro_level_in_similarity=use_macro_level_in_similarity,
            use_dollar_volume_absolute=use_dollar_volume_absolute,
            session_metadata_by_symbol=dict(session_metadata_by_symbol or {}),
            macro_series_history=[dict(row) for row in list(macro_series_history or [])],
        )
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
                "transformed_features_json": _json_dumps(dict(meta.get("transformed_features") or {})),
                "embedding_json": _json_dumps(list(embedding or [])),
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
) -> dict[str, Any]:
    query_rows: list[dict[str, Any]] = []
    replay_rows: list[dict[str, Any]] = []
    decision_dates: set[str] = set()
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


def _load_snapshot_cache(snapshot_rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    loaded: dict[str, dict[str, Any]] = {}
    for row in snapshot_rows:
        artifact_path = str(row.get("artifact_path") or "")
        if not artifact_path:
            continue
        loaded[str(row.get("snapshot_id") or "")] = _load_train_snapshot_payload(artifact_path)
    return loaded


def _select_snapshot_row(snapshot_rows: Sequence[Mapping[str, Any]], decision_date: str) -> dict[str, Any] | None:
    eligible = [dict(row) for row in snapshot_rows if str(row.get("snapshot_date") or "") <= decision_date]
    if not eligible:
        return None
    eligible.sort(key=lambda row: (str(row.get("snapshot_date") or ""), int(row.get("id") or 0)))
    return eligible[-1]


def _signal_panel_rows_from_cache(
    *,
    query_rows: Sequence[Mapping[str, Any]],
    snapshot_rows: Sequence[Mapping[str, Any]],
    snapshot_cache: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in query_rows:
        by_date[str(row.get("decision_date") or "")].append(_query_feature_row_dict(row))
    panel_rows: list[dict[str, Any]] = []
    raw_event_row_count = 0
    prototype_count = 0
    for decision_date in sorted(by_date):
        snapshot_row = _select_snapshot_row(snapshot_rows, decision_date)
        if not snapshot_row:
            continue
        snapshot_id = str(snapshot_row.get("snapshot_id") or "")
        snapshot_payload = dict(snapshot_cache.get(snapshot_id) or {})
        if not snapshot_payload:
            continue
        transform = snapshot_payload.get("transform")
        if not isinstance(transform, FeatureTransform):
            continue
        prototype_pool = list(snapshot_payload.get("prototypes") or [])
        if not prototype_pool:
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
        prototype_count += len(prototype_pool)
        for query_row in by_date[decision_date]:
            raw_features = dict(_json_loads(query_row.get("raw_features_json"), {}))
            transformed_features, embedding = transform.apply(raw_features)
            query_meta = dict(_json_loads(query_row.get("query_meta_json"), {}))
            regime_code = str(query_meta.get("regime_code") or query_row.get("regime_code") or "UNKNOWN")
            sector_code = str(query_meta.get("sector_code") or query_row.get("sector_code") or "UNKNOWN")
            surface = build_decision_surface(
                query_embedding=embedding,
                prototype_pool=prototype_pool,
                regime_code=regime_code,
                sector_code=sector_code,
                ev_config=ev_cfg,
                candidate_index=ExactCosineCandidateIndex(),
                calibration=calibration,
                query_date=decision_date,
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
    snapshot_cache = _load_snapshot_cache(snapshot_rows)
    panel_rows, telemetry = _signal_panel_rows_from_cache(
        query_rows=query_rows,
        snapshot_rows=snapshot_rows,
        snapshot_cache=snapshot_cache,
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
                           model_version = :model_version
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
                    worker_count, universe_symbol_count, snapshot_cadence, model_version, started_at
                )
                VALUES (
                    :bundle_key, :market, :strategy_mode, :policy_scope, :seed_profile,
                    :proof_reference_run, 'running', :start_date, :end_date, :chunk_size,
                    :worker_count, :universe_symbol_count, :snapshot_cadence, :model_version, NOW()
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


def list_snapshot_runs(*, session_factory: sessionmaker[Session], bundle_run_id: int) -> list[dict[str, Any]]:
    return _snapshot_metadata_rows(session_factory=session_factory, bundle_run_id=bundle_run_id)


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
) -> dict[str, Any]:
    spec = _spec_or_default(research_spec)
    loader = LocalPostgresLoader(create_backtest_session_factory())
    load_started = time.perf_counter()
    context = loader.load_research_context(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        research_spec=spec,
    )
    load_ms = int((time.perf_counter() - load_started) * 1000)
    query_started = time.perf_counter()
    cache_rows = build_query_feature_cache_rows(
        symbols=symbols,
        bars_by_symbol=context["bars_by_symbol"],
        macro_history_by_date=context["macro_history_by_date"],
        sector_map=context["sector_map"],
        session_metadata_by_symbol=context["session_metadata_by_symbol"],
        macro_series_history=context["macro_series_history"],
        spec=spec,
        start_date=start_date,
        end_date=end_date,
        metadata=metadata,
    )
    query_feature_ms = int((time.perf_counter() - query_started) * 1000)
    query_rows = list(cache_rows["query_rows"])
    replay_rows = list(cache_rows["replay_rows"])
    with write_session_factory() as session:
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
                "symbols": [str(symbol) for symbol in symbols],
                "start_date": start_date,
                "end_date": end_date,
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
                "symbols": [str(symbol) for symbol in symbols],
                "start_date": start_date,
                "end_date": end_date,
            },
        )
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
        session.commit()
    return {
        "status": "ok",
        "bundle_run_id": bundle_run_id,
        "market": market,
        "symbol_count": len(list(symbols)),
        "decision_date_count": int(cache_rows["decision_date_count"]),
        "query_row_count": len(query_rows),
        "replay_bar_count": len(replay_rows),
        "load_ms": load_ms,
        "query_feature_ms": query_feature_ms,
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
    context = loader.load_research_context(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        research_spec=spec,
    )
    snapshot_dates = _snapshot_dates(decision_dates, cadence)
    artifact_store = JsonResearchArtifactStore(output_dir)
    run_id = f"{bundle_key or f'bundle-{bundle_run_id}'}__snapshots"
    created = 0
    reused = 0
    for snapshot_date in snapshot_dates:
        snapshot_id = f"{run_id}:{snapshot_date}:{spec.spec_hash()}"
        snapshot_name = f"train_snapshot_{snapshot_date.replace('-', '')}"
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
                if Path(str(existing["artifact_path"])).exists():
                    reused += 1
                    continue
            session.execute(
                text(
                    """
                    INSERT INTO bt_result.calibration_snapshot_run(
                        bundle_run_id, snapshot_id, snapshot_date, train_start, train_end, spec_hash,
                        memory_version, model_version, snapshot_cadence, status, started_at
                    )
                    VALUES (
                        :bundle_run_id, :snapshot_id, CAST(:snapshot_date AS date), CAST(:train_start AS date), CAST(:train_end AS date), :spec_hash,
                        :memory_version, :model_version, :snapshot_cadence, 'running', NOW()
                    )
                    ON CONFLICT (bundle_run_id, snapshot_id) DO UPDATE
                        SET status = 'running',
                            started_at = NOW(),
                            finished_at = NULL,
                            last_error = NULL,
                            artifact_path = NULL
                    """
                ),
                {
                    "bundle_run_id": bundle_run_id,
                    "snapshot_id": snapshot_id,
                    "snapshot_date": snapshot_date,
                    "train_start": start_date,
                    "train_end": snapshot_date,
                    "spec_hash": spec.spec_hash(),
                    "memory_version": spec.memory_version,
                    "model_version": resolved_model_version,
                    "snapshot_cadence": cadence,
                },
            )
            session.commit()
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
            )
            snapshot_payload = _json_safe_snapshot_payload(train_artifact)
            artifact_path = artifact_store.save_train_snapshot(
                run_id=run_id,
                name=snapshot_name,
                as_of_date=snapshot_date,
                memory_version=spec.memory_version,
                payload=snapshot_payload,
            )
            with write_session_factory() as session:
                session.execute(
                    text(
                        """
                        UPDATE bt_result.calibration_snapshot_run
                           SET status = 'ok',
                               artifact_path = :artifact_path,
                               event_record_count = :event_record_count,
                               prototype_count = :prototype_count,
                               finished_at = NOW()
                         WHERE bundle_run_id = :bundle_run_id
                           AND snapshot_id = :snapshot_id
                        """
                    ),
                    {
                        "bundle_run_id": bundle_run_id,
                        "snapshot_id": snapshot_id,
                        "artifact_path": artifact_path,
                        "event_record_count": _to_int(snapshot_payload.get("event_record_count")),
                        "prototype_count": _to_int(snapshot_payload.get("prototype_count")),
                    },
                )
                session.commit()
            created += 1
        except Exception as exc:
            with write_session_factory() as session:
                session.execute(
                    text(
                        """
                        UPDATE bt_result.calibration_snapshot_run
                           SET status = 'failed',
                               finished_at = NOW(),
                               last_error = :last_error
                         WHERE bundle_run_id = :bundle_run_id
                           AND snapshot_id = :snapshot_id
                        """
                    ),
                    {
                        "bundle_run_id": bundle_run_id,
                        "snapshot_id": snapshot_id,
                        "last_error": str(exc),
                    },
                )
                session.commit()
            raise
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
            snapshot_load_started = time.perf_counter()
            _load_snapshot_cache(snapshot_rows)
            snapshot_load_ms = int((time.perf_counter() - snapshot_load_started) * 1000)
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

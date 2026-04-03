from __future__ import annotations

import hashlib
import json
import pickle
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median, pstdev
from time import monotonic
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import numpy as np
import pandas as pd

from .models import EventOutcomeRecord, PrototypeAnchor, ResearchAnchor, StatePrototype


@dataclass(frozen=True)
class PrototypeConfig:
    dedup_similarity_threshold: float = 0.985
    min_anchor_quality: float = 0.0
    min_support_count: int = 1
    min_decayed_support: float = 0.0
    max_age_days: int = 365
    recency_halflife_days: float = 90.0
    memory_version: str = "memory_asof_v1"


def _cos(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def _liq_bucket(value: float | None) -> str:
    x = float(value or 0.0)
    if x >= 0.8:
        return "HIGH"
    if x >= 0.4:
        return "MID"
    return "LOW"


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.fromisoformat(str(raw)[:10]).date()


def _decayed_support_from_dates(dates: List[date], halflife_days: float, as_of_date: str | None = None) -> float:
    if not dates:
        return 0.0
    ref = _parse_date(as_of_date) or max(dates)
    out = 0.0
    for d in dates:
        age = max(0, (ref - d).days)
        out += 0.5 ** (age / max(halflife_days, 1.0))
    return out


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(np.quantile(np.asarray(values, dtype=float), q))


def _distribution_stats(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "min": 0.0, "p50": 0.0, "p90": 0.0, "max": 0.0, "mean": 0.0}
    ordered = sorted(float(v) for v in values)
    return {
        "count": len(ordered),
        "min": float(ordered[0]),
        "p50": float(np.quantile(np.asarray(ordered, dtype=float), 0.50)),
        "p90": float(np.quantile(np.asarray(ordered, dtype=float), 0.90)),
        "max": float(ordered[-1]),
        "mean": float(sum(ordered) / len(ordered)),
    }


def _categorical_counts(values: Iterable[str | None]) -> dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _state_side_stats(members: list[dict], cfg: PrototypeConfig, *, as_of_date: str | None = None) -> dict:
    returns = [float(m.get("after_cost_return_pct", 0.0) or 0.0) for m in members]
    maes = [abs(float(m.get("mae_pct", 0.0) or 0.0)) for m in members]
    mfes = [float(m.get("mfe_pct", 0.0) or 0.0) for m in members]
    d2_returns = [float(m.get("close_return_d2_pct", 0.0) or 0.0) for m in members]
    d3_returns = [float(m.get("close_return_d3_pct", 0.0) or 0.0) for m in members]
    dates = [_parse_date(m.get("event_date")) for m in members if _parse_date(m.get("event_date")) is not None]
    support_count = len(members)
    dispersion = pstdev(returns) if len(returns) > 1 else 0.0

    def _count(member: dict, key: str, fallback: int) -> int:
        if key in member:
            return int(member.get(key, 0) or 0)
        return fallback

    target_first_count = sum(_count(m, "target_first_count", 1 if str(m.get("first_touch_label") or "").upper() == "UP_FIRST" else 0) for m in members)
    stop_first_count = sum(_count(m, "stop_first_count", 1 if str(m.get("first_touch_label") or "").upper() == "DOWN_FIRST" else 0) for m in members)
    flat_count = sum(_count(m, "flat_count", 1 if bool(m.get("flat")) or str(m.get("first_touch_label") or "").upper() == "FLAT" else 0) for m in members)
    ambiguous_count = sum(_count(m, "ambiguous_count", 1 if bool(m.get("ambiguous")) or str(m.get("first_touch_label") or "").upper() == "AMBIGUOUS" else 0) for m in members)
    no_trade_count = sum(_count(m, "no_trade_count", 1 if bool(m.get("no_trade")) or str(m.get("first_touch_label") or "").upper() == "NO_TRADE" else 0) for m in members)
    horizon_up_count = sum(_count(m, "horizon_up_count", 1 if str(m.get("first_touch_label") or "").upper() == "HORIZON_UP" else 0) for m in members)
    horizon_down_count = sum(_count(m, "horizon_down_count", 1 if str(m.get("first_touch_label") or "").upper() == "HORIZON_DOWN" else 0) for m in members)
    resolved_by_d2_count = sum(1 for m in members if bool(m.get("resolved_by_d2")))
    resolved_by_d3_count = sum(1 for m in members if bool(m.get("resolved_by_d3")))
    total_outcomes = max(target_first_count + stop_first_count + flat_count + ambiguous_count + no_trade_count, support_count, 1)
    return {
        "support_count": support_count,
        "decayed_support": _decayed_support_from_dates(dates, cfg.recency_halflife_days, as_of_date=as_of_date),
        "mean_return_pct": mean(returns) if returns else 0.0,
        "median_return_pct": median(returns) if returns else 0.0,
        "win_rate": sum(1 for r in returns if r > 0) / max(len(returns), 1),
        "mae_mean_pct": mean(maes) if maes else 0.0,
        "mfe_mean_pct": mean(mfes) if mfes else 0.0,
        "return_q10_pct": _quantile(returns, 0.10),
        "return_q50_pct": _quantile(returns, 0.50),
        "return_q90_pct": _quantile(returns, 0.90),
        "return_d2_q50_pct": _quantile(d2_returns, 0.50),
        "return_d3_q50_pct": _quantile(d3_returns, 0.50),
        "return_dispersion": dispersion,
        "uncertainty": dispersion / max(np.sqrt(support_count), 1.0),
        "freshness_days": float(((_parse_date(as_of_date) or date.today()) - max(dates)).days) if dates else 9999.0,
        "target_first_count": target_first_count,
        "stop_first_count": stop_first_count,
        "flat_count": flat_count,
        "ambiguous_count": ambiguous_count,
        "no_trade_count": no_trade_count,
        "horizon_up_count": horizon_up_count,
        "horizon_down_count": horizon_down_count,
        "p_target_first": target_first_count / total_outcomes,
        "p_stop_first": stop_first_count / total_outcomes,
        "p_flat": flat_count / total_outcomes,
        "p_ambiguous": ambiguous_count / total_outcomes,
        "p_no_trade": no_trade_count / total_outcomes,
        "p_resolved_by_d2": resolved_by_d2_count / max(support_count, 1),
        "p_resolved_by_d3": resolved_by_d3_count / max(support_count, 1),
    }


def build_prototype_compression_audit(
    *,
    event_records: Iterable[EventOutcomeRecord],
    prototypes: Iterable[StatePrototype],
    as_of_date: str,
) -> dict:
    event_rows = list(event_records or [])
    prototype_rows = list(prototypes or [])
    cluster_sizes = [int(p.member_count or 0) for p in prototype_rows]
    event_regime_counts = _categorical_counts((e.diagnostics or {}).get("regime_code") for e in event_rows)
    prototype_regime_counts = _categorical_counts(p.regime_code for p in prototype_rows)
    event_sector_counts = _categorical_counts((e.diagnostics or {}).get("sector_code") for e in event_rows)
    prototype_sector_counts = _categorical_counts(p.sector_code for p in prototype_rows)
    event_count = len(event_rows)
    prototype_count = len(prototype_rows)
    compression_ratio = float(event_count / prototype_count) if prototype_count > 0 else 0.0
    cluster_stats = _distribution_stats(cluster_sizes)
    return {
        "as_of_date": as_of_date,
        "event_record_count": event_count,
        "prototype_count": prototype_count,
        "compression_ratio": compression_ratio,
        "cluster_size_stats": cluster_stats,
        "event_regime_counts": event_regime_counts,
        "prototype_regime_counts": prototype_regime_counts,
        "event_sector_counts": event_sector_counts,
        "prototype_sector_counts": prototype_sector_counts,
        "table_row": {
            "as_of_date": as_of_date,
            "event_record_count": event_count,
            "prototype_count": prototype_count,
            "compression_ratio": compression_ratio,
            "cluster_size_min": cluster_stats["min"],
            "cluster_size_p50": cluster_stats["p50"],
            "cluster_size_p90": cluster_stats["p90"],
            "cluster_size_max": cluster_stats["max"],
            "cluster_size_mean": cluster_stats["mean"],
            "event_regime_counts": json.dumps(event_regime_counts, ensure_ascii=False, sort_keys=True),
            "prototype_regime_counts": json.dumps(prototype_regime_counts, ensure_ascii=False, sort_keys=True),
            "event_sector_counts": json.dumps(event_sector_counts, ensure_ascii=False, sort_keys=True),
            "prototype_sector_counts": json.dumps(prototype_sector_counts, ensure_ascii=False, sort_keys=True),
        },
    }


def aggregate_prototype_compression_batches(batches: Iterable[dict] | None) -> dict:
    rows = [dict(batch) for batch in list(batches or []) if isinstance(batch, dict)]
    if not rows:
        return {
            "batch_count": 0,
            "event_record_count_total": 0,
            "prototype_count_total": 0,
            "compression_ratio_mean": 0.0,
            "compression_ratio_max": 0.0,
            "cluster_size_stats": _distribution_stats([]),
            "event_regime_counts": {},
            "prototype_regime_counts": {},
            "event_sector_counts": {},
            "prototype_sector_counts": {},
            "table_rows": [],
        }
    event_regime_totals: Dict[str, int] = {}
    prototype_regime_totals: Dict[str, int] = {}
    event_sector_totals: Dict[str, int] = {}
    prototype_sector_totals: Dict[str, int] = {}
    cluster_size_values: list[float] = []
    for row in rows:
        cluster_size_values.extend(
            [
                float(row.get("cluster_size_stats", {}).get("min", 0.0) or 0.0),
                float(row.get("cluster_size_stats", {}).get("p50", 0.0) or 0.0),
                float(row.get("cluster_size_stats", {}).get("p90", 0.0) or 0.0),
                float(row.get("cluster_size_stats", {}).get("max", 0.0) or 0.0),
            ]
        )
        for source, target in (
            ("event_regime_counts", event_regime_totals),
            ("prototype_regime_counts", prototype_regime_totals),
            ("event_sector_counts", event_sector_totals),
            ("prototype_sector_counts", prototype_sector_totals),
        ):
            for key, value in dict(row.get(source) or {}).items():
                target[str(key)] = target.get(str(key), 0) + int(value or 0)
    return {
        "batch_count": len(rows),
        "event_record_count_total": sum(int(row.get("event_record_count") or 0) for row in rows),
        "prototype_count_total": sum(int(row.get("prototype_count") or 0) for row in rows),
        "compression_ratio_mean": float(sum(float(row.get("compression_ratio") or 0.0) for row in rows) / len(rows)),
        "compression_ratio_max": max(float(row.get("compression_ratio") or 0.0) for row in rows),
        "cluster_size_stats": _distribution_stats(cluster_size_values),
        "event_regime_counts": dict(sorted(event_regime_totals.items(), key=lambda item: (-item[1], item[0]))),
        "prototype_regime_counts": dict(sorted(prototype_regime_totals.items(), key=lambda item: (-item[1], item[0]))),
        "event_sector_counts": dict(sorted(event_sector_totals.items(), key=lambda item: (-item[1], item[0]))),
        "prototype_sector_counts": dict(sorted(prototype_sector_totals.items(), key=lambda item: (-item[1], item[0]))),
        "table_rows": [dict(row.get("table_row") or {}) for row in rows],
    }


def _representative_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:12]


def _prototype_progress(callback, payload: Mapping[str, Any]) -> None:
    if callback is None:
        return
    callback(dict(payload))


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_value(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    if isinstance(raw, (dict, list)):
        return raw
    return json.loads(str(raw))


def _normalized_matrix(embeddings: Sequence[Sequence[float]]) -> np.ndarray:
    matrix = np.asarray(list(embeddings), dtype=np.float64)
    if matrix.ndim != 2:
        raise ValueError("prototype embeddings must form a 2D matrix")
    norms = np.linalg.norm(matrix, axis=1)
    normalized = np.zeros_like(matrix)
    nonzero = norms > 0.0
    if np.any(nonzero):
        normalized[nonzero] = matrix[nonzero] / norms[nonzero, None]
    return normalized


def _prototype_row_from_event(event: EventOutcomeRecord) -> dict[str, Any]:
    embedding = list(((event.diagnostics or {}).get("embedding") or (event.path_summary or {}).get("embedding") or []))
    transformed_features = dict(
        ((event.diagnostics or {}).get("transformed_features") or (event.path_summary or {}).get("transformed_features") or {})
    )
    if transformed_features and not embedding:
        feature_keys = sorted(transformed_features.keys())
        embedding = [float(transformed_features[key]) for key in feature_keys]
    if not embedding:
        embedding = [0.0, 0.0]
    return {
        "event": event,
        "embedding": [float(value) for value in embedding],
        "regime_code": str(event.diagnostics.get("regime_code") or event.path_summary.get("regime_code") or "UNKNOWN"),
        "sector_code": str(event.diagnostics.get("sector_code") or event.path_summary.get("sector_code") or "UNKNOWN"),
        "liquidity_bucket": str(event.path_summary.get("liquidity_bucket") or "UNKNOWN"),
        "anchor_quality": float(event.diagnostics.get("quality_score", 1.0) or 1.0),
        "liquidity_score": float(event.diagnostics.get("liquidity_score", 0.0) or 0.0),
        "symbol": str(event.symbol),
        "event_date": str(event.event_date),
    }


def _serialized_event_record(event: EventOutcomeRecord) -> dict[str, Any]:
    return {
        "symbol": str(event.symbol),
        "event_date": str(event.event_date),
        "outcome_end_date": event.outcome_end_date,
        "schema_version": str(event.schema_version),
        "exchange_code": event.exchange_code,
        "country_code": event.country_code,
        "exchange_tz": event.exchange_tz,
        "session_date_local": event.session_date_local,
        "session_close_ts_local": event.session_close_ts_local,
        "session_close_ts_utc": event.session_close_ts_utc,
        "feature_anchor_ts_utc": event.feature_anchor_ts_utc,
        "macro_asof_ts_utc": event.macro_asof_ts_utc,
        "path_summary": dict(event.path_summary or {}),
        "side_outcomes": dict(event.side_outcomes or {}),
        "diagnostics": dict(event.diagnostics or {}),
    }


def _prototype_row_storage_row(row: Mapping[str, Any]) -> dict[str, Any]:
    event = row["event"]
    return {
        "symbol": str(row.get("symbol") or event.symbol),
        "event_date": str(row.get("event_date") or event.event_date),
        "anchor_quality": float(row.get("anchor_quality") or 0.0),
        "liquidity_score": float(row.get("liquidity_score") or 0.0),
        "regime_code": str(row.get("regime_code") or "UNKNOWN"),
        "sector_code": str(row.get("sector_code") or "UNKNOWN"),
        "liquidity_bucket": str(row.get("liquidity_bucket") or "UNKNOWN"),
        "embedding_json": _json_text(list(row.get("embedding") or [])),
        "event_json": _json_text(_serialized_event_record(event)),
    }


def _prototype_row_from_storage_row(row: Mapping[str, Any]) -> dict[str, Any]:
    event_payload = _json_value(row.get("event_json"), {})
    event = EventOutcomeRecord(**dict(event_payload))
    return {
        "event": event,
        "embedding": [float(value) for value in _json_value(row.get("embedding_json"), [])],
        "regime_code": str(row.get("regime_code") or "UNKNOWN"),
        "sector_code": str(row.get("sector_code") or "UNKNOWN"),
        "liquidity_bucket": str(row.get("liquidity_bucket") or "UNKNOWN"),
        "anchor_quality": float(row.get("anchor_quality") or 0.0),
        "liquidity_score": float(row.get("liquidity_score") or 0.0),
        "symbol": str(row.get("symbol") or event.symbol),
        "event_date": str(row.get("event_date") or event.event_date),
    }


def _prepare_prototype_rows(
    *,
    event_records: Iterable[EventOutcomeRecord],
    as_of_date: str,
    progress_callback=None,
) -> dict[str, Any]:
    eligible_events = [event for event in event_records if not event.outcome_end_date or event.outcome_end_date < as_of_date]
    total = len(eligible_events)
    rows: list[dict[str, Any]] = []
    if total == 0:
        _prototype_progress(
            progress_callback,
            {
                "phase": "prototype_prepare",
                "status": "ok",
                "prototype_rows_total": 0,
                "prototype_rows_done": 0,
                "cluster_count": 0,
                "current_symbol": "",
            },
        )
        return {"rows": [], "row_normed_matrix": np.zeros((0, 0), dtype=np.float64)}
    progress_every = max(1, min(500, total))
    last_progress_at = monotonic()
    _prototype_progress(
        progress_callback,
        {
            "phase": "prototype_prepare",
            "status": "running",
            "prototype_rows_total": total,
            "prototype_rows_done": 0,
            "cluster_count": 0,
            "current_symbol": "",
        },
    )
    for row_idx, event in enumerate(eligible_events, start=1):
        row = _prototype_row_from_event(event)
        rows.append(row)
        now = monotonic()
        if row_idx == total or row_idx == 1 or row_idx % progress_every == 0 or (now - last_progress_at) >= 10.0:
            _prototype_progress(
                progress_callback,
                {
                    "phase": "prototype_prepare",
                    "status": "running",
                    "prototype_rows_total": total,
                    "prototype_rows_done": row_idx,
                    "cluster_count": 0,
                    "current_symbol": row["symbol"],
                },
            )
            last_progress_at = now
    dims = {len(row["embedding"]) for row in rows}
    if len(dims) != 1:
        raise ValueError(f"prototype embeddings must share one dimension, got {sorted(dims)}")
    row_normed_matrix = _normalized_matrix([row["embedding"] for row in rows])
    _prototype_progress(
        progress_callback,
        {
            "phase": "prototype_prepare",
            "status": "ok",
            "prototype_rows_total": total,
            "prototype_rows_done": total,
            "cluster_count": 0,
            "current_symbol": "",
        },
    )
    return {"rows": rows, "row_normed_matrix": row_normed_matrix}


def _prototype_rows_dir(checkpoint_path: str | Path) -> Path:
    return Path(checkpoint_path).with_name("prototype_rows")


def _prototype_norms_path(checkpoint_path: str | Path) -> Path:
    return Path(checkpoint_path).with_name("prototype_norms.npy")


def _prototype_representative_norms_path(checkpoint_path: str | Path) -> Path:
    return Path(checkpoint_path).with_name("prototype_representatives.npy")


def _write_pickle_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("wb") as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
    tmp_path.replace(path)


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as handle:
        return pickle.load(handle)


def _write_numpy_atomic(path: Path, payload: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("wb") as handle:
        np.save(handle, np.asarray(payload, dtype=np.float64))
    tmp_path.replace(path)


def _checkpoint_identity(*, as_of_date: str, memory_version: str, spec_hash: str) -> dict[str, str]:
    return {
        "snapshot_date": str(as_of_date),
        "memory_version": str(memory_version),
        "spec_hash": str(spec_hash),
    }


def _write_prototype_resume_assets(
    *,
    checkpoint_path: str | Path,
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
    row_payloads: Sequence[Mapping[str, Any]],
    row_normed_matrix: np.ndarray,
) -> dict[str, Any]:
    rows_dir = _prototype_rows_dir(checkpoint_path)
    if rows_dir.exists():
        for child in rows_dir.rglob("*"):
            if child.is_file():
                child.unlink()
        for child in sorted(rows_dir.rglob("*"), reverse=True):
            if child.is_dir():
                child.rmdir()
    rows_dir.mkdir(parents=True, exist_ok=True)
    part_paths: list[str] = []
    part_size = 1000
    for part_index, offset in enumerate(range(0, len(row_payloads), part_size), start=1):
        chunk = row_payloads[offset : offset + part_size]
        frame = pd.DataFrame(_prototype_row_storage_row(row) for row in chunk)
        part_path = rows_dir / f"part-{part_index:03d}.parquet"
        frame.to_parquet(part_path, index=False)
        part_paths.append(str(part_path))
    norms_path = _prototype_norms_path(checkpoint_path)
    _write_numpy_atomic(norms_path, np.asarray(row_normed_matrix, dtype=np.float64))
    return {
        **_checkpoint_identity(as_of_date=as_of_date, memory_version=memory_version, spec_hash=spec_hash),
        "prototype_rows_total": len(row_payloads),
        "row_parts": part_paths,
        "row_normed_matrix_path": str(norms_path),
    }


def _load_prototype_rows(row_parts: Sequence[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for part_path in row_parts:
        frame = pd.read_parquet(str(part_path))
        rows.extend(_prototype_row_from_storage_row(row) for row in frame.to_dict(orient="records"))
    return rows


def _load_prototype_resume_state(
    *,
    checkpoint_path: str | Path,
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
) -> dict[str, Any] | None:
    checkpoint_file = Path(checkpoint_path)
    if not checkpoint_file.exists():
        return None
    identity = _checkpoint_identity(as_of_date=as_of_date, memory_version=memory_version, spec_hash=spec_hash)
    checkpoint_payload = _load_pickle(checkpoint_file)
    for key, expected in identity.items():
        if str(checkpoint_payload.get(key) or "") != expected:
            return None
    row_parts = [str(path) for path in list(checkpoint_payload.get("row_parts") or [])]
    row_normed_matrix_path = Path(str(checkpoint_payload.get("row_normed_matrix_path") or ""))
    representative_normed_matrix_path = Path(str(checkpoint_payload.get("representative_normed_matrix_path") or ""))
    if not row_parts or not row_normed_matrix_path.exists() or not representative_normed_matrix_path.exists():
        return None
    prototype_rows_total = int(checkpoint_payload.get("prototype_rows_total") or 0)
    representative_indices = [int(value) for value in list(checkpoint_payload.get("representative_indices") or [])]
    cluster_member_indices = [[int(member) for member in members] for members in list(checkpoint_payload.get("cluster_member_indices") or [])]
    row_normed_matrix = np.load(row_normed_matrix_path, mmap_mode="r")
    representative_normed_matrix = np.load(representative_normed_matrix_path)
    row_payloads = _load_prototype_rows(row_parts)
    total = prototype_rows_total
    if len(row_payloads) != total or row_normed_matrix.shape[0] != total:
        return None
    if len(representative_indices) != len(cluster_member_indices):
        return None
    if representative_normed_matrix.shape[0] != len(representative_indices):
        return None
    next_event_index = int(checkpoint_payload.get("next_event_index") or 0)
    if next_event_index < 0 or next_event_index > total:
        return None
    return {
        "next_event_index": next_event_index,
        "prototype_rows_total": total,
        "row_payloads": row_payloads,
        "row_normed_matrix": row_normed_matrix,
        "row_parts": row_parts,
        "row_normed_matrix_path": str(row_normed_matrix_path),
        "cluster_member_indices": cluster_member_indices,
        "representative_indices": representative_indices,
        "representative_normed_matrix": representative_normed_matrix,
        "representative_normed_matrix_path": str(representative_normed_matrix_path),
    }


def _write_prototype_checkpoint(
    *,
    checkpoint_path: str | Path,
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
    next_event_index: int,
    prototype_rows_total: int,
    row_parts: Sequence[str],
    row_normed_matrix_path: str,
    cluster_member_indices: Sequence[Sequence[int]],
    representative_indices: Sequence[int],
    representative_normed_matrix: np.ndarray,
) -> None:
    representative_normed_matrix_path = _prototype_representative_norms_path(checkpoint_path)
    _write_numpy_atomic(representative_normed_matrix_path, np.asarray(representative_normed_matrix, dtype=np.float64))
    _write_pickle_atomic(
        Path(checkpoint_path),
        {
            **_checkpoint_identity(as_of_date=as_of_date, memory_version=memory_version, spec_hash=spec_hash),
            "next_event_index": int(next_event_index),
            "prototype_rows_total": int(prototype_rows_total),
            "row_parts": [str(path) for path in row_parts],
            "row_normed_matrix_path": str(row_normed_matrix_path),
            "cluster_member_indices": [list(map(int, members)) for members in cluster_member_indices],
            "representative_indices": [int(value) for value in representative_indices],
            "representative_normed_matrix_path": str(representative_normed_matrix_path),
        },
    )


def _build_state_prototypes_from_event_memory_legacy(
    *,
    event_records: Iterable[EventOutcomeRecord],
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
    config: PrototypeConfig | None = None,
) -> list[StatePrototype]:
    cfg = config or PrototypeConfig(memory_version=memory_version)
    rows = [_prototype_row_from_event(event) for event in event_records if not event.outcome_end_date or event.outcome_end_date < as_of_date]
    cluster_member_indices: list[list[int]] = []
    legacy_clusters: list[list[dict[str, Any]]] = []
    row_order: list[dict[str, Any]] = []
    for row in rows:
        vec = np.asarray(row["embedding"], dtype=float)
        matched = False
        for cluster_index, cluster in enumerate(legacy_clusters):
            rep = np.asarray(cluster[0]["embedding"], dtype=float)
            if _cos(vec, rep) >= cfg.dedup_similarity_threshold:
                cluster.append(row)
                row_order.append(row)
                cluster_member_indices[cluster_index].append(len(row_order) - 1)
                matched = True
                break
        if not matched:
            legacy_clusters.append([row])
            row_order.append(row)
            cluster_member_indices.append([len(row_order) - 1])
    return _finalize_state_prototypes(
        rows=row_order,
        cluster_member_indices=cluster_member_indices,
        as_of_date=as_of_date,
        memory_version=memory_version,
        spec_hash=spec_hash,
        config=cfg,
    )


def _cluster_prototype_rows_greedy(
    *,
    rows: Sequence[Mapping[str, Any]],
    row_normed_matrix: np.ndarray,
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
    config: PrototypeConfig,
    progress_callback=None,
    checkpoint_path: str | None = None,
    resume_from_checkpoint: bool = False,
    comparison_block_size: int = 2048,
) -> dict[str, Any]:
    total = len(rows)
    if total == 0:
        _prototype_progress(
            progress_callback,
            {
                "phase": "prototype_cluster",
                "status": "ok",
                "prototype_rows_total": 0,
                "prototype_rows_done": 0,
                "cluster_count": 0,
                "current_symbol": "",
            },
        )
        return {
            "cluster_member_indices": [],
            "representative_indices": [],
            "prototype_rows_total": 0,
            "prototype_rows_done": 0,
            "cluster_count": 0,
        }

    resumed_state = None
    input_assets = None
    if checkpoint_path:
        if not resume_from_checkpoint:
            input_assets = _write_prototype_resume_assets(
                checkpoint_path=checkpoint_path,
                as_of_date=as_of_date,
                memory_version=memory_version,
                spec_hash=spec_hash,
                row_payloads=rows,
                row_normed_matrix=row_normed_matrix,
            )
        else:
            resumed_state = _load_prototype_resume_state(
                checkpoint_path=checkpoint_path,
                as_of_date=as_of_date,
                memory_version=memory_version,
                spec_hash=spec_hash,
            )
    dim = int(row_normed_matrix.shape[1])
    if resumed_state is None:
        representative_normed_matrix = np.zeros((max(total, 1), dim), dtype=np.float64)
        representative_indices: list[int] = []
        cluster_member_indices: list[list[int]] = []
        next_event_index = 0
    else:
        representative_indices = list(resumed_state["representative_indices"])
        cluster_member_indices = [list(members) for members in list(resumed_state["cluster_member_indices"] or [])]
        next_event_index = int(resumed_state["next_event_index"])
        representative_normed_matrix = np.zeros((max(total, 1), dim), dtype=np.float64)
        loaded_matrix = np.asarray(resumed_state["representative_normed_matrix"], dtype=np.float64)
        if loaded_matrix.size:
            representative_normed_matrix[: loaded_matrix.shape[0], :] = loaded_matrix
        input_assets = {
            "row_parts": list(resumed_state["row_parts"]),
            "row_normed_matrix_path": str(resumed_state["row_normed_matrix_path"]),
        }

    block_size = max(1, int(comparison_block_size or 1))
    progress_every = max(1, min(1000, total))
    last_progress_at = monotonic()
    last_checkpoint_at = monotonic()
    last_checkpoint_row = next_event_index
    _prototype_progress(
        progress_callback,
        {
            "phase": "prototype_cluster",
            "status": "running",
            "prototype_rows_total": total,
            "prototype_rows_done": next_event_index,
            "cluster_count": len(representative_indices),
            "current_symbol": str(rows[next_event_index - 1]["symbol"]) if next_event_index > 0 else "",
            "checkpoint_path": str(checkpoint_path or ""),
        },
    )
    for row_index in range(next_event_index, total):
        current_symbol = str(rows[row_index]["symbol"])
        candidate_vector = row_normed_matrix[row_index]
        match_index: int | None = None
        representative_count = len(representative_indices)
        if representative_count:
            for block_start in range(0, representative_count, block_size):
                block_end = min(representative_count, block_start + block_size)
                similarities = representative_normed_matrix[block_start:block_end] @ candidate_vector
                matches = np.flatnonzero(similarities >= config.dedup_similarity_threshold)
                if matches.size:
                    match_index = block_start + int(matches[0])
                    break
        if match_index is None:
            match_index = len(cluster_member_indices)
            cluster_member_indices.append([row_index])
            representative_indices.append(row_index)
            representative_normed_matrix[match_index, :] = candidate_vector
        else:
            cluster_member_indices[match_index].append(row_index)
        done = row_index + 1
        now = monotonic()
        should_checkpoint = bool(
            checkpoint_path
            and (
                done == total
                or (done - last_checkpoint_row) >= 1000
                or (now - last_checkpoint_at) >= 30.0
            )
        )
        if should_checkpoint:
            _write_prototype_checkpoint(
                checkpoint_path=checkpoint_path,
                as_of_date=as_of_date,
                memory_version=memory_version,
                spec_hash=spec_hash,
                next_event_index=done,
                prototype_rows_total=total,
                row_parts=list((input_assets or {}).get("row_parts") or []),
                row_normed_matrix_path=str((input_assets or {}).get("row_normed_matrix_path") or ""),
                cluster_member_indices=cluster_member_indices,
                representative_indices=representative_indices,
                representative_normed_matrix=representative_normed_matrix[: len(representative_indices), :],
            )
            last_checkpoint_at = now
            last_checkpoint_row = done
        if done == total or done == 1 or done % progress_every == 0 or (now - last_progress_at) >= 10.0 or should_checkpoint:
            _prototype_progress(
                progress_callback,
                {
                    "phase": "prototype_cluster",
                    "status": "running",
                    "prototype_rows_total": total,
                    "prototype_rows_done": done,
                    "cluster_count": len(representative_indices),
                    "current_symbol": current_symbol,
                    "checkpoint_path": str(checkpoint_path or ""),
                    "last_checkpoint_at": datetime.utcnow().isoformat() if should_checkpoint else None,
                },
            )
            last_progress_at = now
    _prototype_progress(
        progress_callback,
        {
            "phase": "prototype_cluster",
            "status": "ok",
            "prototype_rows_total": total,
            "prototype_rows_done": total,
            "cluster_count": len(representative_indices),
            "current_symbol": "",
            "checkpoint_path": str(checkpoint_path or ""),
        },
    )
    return {
        "cluster_member_indices": cluster_member_indices,
        "representative_indices": representative_indices,
        "prototype_rows_total": total,
        "prototype_rows_done": total,
        "cluster_count": len(representative_indices),
    }


def _finalize_state_prototypes(
    *,
    rows: Sequence[Mapping[str, Any]],
    cluster_member_indices: Sequence[Sequence[int]],
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
    config: PrototypeConfig,
) -> list[StatePrototype]:
    prototypes: list[StatePrototype] = []
    for member_indices in cluster_member_indices:
        cluster = [dict(rows[index]) for index in member_indices]
        events = [row["event"] for row in cluster]
        cluster_regime_codes = [str(row.get("regime_code") or "UNKNOWN") for row in cluster]
        cluster_sector_codes = [str(row.get("sector_code") or "UNKNOWN") for row in cluster]
        cluster_liquidity_buckets = [str(row.get("liquidity_bucket") or "UNKNOWN") for row in cluster]
        member_dates = [_parse_date(event.event_date) for event in events if _parse_date(event.event_date) is not None]
        decayed_support = _decayed_support_from_dates(member_dates, config.recency_halflife_days, as_of_date=as_of_date)
        if len(cluster) < config.min_support_count or decayed_support < config.min_decayed_support:
            continue
        rep = max(cluster, key=lambda row: (row["anchor_quality"], row["liquidity_score"]))
        rep_event = rep["event"]
        rep_diagnostics = dict(rep_event.diagnostics or {})
        rep_path_summary = dict(rep_event.path_summary or {})
        rep_payload = {
            "symbol": rep_event.symbol,
            "event_date": rep_event.event_date,
            "embedding": [round(float(value), 8) for value in rep["embedding"]],
        }
        representative_hash = _representative_hash(rep_payload)
        prior_buckets = {
            "regime": sorted(set(cluster_regime_codes)),
            "sector": sorted(set(cluster_sector_codes)),
            "liquidity": sorted(set(cluster_liquidity_buckets)),
        }
        member_refs = [
            {
                "symbol": event.symbol,
                "event_date": event.event_date,
                "outcome_end_date": event.outcome_end_date,
            }
            for event in events
        ]
        lineage = [
            {
                "ref": f"{event.symbol}:{event.event_date}",
                "side_outcomes": dict(event.side_outcomes or {}),
                "raw_features": dict((event.diagnostics or {}).get("raw_features") or (event.path_summary or {}).get("raw_features") or {}),
                "transformed_features": dict((event.diagnostics or {}).get("transformed_features") or (event.path_summary or {}).get("transformed_features") or {}),
                "transform_version": (event.diagnostics or {}).get("transform_version") or (event.path_summary or {}).get("transform_version"),
            }
            for event in events
        ]
        side_stats = {
            side: _state_side_stats(
                [{**dict((event.side_outcomes or {}).get(side) or {}), "event_date": event.event_date} for event in events],
                config,
                as_of_date=as_of_date,
            )
            for side in ("BUY", "SELL")
        }
        prototype_id = f"{as_of_date}:{memory_version}:{representative_hash}"
        prototypes.append(
            StatePrototype(
                prototype_id=prototype_id,
                anchor_code="STATE_MEMORY_V1",
                embedding=list(rep["embedding"]),
                member_count=len(cluster),
                representative_symbol=rep_event.symbol,
                representative_date=rep_event.event_date,
                representative_hash=representative_hash,
                shape_vector=list((rep_diagnostics.get("shape_vector") or rep_path_summary.get("shape_vector") or [])),
                ctx_vector=list((rep_diagnostics.get("ctx_vector") or rep_path_summary.get("ctx_vector") or [])),
                vector_version=memory_version,
                feature_version=spec_hash,
                embedding_model="event-memory-state",
                vector_dim=len(rep["embedding"]),
                anchor_quality=float(mean([row["anchor_quality"] for row in cluster])),
                regime_code=rep["regime_code"],
                sector_code=rep["sector_code"],
                liquidity_score=float(mean([row["liquidity_score"] for row in cluster])),
                support_count=len(cluster),
                decayed_support=decayed_support,
                freshness_days=float(((_parse_date(as_of_date) or date.today()) - max(member_dates)).days) if member_dates else 9999.0,
                exchange_code=rep_event.exchange_code,
                country_code=rep_event.country_code,
                exchange_tz=rep_event.exchange_tz,
                session_date_local=rep_event.session_date_local,
                session_close_ts_utc=rep_event.session_close_ts_utc,
                feature_anchor_ts_utc=rep_event.feature_anchor_ts_utc,
                prototype_membership={"member_refs": member_refs, "lineage": lineage},
                side_stats=side_stats,
                metadata={
                    "as_of_date": as_of_date,
                    "memory_version": memory_version,
                    "spec_hash": spec_hash,
                    "representative_hash": representative_hash,
                    "prior_buckets": prior_buckets,
                    "raw_features": dict(rep_diagnostics.get("raw_features") or rep_path_summary.get("raw_features") or {}),
                    "transformed_features": dict(rep_diagnostics.get("transformed_features") or rep_path_summary.get("transformed_features") or {}),
                    "transform_version": rep_diagnostics.get("transform_version") or rep_path_summary.get("transform_version"),
                    "exchange_code": rep_event.exchange_code,
                    "country_code": rep_event.country_code,
                    "exchange_tz": rep_event.exchange_tz,
                    "session_date_local": rep_event.session_date_local,
                    "session_close_ts_utc": rep_event.session_close_ts_utc,
                    "feature_anchor_ts_utc": rep_event.feature_anchor_ts_utc,
                },
            )
        )
    prototypes.sort(key=lambda item: item.prototype_id)
    return prototypes


def build_state_prototypes_from_event_memory(
    *,
    event_records: Iterable[EventOutcomeRecord],
    as_of_date: str,
    memory_version: str,
    spec_hash: str,
    config: PrototypeConfig | None = None,
    progress_callback=None,
    checkpoint_path: str | None = None,
    resume_from_checkpoint: bool = False,
    comparison_block_size: int = 2048,
) -> list[StatePrototype]:
    cfg = config or PrototypeConfig(memory_version=memory_version)
    resumed_state = (
        _load_prototype_resume_state(
            checkpoint_path=checkpoint_path,
            as_of_date=as_of_date,
            memory_version=memory_version,
            spec_hash=spec_hash,
        )
        if checkpoint_path and resume_from_checkpoint
        else None
    )
    if resumed_state is None:
        prepared = _prepare_prototype_rows(
            event_records=event_records,
            as_of_date=as_of_date,
            progress_callback=progress_callback,
        )
        rows = list(prepared["rows"])
        if not rows:
            return []
        row_normed_matrix = np.asarray(prepared["row_normed_matrix"], dtype=np.float64)
    else:
        rows = list(resumed_state["row_payloads"])
        if not rows:
            return []
        row_normed_matrix = resumed_state["row_normed_matrix"]
    clustered = _cluster_prototype_rows_greedy(
        rows=rows,
        row_normed_matrix=row_normed_matrix,
        as_of_date=as_of_date,
        memory_version=memory_version,
        spec_hash=spec_hash,
        config=cfg,
        progress_callback=progress_callback,
        checkpoint_path=checkpoint_path,
        resume_from_checkpoint=resume_from_checkpoint,
        comparison_block_size=comparison_block_size,
    )
    return _finalize_state_prototypes(
        rows=rows,
        cluster_member_indices=list(clustered["cluster_member_indices"]),
        as_of_date=as_of_date,
        memory_version=memory_version,
        spec_hash=spec_hash,
        config=cfg,
    )


def build_anchor_prototypes(anchors: Iterable[ResearchAnchor], config: PrototypeConfig | None = None, *, as_of_date: str | None = None) -> List[PrototypeAnchor]:
    cfg = config or PrototypeConfig()
    out: List[PrototypeAnchor] = []
    for anchor in anchors:
        rep_hash = _representative_hash({"symbol": anchor.symbol, "reference_date": anchor.reference_date, "embedding": [round(float(x), 8) for x in anchor.embedding]})
        out.append(PrototypeAnchor(prototype_id=f"{as_of_date or anchor.reference_date}:{cfg.memory_version}:{anchor.side}:{rep_hash}", anchor_code=anchor.anchor_code, side=anchor.side, embedding=list(anchor.embedding), member_count=1, representative_symbol=anchor.symbol, representative_date=anchor.reference_date, shape_vector=list(anchor.shape_vector), ctx_vector=list(anchor.ctx_vector), vector_version=anchor.vector_version, feature_version=anchor.metadata.get("feature_version"), embedding_model=anchor.embedding_model, vector_dim=anchor.vector_dim, anchor_quality=anchor.anchor_quality, regime_code=anchor.regime_code, sector_code=anchor.sector_code, liquidity_score=anchor.liquidity_score, support_count=1, decayed_support=1.0, mean_return_pct=float(anchor.after_cost_return_pct or 0.0), median_return_pct=float(anchor.after_cost_return_pct or 0.0), win_rate=1.0 if float(anchor.after_cost_return_pct or 0.0) > 0 else 0.0, mae_mean_pct=abs(float(anchor.mae_pct or 0.0)), mfe_mean_pct=float(anchor.mfe_pct or 0.0), return_dispersion=0.0, uncertainty=0.0, freshness_days=0.0, liquidity_bucket=_liq_bucket(anchor.liquidity_score), regime_bucket=anchor.regime_code, sector_bucket=anchor.sector_code, exchange_code=anchor.exchange_code, country_code=anchor.country_code, exchange_tz=anchor.exchange_tz, session_date_local=anchor.session_date_local, session_close_ts_utc=anchor.session_close_ts_utc, feature_anchor_ts_utc=anchor.feature_anchor_ts_utc, prototype_membership=anchor.prototype_membership, metadata={"representative_hash": rep_hash, "legacy_wrapper": True}))
    return out


def build_prototype_snapshot_from_event_memory(*, event_records: Iterable[EventOutcomeRecord], as_of_date: str, memory_version: str, config: PrototypeConfig | None = None, spec_hash: str = "unknown") -> dict:
    prototypes = build_state_prototypes_from_event_memory(event_records=event_records, as_of_date=as_of_date, memory_version=memory_version, spec_hash=spec_hash, config=config)
    return {"as_of_date": as_of_date, "memory_version": memory_version, "spec_hash": spec_hash, "prototype_count": len(prototypes), "prototypes": [p.__dict__ for p in prototypes], "lineage": {p.prototype_id: p.prototype_membership.get("member_refs", []) for p in prototypes}}

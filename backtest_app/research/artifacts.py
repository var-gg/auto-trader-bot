from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PROTOTYPE_SNAPSHOT_FORMAT_V2 = "prototype_snapshot_v2"
PROTOTYPE_SNAPSHOT_FORMAT_V3 = "prototype_snapshot_v3"
PROTOTYPE_JSON_TEXT_FIELDS = (
    "embedding",
    "shape_vector",
    "ctx_vector",
    "prototype_membership",
    "side_stats",
    "metadata",
)
PROTOTYPE_CORE_JSON_TEXT_FIELDS = (
    "embedding",
    "shape_vector",
    "ctx_vector",
    "side_stats",
    "metadata",
)
PROTOTYPE_MEMBER_JSON_TEXT_FIELDS = (
    "side_outcomes",
    "raw_features",
    "transformed_features",
)


@dataclass(frozen=True)
class PrototypeSnapshotHandle:
    format_version: str
    manifest_path: str
    as_of_date: str
    memory_version: str
    spec_hash: str
    snapshot_id: str
    prototype_count: int
    core_path: str
    core_embeddings_path: str
    members_path: str
    member_embeddings_path: str

    def load_core_frame(self) -> pd.DataFrame:
        if not self.core_path or not Path(self.core_path).exists():
            return pd.DataFrame()
        return pd.read_parquet(self.core_path)

    def load_core_embeddings(self, *, mmap_mode: str = "r") -> np.ndarray:
        if not self.core_embeddings_path or not Path(self.core_embeddings_path).exists():
            return np.zeros((0, 0), dtype=np.float64)
        return np.load(self.core_embeddings_path, mmap_mode=mmap_mode)

    def load_member_frame(self, prototype_ids: Sequence[str] | None = None) -> pd.DataFrame:
        if not self.members_path or not Path(self.members_path).exists():
            return pd.DataFrame()
        if not prototype_ids:
            return pd.read_parquet(self.members_path)
        try:
            import duckdb
        except ImportError:
            frame = pd.read_parquet(self.members_path)
            wanted = {str(item) for item in prototype_ids}
            return frame[frame["prototype_id"].astype(str).isin(wanted)].reset_index(drop=True)
        con = duckdb.connect()
        try:
            placeholders = ", ".join(["?"] * len(prototype_ids))
            sql = f"SELECT * FROM read_parquet(?) WHERE prototype_id IN ({placeholders})"
            return con.execute(sql, [self.members_path, *[str(item) for item in prototype_ids]]).fetch_df()
        finally:
            con.close()

    def load_member_embeddings(self, *, mmap_mode: str = "r") -> np.ndarray:
        if not self.member_embeddings_path or not Path(self.member_embeddings_path).exists():
            return np.zeros((0, 0), dtype=np.float64)
        return np.load(self.member_embeddings_path, mmap_mode=mmap_mode)


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_value(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    if isinstance(raw, (list, dict)):
        return raw
    try:
        return json.loads(str(raw))
    except json.JSONDecodeError:
        return default


def _prototype_payload_row(raw: Any) -> dict[str, Any]:
    payload = dict(raw if isinstance(raw, Mapping) else getattr(raw, "__dict__", {}) or {})
    return {
        "prototype_id": str(payload.get("prototype_id") or ""),
        "anchor_code": str(payload.get("anchor_code") or ""),
        "member_count": int(payload.get("member_count") or 0),
        "representative_symbol": payload.get("representative_symbol"),
        "representative_date": payload.get("representative_date"),
        "representative_hash": payload.get("representative_hash"),
        "vector_version": payload.get("vector_version"),
        "feature_version": payload.get("feature_version"),
        "embedding_model": payload.get("embedding_model"),
        "vector_dim": int(payload.get("vector_dim") or 0),
        "anchor_quality": float(payload.get("anchor_quality") or 0.0),
        "regime_code": payload.get("regime_code"),
        "sector_code": payload.get("sector_code"),
        "liquidity_score": float(payload.get("liquidity_score") or 0.0),
        "support_count": int(payload.get("support_count") or 0),
        "decayed_support": float(payload.get("decayed_support") or 0.0),
        "freshness_days": float(payload.get("freshness_days") or 0.0),
        "exchange_code": payload.get("exchange_code"),
        "country_code": payload.get("country_code"),
        "exchange_tz": payload.get("exchange_tz"),
        "session_date_local": payload.get("session_date_local"),
        "session_close_ts_utc": payload.get("session_close_ts_utc"),
        "feature_anchor_ts_utc": payload.get("feature_anchor_ts_utc"),
        "embedding": _json_text(list(payload.get("embedding") or [])),
        "shape_vector": _json_text(list(payload.get("shape_vector") or [])),
        "ctx_vector": _json_text(list(payload.get("ctx_vector") or [])),
        "prototype_membership": _json_text(dict(payload.get("prototype_membership") or {})),
        "side_stats": _json_text(dict(payload.get("side_stats") or {})),
        "metadata": _json_text(dict(payload.get("metadata") or {})),
    }


def _prototype_payload_from_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(raw)
    for field_name in PROTOTYPE_JSON_TEXT_FIELDS:
        default = [] if field_name in {"embedding", "shape_vector", "ctx_vector"} else {}
        payload[field_name] = _json_value(payload.get(field_name), default)
    payload["member_count"] = int(payload.get("member_count") or 0)
    payload["vector_dim"] = int(payload.get("vector_dim") or 0)
    payload["support_count"] = int(payload.get("support_count") or 0)
    payload["anchor_quality"] = float(payload.get("anchor_quality") or 0.0)
    payload["liquidity_score"] = float(payload.get("liquidity_score") or 0.0)
    payload["decayed_support"] = float(payload.get("decayed_support") or 0.0)
    payload["freshness_days"] = float(payload.get("freshness_days") or 0.0)
    return payload


def _prototype_core_row(raw: Any, row_index: int) -> dict[str, Any]:
    payload = dict(raw if isinstance(raw, Mapping) else getattr(raw, "__dict__", {}) or {})
    return {
        "prototype_row_index": int(row_index),
        "prototype_id": str(payload.get("prototype_id") or ""),
        "anchor_code": str(payload.get("anchor_code") or ""),
        "member_count": int(payload.get("member_count") or 0),
        "representative_symbol": payload.get("representative_symbol"),
        "representative_date": payload.get("representative_date"),
        "representative_hash": payload.get("representative_hash"),
        "vector_version": payload.get("vector_version"),
        "feature_version": payload.get("feature_version"),
        "embedding_model": payload.get("embedding_model"),
        "vector_dim": int(payload.get("vector_dim") or 0),
        "anchor_quality": float(payload.get("anchor_quality") or 0.0),
        "regime_code": payload.get("regime_code"),
        "sector_code": payload.get("sector_code"),
        "liquidity_score": float(payload.get("liquidity_score") or 0.0),
        "support_count": int(payload.get("support_count") or 0),
        "decayed_support": float(payload.get("decayed_support") or 0.0),
        "freshness_days": float(payload.get("freshness_days") or 0.0),
        "exchange_code": payload.get("exchange_code"),
        "country_code": payload.get("country_code"),
        "exchange_tz": payload.get("exchange_tz"),
        "session_date_local": payload.get("session_date_local"),
        "session_close_ts_utc": payload.get("session_close_ts_utc"),
        "feature_anchor_ts_utc": payload.get("feature_anchor_ts_utc"),
        "embedding": _json_text(list(payload.get("embedding") or [])),
        "shape_vector": _json_text(list(payload.get("shape_vector") or [])),
        "ctx_vector": _json_text(list(payload.get("ctx_vector") or [])),
        "side_stats": _json_text(dict(payload.get("side_stats") or {})),
        "metadata": _json_text(dict(payload.get("metadata") or {})),
    }


def _prototype_core_payload_from_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(raw)
    for field_name in PROTOTYPE_CORE_JSON_TEXT_FIELDS:
        default = [] if field_name in {"embedding", "shape_vector", "ctx_vector"} else {}
        payload[field_name] = _json_value(payload.get(field_name), default)
    payload["prototype_row_index"] = int(payload.get("prototype_row_index") or 0)
    payload["member_count"] = int(payload.get("member_count") or 0)
    payload["vector_dim"] = int(payload.get("vector_dim") or 0)
    payload["support_count"] = int(payload.get("support_count") or 0)
    payload["anchor_quality"] = float(payload.get("anchor_quality") or 0.0)
    payload["liquidity_score"] = float(payload.get("liquidity_score") or 0.0)
    payload["decayed_support"] = float(payload.get("decayed_support") or 0.0)
    payload["freshness_days"] = float(payload.get("freshness_days") or 0.0)
    return payload


def _prototype_member_row(
    *,
    prototype_id: str,
    member_index: int,
    embedding_row_index: int,
    member_payload: Mapping[str, Any],
) -> dict[str, Any]:
    symbol = str(member_payload.get("symbol") or "")
    event_date = str(member_payload.get("event_date") or "")
    member_ref = str(member_payload.get("ref") or f"{symbol}:{event_date}")
    metadata = {
        key: value
        for key, value in dict(member_payload).items()
        if key
        not in {
            "ref",
            "symbol",
            "event_date",
            "outcome_end_date",
            "transform_version",
            "side_outcomes",
            "raw_features",
            "transformed_features",
        }
    }
    return {
        "prototype_id": str(prototype_id),
        "member_index": int(member_index),
        "embedding_row_index": int(embedding_row_index),
        "member_ref": member_ref,
        "symbol": symbol,
        "event_date": event_date,
        "outcome_end_date": member_payload.get("outcome_end_date"),
        "transform_version": member_payload.get("transform_version"),
        "side_outcomes": _json_text(dict(member_payload.get("side_outcomes") or {})),
        "raw_features": _json_text(dict(member_payload.get("raw_features") or {})),
        "transformed_features": _json_text(dict(member_payload.get("transformed_features") or {})),
        "metadata": _json_text(metadata),
    }


def _prototype_member_payload_from_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(raw)
    for field_name in PROTOTYPE_MEMBER_JSON_TEXT_FIELDS + ("metadata",):
        payload[field_name] = _json_value(payload.get(field_name), {})
    payload["member_index"] = int(payload.get("member_index") or 0)
    payload["embedding_row_index"] = int(payload.get("embedding_row_index") or 0)
    return payload


def _normalized_matrix(rows: Sequence[Sequence[float]]) -> np.ndarray:
    if not rows:
        return np.zeros((0, 0), dtype=np.float64)
    matrix = np.asarray(rows, dtype=np.float64)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms = np.where(norms <= 1e-12, 1.0, norms)
    return matrix / norms


def _write_numpy_atomic(path: Path, payload: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("wb") as handle:
        np.save(handle, np.asarray(payload, dtype=np.float64))
    tmp_path.replace(path)


def _remove_tree(path: Path) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        if child.is_file():
            child.unlink()
    for child in sorted(path.rglob("*"), reverse=True):
        if child.is_dir():
            child.rmdir()
    if path.exists():
        path.rmdir()


def _append_parquet_batches(path: Path, rows: Iterable[Mapping[str, Any]], *, batch_size: int = 1000) -> tuple[int, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    writer: pq.ParquetWriter | None = None
    buffered: list[dict[str, Any]] = []
    total_rows = 0
    try:
        for row in rows:
            buffered.append(dict(row))
            if len(buffered) < batch_size:
                continue
            frame = pd.DataFrame(buffered)
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema)
            writer.write_table(table)
            total_rows += len(buffered)
            buffered = []
        if buffered:
            frame = pd.DataFrame(buffered)
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema)
            writer.write_table(table)
            total_rows += len(buffered)
    finally:
        if writer is not None:
            writer.close()
    return total_rows, int(path.stat().st_size) if path.exists() else 0


def open_prototype_snapshot_handle(
    *,
    run_id: str = "",
    name: str = "prototype_snapshot",
    manifest_path: str = "",
    output_dir: str = "",
) -> PrototypeSnapshotHandle | None:
    manifest_text = str(manifest_path or "").strip()
    resolved_manifest = Path(manifest_text) if manifest_text else None
    if resolved_manifest is None:
        if not output_dir or not run_id:
            return None
        resolved_manifest = Path(output_dir) / run_id / name / "manifest.json"
    if not resolved_manifest.exists():
        return None
    manifest = json.loads(resolved_manifest.read_text(encoding="utf-8"))
    format_version = str(manifest.get("format_version") or "")
    if format_version not in {PROTOTYPE_SNAPSHOT_FORMAT_V2, PROTOTYPE_SNAPSHOT_FORMAT_V3}:
        return None
    snapshot_dir = resolved_manifest.parent
    if format_version == PROTOTYPE_SNAPSHOT_FORMAT_V3:
        return PrototypeSnapshotHandle(
            format_version=format_version,
            manifest_path=str(resolved_manifest),
            as_of_date=str(manifest.get("as_of_date") or ""),
            memory_version=str(manifest.get("memory_version") or ""),
            spec_hash=str(manifest.get("spec_hash") or ""),
            snapshot_id=str(manifest.get("snapshot_id") or ""),
            prototype_count=int(manifest.get("prototype_count") or 0),
            core_path=str(snapshot_dir / str(manifest.get("core_file") or "core.parquet")),
            core_embeddings_path=str(snapshot_dir / str(manifest.get("core_embeddings_file") or "core_embeddings.f64.npy")),
            members_path=str(snapshot_dir / str(manifest.get("members_file") or "members.parquet")),
            member_embeddings_path=str(snapshot_dir / str(manifest.get("member_embeddings_file") or "member_embeddings.f64.npy")),
        )
    return PrototypeSnapshotHandle(
        format_version=format_version,
        manifest_path=str(resolved_manifest),
        as_of_date=str(manifest.get("as_of_date") or ""),
        memory_version=str(manifest.get("memory_version") or ""),
        spec_hash=str(manifest.get("spec_hash") or ""),
        snapshot_id=str(manifest.get("snapshot_id") or ""),
        prototype_count=int(manifest.get("prototype_count") or 0),
        core_path="",
        core_embeddings_path="",
        members_path="",
        member_embeddings_path="",
    )


def load_prototype_subset(
    *,
    artifact_store: "JsonResearchArtifactStore",
    run_id: str = "",
    name: str = "prototype_snapshot",
    prototype_ids: Sequence[str],
    manifest_path: str = "",
) -> list[dict[str, Any]]:
    wanted = [str(item) for item in prototype_ids if item]
    if not wanted:
        return []
    handle = artifact_store.open_prototype_snapshot_handle(
        run_id=run_id,
        name=name,
        manifest_path=manifest_path,
    )
    if handle is None or handle.format_version != PROTOTYPE_SNAPSHOT_FORMAT_V3:
        payload = artifact_store.load_prototype_snapshot(run_id=run_id, name=name)
        prototypes = [dict(item) for item in list((payload or {}).get("prototypes") or [])]
        order = {prototype_id: index for index, prototype_id in enumerate(wanted)}
        return sorted(
            [item for item in prototypes if str(item.get("prototype_id") or "") in order],
            key=lambda item: order[str(item.get("prototype_id") or "")],
        )
    order = {prototype_id: index for index, prototype_id in enumerate(wanted)}
    core_frame = handle.load_core_frame()
    if core_frame.empty:
        return []
    core_rows = [
        _prototype_core_payload_from_row(row)
        for row in core_frame[core_frame["prototype_id"].astype(str).isin(set(wanted))].to_dict(orient="records")
    ]
    if not core_rows:
        return []
    member_frame = handle.load_member_frame(wanted)
    member_rows = [_prototype_member_payload_from_row(row) for row in member_frame.to_dict(orient="records")]
    grouped_members: dict[str, list[dict[str, Any]]] = {}
    for row in member_rows:
        grouped_members.setdefault(str(row.get("prototype_id") or ""), []).append(dict(row))
    out: list[dict[str, Any]] = []
    for core_row in sorted(core_rows, key=lambda item: order.get(str(item.get("prototype_id") or ""), 10**9)):
        prototype_id = str(core_row.get("prototype_id") or "")
        grouped = sorted(grouped_members.get(prototype_id, []), key=lambda item: int(item.get("member_index") or 0))
        lineage = []
        member_refs = []
        for member_row in grouped:
            symbol = str(member_row.get("symbol") or "")
            event_date = str(member_row.get("event_date") or "")
            outcome_end_date = member_row.get("outcome_end_date")
            lineage_payload = {
                "ref": str(member_row.get("member_ref") or f"{symbol}:{event_date}"),
                "symbol": symbol,
                "event_date": event_date,
                "outcome_end_date": outcome_end_date,
                "transform_version": member_row.get("transform_version"),
                "side_outcomes": dict(member_row.get("side_outcomes") or {}),
                "raw_features": dict(member_row.get("raw_features") or {}),
                "transformed_features": dict(member_row.get("transformed_features") or {}),
                **dict(member_row.get("metadata") or {}),
            }
            lineage.append(lineage_payload)
            member_refs.append(
                {
                    "symbol": symbol,
                    "event_date": event_date,
                    "outcome_end_date": outcome_end_date,
                }
            )
        payload = {
            "prototype_id": prototype_id,
            "anchor_code": str(core_row.get("anchor_code") or ""),
            "embedding": list(core_row.get("embedding") or []),
            "member_count": int(core_row.get("member_count") or len(grouped)),
            "representative_symbol": core_row.get("representative_symbol"),
            "representative_date": core_row.get("representative_date"),
            "representative_hash": core_row.get("representative_hash"),
            "shape_vector": list(core_row.get("shape_vector") or []),
            "ctx_vector": list(core_row.get("ctx_vector") or []),
            "vector_version": core_row.get("vector_version"),
            "feature_version": core_row.get("feature_version"),
            "embedding_model": core_row.get("embedding_model"),
            "vector_dim": int(core_row.get("vector_dim") or 0),
            "anchor_quality": float(core_row.get("anchor_quality") or 0.0),
            "regime_code": core_row.get("regime_code"),
            "sector_code": core_row.get("sector_code"),
            "liquidity_score": float(core_row.get("liquidity_score") or 0.0),
            "support_count": int(core_row.get("support_count") or 0),
            "decayed_support": float(core_row.get("decayed_support") or 0.0),
            "freshness_days": float(core_row.get("freshness_days") or 0.0),
            "exchange_code": core_row.get("exchange_code"),
            "country_code": core_row.get("country_code"),
            "exchange_tz": core_row.get("exchange_tz"),
            "session_date_local": core_row.get("session_date_local"),
            "session_close_ts_utc": core_row.get("session_close_ts_utc"),
            "feature_anchor_ts_utc": core_row.get("feature_anchor_ts_utc"),
            "prototype_membership": {"member_refs": member_refs, "lineage": lineage},
            "side_stats": dict(core_row.get("side_stats") or {}),
            "metadata": dict(core_row.get("metadata") or {}),
        }
        out.append(payload)
    return out


class JsonResearchArtifactStore:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def _dir(self, run_id: str) -> Path:
        out_dir = Path(self.output_dir) / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def _prototype_snapshot_dir(self, *, run_id: str, name: str) -> Path:
        return self._dir(run_id) / name

    def _prototype_snapshot_manifest(self, *, run_id: str, name: str) -> Path:
        return self._prototype_snapshot_dir(run_id=run_id, name=name) / "manifest.json"

    def save(self, *, run_id: str, name: str, payload: Mapping[str, Any]) -> str:
        path = self._dir(run_id) / f"{name}.json"
        path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(path)

    def save_snapshot(self, *, run_id: str, name: str, spec: Mapping[str, Any], as_of_date: str, coverage: Mapping[str, Any], excluded_reasons: list[dict], payload: Mapping[str, Any], format: str = "json") -> str:
        envelope = {"spec": dict(spec), "spec_hash": dict(spec).get("spec_hash"), "as_of_date": as_of_date, "coverage": dict(coverage), "excluded_reasons": list(excluded_reasons), "payload": dict(payload)}
        ext = "json" if format not in {"json", "parquet"} else format
        path = self._dir(run_id) / f"{name}.{ext}"
        path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(path)

    def load_snapshot(self, *, run_id: str, name: str, format: str = "json") -> dict | None:
        ext = "json" if format not in {"json", "parquet"} else format
        path = self._dir(run_id) / f"{name}.{ext}"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def save_prototype_snapshot(
        self,
        *,
        run_id: str,
        name: str = "prototype_snapshot",
        as_of_date: str,
        memory_version: str,
        payload: Mapping[str, Any],
        progress_callback=None,
    ) -> str:
        snapshot_dir = self._prototype_snapshot_dir(run_id=run_id, name=name)
        if snapshot_dir.exists():
            _remove_tree(snapshot_dir)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        prototypes = [
            dict(item if isinstance(item, Mapping) else getattr(item, "__dict__", {}) or {})
            for item in list(payload.get("prototypes") or [])
        ]
        prototype_count = len(prototypes)
        bytes_written = 0
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "artifact_write",
                    "status": "running",
                    "artifact_rows_total": prototype_count,
                    "artifact_rows_done": 0,
                    "artifact_part_count": 0,
                    "artifact_bytes_written": 0,
                }
            )
        core_embeddings = _normalized_matrix([list(item.get("embedding") or []) for item in prototypes])
        member_embedding_rows: list[list[float]] = []

        def _core_rows() -> Iterable[dict[str, Any]]:
            for row_index, prototype in enumerate(prototypes):
                yield _prototype_core_row(prototype, row_index)

        core_path = snapshot_dir / "core.parquet"
        core_rows_written, _ = _append_parquet_batches(core_path, _core_rows())
        bytes_written += int(core_path.stat().st_size) if core_path.exists() else 0
        core_embeddings_path = snapshot_dir / "core_embeddings.f64.npy"
        _write_numpy_atomic(core_embeddings_path, core_embeddings)
        bytes_written += int(core_embeddings_path.stat().st_size) if core_embeddings_path.exists() else 0
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "artifact_write",
                    "status": "running",
                    "artifact_rows_total": prototype_count,
                    "artifact_rows_done": min(core_rows_written, prototype_count),
                    "artifact_part_count": 2,
                    "artifact_bytes_written": bytes_written,
                }
            )
        member_row_count = 0

        def _member_rows() -> Iterable[dict[str, Any]]:
            nonlocal member_row_count
            embedding_row_index = 0
            for prototype in prototypes:
                lineage = list((prototype.get("prototype_membership") or {}).get("lineage") or [])
                for member_index, member in enumerate(lineage):
                    transformed = dict(member.get("transformed_features") or {})
                    member_embedding_rows.append([float(transformed[key]) for key in sorted(transformed.keys())])
                    member_row_count += 1
                    yield _prototype_member_row(
                        prototype_id=str(prototype.get("prototype_id") or ""),
                        member_index=member_index,
                        embedding_row_index=embedding_row_index,
                        member_payload=member,
                    )
                    embedding_row_index += 1

        members_path = snapshot_dir / "members.parquet"
        member_rows_written, _ = _append_parquet_batches(members_path, _member_rows())
        bytes_written += int(members_path.stat().st_size) if members_path.exists() else 0
        member_embeddings_path = snapshot_dir / "member_embeddings.f64.npy"
        _write_numpy_atomic(member_embeddings_path, _normalized_matrix(member_embedding_rows))
        bytes_written += int(member_embeddings_path.stat().st_size) if member_embeddings_path.exists() else 0
        manifest = {
            "format_version": PROTOTYPE_SNAPSHOT_FORMAT_V3,
            "schema_version": "state_prototype_v3",
            "as_of_date": str(as_of_date),
            "memory_version": str(memory_version),
            "spec_hash": str(payload.get("spec_hash") or ""),
            "snapshot_id": str(payload.get("snapshot_id") or ""),
            "prototype_count": int(payload.get("prototype_count") or prototype_count),
            "row_count": prototype_count,
            "member_row_count": int(member_rows_written),
            "core_file": "core.parquet",
            "core_embeddings_file": "core_embeddings.f64.npy",
            "members_file": "members.parquet",
            "member_embeddings_file": "member_embeddings.f64.npy",
        }
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        bytes_written += int(manifest_path.stat().st_size)
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "artifact_write",
                    "status": "running",
                    "artifact_rows_total": prototype_count,
                    "artifact_rows_done": prototype_count,
                    "artifact_part_count": 5,
                    "artifact_bytes_written": bytes_written,
                }
            )
        return str(manifest_path)

    def open_prototype_snapshot_handle(
        self,
        *,
        run_id: str = "",
        name: str = "prototype_snapshot",
        manifest_path: str = "",
    ) -> PrototypeSnapshotHandle | None:
        return open_prototype_snapshot_handle(
            run_id=run_id,
            name=name,
            manifest_path=manifest_path,
            output_dir=self.output_dir,
        )

    def load_prototype_snapshot(self, *, run_id: str, name: str = "prototype_snapshot") -> dict | None:
        handle = self.open_prototype_snapshot_handle(run_id=run_id, name=name)
        if handle is not None:
            if handle.format_version == PROTOTYPE_SNAPSHOT_FORMAT_V3:
                core_frame = handle.load_core_frame()
                prototype_ids = [str(item) for item in core_frame.get("prototype_id", pd.Series(dtype=str)).tolist()]
                prototypes = load_prototype_subset(
                    artifact_store=self,
                    run_id=run_id,
                    name=name,
                    prototype_ids=prototype_ids,
                    manifest_path=handle.manifest_path,
                )
                return {
                    "as_of_date": handle.as_of_date,
                    "memory_version": handle.memory_version,
                    "spec_hash": handle.spec_hash,
                    "snapshot_id": handle.snapshot_id,
                    "prototype_count": handle.prototype_count,
                    "prototype_snapshot_format": PROTOTYPE_SNAPSHOT_FORMAT_V3,
                    "prototype_snapshot_manifest_path": handle.manifest_path,
                    "prototypes": prototypes,
                }
            manifest = json.loads(Path(handle.manifest_path).read_text(encoding="utf-8"))
            snapshot_dir = Path(handle.manifest_path).parent
            prototypes: list[dict[str, Any]] = []
            for relative_part in list(manifest.get("part_files") or []):
                part_path = snapshot_dir / str(relative_part)
                if not part_path.exists():
                    raise FileNotFoundError(f"prototype snapshot part missing: {part_path}")
                frame = pd.read_parquet(part_path)
                prototypes.extend(_prototype_payload_from_row(row) for row in frame.to_dict(orient="records"))
            return {
                "as_of_date": str(manifest.get("as_of_date") or ""),
                "memory_version": str(manifest.get("memory_version") or ""),
                "spec_hash": str(manifest.get("spec_hash") or ""),
                "snapshot_id": str(manifest.get("snapshot_id") or ""),
                "prototype_count": int(manifest.get("prototype_count") or len(prototypes)),
                "prototype_snapshot_format": PROTOTYPE_SNAPSHOT_FORMAT_V2,
                "prototype_snapshot_manifest_path": str(handle.manifest_path),
                "prototypes": prototypes,
            }
        path = self._dir(run_id) / f"{name}.json"
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.setdefault("prototype_snapshot_format", "legacy_json")
        payload.setdefault("prototype_snapshot_manifest_path", "")
        return payload

    def save_train_snapshot(
        self,
        *,
        run_id: str,
        name: str = "train_snapshot",
        as_of_date: str,
        memory_version: str,
        payload: Mapping[str, Any],
    ) -> str:
        envelope = {"as_of_date": as_of_date, "memory_version": memory_version, **dict(payload)}
        path = self._dir(run_id) / f"{name}.json"
        path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(path)

    def load_train_snapshot(self, *, run_id: str, name: str = "train_snapshot") -> dict | None:
        path = self._dir(run_id) / f"{name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

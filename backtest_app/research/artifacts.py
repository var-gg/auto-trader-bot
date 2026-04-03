from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


PROTOTYPE_SNAPSHOT_FORMAT_V2 = "prototype_snapshot_v2"
PROTOTYPE_JSON_TEXT_FIELDS = (
    "embedding",
    "shape_vector",
    "ctx_vector",
    "prototype_membership",
    "side_stats",
    "metadata",
)


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
        parts_dir = snapshot_dir / "parts"
        if snapshot_dir.exists():
            for child in snapshot_dir.rglob("*"):
                if child.is_file():
                    child.unlink()
            for child in sorted(snapshot_dir.rglob("*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
        parts_dir.mkdir(parents=True, exist_ok=True)
        prototypes = list(payload.get("prototypes") or [])
        part_paths: list[str] = []
        part_size = 1000
        bytes_written = 0
        for part_index, offset in enumerate(range(0, len(prototypes), part_size), start=1):
            chunk = prototypes[offset : offset + part_size]
            frame = pd.DataFrame(_prototype_payload_row(item) for item in chunk)
            part_path = parts_dir / f"part-{part_index:03d}.parquet"
            frame.to_parquet(part_path, index=False)
            part_paths.append(str(part_path.relative_to(snapshot_dir)))
            bytes_written += int(part_path.stat().st_size)
            if progress_callback is not None:
                progress_callback(
                    {
                        "phase": "artifact_write",
                        "status": "running",
                        "artifact_rows_total": len(prototypes),
                        "artifact_rows_done": min(offset + len(chunk), len(prototypes)),
                        "artifact_part_count": part_index,
                        "artifact_bytes_written": bytes_written,
                    }
                )
        manifest = {
            "format_version": PROTOTYPE_SNAPSHOT_FORMAT_V2,
            "schema_version": "state_prototype_row_v1",
            "as_of_date": str(as_of_date),
            "memory_version": str(memory_version),
            "spec_hash": str(payload.get("spec_hash") or ""),
            "snapshot_id": str(payload.get("snapshot_id") or ""),
            "prototype_count": int(payload.get("prototype_count") or len(prototypes)),
            "row_count": len(prototypes),
            "part_files": part_paths,
        }
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        bytes_written += int(manifest_path.stat().st_size)
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "artifact_write",
                    "status": "running",
                    "artifact_rows_total": len(prototypes),
                    "artifact_rows_done": len(prototypes),
                    "artifact_part_count": len(part_paths),
                    "artifact_bytes_written": bytes_written,
                }
            )
        return str(manifest_path)

    def load_prototype_snapshot(self, *, run_id: str, name: str = "prototype_snapshot") -> dict | None:
        manifest_path = self._prototype_snapshot_manifest(run_id=run_id, name=name)
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            snapshot_dir = manifest_path.parent
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
                "prototype_snapshot_manifest_path": str(manifest_path),
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

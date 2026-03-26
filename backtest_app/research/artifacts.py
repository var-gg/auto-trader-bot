from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


class JsonResearchArtifactStore:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def _dir(self, run_id: str) -> Path:
        out_dir = Path(self.output_dir) / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

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

    def save_prototype_snapshot(self, *, run_id: str, name: str = "prototype_snapshot", as_of_date: str, memory_version: str, payload: Mapping[str, Any]) -> str:
        envelope = {"as_of_date": as_of_date, "memory_version": memory_version, **dict(payload)}
        path = self._dir(run_id) / f"{name}.json"
        path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(path)

    def load_prototype_snapshot(self, *, run_id: str, name: str = "prototype_snapshot") -> dict | None:
        path = self._dir(run_id) / f"{name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

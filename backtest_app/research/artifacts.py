from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


class JsonResearchArtifactStore:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def save(self, *, run_id: str, name: str, payload: Mapping[str, Any]) -> str:
        out_dir = Path(self.output_dir) / run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{name}.json"
        path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return str(path)

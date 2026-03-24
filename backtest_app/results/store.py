from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping

from shared.domain.models import FillOutcome, OrderPlan


@dataclass
class JsonResultStore:
    output_dir: str

    def save_run(self, *, run_id: str, plans: Iterable[OrderPlan], fills: Iterable[FillOutcome], summary: Mapping[str, object]) -> str:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{run_id}.json"
        payload = {
            "run_id": run_id,
            "plans": [p.to_dict() for p in plans],
            "fills": [f.to_dict() for f in fills],
            "summary": dict(summary),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

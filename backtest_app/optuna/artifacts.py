from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from .models import ExperimentConfig, TrialRecord


def parameter_set_hash(params: dict) -> str:
    payload = json.dumps(params, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass
class JsonExperimentStore:
    output_dir: str

    def save_study(self, *, config: ExperimentConfig, trials: Iterable[TrialRecord], best_trial: TrialRecord | None) -> str:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{config.experiment_id}.json"
        payload = {
            "experiment": asdict(config),
            "best_trial": asdict(best_trial) if best_trial else None,
            "trials": [asdict(t) for t in trials],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

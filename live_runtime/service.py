from __future__ import annotations

import json
from pathlib import Path

from backtest_app.configs.models import ResearchExperimentSpec
from backtest_app.research.artifacts import JsonResearchArtifactStore


def load_live_bundle(*, manifest_path: str, artifact_dir: str, run_id: str, snapshot_name: str = "prototype_snapshot") -> dict:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    store = JsonResearchArtifactStore(artifact_dir)
    snapshot = store.load_prototype_snapshot(run_id=run_id, name=snapshot_name)
    if not snapshot:
        raise FileNotFoundError(f"prototype snapshot not found: {run_id}/{snapshot_name}")
    spec_payload = manifest.get("research_spec") or snapshot.get("spec") or {}
    spec = ResearchExperimentSpec(**spec_payload) if spec_payload else ResearchExperimentSpec()
    return {"manifest": manifest, "snapshot": snapshot, "spec": spec}

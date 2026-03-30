from __future__ import annotations

import json
from pathlib import Path

from scripts.prune_run_payloads import analyze_root, prune_root


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_analyze_root_marks_failed_medium_root_as_prunable(tmp_path: Path) -> None:
    root = tmp_path / "failed_medium_root"
    _write_json(root / "medium_viability_summary.json", {"viable": False, "verdict": "current TOBE v1 fails"})
    _write_json(root / "best1" / "summary.json", {"candidate_count": 0})
    _write_json(root / "best1" / "research" / "payload.json", {"x": "y"})
    _write_json(root / "best1" / "diagnostics.json", {"diag": True})

    analysis = analyze_root(root)

    assert analysis.medium_failed is True
    assert analysis.active is False
    assert analysis.prunable_bytes > 0
    assert analysis.recommended_action == "prune_large_payloads"


def test_analyze_root_marks_running_root_as_active(tmp_path: Path) -> None:
    root = tmp_path / "running_root"
    _write_json(root / "best1" / "status.json", {"status": "running"})
    _write_json(root / "best1" / "research" / "payload.json", {"x": "y"})

    analysis = analyze_root(root)

    assert analysis.active is True
    assert analysis.recommended_action == "keep_active"


def test_prune_root_keeps_summary_and_deletes_large_payloads(tmp_path: Path) -> None:
    root = tmp_path / "stale_root"
    _write_json(root / "medium_viability_summary.json", {"viable": False})
    _write_json(root / "best1" / "summary.json", {"candidate_count": 0})
    research_path = root / "best1" / "research" / "payload.json"
    diagnostics_path = root / "best1" / "diagnostics.json"
    _write_json(research_path, {"x": "y"})
    _write_json(diagnostics_path, {"diag": True})

    receipt = prune_root(root, apply=True)

    assert receipt["deleted_file_count"] == 2
    assert not research_path.exists()
    assert not diagnostics_path.exists()
    assert (root / "best1" / "summary.json").exists()

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backtest_app.configs.models import BacktestScenario
from scripts.medium_viability_check import ProgressRecorder, _pick_best_two_allowed, _rebuild_medium_root_outputs, _resolve_or_lock_support_pair, _summarize_authoritative_artifact, _support_pair_payload, _verify_metadata_applied
from scripts.support_config_audit import build_audit


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _tiny_row(run_label: str, metadata: dict[str, str], *, candidate_count: int, fills_count: int = 1, trades_count: int = 1) -> dict:
    return {
        "run_label": run_label,
        "candidate_count": candidate_count,
        "fills_count": fills_count,
        "trades_count": trades_count,
        "buy_pass_count": candidate_count,
        "sell_pass_count": 0,
        "metadata": metadata,
    }


def test_pick_best_two_allowed_dedupes_duplicate_support_fingerprints():
    rows = [
        _tiny_row("tiny_duplicate_top", {"top_k": "5", "kernel_temperature": "6"}, candidate_count=3),
        _tiny_row("tiny_duplicate_lower", {"top_k": "5", "kernel_temperature": "6"}, candidate_count=2),
        _tiny_row("tiny_distinct", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1),
    ]

    selected = _pick_best_two_allowed(rows)

    assert [row["run_label"] for row in selected] == ["tiny_duplicate_top", "tiny_distinct"]
    assert len({row["support_fingerprint"] for row in selected}) == 2


def test_pick_best_two_allowed_returns_single_row_when_only_one_distinct_fingerprint_exists():
    rows = [
        _tiny_row("tiny_dup_1", {"top_k": "5", "kernel_temperature": "6"}, candidate_count=3),
        _tiny_row("tiny_dup_2", {"top_k": "5", "kernel_temperature": "6"}, candidate_count=2),
        _tiny_row("tiny_dup_3", {"top_k": "5", "kernel_temperature": "6"}, candidate_count=1),
    ]

    selected = _pick_best_two_allowed(rows)

    assert len(selected) == 1
    assert selected[0]["run_label"] == "tiny_dup_1"


def test_support_pair_payload_records_pair_distinct_state():
    selected = _pick_best_two_allowed(
        [
            _tiny_row("tiny_best1", {"top_k": "5", "kernel_temperature": "6"}, candidate_count=2),
            _tiny_row("tiny_best2", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1),
        ]
    )

    payload = _support_pair_payload(selected)

    assert payload["selected_best1_source_run_label"] == "tiny_best1"
    assert payload["selected_best2_source_run_label"] == "tiny_best2"
    assert payload["pair_distinct"] is True
    assert payload["selected_best1_support_fingerprint"] != payload["selected_best2_support_fingerprint"]


def test_support_config_audit_classifies_distinct_current_pair_as_a(tmp_path: Path):
    tiny_root = tmp_path / "tiny"
    medium_root = tmp_path / "medium"
    _write_json(tiny_root / "tiny_best1" / "summary.json", _tiny_row("tiny_best1", {"kernel_temperature": "6", "top_k": "5"}, candidate_count=1))
    _write_json(tiny_root / "tiny_best2" / "summary.json", _tiny_row("tiny_best2", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1))
    best1_summary = {
        "run_label": "best1",
        "authoritative": True,
        "verdict_eligible": True,
        "exclusion_reasons": [],
        "metadata_application": {"expected": {"kernel_temperature": "6", "top_k": "5"}, "observed": {"kernel_temperature": 6.0, "top_k": 5}},
        "child_summary": {"ok": True, "status": {"status": "ok", "phase": "complete"}},
    }
    best2_summary = {
        "run_label": "best2",
        "authoritative": True,
        "verdict_eligible": True,
        "exclusion_reasons": [],
        "metadata_application": {"expected": {"diagnostic_disable_ess_gate": "true"}, "observed": {"diagnostic_disable_ess_gate": True}},
        "child_summary": {"ok": True, "status": {"status": "ok", "phase": "complete"}},
    }
    _write_json(medium_root / "best1" / "summary.json", best1_summary)
    _write_json(medium_root / "best2" / "summary.json", best2_summary)
    _write_json(medium_root / "best1" / "manifest.json", {"metadata": {"kernel_temperature": "6", "top_k": "5"}})
    _write_json(medium_root / "best2" / "manifest.json", {"metadata": {"diagnostic_disable_ess_gate": "true"}})
    _write_json(
        medium_root / "medium_viability_summary.json",
        {
            "selected_best1_source_run_label": "tiny_best1",
            "selected_best2_source_run_label": "tiny_best2",
            "best1_support_metadata": {"kernel_temperature": "6", "top_k": "5"},
            "best2_support_metadata": {"diagnostic_disable_ess_gate": "true"},
            "medium_runs": [
                {"run_label": "best1", **best1_summary},
                {"run_label": "best2", **best2_summary},
            ],
        },
    )

    audit = build_audit(tiny_root=tiny_root, medium_root=medium_root, strict_policy="C")

    assert audit["classification"] == "A"
    assert audit["pair_is_distinct_under_allowed_support_keys"] is True
    assert audit["medium_pair_matches_tiny_sources"] is True
    assert audit["can_use_completed_medium_failures_as_final_evidence"] is True


def test_support_config_audit_blocks_incomplete_pair_as_c(tmp_path: Path):
    tiny_root = tmp_path / "tiny"
    medium_root = tmp_path / "medium"
    _write_json(tiny_root / "tiny_best1" / "summary.json", _tiny_row("tiny_best1", {"kernel_temperature": "6", "top_k": "5"}, candidate_count=1))
    _write_json(medium_root / "best1" / "summary.json", {"run_label": "best1"})
    _write_json(medium_root / "best1" / "manifest.json", {"metadata": {"kernel_temperature": "6", "top_k": "5"}})
    _write_json(
        medium_root / "medium_viability_summary.json",
        {
            "selected_best1_source_run_label": "tiny_best1",
            "selected_best2_source_run_label": "tiny_missing",
            "best1_support_metadata": {"kernel_temperature": "6", "top_k": "5"},
            "best2_support_metadata": {"diagnostic_disable_ess_gate": "true"},
            "medium_runs": [{"run_label": "best1", "run_label_alias": "best1"}],
        },
    )

    audit = build_audit(tiny_root=tiny_root, medium_root=medium_root, strict_policy="C")

    assert audit["classification"] == "C"
    assert "incomplete_pair" in audit["blocked_reasons"]
    assert audit["can_use_completed_medium_failures_as_final_evidence"] is False


def test_resolve_or_lock_support_pair_reuses_locked_pair_and_rejects_tiny_root_mismatch(tmp_path: Path):
    tiny_root = tmp_path / "tiny_a"
    other_tiny_root = tmp_path / "tiny_b"
    output_root = tmp_path / "medium"
    _write_json(tiny_root / "tiny_best1" / "summary.json", _tiny_row("tiny_best1", {"kernel_temperature": "6", "top_k": "5"}, candidate_count=2))
    _write_json(tiny_root / "tiny_best2" / "summary.json", _tiny_row("tiny_best2", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1))
    _write_json(other_tiny_root / "tiny_best1" / "summary.json", _tiny_row("tiny_best1", {"kernel_temperature": "6", "top_k": "5"}, candidate_count=2))
    _write_json(other_tiny_root / "tiny_best2" / "summary.json", _tiny_row("tiny_best2", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1))

    first = _resolve_or_lock_support_pair(output_root=output_root, tiny_root=tiny_root)
    second = _resolve_or_lock_support_pair(output_root=output_root, tiny_root=tiny_root)

    assert first["selected_best1_source_run_label"] == "tiny_best1"
    assert second["source"] == "locked_pair"
    with pytest.raises(RuntimeError, match="already locked to tiny root"):
        _resolve_or_lock_support_pair(output_root=output_root, tiny_root=other_tiny_root)


def test_rebuild_medium_root_outputs_merges_existing_best_summaries(tmp_path: Path):
    tiny_root = tmp_path / "tiny"
    output_root = tmp_path / "medium"
    _write_json(tiny_root / "tiny_best1" / "summary.json", _tiny_row("tiny_best1", {"kernel_temperature": "6", "top_k": "5"}, candidate_count=2))
    _write_json(tiny_root / "tiny_best2" / "summary.json", _tiny_row("tiny_best2", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1))
    pair_payload = _resolve_or_lock_support_pair(output_root=output_root, tiny_root=tiny_root)
    _write_json(output_root / "best1" / "summary.json", {"run_label": "best1", "candidate_count": 0, "fills_count": 0, "trades_count": 0, "verdict_eligible": True, "authoritative": True, "metadata_application": {"expected": {"kernel_temperature": "6"}, "observed": {"kernel_temperature": 6.0}}, "exclusion_reasons": [], "result_path": "best1.json", "metadata": pair_payload["best1_support_metadata"]})
    _write_json(output_root / "best2" / "summary.json", {"run_label": "best2", "candidate_count": 0, "fills_count": 0, "trades_count": 0, "verdict_eligible": True, "authoritative": True, "metadata_application": {"expected": {"diagnostic_disable_ess_gate": "true"}, "observed": {"diagnostic_disable_ess_gate": True}}, "exclusion_reasons": [], "result_path": "best2.json", "metadata": pair_payload["best2_support_metadata"]})

    payload = _rebuild_medium_root_outputs(
        output_root=output_root,
        tiny_root=tiny_root,
        driver={"authoritative": True, "head_commit": "abc"},
        preflight={"schema": "trading"},
        baseline_summary=None,
        pair_payload=pair_payload,
    )

    assert [row["run_label"] for row in payload["medium_runs"]] == ["best1", "best2"]
    assert (output_root / "medium_viability_summary.json").exists()
    assert (output_root / "diagnosis.md").exists()
    saved = _read_json(output_root / "medium_viability_summary.json")
    assert saved["selected_best1_source_run_label"] == "tiny_best1"
    assert saved["selected_best2_source_run_label"] == "tiny_best2"


def test_support_config_audit_uses_selected_support_pair_fallback_when_medium_summary_missing(tmp_path: Path):
    tiny_root = tmp_path / "tiny"
    medium_root = tmp_path / "medium"
    _write_json(tiny_root / "tiny_best1" / "summary.json", _tiny_row("tiny_best1", {"kernel_temperature": "6", "top_k": "5"}, candidate_count=1))
    _write_json(tiny_root / "tiny_best2" / "summary.json", _tiny_row("tiny_best2", {"diagnostic_disable_ess_gate": "true"}, candidate_count=1))
    pair_payload = _resolve_or_lock_support_pair(output_root=medium_root, tiny_root=tiny_root)
    _write_json(medium_root / "best1" / "summary.json", {"run_label": "best1", "authoritative": True, "verdict_eligible": True, "exclusion_reasons": [], "metadata_application": {"expected": {"kernel_temperature": "6"}, "observed": {"kernel_temperature": 6.0}}, "child_summary": {"ok": True, "status": {"status": "ok", "phase": "complete"}}})
    _write_json(medium_root / "best2" / "summary.json", {"run_label": "best2", "authoritative": True, "verdict_eligible": True, "exclusion_reasons": [], "metadata_application": {"expected": {"diagnostic_disable_ess_gate": "true"}, "observed": {"diagnostic_disable_ess_gate": True}}, "child_summary": {"ok": True, "status": {"status": "ok", "phase": "complete"}}})
    _write_json(medium_root / "best1" / "manifest.json", {"metadata": pair_payload["best1_support_metadata"]})
    _write_json(medium_root / "best2" / "manifest.json", {"metadata": pair_payload["best2_support_metadata"]})

    audit = build_audit(tiny_root=tiny_root, medium_root=medium_root, strict_policy="C")

    assert audit["classification"] == "A"
    assert audit["selected_best1_source_run_label"] == "tiny_best1"
    assert audit["selected_best2_source_run_label"] == "tiny_best2"
    assert audit["medium_pair_matches_tiny_sources"] is True


def test_progress_recorder_refreshes_last_progress_on_phase_change_without_counter_delta(tmp_path: Path):
    recorder = ProgressRecorder(tmp_path, {"run_label": "best1", "child_run_label": "best1"})

    recorder.emit(phase="candidate_generation", status="running", extra={"completed_trading_dates": 5})
    first = _read_json(tmp_path / "status.json")["last_progress_at"]

    recorder.emit(
        phase="write_artifacts",
        status="running",
        extra={
            "completed_trading_dates": 5,
            "artifact_name": "authoritative_summary_prewrite",
            "artifact_index": 1,
            "artifact_total": 5,
        },
    )
    second = _read_json(tmp_path / "status.json")["last_progress_at"]

    assert second is not None
    assert second != first


def test_verify_metadata_applied_uses_authoritative_artifact_observed_fields():
    saved_run = {
        "support_metadata_observed": {
            "top_k": 5,
            "kernel_temperature": 6.0,
            "use_kernel_weighting": True,
            "min_effective_sample_size": 1.5,
            "diagnostic_disable_ess_gate": True,
            "diagnostic_disable_ess_gate_rows": [True],
        },
        "signal_panel_rows": [
            {"decision_surface": {"gate_ablation": {"diagnostic_disable_ess_gate": True}}},
        ],
    }

    applied = _verify_metadata_applied(
        saved_run,
        {
            "top_k": "5",
            "kernel_temperature": "6",
            "use_kernel_weighting": "true",
            "min_effective_sample_size": "1.5",
            "diagnostic_disable_ess_gate": "true",
        },
    )

    assert applied["applied"] is True
    assert applied["checks"]["diagnostic_disable_ess_gate_rows"] is True


def test_summarize_authoritative_artifact_rescues_child_failure(tmp_path: Path):
    csv_path = tmp_path / "forecast_panel.csv"
    parquet_path = tmp_path / "forecast_panel.parquet"
    artifact_path = tmp_path / "authoritative_summary.json"
    csv_path.write_text("decision_date,symbol\n2025-03-28,AAPL\n", encoding="utf-8")
    parquet_path.write_text("placeholder", encoding="utf-8")
    artifact_path.write_text("{}", encoding="utf-8")

    summary = _summarize_authoritative_artifact(
        "best1",
        {"diagnostic_disable_ess_gate": "true"},
        {
            "scenario_id": "medium_smoke",
            "window": {"start_date": "2025-03-28", "end_date": "2025-04-10"},
            "symbols": ["AAPL"],
            "authoritative": True,
            "forecast_selected_count": 1,
            "forecast_selected_dates": ["2025-03-28"],
            "forecast_viable": True,
            "candidate_count": 1,
            "candidate_dates": ["2025-03-28"],
            "fills_count": 0,
            "trades_count": 0,
            "deploy_viable": False,
            "forecast_panel": {"csv_path": str(csv_path), "parquet_path": str(parquet_path)},
            "support_metadata_observed": {"diagnostic_disable_ess_gate": True, "diagnostic_disable_ess_gate_rows": [True]},
            "signal_panel_rows": [{"decision_surface": {"gate_ablation": {"diagnostic_disable_ess_gate": True}}}],
        },
        BacktestScenario(scenario_id="medium_smoke", market="US", start_date="2025-03-28", end_date="2025-04-10", symbols=["AAPL"]),
        artifact_path=artifact_path,
        child_summary={"ok": False},
    )

    assert summary is not None
    assert summary["artifact_rescued"] is True
    assert summary["verdict_eligible"] is True
    assert "child_failed" not in summary["exclusion_reasons"]

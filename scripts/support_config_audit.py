from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DRIVER_PATH = ROOT / "scripts" / "medium_viability_check.py"
ALLOWED_SUPPORT_KEYS = {
    "kernel_temperature",
    "top_k",
    "use_kernel_weighting",
    "min_effective_sample_size",
    "diagnostic_disable_ess_gate",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _support_metadata(metadata: dict[str, Any] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in dict(metadata or {}).items():
        if key in ALLOWED_SUPPORT_KEYS:
            normalized[str(key)] = str(value).lower() if isinstance(value, bool) else str(value)
    return normalized


def _support_fingerprint(metadata: dict[str, Any] | None) -> str:
    canonical = json.dumps(sorted(_support_metadata(metadata).items()), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def _load_tiny_rows(tiny_root: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    if not tiny_root.exists():
        return rows
    for path in sorted(tiny_root.rglob("summary.json")):
        payload = _read_json(path)
        run_label = str(payload.get("run_label", path.parent.name))
        support_metadata = _support_metadata(payload.get("metadata"))
        rows[run_label] = {
            "run_label": run_label,
            "path": str(path.resolve()),
            "support_metadata": support_metadata,
            "support_fingerprint": _support_fingerprint(support_metadata),
            "candidate_count": int(payload.get("candidate_count", 0)),
            "fills_count": int(payload.get("fills_count", 0)),
            "trades_count": int(payload.get("trades_count", 0)),
        }
    return rows


def _driver_has_support_dedupe(driver_path: Path) -> bool:
    source = driver_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    pick_best_two_source = ""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_pick_best_two_allowed":
            pick_best_two_source = ast.get_source_segment(source, node) or ""
            break
    return "support_fingerprint" in pick_best_two_source and "seen_fingerprints" in pick_best_two_source


def _medium_run_row(summary_payload: dict[str, Any], run_label: str) -> dict[str, Any]:
    for row in list(summary_payload.get("medium_runs") or []):
        if str(row.get("run_label")) == run_label:
            return dict(row)
    return {}


def _load_optional_json(path: Path) -> dict[str, Any]:
    return _read_json(path) if path.exists() else {}


def _resolve_metadata_application(summary_payload: dict[str, Any], medium_row: dict[str, Any]) -> dict[str, Any]:
    metadata_application = summary_payload.get("metadata_application")
    if isinstance(metadata_application, dict):
        return dict(metadata_application)
    metadata_application = medium_row.get("metadata_application")
    if isinstance(metadata_application, dict):
        return dict(metadata_application)
    return {}


def _resolve_scalar(summary_payload: dict[str, Any], medium_row: dict[str, Any], key: str) -> Any:
    if key in summary_payload:
        return summary_payload.get(key)
    return medium_row.get(key)


def _run_contract_presence(summary_payload: dict[str, Any], medium_row: dict[str, Any]) -> dict[str, Any]:
    metadata_application = _resolve_metadata_application(summary_payload, medium_row)
    child_summary = dict(summary_payload.get("child_summary") or medium_row.get("child_summary") or {})
    child_status = dict(child_summary.get("status") or {})
    completed = bool(
        summary_payload
        and (
            child_status.get("status") == "ok"
            or child_status.get("phase") == "complete"
            or child_summary.get("ok") is True
        )
    )
    return {
        "summary_exists": bool(summary_payload),
        "medium_row_exists": bool(medium_row),
        "completed": completed,
        "has_metadata_application_expected": "expected" in metadata_application,
        "has_metadata_application_observed": "observed" in metadata_application,
        "has_authoritative": _resolve_scalar(summary_payload, medium_row, "authoritative") is not None,
        "has_verdict_eligible": _resolve_scalar(summary_payload, medium_row, "verdict_eligible") is not None,
        "has_exclusion_reasons": _resolve_scalar(summary_payload, medium_row, "exclusion_reasons") is not None,
    }


def _selected_support_from_medium_summary(summary_payload: dict[str, Any], prefix: str) -> dict[str, str]:
    return _support_metadata(summary_payload.get(f"{prefix}_support_metadata"))


def _selected_source_label(summary_payload: dict[str, Any], prefix: str) -> str | None:
    value = summary_payload.get(f"selected_{prefix}_source_run_label")
    return str(value) if value is not None else None


def _classification_name(classification: str) -> str:
    if classification == "A":
        return "distinct_support_configs_confirmed"
    if classification == "B_duplicate_support_pair":
        return "duplicate_support_pair"
    return "metadata_application_mismatch_or_incomplete_pair"


def build_audit(*, tiny_root: Path, medium_root: Path, strict_policy: str) -> dict[str, Any]:
    if str(strict_policy).upper() != "C":
        raise RuntimeError("Only strict policy C is supported in this audit")

    medium_summary_path = medium_root / "medium_viability_summary.json"
    medium_summary = _load_optional_json(medium_summary_path)
    best1_summary_path = medium_root / "best1" / "summary.json"
    best2_summary_path = medium_root / "best2" / "summary.json"
    best1_manifest_path = medium_root / "best1" / "manifest.json"
    best2_manifest_path = medium_root / "best2" / "manifest.json"
    best1_summary = _load_optional_json(best1_summary_path)
    best2_summary = _load_optional_json(best2_summary_path)
    best1_manifest = _load_optional_json(best1_manifest_path)
    best2_manifest = _load_optional_json(best2_manifest_path)
    best1_medium_row = _medium_run_row(medium_summary, "best1")
    best2_medium_row = _medium_run_row(medium_summary, "best2")
    best1_presence = _run_contract_presence(best1_summary, best1_medium_row)
    best2_presence = _run_contract_presence(best2_summary, best2_medium_row)
    best1_metadata_application = _resolve_metadata_application(best1_summary, best1_medium_row)
    best2_metadata_application = _resolve_metadata_application(best2_summary, best2_medium_row)
    best1_authoritative = _resolve_scalar(best1_summary, best1_medium_row, "authoritative")
    best2_authoritative = _resolve_scalar(best2_summary, best2_medium_row, "authoritative")
    best1_verdict_eligible = _resolve_scalar(best1_summary, best1_medium_row, "verdict_eligible")
    best2_verdict_eligible = _resolve_scalar(best2_summary, best2_medium_row, "verdict_eligible")
    best1_exclusion_reasons = list(_resolve_scalar(best1_summary, best1_medium_row, "exclusion_reasons") or [])
    best2_exclusion_reasons = list(_resolve_scalar(best2_summary, best2_medium_row, "exclusion_reasons") or [])
    selected_best1_source_run_label = _selected_source_label(medium_summary, "best1")
    selected_best2_source_run_label = _selected_source_label(medium_summary, "best2")
    best1_support_metadata = _selected_support_from_medium_summary(medium_summary, "best1")
    best2_support_metadata = _selected_support_from_medium_summary(medium_summary, "best2")
    tiny_rows = _load_tiny_rows(tiny_root)
    tiny_best1 = dict(tiny_rows.get(selected_best1_source_run_label or "") or {})
    tiny_best2 = dict(tiny_rows.get(selected_best2_source_run_label or "") or {})
    tiny_source_best1_support_metadata = dict(tiny_best1.get("support_metadata") or {})
    tiny_source_best2_support_metadata = dict(tiny_best2.get("support_metadata") or {})
    pair_is_distinct = bool(best1_support_metadata and best2_support_metadata and best1_support_metadata != best2_support_metadata)
    medium_pair_matches_tiny_sources = bool(
        selected_best1_source_run_label
        and selected_best2_source_run_label
        and tiny_source_best1_support_metadata == best1_support_metadata
        and tiny_source_best2_support_metadata == best2_support_metadata
    )
    driver_has_support_dedupe = _driver_has_support_dedupe(DRIVER_PATH)

    both_completed = best1_presence["completed"] and best2_presence["completed"]
    current_fields_present = all(
        (
            best1_presence["has_metadata_application_expected"],
            best1_presence["has_metadata_application_observed"],
            best1_presence["has_authoritative"],
            best1_presence["has_verdict_eligible"],
            best1_presence["has_exclusion_reasons"],
            best2_presence["has_metadata_application_expected"],
            best2_presence["has_metadata_application_observed"],
            best2_presence["has_authoritative"],
            best2_presence["has_verdict_eligible"],
            best2_presence["has_exclusion_reasons"],
        )
    )
    both_authoritative = best1_authoritative is True and best2_authoritative is True
    both_verdict_eligible = best1_verdict_eligible is True and best2_verdict_eligible is True

    if both_completed and current_fields_present and both_authoritative and both_verdict_eligible and pair_is_distinct and medium_pair_matches_tiny_sources:
        classification = "A"
    elif both_completed and current_fields_present and not pair_is_distinct:
        classification = "B_duplicate_support_pair"
    else:
        classification = "C"

    blocked_reasons: list[str] = []
    if not both_completed:
        blocked_reasons.append("incomplete_pair")
    if not current_fields_present:
        blocked_reasons.append("metadata_application_missing_or_verdict_fields_missing")
    if current_fields_present and not both_authoritative:
        blocked_reasons.append("non_authoritative_run_present")
    if current_fields_present and not both_verdict_eligible:
        blocked_reasons.append("verdict_ineligible_run_present")
    if current_fields_present and not medium_pair_matches_tiny_sources:
        blocked_reasons.append("medium_pair_does_not_match_tiny_sources")
    if current_fields_present and both_completed and not pair_is_distinct:
        blocked_reasons.append("duplicate_support_pair")

    if classification == "A":
        unblock_recommendation = "unblocked_for_gate_census"
    elif classification == "B_duplicate_support_pair":
        unblock_recommendation = "rerun_best1_best2_with_dedupe_enabled_driver"
    else:
        unblock_recommendation = "fix_only_the_reported_block_reason_then_reaudit"

    return {
        "policy": "C",
        "classification": classification,
        "classification_name": _classification_name(classification),
        "can_use_completed_medium_failures_as_final_evidence": classification == "A",
        "current_medium_root": str(medium_root.resolve()),
        "tiny_root": str(tiny_root.resolve()),
        "best1_current_summary_path": str(best1_summary_path.resolve()),
        "best2_current_summary_path": str(best2_summary_path.resolve()),
        "best1_current_contract_presence": best1_presence,
        "best2_current_contract_presence": best2_presence,
        "best1_metadata_application": best1_metadata_application,
        "best2_metadata_application": best2_metadata_application,
        "best1_authoritative": best1_authoritative,
        "best2_authoritative": best2_authoritative,
        "best1_verdict_eligible": best1_verdict_eligible,
        "best2_verdict_eligible": best2_verdict_eligible,
        "best1_exclusion_reasons": best1_exclusion_reasons,
        "best2_exclusion_reasons": best2_exclusion_reasons,
        "selected_best1_source_run_label": selected_best1_source_run_label,
        "selected_best2_source_run_label": selected_best2_source_run_label,
        "best1_support_metadata": best1_support_metadata,
        "best2_support_metadata": best2_support_metadata,
        "tiny_source_best1_support_metadata": tiny_source_best1_support_metadata,
        "tiny_source_best2_support_metadata": tiny_source_best2_support_metadata,
        "pair_is_distinct_under_allowed_support_keys": pair_is_distinct,
        "medium_pair_matches_tiny_sources": medium_pair_matches_tiny_sources,
        "driver_has_support_dedupe": driver_has_support_dedupe,
        "unblock_recommendation": unblock_recommendation,
        "blocked_reasons": blocked_reasons,
        "best1_current_support_fingerprint": _support_fingerprint(best1_support_metadata),
        "best2_current_support_fingerprint": _support_fingerprint(best2_support_metadata),
        "tiny_source_best1_support_fingerprint": _support_fingerprint(tiny_source_best1_support_metadata),
        "tiny_source_best2_support_fingerprint": _support_fingerprint(tiny_source_best2_support_metadata),
        "best1_manifest_path": str(best1_manifest_path.resolve()),
        "best2_manifest_path": str(best2_manifest_path.resolve()),
        "best1_manifest_support_metadata": _support_metadata(best1_manifest.get("metadata")),
        "best2_manifest_support_metadata": _support_metadata(best2_manifest.get("metadata")),
        "medium_viability_summary_path": str(medium_summary_path.resolve()),
    }


def _render_markdown(audit: dict[str, Any]) -> str:
    classification = audit["classification"]
    blocked = classification != "A"
    why_line = "why blocked" if blocked else "why unblocked"
    return "\n".join(
        [
            f"official_classification: {audit['classification_name']}",
            f"can_use_completed_medium_failures_as_final_evidence: {str(audit['can_use_completed_medium_failures_as_final_evidence']).lower()}",
            "",
            f"- exact current medium root: `{audit['current_medium_root']}`",
            f"- exact tiny root: `{audit['tiny_root']}`",
            f"- exact selected best1 source label: `{audit['selected_best1_source_run_label']}`",
            f"- exact selected best2 source label: `{audit['selected_best2_source_run_label']}`",
            f"- pair distinct under allowed support keys: `{audit['pair_is_distinct_under_allowed_support_keys']}`",
            f"- metadata_application mismatch present: `{bool('metadata_application_missing_or_verdict_fields_missing' in audit['blocked_reasons'])}`",
            f"- best1 authoritative/verdict_eligible: `{audit['best1_authoritative']}` / `{audit['best1_verdict_eligible']}`",
            f"- best2 authoritative/verdict_eligible: `{audit['best2_authoritative']}` / `{audit['best2_verdict_eligible']}`",
            f"- {why_line}: `{', '.join(audit['blocked_reasons']) if audit['blocked_reasons'] else 'none'}`",
            f"- medium_pair_matches_tiny_sources: `{audit['medium_pair_matches_tiny_sources']}`",
            f"- driver_has_support_dedupe: `{audit['driver_has_support_dedupe']}`",
            f"- unblock_recommendation: `{audit['unblock_recommendation']}`",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Canonical current-pair support audit for medium viability roots.")
    parser.add_argument("--tiny-root", type=Path, required=True, help="Absolute tiny source root used by medium_viability_check.py")
    parser.add_argument("--medium-root", type=Path, required=True, help="Absolute medium output root to audit")
    parser.add_argument("--strict-policy", type=str, default="C", help="Strict audit policy. Only C is supported.")
    parser.add_argument("--output-json", type=Path, default=None, help="Optional output json path. Defaults to <medium-root>/support_config_audit.json")
    parser.add_argument("--output-md", type=Path, default=None, help="Optional output md path. Defaults to <medium-root>/support_config_audit.md")
    args = parser.parse_args()

    tiny_root = args.tiny_root.resolve()
    medium_root = args.medium_root.resolve()
    if not tiny_root.is_absolute():
        raise RuntimeError("--tiny-root must be an absolute path")
    if not medium_root.is_absolute():
        raise RuntimeError("--medium-root must be an absolute path")
    audit = build_audit(tiny_root=tiny_root, medium_root=medium_root, strict_policy=args.strict_policy)
    output_json = args.output_json.resolve() if args.output_json else medium_root / "support_config_audit.json"
    output_md = args.output_md.resolve() if args.output_md else medium_root / "support_config_audit.md"
    _write_json(output_json, audit)
    _write_text(output_md, _render_markdown(audit) + "\n")
    print(f"Wrote {output_json}")
    print(f"Wrote {output_md}")


if __name__ == "__main__":
    main()

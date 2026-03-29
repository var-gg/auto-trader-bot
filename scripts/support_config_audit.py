from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DRIVER_PATH = ROOT / "scripts" / "medium_viability_check.py"
TINY_ROOT = ROOT / "runs" / "medium_viability_check_parity_fix_20260329" / "tiny_seed"
CURRENT_SCHEMA_SUMMARY_PATH = ROOT / "runs" / "medium_viability_check" / "20260329_e21ff63_best1_rerun" / "medium_viability_summary.json"
OLD_MEDIUM_RUNS = {
    "best1": {
        "summary_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best1" / "best1" / "summary.json",
        "manifest_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best1" / "best1" / "manifest.json",
        "medium_summary_json_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best1" / "medium_viability_summary.json",
        "medium_summary_csv_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best1" / "medium_viability_summary.csv",
    },
    "best2": {
        "summary_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best2" / "best2" / "summary.json",
        "manifest_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best2" / "best2" / "manifest.json",
        "medium_summary_json_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best2" / "medium_viability_summary.json",
        "medium_summary_csv_path": ROOT / "runs" / "medium_viability_check_stallfix_20260329_best2" / "medium_viability_summary.csv",
    },
}
OUTPUT_JSON = ROOT / "support_config_audit.json"
OUTPUT_MD = ROOT / "support_config_audit.md"

ALLOWED_SUPPORT_KEYS = {
    "kernel_temperature",
    "top_k",
    "use_kernel_weighting",
    "min_effective_sample_size",
    "diagnostic_disable_ess_gate",
}
STRICT_CLASSIFICATION = "C"
STRICT_CLASSIFICATION_NAME = "metadata_application_mismatch"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, payload: str) -> None:
    path.write_text(payload, encoding="utf-8")


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in dict(metadata or {}).items():
        if key in ALLOWED_SUPPORT_KEYS:
            normalized[str(key)] = str(value)
    return normalized


def _extract_tiny_rows(tiny_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(tiny_root.rglob("summary.json")):
        payload = _read_json(path)
        rows.append(
            {
                "path": str(path),
                "run_label": str(payload.get("run_label", path.parent.name)),
                "candidate_count": int(payload.get("candidate_count", 0)),
                "fills_count": int(payload.get("fills_count", 0)),
                "trades_count": int(payload.get("trades_count", 0)),
                "buy_pass_count": int(payload.get("buy_pass_count", 0)),
                "sell_pass_count": int(payload.get("sell_pass_count", 0)),
                "support_metadata": _normalize_metadata(payload.get("metadata")),
            }
        )
    return rows


def _load_driver_logic(driver_path: Path) -> dict[str, Any]:
    source = driver_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    allowed_support_keys_literal: list[str] = []
    pick_best_two_source = ""
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "ALLOWED_SUPPORT_KEYS":
                    value = ast.literal_eval(node.value)
                    allowed_support_keys_literal = sorted(str(item) for item in value)
        if isinstance(node, ast.FunctionDef) and node.name == "_pick_best_two_allowed":
            pick_best_two_source = ast.get_source_segment(source, node) or ""
    dedupe_present = any(token in pick_best_two_source for token in ("dedupe", "distinct", "seen", "unique"))
    return {
        "path": str(driver_path),
        "allowed_support_keys": allowed_support_keys_literal,
        "supports_current_observability_fields": all(token in source for token in ("metadata_application", "authoritative", "verdict_eligible", "exclusion_reasons")),
        "selection_function_has_dedupe": dedupe_present,
        "selection_function_excerpt": pick_best_two_source.strip(),
    }


def _summary_contract_presence(summary: dict[str, Any]) -> dict[str, Any]:
    metadata_application = summary.get("metadata_application")
    has_metadata_application = isinstance(metadata_application, dict)
    return {
        "has_metadata_application": has_metadata_application,
        "has_metadata_application_expected": has_metadata_application and "expected" in metadata_application,
        "has_metadata_application_observed": has_metadata_application and "observed" in metadata_application,
        "has_authoritative": "authoritative" in summary,
        "has_verdict_eligible": "verdict_eligible" in summary,
        "has_exclusion_reasons": "exclusion_reasons" in summary,
    }


def _match_tiny_source(support_metadata: dict[str, str], tiny_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in tiny_rows:
        if row["support_metadata"] == support_metadata:
            return row
    return None


def _build_old_run_entry(run_label: str, paths: dict[str, Path], tiny_rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = _read_json(paths["summary_path"])
    manifest = _read_json(paths["manifest_path"])
    medium_summary = _read_json(paths["medium_summary_json_path"])
    support_metadata = _normalize_metadata(summary.get("metadata"))
    manifest_support_metadata = _normalize_metadata(manifest.get("metadata"))
    medium_support_metadata = _normalize_metadata(
        (next(iter(medium_summary.get("medium_runs") or []), {}) or {}).get("metadata")
    )
    tiny_match = _match_tiny_source(manifest_support_metadata, tiny_rows)
    contract_presence = _summary_contract_presence(summary)
    return {
        "run_label": run_label,
        "summary_path": str(paths["summary_path"]),
        "manifest_path": str(paths["manifest_path"]),
        "medium_summary_json_path": str(paths["medium_summary_json_path"]),
        "medium_summary_csv_path": str(paths["medium_summary_csv_path"]),
        "medium_summary_csv_exists": paths["medium_summary_csv_path"].exists(),
        "candidate_count": int(summary.get("candidate_count", 0)),
        "fills_count": int(summary.get("fills_count", 0)),
        "trades_count": int(summary.get("trades_count", 0)),
        "support_metadata_from_summary": support_metadata,
        "support_metadata_from_manifest": manifest_support_metadata,
        "support_metadata_from_medium_summary": medium_support_metadata,
        "current_contract_presence": contract_presence,
        "tiny_source_match": {
            "matched": tiny_match is not None,
            "run_label": tiny_match.get("run_label") if tiny_match else None,
            "path": tiny_match.get("path") if tiny_match else None,
        },
    }


def _build_current_schema_probe(current_schema_summary: dict[str, Any]) -> dict[str, Any]:
    medium_runs = list(current_schema_summary.get("medium_runs") or [])
    present_labels = [str(row.get("run_label")) for row in medium_runs if row.get("run_label")]
    run_lookup = {str(row.get("run_label")): row for row in medium_runs if row.get("run_label")}
    best1_probe = dict(run_lookup.get("best1") or {})
    best2_probe = dict(run_lookup.get("best2") or {})
    return {
        "path": str(CURRENT_SCHEMA_SUMMARY_PATH),
        "exists": CURRENT_SCHEMA_SUMMARY_PATH.exists(),
        "medium_runs_present": present_labels,
        "completed_pair_available": bool(best1_probe) and bool(best2_probe),
        "selected_best1_source_run_label": current_schema_summary.get("selected_best1_source_run_label"),
        "selected_best2_source_run_label": current_schema_summary.get("selected_best2_source_run_label"),
        "best1_probe": {
            "authoritative": best1_probe.get("authoritative"),
            "verdict_eligible": best1_probe.get("verdict_eligible"),
            "exclusion_reasons": best1_probe.get("exclusion_reasons"),
            "metadata_application": best1_probe.get("metadata_application"),
        },
        "best2_probe": {
            "authoritative": best2_probe.get("authoritative"),
            "verdict_eligible": best2_probe.get("verdict_eligible"),
            "exclusion_reasons": best2_probe.get("exclusion_reasons"),
            "metadata_application": best2_probe.get("metadata_application"),
        },
    }


def _build_audit() -> dict[str, Any]:
    driver_logic = _load_driver_logic(DRIVER_PATH)
    tiny_rows = _extract_tiny_rows(TINY_ROOT)
    current_schema_summary = _read_json(CURRENT_SCHEMA_SUMMARY_PATH)
    best1 = _build_old_run_entry("best1", OLD_MEDIUM_RUNS["best1"], tiny_rows)
    best2 = _build_old_run_entry("best2", OLD_MEDIUM_RUNS["best2"], tiny_rows)
    current_schema_probe = _build_current_schema_probe(current_schema_summary)

    missing_metadata_application = []
    missing_verdict_fields = []
    for row in (best1, best2):
        presence = row["current_contract_presence"]
        if not (presence["has_metadata_application_expected"] and presence["has_metadata_application_observed"]):
            missing_metadata_application.append(row["run_label"])
        if not (presence["has_authoritative"] and presence["has_verdict_eligible"] and presence["has_exclusion_reasons"]):
            missing_verdict_fields.append(row["run_label"])

    distinct_support_configs = best1["support_metadata_from_manifest"] != best2["support_metadata_from_manifest"]
    manifests_match_tiny_sources = bool(best1["tiny_source_match"]["matched"] and best2["tiny_source_match"]["matched"])
    completed_medium_mapping = {
        "best1": {
            "source_run_label": best1["tiny_source_match"]["run_label"],
            "support_metadata": best1["support_metadata_from_manifest"],
        },
        "best2": {
            "source_run_label": best2["tiny_source_match"]["run_label"],
            "support_metadata": best2["support_metadata_from_manifest"],
        },
    }
    current_probe_mapping = {
        "selected_best1_source_run_label": current_schema_probe["selected_best1_source_run_label"],
        "selected_best2_source_run_label": current_schema_probe["selected_best2_source_run_label"],
        "best1_support_metadata": _normalize_metadata(current_schema_summary.get("best1_support_metadata")),
        "best2_support_metadata": _normalize_metadata(current_schema_summary.get("best2_support_metadata")),
    }

    official_result = {
        "policy": "Strict C",
        "classification": STRICT_CLASSIFICATION,
        "classification_name": STRICT_CLASSIFICATION_NAME,
        "can_use_completed_medium_failures_as_final_evidence": False,
        "gate_census_blocked": True,
        "gate_census_block_reason": (
            "Completed best1/best2 artifacts do not expose current-schema metadata_application.expected/observed "
            "or authoritative/verdict_eligible/exclusion_reasons, and the only current-schema probe is a partial best1 rerun."
        ),
        "completed_runs_missing_metadata_application": missing_metadata_application,
        "completed_runs_missing_current_verdict_fields": missing_verdict_fields,
        "current_schema_probe": current_schema_probe,
    }
    supplemental_inference = {
        "driver_logic": {
            "path": driver_logic["path"],
            "allowed_support_keys": driver_logic["allowed_support_keys"],
            "supports_current_observability_fields": driver_logic["supports_current_observability_fields"],
            "selection_function_has_dedupe": driver_logic["selection_function_has_dedupe"],
        },
        "tiny_top2_distinct": distinct_support_configs,
        "tiny_top2_run_labels": [row["run_label"] for row in tiny_rows],
        "tiny_source_root": str(TINY_ROOT),
        "old_medium_manifest_matches_tiny_sources": manifests_match_tiny_sources,
        "completed_medium_artifact_mapping": completed_medium_mapping,
        "current_rerun_probe_mapping": current_probe_mapping,
        "legacy_completed_label_mapping_statement": {
            "best1": "kernel/top_k/use_kernel_weighting/min_effective_sample_size",
            "best2": "diagnostic_disable_ess_gate",
        },
        "does_not_upgrade_official_classification": True,
        "note": (
            "Distinct support configs are visible in the tiny source rows and the legacy completed medium manifests, "
            "but Strict C keeps the official classification at metadata_application_mismatch until both completed best1/best2 runs exist under the current observability contract."
        ),
    }

    return {
        "classification": STRICT_CLASSIFICATION,
        "official_result": official_result,
        "supplemental_inference": supplemental_inference,
        "best1": best1,
        "best2": best2,
        "tiny_sources": {
            "root": str(TINY_ROOT),
            "rows": tiny_rows,
            "distinct_support_metadata": distinct_support_configs,
        },
    }


def _render_markdown(audit: dict[str, Any]) -> str:
    best1 = audit["best1"]
    best2 = audit["best2"]
    official = audit["official_result"]
    supplemental = audit["supplemental_inference"]
    current_probe = official["current_schema_probe"]
    return "\n".join(
        [
            "official_classification: metadata_application_mismatch",
            "medium best1/best2 failure must not be used as final support-family evidence under the current strict audit policy",
            "",
            "Strict C result:",
            f"- Completed `best1` summary lacks `metadata_application.expected/observed`: {best1['current_contract_presence']['has_metadata_application_expected'] is False or best1['current_contract_presence']['has_metadata_application_observed'] is False}.",
            f"- Completed `best2` summary lacks `metadata_application.expected/observed`: {best2['current_contract_presence']['has_metadata_application_expected'] is False or best2['current_contract_presence']['has_metadata_application_observed'] is False}.",
            f"- Completed `best1` summary exposes current verdict fields (`authoritative`, `verdict_eligible`, `exclusion_reasons`): {best1['current_contract_presence']['has_authoritative'] and best1['current_contract_presence']['has_verdict_eligible'] and best1['current_contract_presence']['has_exclusion_reasons']}.",
            f"- Completed `best2` summary exposes current verdict fields (`authoritative`, `verdict_eligible`, `exclusion_reasons`): {best2['current_contract_presence']['has_authoritative'] and best2['current_contract_presence']['has_verdict_eligible'] and best2['current_contract_presence']['has_exclusion_reasons']}.",
            f"- Current-schema probe path: `{current_probe['path']}`.",
            f"- Current-schema probe medium runs present: `{', '.join(current_probe['medium_runs_present']) or 'none'}`.",
            f"- Probe `best1` authoritative={current_probe['best1_probe']['authoritative']}, verdict_eligible={current_probe['best1_probe']['verdict_eligible']}, exclusion_reasons={json.dumps(current_probe['best1_probe']['exclusion_reasons'] or [], ensure_ascii=False)}.",
            "",
            "Why C and why the official path stops here:",
            f"- Official classification is `{official['classification_name']}` because completed `best1`/`best2` artifacts cannot be audited with the current `metadata_application` contract.",
            f"- `medium_gate_census`, `gate_family_viability_check.py`, and `label_family_viability_check.py` stay blocked because `{official['gate_census_block_reason']}`",
            "",
            "Supplemental inference:",
            f"- Tiny source top-2 metadata are distinct: {supplemental['tiny_top2_distinct']}.",
            f"- Legacy completed medium manifests match tiny source metadata: {supplemental['old_medium_manifest_matches_tiny_sources']}.",
            f"- Legacy completed mapping: `best1 -> {best1['tiny_source_match']['run_label']}`, `best2 -> {best2['tiny_source_match']['run_label']}`.",
            f"- Current probe mapping: `best1 -> {current_probe['selected_best1_source_run_label']}`, `best2 -> {current_probe['selected_best2_source_run_label']}`.",
            f"- Driver support dedupe present: {supplemental['driver_logic']['selection_function_has_dedupe']}.",
            f"- These supplemental facts do not upgrade the official classification: {supplemental['does_not_upgrade_official_classification']}.",
            "",
            "Unblock conditions:",
            "- Audit policy changes to `Infer A` or `Hybrid`, or",
            "- Both `best1` and `best2` are re-collected as completed authoritative current-schema artifacts.",
            "",
            "Execution guardrails:",
            "- No new medium reruns were started by this audit.",
            "- `medium_gate_census`, `gate_family_viability`, `label_family_viability`, `matrix`, and `optuna` remain out of scope until the unblock condition is met.",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Strict-C support configuration audit for completed medium artifacts.")
    parser.add_argument("--output-json", type=Path, default=OUTPUT_JSON, help="Path to write support_config_audit.json")
    parser.add_argument("--output-md", type=Path, default=OUTPUT_MD, help="Path to write support_config_audit.md")
    args = parser.parse_args()

    audit = _build_audit()
    _write_json(args.output_json, audit)
    _write_text(args.output_md, _render_markdown(audit) + "\n")
    print(f"Wrote {args.output_json}")
    print(f"Wrote {args.output_md}")


if __name__ == "__main__":
    main()

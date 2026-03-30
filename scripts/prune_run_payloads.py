from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PRUNABLE_FILENAME = "diagnostics.json"
RESEARCH_DIRNAME = "research"
RUNS_ROOT_DEFAULT = Path("runs")
AUDIT_OUTPUT_DIR_DEFAULT = Path("runs") / "diagnostics" / "storage_audit"


@dataclass(frozen=True)
class RootAnalysis:
    root: Path
    total_bytes: int
    prunable_bytes: int
    prunable_files: tuple[Path, ...]
    active: bool
    completed: bool
    medium_failed: bool
    verdict: str | None
    viable: bool | None
    recommended_action: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _iter_candidate_roots(runs_root: Path) -> list[Path]:
    roots: list[Path] = []
    for child in sorted(runs_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name == "medium_viability_check":
            for subdir in sorted(child.iterdir()):
                if subdir.is_dir():
                    roots.append(subdir)
            continue
        if child.name == "feature_contract_diagnosis":
            for subdir in sorted(child.iterdir()):
                if subdir.is_dir():
                    roots.append(subdir)
            continue
        if child.name == "diagnostics":
            continue
        roots.append(child)
    return roots


def _iter_prunable_files(root: Path) -> list[Path]:
    files: set[Path] = set()
    for research_dir in root.rglob(RESEARCH_DIRNAME):
        if research_dir.is_dir():
            files.update(path for path in research_dir.glob("*.json") if path.is_file())
    files.update(path for path in root.rglob(PRUNABLE_FILENAME) if path.is_file())
    return sorted(files)


def _has_running_status(root: Path) -> bool:
    for status_path in root.rglob("status.json"):
        payload = _load_json(status_path) or {}
        if str(payload.get("status", "")).lower() in {"running", "queued"}:
            return True
    return False


def _is_completed(root: Path) -> bool:
    summary_path = root / "medium_viability_summary.json"
    if summary_path.exists():
        return True
    summary_path = root / "summary.json"
    if summary_path.exists():
        return True
    for status_path in root.rglob("status.json"):
        payload = _load_json(status_path) or {}
        if str(payload.get("phase", "")).lower() == "complete" or str(payload.get("status", "")).lower() == "ok":
            return True
    return False


def _has_live_monitor(root: Path) -> bool:
    for monitor_path in root.rglob("monitor.json"):
        payload = _load_json(monitor_path) or {}
        if bool(payload.get("child_alive")):
            return True
    return False


def _medium_summary(root: Path) -> dict[str, Any] | None:
    return _load_json(root / "medium_viability_summary.json")


def _recommended_action(active: bool, medium_failed: bool, prunable_bytes: int) -> str:
    if active:
        return "keep_active"
    if medium_failed and prunable_bytes > 0:
        return "prune_large_payloads"
    if prunable_bytes >= 250 * 1024 * 1024:
        return "manual_review_large_payloads"
    return "keep"


def analyze_root(root: Path) -> RootAnalysis:
    prunable_files = tuple(_iter_prunable_files(root))
    prunable_bytes = sum(path.stat().st_size for path in prunable_files)
    total_bytes = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    completed = _is_completed(root)
    active = _has_running_status(root) or (_has_live_monitor(root) and not completed)
    medium_summary = _medium_summary(root) or {}
    viable = medium_summary.get("viable")
    verdict = medium_summary.get("verdict")
    medium_failed = bool((root / "medium_viability_summary.json").exists() and viable is False)
    return RootAnalysis(
        root=root,
        total_bytes=total_bytes,
        prunable_bytes=prunable_bytes,
        prunable_files=prunable_files,
        active=active,
        completed=completed,
        medium_failed=medium_failed,
        verdict=verdict if isinstance(verdict, str) else None,
        viable=viable if isinstance(viable, bool) else None,
        recommended_action=_recommended_action(active, medium_failed, prunable_bytes),
    )


def _bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 1)


def build_audit(runs_root: Path) -> dict[str, Any]:
    roots = [analyze_root(path) for path in _iter_candidate_roots(runs_root)]
    usage = shutil.disk_usage(runs_root)
    root_payloads = []
    for analysis in sorted(roots, key=lambda item: item.prunable_bytes, reverse=True):
        root_payloads.append(
            {
                "root": str(analysis.root.resolve()),
                "relative_root": str(analysis.root.relative_to(runs_root.resolve())),
                "total_mb": _bytes_to_mb(analysis.total_bytes),
                "prunable_mb": _bytes_to_mb(analysis.prunable_bytes),
                "prunable_file_count": len(analysis.prunable_files),
                "active": analysis.active,
                "completed": analysis.completed,
                "medium_failed": analysis.medium_failed,
                "verdict": analysis.verdict,
                "viable": analysis.viable,
                "recommended_action": analysis.recommended_action,
            }
        )
    return {
        "generated_at": _utc_now(),
        "runs_root": str(runs_root.resolve()),
        "disk_free_gb": round(usage.free / (1024**3), 2),
        "disk_used_gb": round(usage.used / (1024**3), 2),
        "total_prunable_mb": round(sum(item["prunable_mb"] for item in root_payloads), 1),
        "total_prunable_files": sum(item["prunable_file_count"] for item in root_payloads),
        "roots": root_payloads,
    }


def _render_audit_md(audit: dict[str, Any]) -> str:
    lines = [
        "# Run Storage Audit",
        "",
        f"- generated_at: `{audit['generated_at']}`",
        f"- runs_root: `{audit['runs_root']}`",
        f"- disk_free_gb: `{audit['disk_free_gb']}`",
        f"- total_prunable_mb: `{audit['total_prunable_mb']}`",
        "",
        "## Largest Prunable Roots",
        "",
        "| root | prunable_mb | action | active | medium_failed |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for item in audit["roots"][:10]:
        lines.append(
            f"| `{item['relative_root']}` | {item['prunable_mb']} | "
            f"{item['recommended_action']} | {item['active']} | {item['medium_failed']} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `prune_large_payloads` means only `research/*.json` and `diagnostics.json` are targeted.",
            "- `keep_active` roots are excluded from pruning because a child process is still running.",
            "- `manual_review_large_payloads` means the root is large but not a clearly failed medium root.",
        ]
    )
    return "\n".join(lines) + "\n"


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def prune_root(root: Path, apply: bool) -> dict[str, Any]:
    analysis = analyze_root(root)
    if analysis.active:
        raise RuntimeError(f"refusing to prune active root: {root}")
    deleted_files: list[str] = []
    deleted_bytes = 0
    for path in analysis.prunable_files:
        size = path.stat().st_size
        if apply:
            path.unlink()
        deleted_files.append(str(path.resolve()))
        deleted_bytes += size
    if apply:
        for research_dir in sorted(root.rglob(RESEARCH_DIRNAME), reverse=True):
            if research_dir.is_dir() and not any(research_dir.iterdir()):
                research_dir.rmdir()
    return {
        "root": str(root.resolve()),
        "apply": apply,
        "deleted_file_count": len(deleted_files),
        "deleted_mb": _bytes_to_mb(deleted_bytes),
        "deleted_files": deleted_files,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and prune stale run payloads.")
    subparsers = parser.add_subparsers(dest="command")

    audit_parser = subparsers.add_parser("audit", help="scan run roots and emit a storage audit")
    audit_parser.add_argument("--runs-root", type=Path, default=RUNS_ROOT_DEFAULT)
    audit_parser.add_argument("--output-json", type=Path, default=AUDIT_OUTPUT_DIR_DEFAULT / "run_storage_audit.json")
    audit_parser.add_argument("--output-md", type=Path, default=AUDIT_OUTPUT_DIR_DEFAULT / "run_storage_audit.md")

    prune_parser = subparsers.add_parser("prune", help="delete large payload files from explicit roots")
    prune_parser.add_argument("--root", action="append", type=Path, required=True)
    prune_parser.add_argument("--apply", action="store_true")
    prune_parser.add_argument("--receipt-json", type=Path)

    parser.set_defaults(command="audit")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.command in {None, "audit"}:
        runs_root = getattr(args, "runs_root", RUNS_ROOT_DEFAULT).resolve()
        audit = build_audit(runs_root)
        output_json = getattr(args, "output_json", AUDIT_OUTPUT_DIR_DEFAULT / "run_storage_audit.json")
        output_md = getattr(args, "output_md", AUDIT_OUTPUT_DIR_DEFAULT / "run_storage_audit.md")
        _write_json(output_json, audit)
        _write_text(output_md, _render_audit_md(audit))
        print(json.dumps(audit, indent=2, sort_keys=True))
        return 0

    receipts = {"generated_at": _utc_now(), "results": []}
    for root in args.root:
        receipts["results"].append(prune_root(root.resolve(), apply=args.apply))
    if args.receipt_json:
        _write_json(args.receipt_json, receipts)
    print(json.dumps(receipts, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import csv
import importlib
import json
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

ROOT = Path(__file__).resolve().parents[1]
OUT_ROOT = ROOT / "runs" / "feature_contract_diagnosis"
DEFAULT_BASELINE_REF = "cc47f4d84caf2a7d4341374eca7faef508c8f04b"
BASE_SPEC = {
    "feature_window_bars": 60,
    "lookback_horizons": [5],
    "horizon_days": 5,
    "target_return_pct": 0.04,
    "stop_return_pct": 0.03,
    "fee_bps": 0.0,
    "slippage_bps": 0.0,
    "flat_return_band_pct": 0.005,
    "feature_version": "multiscale_v2",
    "label_version": "event_outcome_v1",
    "memory_version": "memory_asof_v1",
}
BASE_METADATA = {
    "portfolio_top_n": "3",
    "portfolio_risk_budget_fraction": "0.60",
    "quote_ev_threshold": "0.005",
    "quote_uncertainty_cap": "0.12",
    "quote_min_effective_sample_size": "1.5",
    "quote_min_fill_probability": "0.10",
    "quote_min_regime_alignment": "0.5",
    "quote_max_return_interval_width": "0.08",
    "abstain_margin": "0.00",
}


@dataclass
class SourceState:
    label: str
    source_root: Path
    source_kind: str
    authoritative: bool
    branch: str | None
    head_commit: str | None
    dirty_worktree: bool
    changed_tracked_files: list[str]
    diff_fingerprint: str | None
    ref: str | None = None
    temp_root: Path | None = None


def _json_default(value: Any):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = columns or sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(row.get(key), ensure_ascii=False, default=_json_default) if isinstance(row.get(key), (dict, list)) else row.get(key)
                for key in fieldnames
            })


def _run_git(repo_root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo_root, capture_output=True, text=True, check=check)


def _stdout_lines(text: str) -> list[str]:
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def _git_provenance(repo_root: Path) -> dict[str, Any]:
    branch = _run_git(repo_root, "branch", "--show-current").stdout.strip() or None
    head_commit = _run_git(repo_root, "rev-parse", "HEAD").stdout.strip() or None
    changed_tracked_files = _stdout_lines(_run_git(repo_root, "diff", "--name-only", "--").stdout)
    dirty_worktree = bool(changed_tracked_files)
    diff_fingerprint = None
    if dirty_worktree:
        diff_proc = _run_git(repo_root, "diff", "--", check=False)
        if diff_proc.stdout:
            diff_fingerprint = subprocess.run(
                [sys.executable, "-c", "import hashlib,sys;print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())"],
                input=diff_proc.stdout,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
    return {
        "branch": branch,
        "head_commit": head_commit,
        "dirty_worktree": dirty_worktree,
        "changed_tracked_files": changed_tracked_files,
        "diff_fingerprint": diff_fingerprint,
        "authoritative": not dirty_worktree,
    }


def _load_project_symbols_and_preflight(source_root: Path) -> tuple[list[str], Any]:
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    mod = importlib.import_module("scripts.research_first_batch")
    universe = list(getattr(mod, "UNIVERSE"))
    return universe, getattr(mod, "preflight_local_db")


def _resolve_symbols(args: argparse.Namespace, default_symbols: list[str]) -> list[str]:
    raw = args.symbols or ",".join(default_symbols)
    symbols = [item.strip().upper() for item in str(raw).split(",") if item.strip()]
    if not symbols:
        raise RuntimeError("No symbols resolved for diagnosis")
    return symbols


def _date_range_from_preflight(preflight: dict[str, Any], smoke_days: int | None) -> tuple[str, str]:
    if not smoke_days:
        return str(preflight["discovery_start"]), str(preflight["holdout_end"])
    end_dt = date.fromisoformat(str(preflight["holdout_end"]))
    min_start = date.fromisoformat(str(preflight["first_date"]))
    start_dt = max(min_start, end_dt - timedelta(days=max(int(smoke_days), 14)))
    return start_dt.isoformat(), end_dt.isoformat()


def _worktree_root(output_root: Path) -> Path:
    root = output_root / "_worktrees"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _prepare_ref_state(*, label: str, ref: str, output_root: Path) -> SourceState:
    temp_root = Path(tempfile.mkdtemp(prefix=f"{label}_", dir=str(_worktree_root(output_root))))
    _run_git(ROOT, "worktree", "add", "--detach", str(temp_root), ref)
    provenance = _git_provenance(temp_root)
    return SourceState(
        label=label,
        source_root=temp_root,
        source_kind="git_ref",
        authoritative=bool(provenance.get("authoritative", False)),
        branch=provenance.get("branch"),
        head_commit=provenance.get("head_commit"),
        dirty_worktree=bool(provenance.get("dirty_worktree", False)),
        changed_tracked_files=list(provenance.get("changed_tracked_files", [])),
        diff_fingerprint=provenance.get("diff_fingerprint"),
        ref=ref,
        temp_root=temp_root,
    )


def _prepare_working_tree_state(*, label: str, source_root: Path) -> SourceState:
    provenance = _git_provenance(source_root)
    return SourceState(
        label=label,
        source_root=source_root,
        source_kind="working_tree",
        authoritative=bool(provenance.get("authoritative", False)),
        branch=provenance.get("branch"),
        head_commit=provenance.get("head_commit"),
        dirty_worktree=bool(provenance.get("dirty_worktree", False)),
        changed_tracked_files=list(provenance.get("changed_tracked_files", [])),
        diff_fingerprint=provenance.get("diff_fingerprint"),
    )


def _cleanup_state(state: SourceState) -> None:
    if state.temp_root is None:
        return
    try:
        _run_git(ROOT, "worktree", "remove", "--force", str(state.temp_root), check=False)
    finally:
        shutil.rmtree(state.temp_root, ignore_errors=True)


def _run_child(*, state: SourceState, output_root: Path, start_date: str, end_date: str, symbols: list[str]) -> dict[str, Any]:
    state_output = output_root / state.label
    state_output.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child-run",
            "--state-label",
            state.label,
            "--source-root",
            str(state.source_root),
            "--output-root",
            str(state_output),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--symbols",
            ",".join(symbols),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    stdout_path = state_output / "child_stdout.log"
    stdout_path.write_text((proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else ""), encoding="utf-8")
    summary = json.loads((state_output / "summary.json").read_text(encoding="utf-8")) if (state_output / "summary.json").exists() else {}
    summary["child_exit_code"] = proc.returncode
    summary["stdout_path"] = str(stdout_path.resolve())
    summary["source_state"] = {
        "label": state.label,
        "source_root": str(state.source_root.resolve()),
        "source_kind": state.source_kind,
        "ref": state.ref,
        "authoritative": state.authoritative,
        "branch": state.branch,
        "head_commit": state.head_commit,
        "dirty_worktree": state.dirty_worktree,
        "changed_tracked_files": state.changed_tracked_files,
        "diff_fingerprint": state.diff_fingerprint,
    }
    _write_json(state_output / "summary.json", summary)
    if proc.returncode != 0:
        raise RuntimeError(f"{state.label} child run failed; see {stdout_path}")
    return summary


def _remove_sys_path(path: Path) -> None:
    resolved = str(path.resolve())
    sys.path[:] = [entry for entry in sys.path if str(Path(entry).resolve()) != resolved] if sys.path else []


def _load_child_modules(source_root: Path) -> dict[str, Any]:
    if load_dotenv is not None:
        load_dotenv(dotenv_path=source_root / ".env")
    source_str = str(source_root)
    if source_str not in sys.path:
        sys.path.insert(0, source_str)
    importlib.invalidate_caches()
    models = importlib.import_module("backtest_app.configs.models")
    engine = importlib.import_module("backtest_app.research_runtime.engine")
    return {
        "BacktestConfig": getattr(models, "BacktestConfig"),
        "BacktestScenario": getattr(models, "BacktestScenario"),
        "ResearchExperimentSpec": getattr(models, "ResearchExperimentSpec"),
        "RunnerRequest": getattr(models, "RunnerRequest"),
        "run_backtest": getattr(engine, "run_backtest"),
    }


def _signal_panel(result: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics = dict(result.get("diagnostics") or {})
    artifacts = dict(result.get("artifacts") or {})
    signal_diagnostics = dict(diagnostics.get("signal_diagnostics") or {})
    panel = artifacts.get("signal_panel") or diagnostics.get("signal_panel") or signal_diagnostics.get("signal_panel") or []
    return [row for row in panel if isinstance(row, dict)]


def _chosen_side_key(row: dict[str, Any]) -> str | None:
    chosen_side = str(((row.get("decision_surface") or {}).get("chosen_side") or "")).upper()
    if chosen_side == "BUY":
        return "long"
    if chosen_side == "SELL":
        return "short"
    return None


def _top_match_rows(panel_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in panel_rows:
        match_sides = []
        chosen_side_key = _chosen_side_key(row)
        if chosen_side_key:
            match_sides.append(chosen_side_key)
        else:
            match_sides.extend(["long", "short"])
        for match_side in match_sides:
            matches = list(((row.get("top_matches") or {}).get(match_side)) or [])
            for rank, match in enumerate(matches, start=1):
                why = dict((match or {}).get("why") or {})
                out.append({
                    "decision_date": row.get("decision_date"),
                    "symbol": row.get("symbol"),
                    "chosen_side": ((row.get("decision_surface") or {}).get("chosen_side")),
                    "match_side": match_side.upper(),
                    "rank": rank,
                    "prototype_id": match.get("prototype_id"),
                    "representative_symbol": match.get("representative_symbol"),
                    "weight": float(match.get("weight", 0.0) or 0.0),
                    "similarity": float(why.get("similarity", 0.0) or 0.0),
                    "support": float(why.get("support", 0.0) or 0.0),
                    "expected_return": float(match.get("expected_return", 0.0) or 0.0),
                    "uncertainty": float(match.get("uncertainty", 0.0) or 0.0),
                })
    return out


def _aggregate_top_match_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("prototype_id") or ""), str(row.get("representative_symbol") or ""))
        bucket = buckets.setdefault(key, {
            "prototype_id": key[0],
            "representative_symbol": key[1],
            "match_count": 0,
            "mean_weight": 0.0,
            "mean_similarity": 0.0,
            "mean_support": 0.0,
        })
        bucket["match_count"] += 1
        bucket["mean_weight"] += float(row.get("weight", 0.0) or 0.0)
        bucket["mean_similarity"] += float(row.get("similarity", 0.0) or 0.0)
        bucket["mean_support"] += float(row.get("support", 0.0) or 0.0)
    out: list[dict[str, Any]] = []
    for bucket in buckets.values():
        denom = max(int(bucket["match_count"]), 1)
        out.append({
            **bucket,
            "mean_weight": bucket["mean_weight"] / denom,
            "mean_similarity": bucket["mean_similarity"] / denom,
            "mean_support": bucket["mean_support"] / denom,
        })
    out.sort(key=lambda row: (-int(row["match_count"]), -float(row["mean_weight"]), row["prototype_id"]))
    return out


def _panel_metric(panel_rows: list[dict[str, Any]], field: str) -> dict[str, float | None]:
    values: list[float] = []
    for row in panel_rows:
        scorer_key = "buy" if _chosen_side_key(row) == "long" else "sell" if _chosen_side_key(row) == "short" else None
        if scorer_key is None:
            continue
        scorer = dict(((row.get("scorer_diagnostics") or {}).get(scorer_key)) or {})
        value = scorer.get(field)
        if value is not None:
            values.append(float(value))
    if not values:
        return {"mean": None, "min": None, "max": None}
    return {"mean": float(mean(values)), "min": float(min(values)), "max": float(max(values))}


def _filled_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    fills = [row for row in (result.get("fills") or []) if isinstance(row, dict)]
    return [row for row in fills if str(row.get("fill_status") or "").upper() in {"FULL", "PARTIAL"}]


def _portfolio_payload(result: dict[str, Any]) -> dict[str, Any]:
    portfolio = result.get("portfolio")
    return dict(portfolio if isinstance(portfolio, dict) else {})


def _child_run(args: argparse.Namespace) -> int:
    source_root = Path(args.source_root).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    modules = _load_child_modules(source_root)
    BacktestConfig = modules["BacktestConfig"]
    BacktestScenario = modules["BacktestScenario"]
    ResearchExperimentSpec = modules["ResearchExperimentSpec"]
    RunnerRequest = modules["RunnerRequest"]
    run_backtest = modules["run_backtest"]

    spec = ResearchExperimentSpec(**BASE_SPEC)
    metadata = dict(BASE_METADATA)
    metadata["diagnostic_run_label"] = f"feature_contract_diag_{args.state_label}"
    scenario = BacktestScenario(
        scenario_id=f"feature_contract_diag_{args.state_label}",
        market="US",
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=[item.strip().upper() for item in str(args.symbols).split(",") if item.strip()],
    )
    request = RunnerRequest(scenario=scenario, config=BacktestConfig(initial_capital=10000.0, metadata=metadata, research_spec=spec))
    result = run_backtest(
        request=request,
        data_path=None,
        data_source="local-db",
        scenario_id=scenario.scenario_id,
        strategy_mode="research_similarity_v2",
        output_dir=str(output_root),
        save_json=True,
        enable_validation=False,
        validation_max_folds=0,
    )
    panel_rows = _signal_panel(result)
    top_match_rows = _top_match_rows(panel_rows)
    top_match_summary = _aggregate_top_match_rows(top_match_rows)[:20]
    portfolio = _portfolio_payload(result)
    provenance = _git_provenance(source_root)
    summary = {
        "state_label": args.state_label,
        "source_root": str(source_root),
        "source_kind": "working_tree" if source_root == ROOT.resolve() else "git_ref",
        "authoritative": bool(provenance.get("authoritative", False)),
        "branch": provenance.get("branch"),
        "head_commit": provenance.get("head_commit"),
        "dirty_worktree": provenance.get("dirty_worktree"),
        "changed_tracked_files": provenance.get("changed_tracked_files", []),
        "diff_fingerprint": provenance.get("diff_fingerprint"),
        "window": {"start_date": args.start_date, "end_date": args.end_date},
        "symbols": list(scenario.symbols),
        "candidate_count": len(list(portfolio.get("selected_symbols") or [])),
        "fills_count": len(_filled_rows(result)),
        "trades_count": len(list(result.get("plans") or [])),
        "signal_panel_rows": len(panel_rows),
        "lower_bound": _panel_metric(panel_rows, "lower_bound"),
        "n_eff": _panel_metric(panel_rows, "n_eff"),
        "top_match_contributors": top_match_summary,
        "result_path": result.get("result_path"),
    }
    _write_json(output_root / "summary.json", summary)
    _write_csv(output_root / "top_match_contributors.csv", top_match_rows)
    _write_csv(output_root / "top_match_contributors_aggregate.csv", top_match_summary)
    return 0


def _comparison_rows(baseline: dict[str, Any], fixed: dict[str, Any]) -> list[dict[str, Any]]:
    def _scalar(summary: dict[str, Any], key: str) -> float | int | None:
        value = summary.get(key)
        return value if isinstance(value, (int, float)) else None

    rows: list[dict[str, Any]] = []
    for key in ("candidate_count", "fills_count", "trades_count"):
        base = _scalar(baseline, key)
        new = _scalar(fixed, key)
        rows.append({"metric": key, "baseline": base, "fixed": new, "delta": (new - base) if base is not None and new is not None else None})
    for prefix in ("lower_bound", "n_eff"):
        for stat in ("mean", "min", "max"):
            base = ((baseline.get(prefix) or {}).get(stat))
            new = ((fixed.get(prefix) or {}).get(stat))
            rows.append({"metric": f"{prefix}_{stat}", "baseline": base, "fixed": new, "delta": (new - base) if base is not None and new is not None else None})
    return rows


def _top_contributor_labels(summary: dict[str, Any]) -> list[str]:
    out = []
    for row in list(summary.get("top_match_contributors") or [])[:5]:
        out.append(f"{row.get('representative_symbol') or 'UNKNOWN'}:{row.get('prototype_id') or 'unknown'} x{row.get('match_count')}")
    return out


def _write_parent_outputs(*, output_root: Path, baseline: dict[str, Any], fixed: dict[str, Any], comparison_rows: list[dict[str, Any]], metadata: dict[str, Any]) -> None:
    payload = {
        **metadata,
        "baseline": baseline,
        "fixed": fixed,
        "comparison": comparison_rows,
        "authoritative": bool(baseline.get("authoritative")) and bool(fixed.get("authoritative")),
        "do_not_run_medium": True,
    }
    _write_json(output_root / "comparison.json", payload)
    _write_csv(output_root / "comparison.csv", comparison_rows, columns=["metric", "baseline", "fixed", "delta"])
    lines = [
        "# Feature contract diagnosis",
        "",
        f"- Output root: `{output_root}`",
        f"- Baseline ref: `{metadata.get('baseline_ref')}`",
        f"- Fixed source: `{metadata.get('fixed_source_label')}`",
        f"- Window: `{metadata.get('start_date')}` -> `{metadata.get('end_date')}`",
        f"- Symbols: `{', '.join(metadata.get('symbols') or [])}`",
        f"- Authoritative comparison: `{payload['authoritative']}`",
        "- Medium verdict use: disabled until clean fixed commit rerun is available.",
        "",
        "## Metrics",
    ]
    for row in comparison_rows:
        lines.append(f"- {row['metric']}: baseline={row['baseline']} fixed={row['fixed']} delta={row['delta']}")
    lines.extend([
        "",
        "## Top Match Contributors",
        f"- baseline: {json.dumps(_top_contributor_labels(baseline), ensure_ascii=False)}",
        f"- fixed: {json.dumps(_top_contributor_labels(fixed), ensure_ascii=False)}",
        "",
        "## Provenance",
        f"- baseline authoritative: `{baseline.get('authoritative')}` ({baseline.get('head_commit')})",
        f"- fixed authoritative: `{fixed.get('authoritative')}` ({fixed.get('head_commit')})",
    ])
    (output_root / "diagnosis.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _main(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    default_symbols, preflight_local_db = _load_project_symbols_and_preflight(ROOT)
    symbols = _resolve_symbols(args, default_symbols)
    preflight = preflight_local_db(symbols)
    start_date, end_date = (
        (args.start_date, args.end_date)
        if args.start_date and args.end_date
        else _date_range_from_preflight(preflight, args.smoke_days)
    )
    baseline_state = _prepare_ref_state(label="baseline", ref=args.baseline_ref, output_root=output_root)
    fixed_state = _prepare_ref_state(label="fixed", ref=args.fixed_ref, output_root=output_root) if args.fixed_ref else _prepare_working_tree_state(label="fixed", source_root=ROOT)
    try:
        baseline_summary = _run_child(state=baseline_state, output_root=output_root, start_date=start_date, end_date=end_date, symbols=symbols)
        fixed_summary = _run_child(state=fixed_state, output_root=output_root, start_date=start_date, end_date=end_date, symbols=symbols)
        comparison = _comparison_rows(baseline_summary, fixed_summary)
        _write_parent_outputs(
            output_root=output_root,
            baseline=baseline_summary,
            fixed=fixed_summary,
            comparison_rows=comparison,
            metadata={
                "baseline_ref": args.baseline_ref,
                "fixed_source_label": args.fixed_ref or "working_tree",
                "start_date": start_date,
                "end_date": end_date,
                "symbols": symbols,
                "preflight": preflight,
            },
        )
        print(json.dumps({"output_root": str(output_root), "baseline": baseline_summary, "fixed": fixed_summary, "comparison_rows": comparison}, ensure_ascii=False, indent=2, default=_json_default))
        return 0
    finally:
        if not args.keep_worktrees:
            _cleanup_state(baseline_state)
            _cleanup_state(fixed_state)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare clean baseline ref vs fixed feature-contract state on the same tiny backtest window.")
    parser.add_argument("--child-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--state-label", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--source-root", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--output-root", type=str, default=str(OUT_ROOT.resolve()))
    parser.add_argument("--baseline-ref", type=str, default=DEFAULT_BASELINE_REF)
    parser.add_argument("--fixed-ref", type=str, default=None)
    parser.add_argument("--symbols", type=str, default=None)
    parser.add_argument("--smoke-days", type=int, default=20)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--keep-worktrees", action="store_true")
    args = parser.parse_args()
    if args.child_run:
        if not args.state_label or not args.source_root or not args.start_date or not args.end_date:
            raise RuntimeError("--child-run requires --state-label, --source-root, --start-date, and --end-date")
        return _child_run(args)
    return _main(args)


if __name__ == "__main__":
    raise SystemExit(main())

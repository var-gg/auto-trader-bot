from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from queue import Empty, SimpleQueue
from threading import Thread
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

try:
    import psutil
except Exception:  # pragma: no cover
    psutil = None

from backtest_app.configs.models import BacktestConfig, BacktestScenario, ResearchExperimentSpec, RunnerRequest
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.observability.git_provenance import collect_git_provenance
from backtest_app.research_runtime.engine import run_backtest
from scripts.research_first_batch import (
    UNIVERSE,
    append_leaderboard,
    build_report_md,
    direct_metrics,
    fold_report_summary,
    load_leaderboard_rows,
    preflight_local_db,
    resolve_git_commit,
    summarize_regime_split,
    summarize_side_split,
    write_csv,
)
from sqlalchemy import text

OUT_ROOT = ROOT / "runs" / "medium_viability_check"
DEFAULT_TINY_ROOT = ROOT / "runs" / "ess_support_verification_20260328" / "tiny"
BASELINE_RUN_LABEL = "baseline"
BASELINE_SYMBOLS = ["AAPL", "MSFT", "NVDA"]
BASELINE_TRADING_DATES = 25
STALL_SECONDS = 45
MONITOR_INTERVAL_SECONDS = 2.0
CPU_HIGH_DELTA_SECONDS = 0.5
PHASE_TIMEOUT_SECONDS = {
    "load_historical": 20 * 60,
    "daily_execution": 5 * 60,
    "write_artifacts": 2 * 60,
}
BASE_SPEC = ResearchExperimentSpec(
    feature_window_bars=60,
    lookback_horizons=[5],
    horizon_days=5,
    target_return_pct=0.04,
    stop_return_pct=0.03,
    fee_bps=0.0,
    slippage_bps=0.0,
    flat_return_band_pct=0.005,
    feature_version="multiscale_v2",
    label_version="event_outcome_v1",
    memory_version="memory_asof_v1",
)
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
BASE_KEYS = set(BASE_METADATA.keys()) | {"diagnostic_run_label"}
ALLOWED_SUPPORT_KEYS = {
    "kernel_temperature",
    "top_k",
    "use_kernel_weighting",
    "min_effective_sample_size",
    "diagnostic_disable_ess_gate",
}
COUNTER_KEYS = [
    "loaded_ohlcv_rows",
    "loaded_macro_rows",
    "loaded_sector_rows",
    "event_records_built",
    "prototype_batches_built",
    "total_trading_dates",
    "completed_trading_dates",
    "candidate_rows",
    "plans_count",
    "fills_count",
    "bytes_written",
]
SUMMARY_COLS = [
    "run_label",
    "authoritative",
    "verdict_eligible",
    "branch",
    "head_commit",
    "dirty_worktree",
    "diff_fingerprint",
    "candidate_count",
    "candidate_dates",
    "buy_pass_count",
    "sell_pass_count",
    "fills_count",
    "trades_count",
    "n_eff_histogram",
    "top1_weight_histogram",
    "abstain_reason_histogram",
    "metadata_application",
    "changed_tracked_files",
    "exclusion_reasons",
    "result_path",
    "metadata",
]


@dataclass
class BaselineWindow:
    start_date: str
    end_date: str
    trading_dates: list[str]
    symbols: list[str]
    db_url: str
    schema: str


def _json_default(value: Any):
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _write_csv_simple(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                k: json.dumps(row.get(k), ensure_ascii=False, default=_json_default) if isinstance(row.get(k), (dict, list)) else row.get(k)
                for k in columns
            })


def _resolve_branch() -> str:
    proc = subprocess.run(["git", "branch", "--show-current"], cwd=ROOT, capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def _resolve_head_commit() -> str:
    proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def _resolve_script_commit() -> str:
    proc = subprocess.run(["git", "log", "-1", "--format=%H", "--", str(Path(__file__).relative_to(ROOT)).replace("\\", "/")], cwd=ROOT, capture_output=True, text=True, check=True)
    return proc.stdout.strip()


def _metadata_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _saved_signal_diagnostics(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = dict(result.get("diagnostics") or {})
    signal_diagnostics = diagnostics.get("signal_diagnostics")
    return dict(signal_diagnostics if isinstance(signal_diagnostics, dict) else diagnostics)


def _saved_portfolio_payload(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = dict(result.get("diagnostics") or {})
    portfolio = diagnostics.get("portfolio")
    if isinstance(portfolio, dict):
        return dict(portfolio)
    return dict(result.get("portfolio") or {})


def _saved_reproducibility(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = dict(result.get("diagnostics") or {})
    signal_diagnostics = diagnostics.get("signal_diagnostics")
    return dict(
        result.get("reproducibility")
        or diagnostics.get("reproducibility")
        or (signal_diagnostics.get("reproducibility") if isinstance(signal_diagnostics, dict) else {})
        or {}
    )


def _verify_metadata_applied(saved_run: dict[str, Any], metadata: dict[str, str]) -> dict[str, Any]:
    support_metadata = {k: v for k, v in (metadata or {}).items() if k in ALLOWED_SUPPORT_KEYS}
    if not support_metadata:
        return {"checked": False, "applied": True, "expected": {}, "observed": {}, "checks": {}}
    signal_diagnostics = _saved_signal_diagnostics(saved_run)
    pipeline = dict(signal_diagnostics.get("pipeline") or {})
    ev_config = dict(pipeline.get("ev_config") or {})
    panel_rows = list(signal_diagnostics.get("signal_panel") or [])
    observed = {
        "top_k": pipeline.get("top_k"),
        "kernel_temperature": ev_config.get("kernel_temperature"),
        "use_kernel_weighting": ev_config.get("use_kernel_weighting"),
        "min_effective_sample_size": ev_config.get("min_effective_sample_size"),
        "diagnostic_disable_ess_gate": ev_config.get("diagnostic_disable_ess_gate"),
    }
    checks: dict[str, bool] = {}
    for key, raw_value in support_metadata.items():
        if key == "top_k":
            checks[key] = int(observed.get(key) or 0) == int(raw_value)
        elif key == "kernel_temperature":
            checks[key] = float(observed.get(key) or 0.0) == float(raw_value)
        elif key == "use_kernel_weighting":
            checks[key] = bool(observed.get(key)) == _metadata_bool(raw_value)
        elif key == "min_effective_sample_size":
            checks[key] = float(observed.get(key) or 0.0) == float(raw_value)
        elif key == "diagnostic_disable_ess_gate":
            expected_bool = _metadata_bool(raw_value)
            checks[key] = bool(observed.get(key)) == expected_bool
            gate_values = {
                bool(((row.get("decision_surface") or {}).get("gate_ablation") or {}).get("diagnostic_disable_ess_gate"))
                for row in panel_rows
                if isinstance(row, dict)
            }
            if gate_values:
                observed["diagnostic_disable_ess_gate_rows"] = sorted(gate_values)
                checks[f"{key}_rows"] = gate_values == {expected_bool}
    return {
        "checked": True,
        "applied": all(checks.values()),
        "expected": support_metadata,
        "observed": observed,
        "checks": checks,
    }


def _summary_exclusion_reasons(summary: dict[str, Any] | None) -> list[str]:
    row = dict(summary or {})
    reasons: list[str] = []
    child_summary = dict(row.get("child_summary") or {})
    if child_summary and not bool(child_summary.get("ok")):
        reasons.append("child_failed")
    if not bool(row.get("authoritative")):
        reasons.append("non_authoritative")
    metadata_application = dict(row.get("metadata_application") or {})
    if metadata_application.get("checked") and not metadata_application.get("applied", False):
        reasons.append("metadata_not_applied")
    return reasons


def _verdict_eligible(summary: dict[str, Any] | None) -> bool:
    return not _summary_exclusion_reasons(summary)


def _live_result_reproducibility(result: dict[str, Any]) -> dict[str, Any]:
    diagnostics = dict(result.get("diagnostics") or {})
    artifacts = dict(result.get("artifacts") or {})
    return dict(diagnostics.get("reproducibility") or artifacts.get("reproducibility") or {})


def _execution_start_record(output_root: Path) -> dict[str, Any]:
    return {
        "__file__": str(Path(__file__).resolve()),
        "git_rev_parse_head": _resolve_head_commit(),
        "cwd": str(Path.cwd().resolve()),
        "argv": list(sys.argv),
        "output_root": str(output_root.resolve()),
    }


def _emit_execution_start(output_root: Path, manifest_path: Path | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    record = _execution_start_record(output_root)
    if extra:
        record.update(extra)
    print(json.dumps({"execution_start": record}, ensure_ascii=False), flush=True)
    if manifest_path is not None and manifest_path.exists():
        manifest = _read_json(manifest_path)
        manifest["execution_start"] = record
        _write_json(manifest_path, manifest)
    return record


def _ensure_public_committed_driver() -> dict[str, Any]:
    branch = _resolve_branch()
    if not branch.startswith("public"):
        raise RuntimeError(f"Refusing run: current branch is not public ({branch})")
    tracked = subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/medium_viability_check.py"], cwd=ROOT, capture_output=True, text=True)
    if tracked.returncode != 0:
        raise RuntimeError("Refusing run: scripts/medium_viability_check.py is not tracked in git")
    provenance = collect_git_provenance(ROOT)
    return {
        "branch": branch,
        "head_commit": resolve_git_commit(),
        "driver_script": "scripts/medium_viability_check.py",
        "driver_script_last_commit": _resolve_script_commit(),
        "dirty_worktree": provenance.get("dirty_worktree", False),
        "changed_tracked_files": provenance.get("changed_tracked_files", []),
        "diff_fingerprint": provenance.get("diff_fingerprint"),
        "authoritative": provenance.get("authoritative", False),
    }


def _resolve_baseline_window(symbols: list[str], trading_days: int) -> BaselineWindow:
    cfg = LocalBacktestDbConfig.from_env()
    guard_backtest_local_only(cfg.url)
    session_factory = create_backtest_session_factory(cfg)
    sql = text(
        f"""
        SELECT trade_date
          FROM {cfg.schema}.bt_mirror_ohlcv_daily
         WHERE symbol = ANY(:symbols)
         GROUP BY trade_date
        HAVING COUNT(DISTINCT symbol) = :symbol_count
         ORDER BY trade_date DESC
        LIMIT :limit
        """
    )
    with session_factory() as session:
        rows = [str(r._mapping["trade_date"]) for r in session.execute(sql, {"symbols": symbols, "symbol_count": len(symbols), "limit": trading_days})]
    if len(rows) < trading_days:
        raise RuntimeError(f"Need at least {trading_days} common trading dates for baseline, found {len(rows)}")
    trading_dates = sorted(rows)
    return BaselineWindow(
        start_date=trading_dates[0],
        end_date=trading_dates[-1],
        trading_dates=trading_dates,
        symbols=list(symbols),
        db_url=cfg.url,
        schema=cfg.schema,
    )


def _result_path_for_run(run_dir: Path, scenario_id: str) -> Path:
    return run_dir / "research" / f"{scenario_id}.json"


def _initial_counters() -> dict[str, int]:
    return {key: 0 for key in COUNTER_KEYS}


class ProgressRecorder:
    def __init__(self, run_dir: Path, static_payload: dict[str, Any]):
        self.run_dir = run_dir
        self.progress_path = run_dir / "progress.jsonl"
        self.status_path = run_dir / "status.json"
        self.static_payload = dict(static_payload)
        self.counters = _initial_counters()
        self.last_progress_at: str | None = None

    def emit(self, *, phase: str, status: str, extra: dict[str, Any] | None = None) -> None:
        raw_extra = dict(extra or {})
        extra = {k: v for k, v in raw_extra.items() if v is not None}
        changed = False
        for key in COUNTER_KEYS:
            value = extra.get(key)
            if value is None:
                continue
            ivalue = int(value)
            if ivalue > int(self.counters.get(key, 0)):
                self.counters[key] = ivalue
                changed = True
            else:
                extra[key] = int(self.counters.get(key, 0))
        should_write = phase in {"child_start", "child_finalize", "complete", "failed_no_artifact", "exception", "stall_detected"} or changed
        if changed:
            self.last_progress_at = datetime.now().isoformat()
        payload = {
            **self.static_payload,
            "status": status,
            "phase": phase,
            "event_at": datetime.now().isoformat(),
            "last_progress_at": self.last_progress_at,
            **self.counters,
            **extra,
        }
        if should_write:
            _append_jsonl(self.progress_path, payload)
        _write_json(self.status_path, payload)


def _sum_dir_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def _scenario_from_window(scenario_id: str, start_date: str, end_date: str, symbols: list[str]) -> BacktestScenario:
    return BacktestScenario(scenario_id=scenario_id, market="US", start_date=start_date, end_date=end_date, symbols=list(symbols))


def _is_allowed_tiny_candidate(row: dict[str, Any]) -> bool:
    metadata = dict(row.get("metadata") or {})
    extra = set(metadata.keys()) - BASE_KEYS
    return extra <= ALLOWED_SUPPORT_KEYS


def _rank_key(row: dict[str, Any]):
    return (
        int(row.get("candidate_count", 0)),
        int(row.get("fills_count", 0)),
        int(row.get("trades_count", 0)),
        int(row.get("buy_pass_count", 0)) + int(row.get("sell_pass_count", 0)),
        row.get("run_label", ""),
    )


def _load_tiny_rows(tiny_root: Path) -> list[dict[str, Any]]:
    if not tiny_root.exists():
        raise RuntimeError(f"Tiny source root not found: {tiny_root}")
    rows = []
    for p in tiny_root.rglob("summary.json"):
        rows.append(json.loads(p.read_text(encoding="utf-8")))
    if not rows:
        raise RuntimeError(f"No tiny summary.json files found under {tiny_root}")
    return rows


def _pick_best_two_allowed(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered = [r for r in rows if _is_allowed_tiny_candidate(r)]
    filtered.sort(key=lambda row: (-int(row.get("candidate_count", 0)), -int(row.get("fills_count", 0)), -int(row.get("trades_count", 0)), -(int(row.get("buy_pass_count", 0)) + int(row.get("sell_pass_count", 0))), str(row.get("run_label", ""))))
    return filtered[:2]


def _summarize_run(run_label: str, metadata: dict[str, str], result: dict[str, Any], scenario: BacktestScenario, *, result_path: str | None = None) -> dict[str, Any]:
    signal_diagnostics = _saved_signal_diagnostics(result)
    portfolio = _saved_portfolio_payload(result)
    reproducibility = _saved_reproducibility(result)
    panel = list(signal_diagnostics.get("signal_panel") or [])
    selected = list((portfolio.get("selected_symbols")) or [])
    fills = list(result.get("fills") or [])
    plans = list(result.get("plans") or [])
    abstain: dict[str, int] = {}
    buy_pass_count = 0
    sell_pass_count = 0
    n_eff_histogram: dict[str, int] = {}
    top1_weight_histogram: dict[str, int] = {}
    candidate_dates = sorted({str(item.get("decision_date")) for item in selected if item.get("decision_date")})
    for row in panel:
        ds = row.get("decision_surface") or {}
        for reason in ds.get("abstain_reasons") or []:
            abstain[str(reason)] = abstain.get(str(reason), 0) + 1
        buy_reasons = (((row.get("ev") or {}).get("buy") or {}).get("abstain_reasons") or [])
        sell_reasons = (((row.get("ev") or {}).get("sell") or {}).get("abstain_reasons") or [])
        if not buy_reasons:
            buy_pass_count += 1
        if not sell_reasons:
            sell_pass_count += 1
        for side_key in ("buy", "sell"):
            n_eff = (((row.get("scorer_diagnostics") or {}).get(side_key) or {}).get("n_eff"))
            if n_eff is not None:
                bucket = f"{float(n_eff):.1f}"
                n_eff_histogram[bucket] = n_eff_histogram.get(bucket, 0) + 1
        for side_key in ("long", "short"):
            matches = (((row.get("top_matches") or {}).get(side_key)) or [])
            if matches:
                w = float((matches[0] or {}).get("weight", 0.0) or 0.0)
                bucket = f"{w:.1f}"
                top1_weight_histogram[bucket] = top1_weight_histogram.get(bucket, 0) + 1
    filled = [f for f in fills if str((f.get("fill_status") or "")).upper() in {"FULL", "PARTIAL"}]
    metadata_application = _verify_metadata_applied(result, metadata)
    summary = {
        "run_label": run_label,
        "scenario_id": scenario.scenario_id,
        "window": {"start_date": scenario.start_date, "end_date": scenario.end_date},
        "symbols": list(scenario.symbols),
        "authoritative": bool(reproducibility.get("authoritative", False)),
        "branch": reproducibility.get("branch"),
        "head_commit": reproducibility.get("head_commit"),
        "dirty_worktree": reproducibility.get("dirty_worktree"),
        "changed_tracked_files": reproducibility.get("changed_tracked_files", []),
        "diff_fingerprint": reproducibility.get("diff_fingerprint"),
        "candidate_count": len(selected),
        "candidate_dates": candidate_dates,
        "buy_pass_count": buy_pass_count,
        "sell_pass_count": sell_pass_count,
        "n_eff_histogram": dict(sorted(n_eff_histogram.items())),
        "top1_weight_histogram": dict(sorted(top1_weight_histogram.items())),
        "abstain_reason_histogram": dict(sorted(abstain.items())),
        "fills_count": len(filled),
        "trades_count": len(plans),
        "result_path": result_path or result.get("result_path"),
        "metadata": metadata,
        "metadata_application": metadata_application,
    }
    summary["exclusion_reasons"] = _summary_exclusion_reasons(summary)
    summary["verdict_eligible"] = not summary["exclusion_reasons"]
    return summary


def _write_contract_bundle(*, run_dir: Path, run_id: str, strategy_mode: str, preflight: dict[str, Any], scenario: BacktestScenario, metadata: dict[str, str], result: dict[str, Any], git_commit: str) -> None:
    reproducibility = _live_result_reproducibility(result)
    manifest = _read_json(run_dir / "manifest.json")
    manifest.update({
        "experiment_group": "medium_viability_check",
        "run_id": run_id,
        "label": run_id,
        "strategy_mode": strategy_mode,
        "git_commit": git_commit,
        "universe": list(scenario.symbols),
        "window": {
            "discovery_start": scenario.start_date,
            "discovery_end": scenario.end_date,
            "holdout_start": None,
            "holdout_end": None,
        },
        "spec": BASE_SPEC.to_dict(),
        "data_snapshot_id": (result.get("manifest") or {}).get("data_snapshot_id"),
        "discovery_manifest": result.get("manifest"),
        "holdout_manifest": None,
        "preflight": preflight,
        "metadata_overrides": metadata,
        "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        "diagnostic_flags": {k: v for k, v in metadata.items() if k.startswith("diagnostic_")},
        "run_reproducibility": reproducibility,
        "authoritative": bool(reproducibility.get("authoritative", False)),
        "branch": reproducibility.get("branch"),
        "head_commit": reproducibility.get("head_commit"),
        "dirty_worktree": reproducibility.get("dirty_worktree"),
        "changed_tracked_files": reproducibility.get("changed_tracked_files", []),
        "diff_fingerprint": reproducibility.get("diff_fingerprint"),
    })
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "reproducibility.json", reproducibility)
    fold_report = {
        "run_id": run_id,
        "strategy_mode": strategy_mode,
        "discovery": fold_report_summary((result.get("validation") or {}).get("fold_engine") or {}),
        "holdout": fold_report_summary({}),
    }
    _write_json(run_dir / "fold_report.json", fold_report)
    decisions_rows = [{"phase": "discovery", **row} for row in ((result.get("portfolio") or {}).get("decisions") or [])]
    trades_rows = [{"phase": "discovery", **row} for row in (result.get("fills") or [])]
    write_csv(run_dir / "decisions.csv", decisions_rows)
    write_csv(run_dir / "trades.csv", trades_rows)
    diagnostics = {
        "discovery": result.get("diagnostics"),
        "holdout": {},
        "validation": fold_report["discovery"],
        "holdout_validation": {},
        "diagnostics_lite": False,
        "reproducibility": reproducibility,
    }
    _write_json(run_dir / "diagnostics.json", diagnostics)
    aggregate = ((result.get("validation") or {}).get("fold_engine") or {}).get("aggregate") or {}
    discovery_direct = direct_metrics(result, len(scenario.symbols))
    side_split = summarize_side_split(result.get("plans") or [])
    regime_split = summarize_regime_split(((result.get("portfolio") or {}).get("decisions") or []))
    run_card = {
        "run_id": run_id,
        "strategy_mode": strategy_mode,
        "discovery_start": scenario.start_date,
        "discovery_end": scenario.end_date,
        "holdout_start": None,
        "holdout_end": None,
        "symbols": "|".join(scenario.symbols),
        "feature_window_bars": BASE_SPEC.feature_window_bars,
        "lookback_horizons": "|".join(map(str, BASE_SPEC.lookback_horizons)),
        "horizon_days": BASE_SPEC.horizon_days,
        "target_return_pct": BASE_SPEC.target_return_pct,
        "stop_return_pct": BASE_SPEC.stop_return_pct,
        "flat_return_band_pct": BASE_SPEC.flat_return_band_pct,
        "top_n": metadata.get("portfolio_top_n", "3"),
        "risk_budget_fraction": metadata.get("portfolio_risk_budget_fraction", "0.60"),
        "discovery_cv_expectancy_after_cost": aggregate.get("expectancy_after_cost", discovery_direct.get("expectancy_after_cost", 0.0)),
        "discovery_cv_psr": aggregate.get("psr", discovery_direct.get("psr", 0.0)),
        "discovery_cv_dsr": aggregate.get("dsr", discovery_direct.get("dsr", 0.0)),
        "discovery_cv_calibration_error": aggregate.get("calibration_error"),
        "discovery_cv_monotonicity": aggregate.get("score_decile_monotonicity"),
        "discovery_cv_max_drawdown": discovery_direct.get("max_drawdown", 0.0),
        "holdout_direct_trade_count": 0,
        "holdout_direct_fill_count": 0,
        "holdout_direct_fill_rate": 0.0,
        "holdout_direct_coverage": 0.0,
        "holdout_direct_no_trade_ratio": 0.0,
        "holdout_direct_expectancy_after_cost": 0.0,
        "holdout_direct_psr": 0.0,
        "holdout_direct_dsr": 0.0,
        "holdout_direct_max_drawdown": 0.0,
        "holdout_fold_expectancy_after_cost": 0.0,
        "long_split": side_split.get("long", 0),
        "short_split": side_split.get("short", 0),
        "regime_split": json.dumps(regime_split, ensure_ascii=False),
        "data_snapshot_id": manifest["data_snapshot_id"],
        "experiment_group": "medium_viability_check",
        "ohlcv_common_coverage": preflight.get("ohlcv_common_coverage"),
        "macro_coverage": preflight.get("macro_coverage"),
        "sector_coverage": preflight.get("sector_coverage"),
        "legacy_snapshot_ready": preflight.get("legacy_snapshot_ready"),
        "authoritative": bool(reproducibility.get("authoritative", False)),
        "branch": reproducibility.get("branch"),
        "head_commit": reproducibility.get("head_commit"),
        "dirty_worktree": reproducibility.get("dirty_worktree"),
        "diff_fingerprint": reproducibility.get("diff_fingerprint"),
    }
    _write_json(run_dir / "run_card.json", run_card)
    report_md = build_report_md(run_card=run_card, discovery=result, holdout={}, previous_rows=load_leaderboard_rows(run_dir.parent / "leaderboard.csv"))
    (run_dir / "report.md").write_text(report_md, encoding="utf-8")
    append_leaderboard(run_dir.parent / "leaderboard.csv", run_card)


def _mark_stall_error(run_dir: Path, stall_reason: str) -> None:
    status_payload = _read_json(run_dir / "status.json")
    status_payload.update({"status": "error", "phase": "stall_detected", "stall_reason": stall_reason, "event_at": datetime.now().isoformat()})
    _write_json(run_dir / "status.json", status_payload)
    _append_jsonl(run_dir / "progress.jsonl", status_payload)


def _monitor_child(run_dir: Path, proc: subprocess.Popen[str]) -> dict[str, Any]:
    monitor_path = run_dir / "monitor.json"
    stdout_path = run_dir / "child_stdout.log"
    manifest = _read_json(run_dir / "manifest.json")
    created_at = manifest.get("created_at")
    last_stdout_at: str | None = None
    last_cpu_total: float | None = None
    stall_reason: str | None = None
    last_metrics = {"cpu_delta": None, "rss": None, "cwd": None, "cmdline": None}
    stdout_queue: SimpleQueue[str | None] = SimpleQueue()

    def _stdout_reader() -> None:
        if not proc.stdout:
            stdout_queue.put(None)
            return
        try:
            for line in proc.stdout:
                stdout_queue.put(line)
        finally:
            stdout_queue.put(None)

    Thread(target=_stdout_reader, daemon=True).start()
    stdout_closed = False
    while True:
        while True:
            try:
                line = stdout_queue.get_nowait()
            except Empty:
                break
            if line is None:
                stdout_closed = True
                break
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            with stdout_path.open("a", encoding="utf-8") as f:
                f.write(line)
            last_stdout_at = datetime.now().isoformat()
        child_alive = proc.poll() is None
        status_payload = _read_json(run_dir / "status.json")
        phase = str(status_payload.get("phase") or "unknown")
        last_progress_at = status_payload.get("last_progress_at")
        progress_anchor = last_progress_at or status_payload.get("event_at") or created_at
        elapsed_since_progress = None
        if progress_anchor:
            elapsed_since_progress = (datetime.now() - datetime.fromisoformat(str(progress_anchor))).total_seconds()
        phase_budget = PHASE_TIMEOUT_SECONDS.get(phase, STALL_SECONDS)
        phase_budget_exceeded = elapsed_since_progress is not None and elapsed_since_progress >= phase_budget
        monitor = {
            "pid": proc.pid,
            "child_alive": child_alive,
            "last_stdout_at": last_stdout_at,
            "stall_reason": stall_reason,
            "phase": phase,
            "phase_budget_seconds": phase_budget,
            "elapsed_since_progress_seconds": elapsed_since_progress,
            "phase_budget_exceeded": phase_budget_exceeded,
        }
        if psutil is not None:
            try:
                p = psutil.Process(proc.pid)
                with p.oneshot():
                    cpu_times = p.cpu_times()
                    cpu_total = float(getattr(cpu_times, "user", 0.0) + getattr(cpu_times, "system", 0.0))
                    cpu_delta = None if last_cpu_total is None else max(cpu_total - last_cpu_total, 0.0)
                    last_cpu_total = cpu_total
                    last_metrics = {"cpu_delta": cpu_delta, "rss": int(p.memory_info().rss), "cwd": p.cwd(), "cmdline": p.cmdline()}
                    monitor.update(last_metrics)
                    if phase == "load_historical" and cpu_delta is not None and cpu_delta > 0:
                        monitor["activity"] = "ACTIVE_COMPUTE"
            except Exception:
                monitor.update(last_metrics)
        else:
            monitor.update(last_metrics)
        actual_result_path = Path(str(status_payload.get("result_path") or "")) if status_payload.get("result_path") else None
        if not child_alive and (actual_result_path is None or not actual_result_path.exists()):
            stall_reason = "FAILED_NO_ARTIFACT"
            monitor["stall_reason"] = stall_reason
        elif phase_budget_exceeded:
            stall_reason = f"PHASE_BUDGET_EXCEEDED:{phase}"
            monitor["stall_reason"] = stall_reason
        _write_json(monitor_path, monitor)
        if stall_reason and status_payload.get("status") != "ok":
            _mark_stall_error(run_dir, stall_reason)
            if child_alive:
                proc.kill()
            break
        if not child_alive:
            break
        time.sleep(MONITOR_INTERVAL_SECONDS)
    return _read_json(monitor_path)


def _child_run(run_dir: Path) -> int:
    manifest_path = run_dir / "manifest.json"
    manifest = _read_json(manifest_path)
    _emit_execution_start(run_dir.parent, manifest_path=manifest_path, extra={"run_label": manifest.get("run_label"), "child_run_label": manifest.get("child_run_label")})
    manifest = _read_json(manifest_path)
    expected_path = Path((run_dir / "result_path_expected").read_text(encoding="utf-8").strip())
    static_payload = {
        "run_label": manifest["run_label"],
        "child_run_label": manifest.get("child_run_label", manifest["run_label"]),
        "scenario_id": manifest["scenario_id"],
        "output_dir": manifest["output_dir"],
        "symbols": manifest["symbols"],
    }
    recorder = ProgressRecorder(run_dir, static_payload)
    recorder.emit(phase="child_start", status="running")
    scenario = _scenario_from_window(manifest["scenario_id"], manifest["window"]["start_date"], manifest["window"]["end_date"], manifest["symbols"])
    metadata = dict(manifest.get("metadata") or {})
    request = RunnerRequest(scenario=scenario, config=BacktestConfig(initial_capital=10000.0, metadata=metadata, research_spec=BASE_SPEC))

    def _progress(update: dict[str, Any]) -> None:
        mapped = {
            "phase": str(update.get("phase") or "running"),
            "status": str(update.get("status") or "running"),
            "loaded_ohlcv_rows": update.get("loaded_ohlcv_rows"),
            "loaded_macro_rows": update.get("loaded_macro_rows"),
            "loaded_sector_rows": update.get("loaded_sector_rows"),
            "event_records_built": update.get("event_records_built"),
            "prototype_batches_built": update.get("prototype_batches_built"),
            "total_trading_dates": update.get("total_trading_dates"),
            "completed_trading_dates": update.get("completed_trading_dates"),
            "candidate_rows": update.get("candidate_rows") or update.get("selected_count_so_far"),
            "plans_count": update.get("plans_count") or update.get("plans_count_so_far"),
            "fills_count": update.get("fills_count") or update.get("fills_count_so_far"),
            "bytes_written": update.get("bytes_written"),
            "current_decision_date": update.get("current_decision_date"),
            "result_path": update.get("result_path"),
        }
        recorder.emit(phase=mapped.pop("phase"), status=mapped.pop("status"), extra=mapped)

    try:
        result = run_backtest(
            request=request,
            data_path=None,
            data_source="local-db",
            scenario_id=scenario.scenario_id,
            strategy_mode="research_similarity_v2",
            output_dir=str(run_dir),
            save_json=True,
            enable_validation=False,
            validation_max_folds=0,
            progress_callback=_progress,
        )
        result_path = Path(str(result.get("result_path") or expected_path))
        bytes_written = _sum_dir_bytes(run_dir)
        recorder.emit(phase="child_finalize", status="running", extra={"result_path": str(result_path), "result_path_expected": str(expected_path), "bytes_written": bytes_written})
        _write_contract_bundle(run_dir=run_dir, run_id=manifest["run_label"], strategy_mode="research_similarity_v2", preflight=manifest["preflight"], scenario=scenario, metadata=metadata, result=result, git_commit=manifest["driver"]["head_commit"])
        if not result_path.exists():
            recorder.emit(phase="failed_no_artifact", status="error", extra={"result_path": str(result_path), "result_path_expected": str(expected_path), "bytes_written": bytes_written, "stall_reason": "FAILED_NO_ARTIFACT"})
            return 1
        recorder.emit(phase="complete", status="ok", extra={"result_path": str(result_path), "bytes_written": _sum_dir_bytes(run_dir)})
        return 0
    except Exception as exc:
        recorder.emit(phase="exception", status="error", extra={"error": repr(exc), "bytes_written": _sum_dir_bytes(run_dir)})
        return 1


def _prepare_child_run(*, output_root: Path, run_label: str, scenario: BacktestScenario, metadata: dict[str, str], preflight: dict[str, Any], driver: dict[str, Any], stage: str, extra_window: dict[str, Any] | None = None) -> tuple[Path, Path]:
    run_dir = output_root / run_label
    if run_dir.exists():
        shutil.rmtree(run_dir)
    expected_path = _result_path_for_run(run_dir, scenario.scenario_id)
    launch_provenance = collect_git_provenance(ROOT)
    manifest = {
        "run_label": run_label,
        "child_run_label": run_label,
        "stage": stage,
        "scenario_id": scenario.scenario_id,
        "symbols": list(scenario.symbols),
        "window": {"start_date": scenario.start_date, "end_date": scenario.end_date, **(extra_window or {})},
        "preflight": preflight,
        "metadata": metadata,
        "output_dir": str(run_dir.resolve()),
        "result_path_expected": str(expected_path.resolve()),
        "result_path_expected_file": str((run_dir / "result_path_expected").resolve()),
        "driver": driver,
        "launch_provenance": launch_provenance,
        "created_at": datetime.now().isoformat(),
    }
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(run_dir / "status.json", {"run_label": run_label, "child_run_label": run_label, "status": "starting", "phase": "parent_prepare", "last_progress_at": None, **_initial_counters()})
    _append_jsonl(run_dir / "progress.jsonl", {"run_label": run_label, "child_run_label": run_label, "status": "starting", "phase": "parent_prepare", "event_at": datetime.now().isoformat(), "last_progress_at": None, **_initial_counters()})
    _write_json(run_dir / "monitor.json", {"pid": None, "child_alive": False, "stall_reason": None, "event_at": datetime.now().isoformat()})
    (run_dir / "result_path_expected").write_text(str(expected_path.resolve()), encoding="utf-8")
    _emit_execution_start(output_root, manifest_path=run_dir / "manifest.json", extra={"run_label": run_label, "child_run_label": run_label})
    return run_dir, expected_path


def _run_prepared_child(run_dir: Path) -> dict[str, Any]:
    proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--child-run", str(run_dir)], cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    monitor = _monitor_child(run_dir, proc)
    exit_code = proc.wait()
    final_status = _read_json(run_dir / "status.json")
    actual_result_path = Path(str(final_status.get("result_path") or "")) if final_status.get("result_path") else None
    success = exit_code == 0 and final_status.get("status") == "ok" and actual_result_path is not None and actual_result_path.exists()
    saved_run = _read_json(actual_result_path) if actual_result_path is not None and actual_result_path.exists() else {}
    reproducibility = _saved_reproducibility(saved_run)
    return {
        "exit_code": exit_code,
        "run_dir": str(run_dir.resolve()),
        "manifest_path": str((run_dir / "manifest.json").resolve()),
        "status_path": str((run_dir / "status.json").resolve()),
        "monitor_path": str((run_dir / "monitor.json").resolve()),
        "progress_path": str((run_dir / "progress.jsonl").resolve()),
        "result_path_expected_file": str((run_dir / "result_path_expected").resolve()),
        "result_path_expected": str(Path((run_dir / "result_path_expected").read_text(encoding="utf-8").strip()).resolve()),
        "run_card_path": str((run_dir / "run_card.json").resolve()),
        "fold_report_path": str((run_dir / "fold_report.json").resolve()),
        "diagnostics_path": str((run_dir / "diagnostics.json").resolve()),
        "report_path": str((run_dir / "report.md").resolve()),
        "status": final_status,
        "monitor": monitor,
        "ok": success,
        "authoritative": bool(reproducibility.get("authoritative", False)),
        "branch": reproducibility.get("branch"),
        "head_commit": reproducibility.get("head_commit"),
        "dirty_worktree": reproducibility.get("dirty_worktree"),
        "changed_tracked_files": reproducibility.get("changed_tracked_files", []),
        "diff_fingerprint": reproducibility.get("diff_fingerprint"),
    }


def _run_baseline_parent(output_root: Path) -> dict[str, Any]:
    driver = _ensure_public_committed_driver()
    window = _resolve_baseline_window(BASELINE_SYMBOLS, BASELINE_TRADING_DATES)
    preflight = preflight_local_db(BASELINE_SYMBOLS)
    metadata = dict(BASE_METADATA)
    metadata["diagnostic_run_label"] = BASELINE_RUN_LABEL
    scenario = _scenario_from_window("medium_viability_contract_baseline", window.start_date, window.end_date, window.symbols)
    run_dir, _ = _prepare_child_run(
        output_root=output_root,
        run_label=BASELINE_RUN_LABEL,
        scenario=scenario,
        metadata=metadata,
        preflight={**preflight, "db_url": window.db_url, "schema": window.schema},
        driver=driver,
        stage="baseline-only",
        extra_window={"trading_dates": window.trading_dates, "trading_date_count": len(window.trading_dates)},
    )
    child_summary = _run_prepared_child(run_dir)
    summary = {
        "baseline_run_completed": child_summary["ok"],
        "baseline_contract_gate_passed": child_summary["ok"] and bool(child_summary.get("authoritative", False)),
        "baseline_gate_fail_reasons": ([] if child_summary["ok"] and bool(child_summary.get("authoritative", False)) else ["child_failed"] if not child_summary["ok"] else ["non_authoritative"]),
        **child_summary,
    }
    _write_json(output_root / "baseline_summary.json", summary)
    return summary


def _run_medium_case(output_root: Path, run_label: str, scenario: BacktestScenario, metadata_overrides: dict[str, str], preflight: dict[str, Any], driver: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(BASE_METADATA)
    metadata.update({k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in metadata_overrides.items()})
    metadata["diagnostic_run_label"] = run_label
    run_dir, _ = _prepare_child_run(
        output_root=output_root,
        run_label=run_label,
        scenario=scenario,
        metadata=metadata,
        preflight=preflight,
        driver=driver,
        stage="medium-run",
    )
    child_summary = _run_prepared_child(run_dir)
    if not child_summary["ok"]:
        metadata_application = {
            "checked": bool({k: v for k, v in metadata.items() if k in ALLOWED_SUPPORT_KEYS}),
            "applied": False,
            "expected": {k: v for k, v in metadata.items() if k in ALLOWED_SUPPORT_KEYS},
            "observed": {},
            "checks": {},
        }
        summary = {
            "run_label": run_label,
            "scenario_id": scenario.scenario_id,
            "result_path": child_summary["status"].get("result_path"),
            "metadata": metadata,
            "candidate_count": 0,
            "fills_count": 0,
            "trades_count": 0,
            "authoritative": bool(child_summary.get("authoritative", False)),
            "branch": child_summary.get("branch"),
            "head_commit": child_summary.get("head_commit"),
            "dirty_worktree": child_summary.get("dirty_worktree"),
            "changed_tracked_files": child_summary.get("changed_tracked_files", []),
            "diff_fingerprint": child_summary.get("diff_fingerprint"),
            "metadata_application": metadata_application,
            "child_summary": child_summary,
        }
        summary["exclusion_reasons"] = _summary_exclusion_reasons(summary)
        summary["verdict_eligible"] = not summary["exclusion_reasons"]
        _write_json(run_dir / "summary.json", summary)
        return summary
    result_path = Path(str(child_summary["status"].get("result_path")))
    result = json.loads(result_path.read_text(encoding="utf-8"))
    summary = _summarize_run(run_label, metadata, result, scenario, result_path=str(result_path.resolve()))
    summary["child_summary"] = child_summary
    summary["exclusion_reasons"] = _summary_exclusion_reasons(summary)
    summary["verdict_eligible"] = not summary["exclusion_reasons"]
    _write_json(run_dir / "summary.json", summary)
    return summary


def _resolve_requested_labels(args: argparse.Namespace) -> list[str]:
    raw = args.run_labels or args.only or "baseline,best1,best2,holdout"
    labels = [x.strip().lower() for x in str(raw).split(",") if x.strip()]
    allowed = {"baseline", "best1", "best2", "holdout"}
    bad = [x for x in labels if x not in allowed]
    if bad:
        raise RuntimeError(f"Unsupported labels: {bad}")
    return labels


def _main_parent(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root).resolve()
    if not output_root.is_absolute():
        raise RuntimeError("--output-root must be an absolute path")
    output_root.mkdir(parents=True, exist_ok=True)
    _emit_execution_start(output_root)
    baseline_summary = _run_baseline_parent(output_root)
    if not baseline_summary.get("baseline_run_completed"):
        print(json.dumps({"baseline": baseline_summary, "aborted_after_baseline": True}, ensure_ascii=False, indent=2, default=_json_default))
        return 1

    driver = _ensure_public_committed_driver()
    preflight = preflight_local_db(UNIVERSE)
    discovery = _scenario_from_window("medium_viability_discovery_20260328", preflight["discovery_start"], preflight["discovery_end"], list(UNIVERSE))
    holdout = _scenario_from_window("medium_viability_holdout_20260328", preflight["holdout_start"], preflight["holdout_end"], list(UNIVERSE))
    tiny_rows = _load_tiny_rows(Path(args.tiny_root).resolve())
    best_two = _pick_best_two_allowed(tiny_rows)
    if len(best_two) < 2:
        raise RuntimeError(f"Expected at least 2 allowed tiny rows, found {len(best_two)}")
    requested = _resolve_requested_labels(args)
    medium_rows: list[dict[str, Any]] = []
    baseline_medium_summary = None
    if "baseline" in requested:
        baseline_medium_summary = _run_medium_case(output_root, "baseline", discovery, {}, preflight, driver)
        medium_rows.append(baseline_medium_summary)
    best1_meta = {k: v for k, v in (best_two[0].get("metadata") or {}).items() if k in ALLOWED_SUPPORT_KEYS}
    best2_meta = {k: v for k, v in (best_two[1].get("metadata") or {}).items() if k in ALLOWED_SUPPORT_KEYS}
    best1_summary = None
    best2_summary = None
    if "best1" in requested:
        best1_summary = _run_medium_case(output_root, "best1", discovery, best1_meta, preflight, driver)
        medium_rows.append(best1_summary)
    if "best2" in requested:
        best2_summary = _run_medium_case(output_root, "best2", discovery, best2_meta, preflight, driver)
        medium_rows.append(best2_summary)
    best_runs = [r for r in [best1_summary, best2_summary] if r is not None]
    eligible_best_runs = [r for r in best_runs if r.get("verdict_eligible")]
    if not best_runs:
        viable = None
        verdict = "best runs not requested"
        verdict_basis = "best1/best2 were not requested, so medium verdict was not evaluated."
    elif not eligible_best_runs:
        viable = None
        verdict = "authoritative rerun required"
        verdict_basis = "best1/best2 completed without any verdict-eligible authoritative runs."
    else:
        viable = any(int(r.get("candidate_count", 0)) > 0 and int(r.get("fills_count", 0)) > 0 and int(r.get("trades_count", 0)) > 0 for r in eligible_best_runs)
        verdict = "TOBE v1 viable" if viable else "current TOBE v1 fails"
        verdict_basis = f"evaluated {len(eligible_best_runs)} authoritative run(s) after excluding non-authoritative or metadata-misaligned runs."
    holdout_summary = None
    if viable is True and "holdout" in requested:
        winner = sorted(eligible_best_runs, key=_rank_key, reverse=True)[0]
        winner_meta = {k: v for k, v in (winner.get("metadata") or {}).items() if k in ALLOWED_SUPPORT_KEYS}
        holdout_summary = _run_medium_case(output_root, "holdout", holdout, winner_meta, preflight, driver)
    summary_payload = {
        "output_root": str(output_root),
        "tiny_root": str(Path(args.tiny_root).resolve()),
        "driver": driver,
        "preflight": preflight,
        "baseline_contract": baseline_summary,
        "selected_best1_source_run_label": best_two[0]["run_label"],
        "selected_best2_source_run_label": best_two[1]["run_label"],
        "best1_support_metadata": best1_meta,
        "best2_support_metadata": best2_meta,
        "medium_runs": medium_rows,
        "eligible_medium_run_labels": [r.get("run_label") for r in eligible_best_runs],
        "verdict": verdict,
        "verdict_basis": verdict_basis,
        "viable": viable,
        "holdout_executed": bool(holdout_summary),
        "holdout_summary": holdout_summary,
    }
    _write_csv_simple(output_root / "medium_viability_summary.csv", medium_rows, SUMMARY_COLS)
    _write_json(output_root / "medium_viability_summary.json", summary_payload)
    diagnosis_lines = [
        "# Medium viability check",
        "",
        f"- Output root: `{output_root}`",
        f"- Tiny root: `{Path(args.tiny_root).resolve()}`",
        f"- scripts/medium_viability_check.py tracked on public branch: yes",
        f"- CLI selective support: `--run-labels` and `--only`",
        f"- Baseline run completed: `{baseline_summary.get('baseline_run_completed')}`",
        f"- Baseline contract gate passed: `{baseline_summary.get('baseline_contract_gate_passed')}`",
        f"- best1 source: `{best_two[0]['run_label']}` -> {json.dumps(best1_meta, ensure_ascii=False)}",
        f"- best2 source: `{best_two[1]['run_label']}` -> {json.dumps(best2_meta, ensure_ascii=False)}",
        "",
        "## Runs",
    ]
    for row in medium_rows:
        diagnosis_lines.append(
            f"- {row['run_label']}: candidates={row['candidate_count']}, fills={row['fills_count']}, trades={row['trades_count']}, authoritative={row.get('authoritative')}, verdict_eligible={row.get('verdict_eligible')}, exclusion_reasons={json.dumps(row.get('exclusion_reasons') or [], ensure_ascii=False)}, result=`{row['result_path']}`"
        )
    diagnosis_lines.append("")
    diagnosis_lines.append(f"## Verdict\n- **{verdict}**")
    diagnosis_lines.append(f"- basis: {verdict_basis}")
    if holdout_summary:
        diagnosis_lines.extend(["", "## Holdout", "- executed: yes", f"- holdout: candidates={holdout_summary['candidate_count']}, fills={holdout_summary['fills_count']}, trades={holdout_summary['trades_count']}, result=`{holdout_summary['result_path']}`"])
    else:
        diagnosis_lines.extend(["", "## Holdout", "- executed: no"])
    (output_root / "diagnosis.md").write_text("\n".join(diagnosis_lines) + "\n", encoding="utf-8")
    print(json.dumps(summary_payload, ensure_ascii=False, indent=2, default=_json_default))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Public-branch medium viability driver")
    parser.add_argument("--child-run", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--output-root", type=str, default=str(OUT_ROOT.resolve()))
    parser.add_argument("--tiny-root", type=str, default=str(DEFAULT_TINY_ROOT.resolve()))
    parser.add_argument("--run-labels", type=str, default=None, help="Comma-separated subset: baseline,best1,best2,holdout")
    parser.add_argument("--only", type=str, default=None, help="Alias of --run-labels")
    parser.add_argument("--alias-baseline", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child_run:
        return _child_run(Path(args.child_run))
    return _main_parent(args)


if __name__ == "__main__":
    raise SystemExit(main())

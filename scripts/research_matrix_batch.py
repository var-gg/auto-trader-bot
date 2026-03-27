from __future__ import annotations

import argparse
import csv
import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any

from scripts.research_first_batch import (
    UNIVERSE,
    append_leaderboard,
    build_report_md,
    build_request,
    build_spec,
    direct_metrics,
    load_leaderboard_rows,
    preflight_local_db,
    stable_hash,
    summarize_regime_split,
    summarize_side_split,
    write_csv,
)
from backtest_app.research_runtime.engine import run_backtest


POLICY_PRESETS = {
    "P1_conservative": {
        "quote_ev_threshold": "0.007",
        "quote_uncertainty_cap": "0.08",
        "quote_min_effective_sample_size": "1.5",
        "quote_min_fill_probability": "0.15",
        "abstain_margin": "0.03",
    },
    "P2_base": {
        "quote_ev_threshold": "0.005",
        "quote_uncertainty_cap": "0.12",
        "quote_min_effective_sample_size": "1.5",
        "quote_min_fill_probability": "0.10",
        "abstain_margin": "0.00",
    },
    "P3_aggressive": {
        "quote_ev_threshold": "0.003",
        "quote_uncertainty_cap": "0.14",
        "quote_min_effective_sample_size": "1.5",
        "quote_min_fill_probability": "0.05",
        "abstain_margin": "0.00",
    },
}

PORTFOLIO_PRESETS = {
    "Q1": {"portfolio_top_n": "2", "portfolio_risk_budget_fraction": "0.45"},
    "Q2": {"portfolio_top_n": "3", "portfolio_risk_budget_fraction": "0.60"},
    "Q3": {"portfolio_top_n": "4", "portfolio_risk_budget_fraction": "0.75"},
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _forced_liquidation_count(result: dict) -> int:
    count = 0
    for plan in result.get("plans") or []:
        if (plan.get("metadata") or {}).get("forced_liquidation"):
            count += 1
    return count


def _avg_holding_days(result: dict) -> float:
    rows = []
    for plan in result.get("plans") or []:
        meta = plan.get("metadata") or {}
        first_fill = meta.get("first_fill_date")
        exit_date = meta.get("realized_exit_date") or meta.get("planned_exit_date")
        if not first_fill or not exit_date:
            continue
        try:
            rows.append((date.fromisoformat(str(exit_date)[:10]) - date.fromisoformat(str(first_fill)[:10])).days)
        except Exception:
            continue
    return sum(rows) / len(rows) if rows else 0.0


def _quote_gap_distribution(result: dict) -> tuple[float, float]:
    gaps = []
    fills_by_plan = {}
    for fill in result.get("fills") or []:
        fills_by_plan.setdefault(fill.get("plan_id"), []).append(fill)
    for plan in result.get("plans") or []:
        requested = float(plan.get("requested_price") or 0.0)
        for fill in fills_by_plan.get(plan.get("plan_id"), []):
            avg = float(fill.get("average_fill_price") or 0.0)
            if requested > 0 and avg > 0:
                gaps.append(abs(avg - requested) / requested)
    if not gaps:
        return 0.0, 0.0
    gaps = sorted(gaps)
    def pct(q: float) -> float:
        idx = min(len(gaps) - 1, max(0, int(round((len(gaps) - 1) * q))))
        return float(gaps[idx])
    return pct(0.50), pct(0.90)


def _abstain_reason_distribution(result: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    decisions = ((result.get("portfolio") or {}).get("decisions") or [])
    skipped = result.get("skipped") or []
    for item in decisions:
        if item.get("selected") is False:
            reason = str(item.get("kill_reason") or "PORTFOLIO")
            out[reason] = out.get(reason, 0) + 1
    for item in skipped:
        reason = str(item.get("code") or item.get("note") or "SKIP")
        out[reason] = out.get(reason, 0) + 1
    return out


def _build_manifest(*, experiment_group: str, run_id: str, label: str, strategy_mode: str, spec, preflight: dict, metadata: dict, discovery_result: dict, holdout_result: dict) -> dict:
    return {
        "experiment_group": experiment_group,
        "run_id": run_id,
        "label": label,
        "strategy_mode": strategy_mode,
        "universe": UNIVERSE,
        "universe_hash": stable_hash(UNIVERSE),
        "spec": spec.to_dict(),
        "spec_hash": spec.spec_hash(),
        "data_snapshot_id": (discovery_result.get("manifest") or {}).get("data_snapshot_id"),
        "discovery_manifest": discovery_result.get("manifest"),
        "holdout_manifest": holdout_result.get("manifest"),
        "preflight": preflight,
        "metadata_overrides": metadata,
    }


def _run_one(*, root: Path, leaderboard_path: Path, experiment_group: str, preflight: dict, spec, label: str, strategy_mode: str, metadata: dict) -> dict:
    universe_hash = stable_hash(UNIVERSE)
    spec_hash = spec.spec_hash()
    run_key = {
        "experiment_group": experiment_group,
        "label": label,
        "strategy_mode": strategy_mode,
        "universe_hash": universe_hash,
        "spec_hash": spec_hash,
        "window": [preflight["discovery_start"], preflight["discovery_end"], preflight["holdout_start"], preflight["holdout_end"]],
        "metadata": metadata,
    }
    run_id = f"{label}_{stable_hash(run_key, 12)}"
    run_dir = root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    discovery_request = build_request(scenario_id=f"{run_id}_discovery", start_date=preflight["discovery_start"], end_date=preflight["discovery_end"], strategy_mode=strategy_mode, spec=spec, metadata=metadata)
    discovery_result = run_backtest(request=discovery_request, data_path=None, data_source="local-db", scenario_id=discovery_request.scenario.scenario_id, strategy_mode=strategy_mode, output_dir=str(run_dir), enable_validation=(strategy_mode == "research_similarity_v2"))
    holdout_request = build_request(scenario_id=f"{run_id}_holdout", start_date=preflight["holdout_start"], end_date=preflight["holdout_end"], strategy_mode=strategy_mode, spec=spec, metadata=metadata)
    holdout_result = run_backtest(request=holdout_request, data_path=None, data_source="local-db", scenario_id=holdout_request.scenario.scenario_id, strategy_mode=strategy_mode, output_dir=str(run_dir), enable_validation=(strategy_mode == "research_similarity_v2"))

    manifest = _build_manifest(experiment_group=experiment_group, run_id=run_id, label=label, strategy_mode=strategy_mode, spec=spec, preflight=preflight, metadata=metadata, discovery_result=discovery_result, holdout_result=holdout_result)
    (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    fold_report = {
        "run_id": run_id,
        "strategy_mode": strategy_mode,
        "discovery": (discovery_result.get("validation") or {}).get("fold_engine") or {},
        "holdout": (holdout_result.get("validation") or {}).get("fold_engine") or {},
    }
    (run_dir / "fold_report.json").write_text(json.dumps(fold_report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    decisions_rows = []
    for phase, result in [("discovery", discovery_result), ("holdout", holdout_result)]:
        for row in ((result.get("portfolio") or {}).get("decisions") or []):
            decisions_rows.append({"phase": phase, **row})
    trades_rows = []
    for phase, result in [("discovery", discovery_result), ("holdout", holdout_result)]:
        for row in result.get("fills") or []:
            trades_rows.append({"phase": phase, **row})
    write_csv(run_dir / "decisions.csv", decisions_rows)
    write_csv(run_dir / "trades.csv", trades_rows)

    diagnostics = {
        "discovery": discovery_result.get("diagnostics"),
        "holdout": holdout_result.get("diagnostics"),
        "validation": discovery_result.get("validation"),
        "holdout_validation": holdout_result.get("validation"),
    }
    (run_dir / "diagnostics.json").write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    discovery_fold = (discovery_result.get("validation") or {}).get("fold_engine") or {}
    holdout_fold = (holdout_result.get("validation") or {}).get("fold_engine") or {}
    discovery_cv = discovery_fold.get("aggregate") or {}
    holdout_direct = direct_metrics(holdout_result, len(UNIVERSE))
    holdout_fold_agg = holdout_fold.get("aggregate") or {}
    holdout_side_source = holdout_result if holdout_result else discovery_result
    side_split = summarize_side_split(holdout_side_source.get("plans") or [])
    regime_split = summarize_regime_split(((holdout_side_source.get("portfolio") or {}).get("decisions") or []))
    q50, q90 = _quote_gap_distribution(holdout_side_source)
    abstain_dist = _abstain_reason_distribution(holdout_side_source)
    run_card = {
        "run_id": run_id,
        "label": label,
        "strategy_mode": strategy_mode,
        "policy_preset": metadata.get("policy_preset", ""),
        "portfolio_preset": metadata.get("portfolio_preset", ""),
        "discovery_start": preflight["discovery_start"],
        "discovery_end": preflight["discovery_end"],
        "holdout_start": preflight["holdout_start"],
        "holdout_end": preflight["holdout_end"],
        "symbols": "|".join(UNIVERSE),
        "feature_window_bars": spec.feature_window_bars,
        "lookback_horizons": "|".join(map(str, spec.lookback_horizons)),
        "horizon_days": spec.horizon_days,
        "target_return_pct": spec.target_return_pct,
        "stop_return_pct": spec.stop_return_pct,
        "flat_return_band_pct": spec.flat_return_band_pct,
        "top_n": metadata.get("portfolio_top_n", "3"),
        "risk_budget_fraction": metadata.get("portfolio_risk_budget_fraction", "0.60"),
        "discovery_cv_expectancy": discovery_cv.get("expectancy_after_cost", direct_metrics(discovery_result, len(UNIVERSE)).get("expectancy_after_cost", 0.0)),
        "discovery_cv_psr": discovery_cv.get("psr", direct_metrics(discovery_result, len(UNIVERSE)).get("psr", 0.0)),
        "discovery_cv_dsr": discovery_cv.get("dsr", direct_metrics(discovery_result, len(UNIVERSE)).get("dsr", 0.0)),
        "holdout_direct_expectancy": holdout_direct.get("expectancy_after_cost", 0.0),
        "trade_count": holdout_direct.get("trade_count", 0),
        "fill_count": holdout_direct.get("fill_count", 0),
        "fill_rate": holdout_direct.get("fill_rate", 0.0),
        "coverage": holdout_direct.get("coverage", 0.0),
        "no_trade_ratio": holdout_direct.get("no_trade_ratio", 0.0),
        "psr": holdout_direct.get("psr", 0.0),
        "dsr": holdout_direct.get("dsr", 0.0),
        "max_drawdown": holdout_direct.get("max_drawdown", 0.0),
        "long_split": side_split.get("long", 0),
        "short_split": side_split.get("short", 0),
        "regime_split": _json_dumps(regime_split),
        "forced_liquidation_count": _forced_liquidation_count(holdout_side_source),
        "avg_holding_days": _avg_holding_days(holdout_side_source),
        "quote_gap_p50": q50,
        "quote_gap_p90": q90,
        "abstain_reason_distribution": _json_dumps(abstain_dist),
        "holdout_fold_expectancy": holdout_fold_agg.get("expectancy_after_cost", 0.0),
        "frozen_validation": all(bool(f.get("artifact", {}).get("test_executed_from_frozen_train_artifacts", False)) for f in discovery_fold.get("folds", [])) if strategy_mode == "research_similarity_v2" else True,
        "legacy_comparable_metrics": True,
        "scenario_end_open_positions_zero": ((holdout_result.get("portfolio") or {}).get("date_artifacts") or [{}])[-1].get("open_position_count", 0) == 0,
        "universe_hash": universe_hash,
        "spec_hash": spec_hash,
        "data_snapshot_id": manifest["data_snapshot_id"],
        "experiment_group": experiment_group,
    }
    (run_dir / "run_card.json").write_text(json.dumps(run_card, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    previous_rows = load_leaderboard_rows(leaderboard_path)
    report_md = build_report_md(run_card={
        "run_id": run_id,
        "strategy_mode": strategy_mode,
        "discovery_start": run_card["discovery_start"],
        "discovery_end": run_card["discovery_end"],
        "holdout_start": run_card["holdout_start"],
        "holdout_end": run_card["holdout_end"],
        "discovery_cv_expectancy_after_cost": run_card["discovery_cv_expectancy"],
        "holdout_direct_expectancy_after_cost": run_card["holdout_direct_expectancy"],
        "holdout_fold_expectancy_after_cost": run_card["holdout_fold_expectancy"],
        "holdout_direct_trade_count": run_card["trade_count"],
        "holdout_direct_fill_count": run_card["fill_count"],
        "holdout_direct_fill_rate": run_card["fill_rate"],
        "holdout_direct_coverage": run_card["coverage"],
        "holdout_direct_no_trade_ratio": run_card["no_trade_ratio"],
        "holdout_direct_psr": run_card["psr"],
        "holdout_direct_dsr": run_card["dsr"],
        "discovery_cv_calibration_error": (discovery_cv.get("calibration_error") if discovery_fold.get("folds") else None),
        "discovery_cv_monotonicity": (discovery_cv.get("score_decile_monotonicity") if discovery_fold.get("folds") else None),
        "holdout_direct_max_drawdown": run_card["max_drawdown"],
    }, discovery=discovery_result, holdout=holdout_result, previous_rows=previous_rows)
    (run_dir / "report.md").write_text(report_md, encoding="utf-8")
    append_leaderboard(leaderboard_path, run_card)
    return run_card


def _delta_row(row: dict, ref: dict, prefix: str) -> dict:
    out = deepcopy(row)
    for key in ["discovery_cv_expectancy", "holdout_direct_expectancy", "trade_count", "fill_rate", "coverage", "no_trade_ratio", "psr", "dsr", "max_drawdown"]:
        out[f"{prefix}_{key}"] = float(row.get(key, 0) or 0) - float(ref.get(key, 0) or 0)
    return out


def _axis_effect_summary(rows: list[dict]) -> dict:
    matrix = [r for r in rows if r.get("policy_preset") and r.get("portfolio_preset")]
    policy = {}
    portfolio = {}
    for label in POLICY_PRESETS:
        vals = [float(r.get("holdout_direct_expectancy", 0) or 0) for r in matrix if r.get("policy_preset") == label]
        policy[label] = {"mean_holdout_direct_expectancy": sum(vals) / len(vals) if vals else 0.0, "count": len(vals)}
    for label in PORTFOLIO_PRESETS:
        vals = [float(r.get("holdout_direct_expectancy", 0) or 0) for r in matrix if r.get("portfolio_preset") == label]
        portfolio[label] = {"mean_holdout_direct_expectancy": sum(vals) / len(vals) if vals else 0.0, "count": len(vals)}
    best_policy = max(policy.items(), key=lambda kv: kv[1]["mean_holdout_direct_expectancy"])[0] if policy else None
    best_portfolio = max(portfolio.items(), key=lambda kv: kv[1]["mean_holdout_direct_expectancy"])[0] if portfolio else None
    return {"policy_effect": policy, "portfolio_effect": portfolio, "best_policy": best_policy, "best_portfolio": best_portfolio}


def _comparison_md(rows: list[dict], axis_summary: dict) -> str:
    legacy = next((r for r in rows if r.get("label") == "legacy_reference"), None)
    base = next((r for r in rows if r.get("label") == "tobe_base_reference"), None)
    failures = [r["run_id"] for r in rows if (not bool(r.get("frozen_validation", True))) or (not bool(r.get("legacy_comparable_metrics", False))) or (not bool(r.get("scenario_end_open_positions_zero", False)))]
    best = max(rows, key=lambda r: float(r.get("holdout_direct_expectancy", -999) or -999)) if rows else None
    lines = [
        "# Policy × Portfolio matrix comparison",
        "",
        f"- total_runs: {len(rows)}",
        f"- best_policy: {axis_summary.get('best_policy')}",
        f"- best_portfolio: {axis_summary.get('best_portfolio')}",
        f"- failed_runs: {'|'.join(failures) if failures else 'none'}",
        "",
        "## References",
        f"- legacy_reference holdout_direct_expectancy: {legacy.get('holdout_direct_expectancy') if legacy else 'n/a'}",
        f"- tobe_base_reference holdout_direct_expectancy: {base.get('holdout_direct_expectancy') if base else 'n/a'}",
        "",
        "## Readout",
    ]
    if best:
        lines.append(f"- best run: {best['run_id']} ({best.get('policy_preset','ref')} / {best.get('portfolio_preset','ref')}) with holdout_direct_expectancy={best.get('holdout_direct_expectancy')}")
    if axis_summary.get("best_policy") and axis_summary.get("best_portfolio"):
        lines.append(f"- current evidence says policy={axis_summary['best_policy']} and portfolio={axis_summary['best_portfolio']} move holdout expectancy the most.")
    if base and best and float(best.get("holdout_direct_expectancy", 0) or 0) > float(base.get("holdout_direct_expectancy", 0) or 0):
        lines.append("- At least one matrix run improved over TOBE-base; feature work can wait until policy/portfolio is exhausted.")
    else:
        lines.append("- No matrix run clearly beat TOBE-base; do not escalate to feature changes until failure mode is read from coverage / fill-rate / drawdown / forced liquidation.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run policy x portfolio research matrix batch")
    parser.add_argument("--output-root", default="runs/research_ledger")
    parser.add_argument("--experiment-group", default="")
    args = parser.parse_args()

    preflight = preflight_local_db(UNIVERSE)
    spec = build_spec()
    today_tag = date.today().strftime("%Y%m%d")
    experiment_group = args.experiment_group or f"matrix_batch_{today_tag}_{preflight['window_mode']}"
    root = Path(args.output_root) / experiment_group
    root.mkdir(parents=True, exist_ok=True)
    (root / "preflight.json").write_text(json.dumps(preflight, ensure_ascii=False, indent=2), encoding="utf-8")
    leaderboard_path = root / "leaderboard.csv"

    rows = []
    rows.append(_run_one(root=root, leaderboard_path=leaderboard_path, experiment_group=experiment_group, preflight=preflight, spec=spec, label="legacy_reference", strategy_mode="legacy_event_window", metadata={"portfolio_top_n": "3", "portfolio_risk_budget_fraction": "0.60"}))
    base_meta = {**POLICY_PRESETS["P2_base"], **PORTFOLIO_PRESETS["Q2"], "policy_preset": "P2_base", "portfolio_preset": "Q2"}
    rows.append(_run_one(root=root, leaderboard_path=leaderboard_path, experiment_group=experiment_group, preflight=preflight, spec=spec, label="tobe_base_reference", strategy_mode="research_similarity_v2", metadata=base_meta))

    for policy_name, policy_meta in POLICY_PRESETS.items():
        for portfolio_name, portfolio_meta in PORTFOLIO_PRESETS.items():
            meta = {**policy_meta, **portfolio_meta, "policy_preset": policy_name, "portfolio_preset": portfolio_name}
            rows.append(_run_one(root=root, leaderboard_path=leaderboard_path, experiment_group=experiment_group, preflight=preflight, spec=spec, label=f"matrix_{policy_name}_{portfolio_name}", strategy_mode="research_similarity_v2", metadata=meta))

    legacy = next(r for r in rows if r["label"] == "legacy_reference")
    base = next(r for r in rows if r["label"] == "tobe_base_reference")
    comparison_rows = []
    for row in rows:
        row = _delta_row(row, legacy, "delta_vs_legacy")
        row = _delta_row(row, base, "delta_vs_tobe_base")
        comparison_rows.append(row)
    write_csv(root / "comparison_table.csv", comparison_rows)

    axis_summary = _axis_effect_summary(rows)
    (root / "axis_effect_summary.json").write_text(json.dumps(axis_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (root / "comparison.md").write_text(_comparison_md(rows, axis_summary), encoding="utf-8")

    failures = [r for r in rows if (not bool(r.get("frozen_validation", True))) or (not bool(r.get("legacy_comparable_metrics", False))) or (not bool(r.get("scenario_end_open_positions_zero", False)))]
    if failures:
        raise SystemExit(f"matrix batch failure: {[r['run_id'] for r in failures]}")
    print(f"comparison_table: {root / 'comparison_table.csv'}")
    print(f"comparison_md: {root / 'comparison.md'}")
    print(f"axis_effect_summary: {root / 'axis_effect_summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

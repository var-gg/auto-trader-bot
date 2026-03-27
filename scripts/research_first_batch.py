from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text

from backtest_app.configs.models import BacktestConfig, BacktestScenario, ResearchExperimentSpec, RunnerRequest
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.research_runtime.engine import run_backtest
from backtest_app.validation import compute_performance_metrics
from shared.domain.models import FillOutcome, OrderPlan

UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "JPM", "XOM", "LLY", "UNH", "COST", "PG"]


def stable_hash(payload: Any, length: int = 16) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()[:length]


def preflight_local_db(symbols: list[str]) -> dict:
    cfg = LocalBacktestDbConfig.from_env()
    guard_backtest_local_only(cfg.url)
    session_factory = create_backtest_session_factory(cfg)
    sql = text(
        f"""
        SELECT trade_date, COUNT(DISTINCT symbol) AS n
        FROM {cfg.schema}.bt_mirror_ohlcv_daily
        WHERE symbol = ANY(:symbols)
        GROUP BY trade_date
        HAVING COUNT(DISTINCT symbol) = :symbol_count
        ORDER BY trade_date
        """
    )
    with session_factory() as session:
        rows = [r._mapping for r in session.execute(sql, {"symbols": symbols, "symbol_count": len(symbols)})]
    if not rows:
        raise RuntimeError("No common daily coverage found for requested universe in local-db")
    dates = [str(r["trade_date"]) for r in rows]
    first_date, latest_date = dates[0], dates[-1]
    latest_dt = datetime.fromisoformat(latest_date).date()
    preferred_start = (latest_dt - timedelta(days=365)).isoformat()
    preferred_disc_end = (latest_dt - timedelta(days=92)).isoformat()
    fallback_start = (latest_dt - timedelta(days=244)).isoformat()
    fallback_disc_end = (latest_dt - timedelta(days=61)).isoformat()
    preferred_ok = preferred_start >= first_date
    return {
        "db_url": cfg.url,
        "schema": cfg.schema,
        "first_date": first_date,
        "latest_date": latest_date,
        "preferred_ok": preferred_ok,
        "discovery_start": preferred_start if preferred_ok else fallback_start,
        "discovery_end": preferred_disc_end if preferred_ok else fallback_disc_end,
        "holdout_start": (datetime.fromisoformat(preferred_disc_end if preferred_ok else fallback_disc_end).date() + timedelta(days=1)).isoformat(),
        "holdout_end": latest_date,
        "window_mode": "9m_3m" if preferred_ok else "6m_2m",
    }


def build_spec() -> ResearchExperimentSpec:
    return ResearchExperimentSpec(
        feature_window_bars=60,
        lookback_horizons=[1, 3, 5, 10, 20, 60],
        horizon_days=5,
        target_return_pct=0.04,
        stop_return_pct=0.03,
        flat_return_band_pct=0.005,
    )


def run_configs() -> list[dict]:
    return [
        {"label": "legacy_event_window", "strategy_mode": "legacy_event_window", "metadata": {"portfolio_top_n": "3", "portfolio_risk_budget_fraction": "0.60"}},
        {"label": "research_similarity_v2_base", "strategy_mode": "research_similarity_v2", "metadata": {"portfolio_top_n": "3", "portfolio_risk_budget_fraction": "0.60", "quote_ev_threshold": "0.005", "quote_uncertainty_cap": "0.12", "quote_min_fill_probability": "0.10", "abstain_margin": "0.00"}},
        {"label": "research_similarity_v2_conservative", "strategy_mode": "research_similarity_v2", "metadata": {"portfolio_top_n": "2", "portfolio_risk_budget_fraction": "0.45", "quote_ev_threshold": "0.007", "quote_uncertainty_cap": "0.08", "quote_min_fill_probability": "0.15", "abstain_margin": "0.03"}},
        {"label": "research_similarity_v2_aggressive", "strategy_mode": "research_similarity_v2", "metadata": {"portfolio_top_n": "4", "portfolio_risk_budget_fraction": "0.75", "quote_ev_threshold": "0.003", "quote_uncertainty_cap": "0.14", "quote_min_fill_probability": "0.05", "abstain_margin": "0.00"}},
    ]


def build_request(*, scenario_id: str, start_date: str, end_date: str, strategy_mode: str, spec: ResearchExperimentSpec, metadata: dict[str, str]) -> RunnerRequest:
    scenario = BacktestScenario(scenario_id=scenario_id, market="US", start_date=start_date, end_date=end_date, symbols=UNIVERSE)
    config = BacktestConfig(initial_capital=10000.0, research_spec=spec, metadata=metadata)
    return RunnerRequest(scenario=scenario, config=config)


def summarize_side_split(plans: list[dict]) -> dict[str, int]:
    out = {"long": 0, "short": 0}
    for p in plans:
        side = str(p.get("side") or "")
        if side == "BUY":
            out["long"] += 1
        elif side == "SELL":
            out["short"] += 1
    return out


def summarize_regime_split(decisions: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for d in decisions:
        regime = (((d.get("diagnostics") or {}).get("query") or {}).get("regime_code") if isinstance(d.get("diagnostics"), dict) else None) or "UNKNOWN"
        out[regime] = out.get(regime, 0) + 1
    return out


def _plan_obj(payload: dict) -> OrderPlan:
    return OrderPlan.from_dict(payload)


def _fill_obj(payload: dict) -> FillOutcome:
    return FillOutcome.from_dict(payload)


def direct_metrics(result: dict, total_symbols: int) -> dict:
    plans = [_plan_obj(p) for p in (result.get("plans") or [])]
    fills = [_fill_obj(f) for f in (result.get("fills") or [])]
    bars_by_symbol = result.get("bars_by_symbol") or (result.get("historical_context") or {}).get("bars_by_symbol") or (result.get("artifacts") or {}).get("historical_context", {}).get("bars_by_symbol") or {}
    metrics = compute_performance_metrics(plans=plans, fills=fills, bars_by_symbol=bars_by_symbol, total_symbols=total_symbols)
    metrics["trade_count"] = len(plans)
    metrics["fill_count"] = len(fills)
    metrics["fill_rate"] = (sum(1 for p in plans if any(f.plan_id == p.plan_id for f in fills)) / max(len(plans), 1)) if plans else 0.0
    return metrics


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({k for r in rows for k in r.keys()})
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def append_leaderboard(path: Path, row: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.exists()
    fields = list(row.keys())
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not existing:
            writer.writeheader()
        writer.writerow(row)


def build_report_md(*, run_card: dict, discovery: dict, holdout: dict, previous_rows: list[dict]) -> str:
    prev = previous_rows[-1] if previous_rows else None
    lines = [
        f"# Report — {run_card['run_id']}",
        "",
        f"- strategy_mode: {run_card['strategy_mode']}",
        f"- discovery: {run_card['discovery_start']} ~ {run_card['discovery_end']}",
        f"- holdout: {run_card['holdout_start']} ~ {run_card['holdout_end']}",
        f"- discovery_cv_expectancy_after_cost: {run_card['discovery_cv_expectancy_after_cost']}",
        f"- holdout_direct_expectancy_after_cost: {run_card['holdout_direct_expectancy_after_cost']}",
        f"- holdout_fold_expectancy_after_cost: {run_card['holdout_fold_expectancy_after_cost']}",
        f"- trade_count: {run_card['holdout_direct_trade_count']}",
        f"- fill_count: {run_card['holdout_direct_fill_count']}",
        f"- fill_rate: {run_card['holdout_direct_fill_rate']}",
        f"- holdout_direct_coverage: {run_card['holdout_direct_coverage']}",
        f"- holdout_direct_no_trade_ratio: {run_card['holdout_direct_no_trade_ratio']}",
        f"- holdout_direct_psr/dsr: {run_card['holdout_direct_psr']} / {run_card['holdout_direct_dsr']}",
        f"- discovery_cv_calibration_error: {run_card['discovery_cv_calibration_error']}",
        f"- discovery_cv_monotonicity: {run_card['discovery_cv_monotonicity']}",
        f"- holdout_direct_max_drawdown: {run_card['holdout_direct_max_drawdown']}",
        "",
        "## What improved",
    ]
    if prev:
        try:
            delta = float(run_card["expectancy_after_cost"] or 0) - float(prev.get("expectancy_after_cost") or 0)
            lines.append(f"- expectancy delta vs previous ledger row: {delta:.6f}")
        except Exception:
            lines.append("- previous ledger row exists but numeric delta unavailable")
    else:
        lines.append("- first row for this ledger; no prior comparison")
    lines += [
        "",
        "## What degraded / risk flags",
        f"- fold_non_empty: {bool(discovery.get('validation', {}).get('fold_engine', {}).get('folds'))}",
        f"- frozen_validation: {all(bool(f.get('artifact', {}).get('test_executed_from_frozen_train_artifacts', False)) for f in discovery.get('validation', {}).get('fold_engine', {}).get('folds', []))}",
        f"- scenario_end_open_positions_zero: {discovery.get('portfolio', {}).get('date_artifacts', [{}])[-1].get('open_position_count', 0) == 0 if discovery.get('portfolio', {}).get('date_artifacts') else True}",
        f"- holdout_direct_vs_fold_gap: {run_card['holdout_direct_expectancy_after_cost']} vs {run_card['holdout_fold_expectancy_after_cost']}",
        "",
        "## Recommendation",
        "- Use leaderboard.csv to compare policy variants first.",
        "- If coverage/no_trade is the main mover, tune policy/portfolio before feature changes.",
    ]
    return "\n".join(lines) + "\n"


def load_leaderboard_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run first research batch with standardized ledger outputs")
    parser.add_argument("--output-root", default="runs/research_ledger")
    parser.add_argument("--experiment-group", default="")
    parser.add_argument("--skip-holdout", action="store_true")
    args = parser.parse_args()

    preflight = preflight_local_db(UNIVERSE)
    spec = build_spec()
    today_tag = date.today().strftime("%Y%m%d")
    experiment_group = args.experiment_group or f"first_batch_{today_tag}_{preflight['window_mode']}"
    universe_hash = stable_hash(UNIVERSE)
    spec_hash = spec.spec_hash()
    root = Path(args.output_root) / experiment_group
    root.mkdir(parents=True, exist_ok=True)
    (root / "preflight.json").write_text(json.dumps(preflight, ensure_ascii=False, indent=2), encoding="utf-8")
    leaderboard_path = root / "leaderboard.csv"

    for cfg in run_configs():
        run_key = {
            "experiment_group": experiment_group,
            "label": cfg["label"],
            "strategy_mode": cfg["strategy_mode"],
            "universe_hash": universe_hash,
            "spec_hash": spec_hash,
            "window": [preflight["discovery_start"], preflight["discovery_end"], preflight["holdout_start"], preflight["holdout_end"]],
            "metadata": cfg["metadata"],
        }
        run_id = f"{cfg['label']}_{stable_hash(run_key, 12)}"
        run_dir = root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        discovery_request = build_request(scenario_id=f"{run_id}_discovery", start_date=preflight["discovery_start"], end_date=preflight["discovery_end"], strategy_mode=cfg["strategy_mode"], spec=spec, metadata=cfg["metadata"])
        discovery_result = run_backtest(request=discovery_request, data_path=None, data_source="local-db", scenario_id=discovery_request.scenario.scenario_id, strategy_mode=cfg["strategy_mode"], output_dir=str(run_dir), enable_validation=(cfg["strategy_mode"] == "research_similarity_v2"))

        holdout_result = {}
        if not args.skip_holdout:
            holdout_request = build_request(scenario_id=f"{run_id}_holdout", start_date=preflight["holdout_start"], end_date=preflight["holdout_end"], strategy_mode=cfg["strategy_mode"], spec=spec, metadata=cfg["metadata"])
            holdout_result = run_backtest(request=holdout_request, data_path=None, data_source="local-db", scenario_id=holdout_request.scenario.scenario_id, strategy_mode=cfg["strategy_mode"], output_dir=str(run_dir), enable_validation=(cfg["strategy_mode"] == "research_similarity_v2"))

        manifest = {
            "experiment_group": experiment_group,
            "run_id": run_id,
            "label": cfg["label"],
            "strategy_mode": cfg["strategy_mode"],
            "universe": UNIVERSE,
            "universe_hash": universe_hash,
            "spec": asdict(spec),
            "spec_hash": spec_hash,
            "data_snapshot_id": (discovery_result.get("manifest") or {}).get("data_snapshot_id"),
            "discovery_manifest": discovery_result.get("manifest"),
            "holdout_manifest": holdout_result.get("manifest"),
            "preflight": preflight,
            "metadata_overrides": cfg["metadata"],
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        fold_engine = (discovery_result.get("validation") or {}).get("fold_engine") or {}
        fold_report = {
            "run_id": run_id,
            "strategy_mode": cfg["strategy_mode"],
            "discovery": fold_engine,
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

        aggregate = fold_engine.get("aggregate") or {}
        holdout_aggregate = ((holdout_result.get("validation") or {}).get("fold_engine") or {}).get("aggregate") or {}
        discovery_direct = direct_metrics(discovery_result, len(UNIVERSE))
        holdout_direct = direct_metrics(holdout_result, len(UNIVERSE)) if holdout_result else {}
        side_split = summarize_side_split(holdout_result.get("plans") or discovery_result.get("plans") or [])
        regime_split = summarize_regime_split(((holdout_result.get("portfolio") or {}).get("decisions") or (discovery_result.get("portfolio") or {}).get("decisions") or []))
        run_card = {
            "run_id": run_id,
            "strategy_mode": cfg["strategy_mode"],
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
            "top_n": cfg["metadata"].get("portfolio_top_n", "3"),
            "risk_budget_fraction": cfg["metadata"].get("portfolio_risk_budget_fraction", "0.60"),
            "discovery_cv_expectancy_after_cost": aggregate.get("expectancy_after_cost", discovery_direct.get("expectancy_after_cost", 0.0)),
            "discovery_cv_psr": aggregate.get("psr", discovery_direct.get("psr", 0.0)),
            "discovery_cv_dsr": aggregate.get("dsr", discovery_direct.get("dsr", 0.0)),
            "discovery_cv_calibration_error": aggregate.get("calibration_error") if fold_engine.get("folds") else None,
            "discovery_cv_monotonicity": aggregate.get("score_decile_monotonicity") if fold_engine.get("folds") else None,
            "discovery_cv_max_drawdown": discovery_direct.get("max_drawdown", 0.0),
            "holdout_direct_trade_count": holdout_direct.get("trade_count", 0),
            "holdout_direct_fill_count": holdout_direct.get("fill_count", 0),
            "holdout_direct_fill_rate": holdout_direct.get("fill_rate", 0.0),
            "holdout_direct_coverage": holdout_direct.get("coverage", 0.0),
            "holdout_direct_no_trade_ratio": holdout_direct.get("no_trade_ratio", 0.0),
            "holdout_direct_expectancy_after_cost": holdout_direct.get("expectancy_after_cost", 0.0),
            "holdout_direct_psr": holdout_direct.get("psr", 0.0),
            "holdout_direct_dsr": holdout_direct.get("dsr", 0.0),
            "holdout_direct_max_drawdown": holdout_direct.get("max_drawdown", 0.0),
            "holdout_fold_expectancy_after_cost": holdout_aggregate.get("expectancy_after_cost", 0.0),
            "long_split": side_split.get("long", 0),
            "short_split": side_split.get("short", 0),
            "regime_split": json.dumps(regime_split, ensure_ascii=False),
            "universe_hash": universe_hash,
            "spec_hash": spec_hash,
            "data_snapshot_id": manifest["data_snapshot_id"],
            "experiment_group": experiment_group,
        }
        (run_dir / "run_card.json").write_text(json.dumps(run_card, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        previous_rows = load_leaderboard_rows(leaderboard_path)
        report_md = build_report_md(run_card=run_card, discovery=discovery_result, holdout=holdout_result, previous_rows=previous_rows)
        (run_dir / "report.md").write_text(report_md, encoding="utf-8")
        append_leaderboard(leaderboard_path, run_card)
        print(f"completed {run_id}")

    print(f"leaderboard: {leaderboard_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import time
import warnings

from dotenv import load_dotenv
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

load_dotenv()

UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "JPM", "XOM", "LLY", "UNH", "COST", "PG"]
MISSING_LEGACY_SNAPSHOT_MESSAGE = "materialize_bt_event_window 먼저 실행 또는 --skip-legacy-reference 사용"
MANIFEST_TABLE = "meta.bt_scenario_snapshot_manifest"


def stable_hash(payload: Any, length: int = 16) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()[:length]


def resolve_git_commit() -> str:
    try:
        proc = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
        return proc.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def _stage_timer(label: str):
    started = time.perf_counter()

    def _done(extra: str = ""):
        elapsed = time.perf_counter() - started
        suffix = f" | {extra}" if extra else ""
        print(f"[{label}] {elapsed:.3f}s{suffix}")

    return _done


def fold_report_summary(payload: dict | None) -> dict:
    payload = payload or {}
    folds = []
    for fold in payload.get("folds") or []:
        folds.append({
            "fold_id": fold.get("fold_id"),
            "split": fold.get("split"),
            "train_metrics": fold.get("train_metrics"),
            "test_metrics": fold.get("test_metrics"),
            "leakage_ok": fold.get("leakage_ok"),
            "rejection_reasons": fold.get("rejection_reasons"),
            "calibration": fold.get("calibration"),
            "artifact": fold.get("artifact"),
        })
    return {
        "mode": payload.get("mode"),
        "purge": payload.get("purge"),
        "embargo": payload.get("embargo"),
        "aggregate": payload.get("aggregate") or {},
        "rejection_reasons": payload.get("rejection_reasons") or [],
        "folds": folds,
    }


def preflight_local_db(symbols: list[str], *, allow_unknown_sector: bool = False) -> dict:
    cfg = LocalBacktestDbConfig.from_env()
    guard_backtest_local_only(cfg.url)
    session_factory = create_backtest_session_factory(cfg)
    common_ohlcv_sql = text(
        f"""
        SELECT trade_date, COUNT(DISTINCT symbol) AS n
        FROM {cfg.schema}.bt_mirror_ohlcv_daily
        WHERE symbol = ANY(:symbols)
        GROUP BY trade_date
        HAVING COUNT(DISTINCT symbol) = :symbol_count
        ORDER BY trade_date
        """
    )
    latest_macro_sql = text(
        f"""
        SELECT COUNT(*) AS macro_series_count,
               MAX(v.obs_date) AS latest_macro_date
          FROM {cfg.schema}.macro_data_series s
          JOIN {cfg.schema}.macro_data_series_value v ON v.series_id = s.id
         WHERE s.is_active IS DISTINCT FROM FALSE
        """
    )
    sector_sql = text(
        f"""
        SELECT COUNT(DISTINCT t.symbol) AS covered_symbols
          FROM {cfg.schema}.bt_mirror_ticker t
          JOIN {cfg.schema}.bt_mirror_ticker_industry ti ON ti.ticker_id = t.ticker_id
          JOIN {cfg.schema}.bt_mirror_industry i ON i.industry_id = ti.industry_id
          JOIN {cfg.schema}.bt_mirror_sector s ON s.sector_id = i.sector_id
         WHERE t.symbol = ANY(:symbols)
        """
    )
    legacy_snapshot_sql = text(
        f"""
        SELECT COUNT(*) AS snapshot_rows,
               MIN(reference_date) AS first_snapshot_date,
               MAX(reference_date) AS latest_snapshot_date
          FROM {cfg.schema}.bt_event_window
         WHERE market = 'US'
           AND symbol = ANY(:symbols)
        """
    )
    manifest_exists_sql = text(
        """
        SELECT EXISTS (
            SELECT 1
              FROM information_schema.tables
             WHERE table_schema = 'meta'
               AND table_name = 'bt_scenario_snapshot_manifest'
        ) AS manifest_exists
        """
    )
    with session_factory() as session:
        rows = [r._mapping for r in session.execute(common_ohlcv_sql, {"symbols": symbols, "symbol_count": len(symbols)})]
        macro_row = session.execute(latest_macro_sql).one()._mapping
        sector_row = session.execute(sector_sql, {"symbols": symbols}).one()._mapping
        legacy_row = session.execute(legacy_snapshot_sql, {"symbols": symbols}).one()._mapping
        manifest_exists = bool(session.execute(manifest_exists_sql).one()._mapping.get("manifest_exists"))
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
    macro_series_count = int(macro_row.get("macro_series_count") or 0)
    latest_macro_date = str(macro_row.get("latest_macro_date") or "")
    macro_coverage = 1.0 if macro_series_count > 0 and latest_macro_date else 0.0
    covered_symbols = int(sector_row.get("covered_symbols") or 0)
    sector_coverage = covered_symbols / max(len(symbols), 1)
    snapshot_rows = int(legacy_row.get("snapshot_rows") or 0)
    legacy_snapshot_ready = snapshot_rows > 0
    out = {
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
        "ohlcv_common_coverage": 1.0,
        "ohlcv_common_first_date": first_date,
        "ohlcv_common_latest_date": latest_date,
        "macro_coverage": macro_coverage,
        "macro_series_count": macro_series_count,
        "latest_macro_date": latest_macro_date or None,
        "sector_coverage": sector_coverage,
        "sector_covered_symbols": covered_symbols,
        "sector_expected_symbols": len(symbols),
        "legacy_snapshot_ready": legacy_snapshot_ready,
        "legacy_snapshot_rows": snapshot_rows,
        "legacy_snapshot_first_date": str(legacy_row.get("first_snapshot_date") or "") or None,
        "legacy_snapshot_latest_date": str(legacy_row.get("latest_snapshot_date") or "") or None,
        "snapshot_manifest_table_ready": manifest_exists,
        "allow_unknown_sector": allow_unknown_sector,
    }
    if macro_coverage < 1.0:
        raise RuntimeError("Local-db preflight failed: macro coverage is incomplete or missing")
    if sector_coverage < 1.0:
        message = f"Local-db preflight failed: sector coverage {sector_coverage:.3f} ({covered_symbols}/{len(symbols)}) is incomplete"
        if allow_unknown_sector:
            warnings.warn(message + "; continuing because --allow-unknown-sector was set")
        else:
            raise RuntimeError(message)
    return out


def build_spec() -> ResearchExperimentSpec:
    return ResearchExperimentSpec(
        feature_window_bars=60,
        lookback_horizons=[1, 3, 5, 10, 20, 60],
        horizon_days=5,
        target_return_pct=0.04,
        stop_return_pct=0.03,
        flat_return_band_pct=0.005,
    )


def run_configs(*, include_legacy: bool = True, smoke_fast: bool = False) -> list[dict]:
    configs = [
        {"label": "research_similarity_v2_base", "strategy_mode": "research_similarity_v2", "metadata": {"portfolio_top_n": "3", "portfolio_risk_budget_fraction": "0.60", "quote_ev_threshold": "0.005", "quote_uncertainty_cap": "0.12", "quote_min_fill_probability": "0.10", "abstain_margin": "0.00"}},
        {"label": "research_similarity_v2_conservative", "strategy_mode": "research_similarity_v2", "metadata": {"portfolio_top_n": "2", "portfolio_risk_budget_fraction": "0.45", "quote_ev_threshold": "0.007", "quote_uncertainty_cap": "0.08", "quote_min_fill_probability": "0.15", "abstain_margin": "0.03"}},
        {"label": "research_similarity_v2_aggressive", "strategy_mode": "research_similarity_v2", "metadata": {"portfolio_top_n": "4", "portfolio_risk_budget_fraction": "0.75", "quote_ev_threshold": "0.003", "quote_uncertainty_cap": "0.14", "quote_min_fill_probability": "0.05", "abstain_margin": "0.00"}},
    ]
    if smoke_fast:
        return [configs[0]]
    if include_legacy:
        return [{"label": "legacy_event_window", "strategy_mode": "legacy_event_window", "metadata": {"portfolio_top_n": "3", "portfolio_risk_budget_fraction": "0.60"}}, *configs]
    return configs


def build_request(*, scenario_id: str, start_date: str, end_date: str, strategy_mode: str, spec: ResearchExperimentSpec, metadata: dict[str, str], symbols: list[str]) -> RunnerRequest:
    scenario = BacktestScenario(scenario_id=scenario_id, market="US", start_date=start_date, end_date=end_date, symbols=symbols)
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
        f"- ohlcv_common_coverage: {run_card.get('ohlcv_common_coverage')}",
        f"- macro_coverage: {run_card.get('macro_coverage')}",
        f"- sector_coverage: {run_card.get('sector_coverage')}",
        f"- legacy_snapshot_ready: {run_card.get('legacy_snapshot_ready')}",
        f"- bt_event_window_snapshot_id: {run_card.get('bt_event_window_snapshot_id')}",
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


def fetch_snapshot_manifest(*, scenario_id: str) -> dict | None:
    cfg = LocalBacktestDbConfig.from_env()
    guard_backtest_local_only(cfg.url)
    session_factory = create_backtest_session_factory(cfg)
    sql = text(
        f"""
        SELECT snapshot_id, scenario_id, phase, source_kind, market, window_start, window_end,
               universe_hash, spec_hash, row_count, created_at, notes
          FROM {MANIFEST_TABLE}
         WHERE scenario_id = :scenario_id
        """
    )
    try:
        with session_factory() as session:
            row = session.execute(sql, {"scenario_id": scenario_id}).fetchone()
    except Exception:
        return None
    return dict(row._mapping) if row else None


def require_snapshot_manifest(*, scenario_id: str, phase: str) -> dict:
    manifest = fetch_snapshot_manifest(scenario_id=scenario_id)
    if not manifest:
        raise RuntimeError(f"{phase} snapshot missing for scenario_id={scenario_id}. {MISSING_LEGACY_SNAPSHOT_MESSAGE}")
    return manifest


def resolve_legacy_scenarios(*, preflight: dict, discovery_scenario_id: str, holdout_scenario_id: str, skip_legacy_reference: bool) -> dict:
    if skip_legacy_reference:
        return {"skip_legacy_reference": True, "discovery": None, "holdout": None}
    discovery = require_snapshot_manifest(scenario_id=discovery_scenario_id, phase="discovery")
    holdout = require_snapshot_manifest(scenario_id=holdout_scenario_id, phase="holdout")
    preflight["legacy_snapshot_ready"] = True
    preflight["legacy_snapshot_rows"] = int(discovery.get("row_count") or 0) + int(holdout.get("row_count") or 0)
    preflight["legacy_snapshot_first_date"] = str(discovery.get("window_start") or preflight.get("legacy_snapshot_first_date"))
    preflight["legacy_snapshot_latest_date"] = str(holdout.get("window_end") or preflight.get("legacy_snapshot_latest_date"))
    return {"skip_legacy_reference": False, "discovery": discovery, "holdout": holdout}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run first research batch with standardized ledger outputs")
    parser.add_argument("--output-root", default="runs/research_ledger")
    parser.add_argument("--experiment-group", default="")
    parser.add_argument("--skip-holdout", action="store_true")
    parser.add_argument("--allow-unknown-sector", action="store_true")
    parser.add_argument("--legacy-discovery-scenario-id", default="legacy_discovery")
    parser.add_argument("--legacy-holdout-scenario-id", default="legacy_holdout")
    parser.add_argument("--skip-legacy-reference", action="store_true")
    parser.add_argument("--smoke-fast", action="store_true", help="PC-local survival smoke: base policy only, skip holdout, max 1 validation fold")
    parser.add_argument("--smoke-universe-size", type=int, default=6, help="Universe size to use when --smoke-fast is enabled")
    parser.add_argument("--smoke-lookback-days", type=int, default=45, help="Discovery lookback window in calendar days for --smoke-fast")
    parser.add_argument("--validation-summary-only", action="store_true", help="Persist only compact fold summaries")
    parser.add_argument("--diagnostics-lite", action="store_true", help="Persist compact diagnostics/signal summaries instead of full panels")
    args = parser.parse_args()

    if args.smoke_fast:
        args.skip_holdout = True
        args.skip_legacy_reference = True
        args.validation_summary_only = True
        args.diagnostics_lite = True

    universe = UNIVERSE[: max(1, args.smoke_universe_size)] if args.smoke_fast else list(UNIVERSE)
    preflight_timer = _stage_timer("preflight")
    preflight = preflight_local_db(universe, allow_unknown_sector=args.allow_unknown_sector)
    if args.smoke_fast:
        latest_dt = datetime.fromisoformat(preflight["latest_date"]).date()
        first_dt = datetime.fromisoformat(preflight["first_date"]).date()
        smoke_start = max(first_dt, latest_dt - timedelta(days=max(7, args.smoke_lookback_days)))
        preflight["discovery_start"] = smoke_start.isoformat()
        preflight["discovery_end"] = latest_dt.isoformat()
        preflight["holdout_start"] = latest_dt.isoformat()
        preflight["holdout_end"] = latest_dt.isoformat()
        preflight["window_mode"] = f"smoke_{args.smoke_lookback_days}d"
    legacy_ref = resolve_legacy_scenarios(preflight=preflight, discovery_scenario_id=args.legacy_discovery_scenario_id, holdout_scenario_id=args.legacy_holdout_scenario_id, skip_legacy_reference=args.skip_legacy_reference)
    preflight_timer(f"symbols={len(universe)} skip_holdout={args.skip_holdout} discovery={preflight['discovery_start']}..{preflight['discovery_end']}")
    spec = build_spec()
    git_commit = resolve_git_commit()
    today_tag = date.today().strftime("%Y%m%d")
    default_group = f"first_batch_{today_tag}_{preflight['window_mode']}"
    if args.smoke_fast:
        default_group = f"smoke_fast_{today_tag}_{preflight['window_mode']}"
    experiment_group = args.experiment_group or default_group
    universe_hash = stable_hash(universe)
    spec_hash = spec.spec_hash()
    root = Path(args.output_root) / experiment_group
    root.mkdir(parents=True, exist_ok=True)
    (root / "preflight.json").write_text(json.dumps({**preflight, "smoke_fast": args.smoke_fast, "smoke_universe_size": len(universe), "validation_summary_only": args.validation_summary_only, "diagnostics_lite": args.diagnostics_lite}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    leaderboard_path = root / "leaderboard.csv"

    discovery_reuse_payload = None
    for cfg in run_configs(include_legacy=not args.skip_legacy_reference, smoke_fast=args.smoke_fast):
        run_key = {
            "experiment_group": experiment_group,
            "label": cfg["label"],
            "strategy_mode": cfg["strategy_mode"],
            "universe_hash": universe_hash,
            "spec_hash": spec_hash,
            "window": [preflight["discovery_start"], preflight["discovery_end"], preflight["holdout_start"], preflight["holdout_end"]],
            "metadata": cfg["metadata"],
            "legacy_reference": legacy_ref,
            "smoke_fast": args.smoke_fast,
        }
        run_id = f"{cfg['label']}_{stable_hash(run_key, 12)}"
        run_dir = root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        discovery_scenario_id = legacy_ref["discovery"]["scenario_id"] if cfg["strategy_mode"] == "legacy_event_window" and legacy_ref["discovery"] else f"{run_id}_discovery"
        holdout_scenario_id = legacy_ref["holdout"]["scenario_id"] if cfg["strategy_mode"] == "legacy_event_window" and legacy_ref["holdout"] else f"{run_id}_holdout"

        metadata = dict(cfg["metadata"])
        metadata.update({
            "validation_summary_only": str(args.validation_summary_only).lower(),
            "diagnostics_lite": str(args.diagnostics_lite).lower(),
        })
        discovery_request = build_request(scenario_id=discovery_scenario_id, start_date=preflight["discovery_start"], end_date=preflight["discovery_end"], strategy_mode=cfg["strategy_mode"], spec=spec, metadata=metadata, symbols=universe)
        discovery_result = run_backtest(request=discovery_request, data_path=None, data_source="local-db", scenario_id=discovery_request.scenario.scenario_id, strategy_mode=cfg["strategy_mode"], output_dir=str(run_dir), enable_validation=(cfg["strategy_mode"] == "research_similarity_v2"), validation_max_folds=1 if args.smoke_fast else None, validation_summary_only=args.validation_summary_only, diagnostics_lite=args.diagnostics_lite, candidate_reuse_payload=discovery_reuse_payload if cfg["label"] in {"research_similarity_v2_conservative", "research_similarity_v2_aggressive"} else None, emit_timing_logs=True)
        if cfg["label"] == "research_similarity_v2_base":
            discovery_reuse_payload = (discovery_result.get("artifacts") or {}).get("candidate_reuse")

        holdout_result = {}
        if not args.skip_holdout:
            holdout_request = build_request(scenario_id=holdout_scenario_id, start_date=preflight["holdout_start"], end_date=preflight["holdout_end"], strategy_mode=cfg["strategy_mode"], spec=spec, metadata=metadata, symbols=universe)
            holdout_result = run_backtest(request=holdout_request, data_path=None, data_source="local-db", scenario_id=holdout_request.scenario.scenario_id, strategy_mode=cfg["strategy_mode"], output_dir=str(run_dir), enable_validation=(cfg["strategy_mode"] == "research_similarity_v2"), validation_max_folds=1 if args.smoke_fast else None, validation_summary_only=args.validation_summary_only, diagnostics_lite=args.diagnostics_lite, emit_timing_logs=True)

        diagnostic_flags = {k: v for k, v in metadata.items() if k.startswith("diagnostic_") or k in {"validation_summary_only", "diagnostics_lite"}}
        manifest = {
            "experiment_group": experiment_group,
            "run_id": run_id,
            "label": cfg["label"],
            "strategy_mode": cfg["strategy_mode"],
            "git_commit": git_commit,
            "universe": universe,
            "universe_hash": universe_hash,
            "window": {
                "discovery_start": preflight["discovery_start"],
                "discovery_end": preflight["discovery_end"],
                "holdout_start": preflight["holdout_start"],
                "holdout_end": preflight["holdout_end"],
            },
            "spec": asdict(spec),
            "spec_hash": spec_hash,
            "data_snapshot_id": (discovery_result.get("manifest") or {}).get("data_snapshot_id"),
            "bt_event_window_snapshot_id": legacy_ref["discovery"].get("snapshot_id") if cfg["strategy_mode"] == "legacy_event_window" and legacy_ref["discovery"] else None,
            "legacy_reference": legacy_ref,
            "discovery_manifest": discovery_result.get("manifest"),
            "holdout_manifest": holdout_result.get("manifest"),
            "preflight": preflight,
            "metadata_overrides": metadata,
            "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            "diagnostic_flags": diagnostic_flags,
            "snapshot_ids": {
                "discovery_data_snapshot_id": ((discovery_result.get("manifest") or {}).get("data_snapshot_id")),
                "holdout_data_snapshot_id": ((holdout_result.get("manifest") or {}).get("data_snapshot_id")),
                "discovery_validation_snapshot_ids": [sid for fold in (((discovery_result.get("validation") or {}).get("fold_engine") or {}).get("folds") or []) for sid in (((fold or {}).get("artifact") or {}).get("snapshot_ids") or [])],
                "holdout_validation_snapshot_ids": [sid for fold in (((holdout_result.get("validation") or {}).get("fold_engine") or {}).get("folds") or []) for sid in (((fold or {}).get("artifact") or {}).get("snapshot_ids") or [])],
            },
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        fold_report = {
            "run_id": run_id,
            "strategy_mode": cfg["strategy_mode"],
            "discovery": fold_report_summary((discovery_result.get("validation") or {}).get("fold_engine") or {}),
            "holdout": fold_report_summary((holdout_result.get("validation") or {}).get("fold_engine") or {}),
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
            "validation": fold_report["discovery"],
            "holdout_validation": fold_report["holdout"],
            "diagnostics_lite": args.diagnostics_lite,
        }
        (run_dir / "diagnostics.json").write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        aggregate = ((discovery_result.get("validation") or {}).get("fold_engine") or {}).get("aggregate") or {}
        holdout_aggregate = ((holdout_result.get("validation") or {}).get("fold_engine") or {}).get("aggregate") or {}
        discovery_direct = direct_metrics(discovery_result, len(universe))
        holdout_direct = direct_metrics(holdout_result, len(universe)) if holdout_result else {}
        side_split = summarize_side_split(holdout_result.get("plans") or discovery_result.get("plans") or [])
        regime_split = summarize_regime_split(((holdout_result.get("portfolio") or {}).get("decisions") or (discovery_result.get("portfolio") or {}).get("decisions") or []))
        run_card = {
            "run_id": run_id,
            "strategy_mode": cfg["strategy_mode"],
            "discovery_start": preflight["discovery_start"],
            "discovery_end": preflight["discovery_end"],
            "holdout_start": preflight["holdout_start"],
            "holdout_end": preflight["holdout_end"],
            "symbols": "|".join(universe),
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
            "discovery_cv_calibration_error": aggregate.get("calibration_error") if (discovery_result.get("validation") or {}).get("fold_engine", {}).get("folds") else None,
            "discovery_cv_monotonicity": aggregate.get("score_decile_monotonicity") if (discovery_result.get("validation") or {}).get("fold_engine", {}).get("folds") else None,
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
            "bt_event_window_snapshot_id": manifest["bt_event_window_snapshot_id"],
            "experiment_group": experiment_group,
            "ohlcv_common_coverage": preflight.get("ohlcv_common_coverage"),
            "macro_coverage": preflight.get("macro_coverage"),
            "sector_coverage": preflight.get("sector_coverage"),
            "legacy_snapshot_ready": preflight.get("legacy_snapshot_ready"),
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

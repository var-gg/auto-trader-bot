from __future__ import annotations

import json
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from backtest_app.configs.models import BacktestConfig, RunnerRequest
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.portfolio import PortfolioConfig, PortfolioState, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab
from backtest_app.reporting.summary import build_summary
from backtest_app.results.store import JsonResultStore, SqlResultStore
from backtest_app.research_runtime.runner import ensure_manifest
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from backtest_app.validation import run_fold_validation, sensitivity_sweep
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, FillStatus, Side

try:
    from backtest_app.observability.git_provenance import collect_git_provenance
except ImportError:  # pragma: no cover - legacy public branches may not ship observability helpers
    def collect_git_provenance() -> dict[str, Any]:
        return {}


def _meta_flag(metadata: dict[str, Any] | None, key: str, default: bool = False) -> bool:
    value = (metadata or {}).get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _meta_int(metadata: dict[str, Any] | None, key: str, default: int | None = None) -> int | None:
    value = (metadata or {}).get(key)
    if value in (None, ""):
        return default
    return int(value)


def _stage_timer(enabled: bool, label: str):
    started = time.perf_counter()

    def _done(extra: str = ""):
        if enabled:
            elapsed = time.perf_counter() - started
            suffix = f" | {extra}" if extra else ""
            print(f"[{label}] {elapsed:.3f}s{suffix}")

    return _done


def _history_from_reuse_payload(payload: dict) -> SimpleNamespace:
    metadata = dict(payload.get("metadata") or {})
    metadata.setdefault("source", "reuse")
    metadata.setdefault("diagnostics", payload.get("diagnostics") or {})
    metadata.setdefault("signal_panel_artifact", payload.get("signal_panel") or [])
    metadata.setdefault("macro_series_history", payload.get("macro_series_history") or [])
    metadata.setdefault("session_metadata_by_symbol", payload.get("session_metadata_by_symbol") or {})
    return SimpleNamespace(
        bars_by_symbol=payload.get("bars_by_symbol") or {},
        candidates=payload.get("candidates") or [],
        session_metadata_by_symbol=payload.get("session_metadata_by_symbol") or {},
        metadata=metadata,
    )


def _policy_reuse_payload(*, historical, grouped_candidates: dict[str, list], warmup_candidates: list, trading_dates: list[str]) -> dict:
    diagnostics = getattr(historical, "metadata", {}) or {}
    return {
        "bars_by_symbol": historical.bars_by_symbol,
        "candidates": list(getattr(historical, "candidates", []) or []),
        "session_metadata_by_symbol": diagnostics.get("session_metadata_by_symbol") or getattr(historical, "session_metadata_by_symbol", {}) or {},
        "metadata": dict(diagnostics),
        "signal_panel": diagnostics.get("signal_panel_artifact", []),
        "macro_series_history": diagnostics.get("macro_series_history", []),
        "warmup_candidates": [{"symbol": c.symbol, "decision_date": _candidate_decision_date(c)} for c in warmup_candidates],
        "candidate_counts": {k: len(v) for k, v in grouped_candidates.items()},
        "trading_dates": list(trading_dates),
    }


def _diagnostics_lite_view(diagnostics: dict | None, *, grouped_candidates: dict[str, list], warmup_candidates: list, trading_dates: list[str], plans: list, fills: list) -> dict:
    source = diagnostics or {}
    throughput = dict(source.get("throughput") or {})
    cache_keys = dict(source.get("cache_keys") or {})
    panel_rows = source.get("signal_panel") or []
    event_records = source.get("event_records") or []
    pipeline = dict(source.get("pipeline") or {})
    summaries = {
        "throughput": throughput,
        "pipeline": pipeline,
        "cache_keys": cache_keys,
        "coverage": {
            "trading_dates": len(trading_dates),
            "candidate_dates": len(grouped_candidates),
            "warmup_candidates": len(warmup_candidates),
            "candidate_count": sum(len(rows) for rows in grouped_candidates.values()),
            "plan_count": len(plans),
            "fill_count": len(fills),
        },
        "signal_panel_summary": {
            "row_count": len(panel_rows),
            "decision_dates": len({str(r.get("decision_date")) for r in panel_rows if isinstance(r, dict) and r.get("decision_date")}),
            "symbols": len({str(r.get("symbol")) for r in panel_rows if isinstance(r, dict) and r.get("symbol")}),
        },
        "event_record_summary": {
            "batch_count": len(event_records),
            "record_count": sum(len(batch.get("records") or []) for batch in event_records if isinstance(batch, dict)),
            "non_empty_batches": sum(1 for batch in event_records if isinstance(batch, dict) and batch.get("records")),
        },
    }
    if "prototype_count" in throughput:
        summaries["prototype_count"] = throughput.get("prototype_count")
    if "anchor_count" in throughput:
        summaries["anchor_count"] = throughput.get("anchor_count")
    if "n_symbols" in throughput:
        summaries["n_symbols"] = throughput.get("n_symbols")
    return summaries


def _git_commit_for_path(pathspec: str | None = None) -> str | None:
    repo_root = Path(__file__).resolve().parents[2]
    command = ["git", "log", "-1", "--format=%H"]
    if pathspec:
        command.extend(["--", pathspec])
    try:
        result = subprocess.run(command, cwd=repo_root, capture_output=True, text=True, check=True)
        commit = result.stdout.strip()
        return commit or None
    except Exception:
        return None


def _portfolio_paths(date_artifacts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    rows = list(date_artifacts or [])
    return {
        "cash_path": [{"decision_date": row.get("decision_date"), "cash": row.get("cash")} for row in rows],
        "exposure_path": [{"decision_date": row.get("decision_date"), "exposure": row.get("exposure")} for row in rows],
        "open_position_count_path": [{"decision_date": row.get("decision_date"), "open_position_count": row.get("open_position_count")} for row in rows],
    }


def _forecast_rows(signal_panel_payload: Any) -> list[dict[str, Any]]:
    if not isinstance(signal_panel_payload, list):
        return []
    rows: list[dict[str, Any]] = []
    for row in signal_panel_payload:
        if not isinstance(row, dict):
            continue
        query = dict(row.get("query") or {})
        decision_surface = dict(row.get("decision_surface") or {})
        scorer = dict(row.get("scorer_diagnostics") or {})
        buy = dict(scorer.get("buy") or {})
        sell = dict(scorer.get("sell") or {})
        chosen_side = str(decision_surface.get("chosen_side") or "ABSTAIN")
        chosen_payload = dict(row.get("chosen_side_payload") or decision_surface.get("chosen_payload") or {})
        if not chosen_payload:
            chosen_payload = buy if chosen_side == "BUY" else sell if chosen_side == "SELL" else {}
        missingness = dict(row.get("missingness") or {})
        rows.append(
            {
                "decision_date": row.get("decision_date"),
                "symbol": row.get("symbol"),
                "exchange_code": query.get("exchange_code"),
                "country_code": query.get("country_code"),
                "exchange_tz": query.get("exchange_tz"),
                "session_date_local": query.get("session_date_local"),
                "session_close_ts_utc": query.get("session_close_ts_utc"),
                "feature_anchor_ts_utc": query.get("feature_anchor_ts_utc"),
                "macro_asof_ts_utc": query.get("macro_asof_ts_utc"),
                "chosen_side_before_deploy": chosen_side,
                "abstain": bool(decision_surface.get("abstain", False)),
                "abstain_reasons": json.dumps(decision_surface.get("abstain_reasons") or [], ensure_ascii=False),
                "forecast_selected": bool(not decision_surface.get("abstain", False) and chosen_side != "ABSTAIN"),
                "lower_bound": decision_surface.get("chosen_lower_bound"),
                "interval_width": decision_surface.get("chosen_interval_width"),
                "expected_net_return": chosen_payload.get("expected_net_return"),
                "q10": chosen_payload.get("q10_return", chosen_payload.get("q10")),
                "q50": chosen_payload.get("q50_return", chosen_payload.get("q50")),
                "q90": chosen_payload.get("q90_return", chosen_payload.get("q90")),
                "expected_mae": chosen_payload.get("expected_mae"),
                "expected_mfe": chosen_payload.get("expected_mfe"),
                "effective_sample_size": chosen_payload.get("effective_sample_size", chosen_payload.get("n_eff")),
                "uncertainty": chosen_payload.get("uncertainty"),
                "regime_alignment": chosen_payload.get("regime_alignment"),
                "buy_expected_net_return": buy.get("expected_net_return"),
                "buy_q10": buy.get("q10"),
                "buy_q50": buy.get("q50"),
                "buy_q90": buy.get("q90"),
                "buy_expected_mae": buy.get("expected_mae"),
                "buy_expected_mfe": buy.get("expected_mfe"),
                "buy_effective_sample_size": buy.get("n_eff"),
                "buy_uncertainty": buy.get("uncertainty"),
                "buy_regime_alignment": dict((row.get("ev") or {}).get("buy") or {}).get("regime_alignment"),
                "buy_abstain_reasons": json.dumps(dict((row.get("ev") or {}).get("buy") or {}).get("abstain_reasons") or [], ensure_ascii=False),
                "sell_expected_net_return": sell.get("expected_net_return"),
                "sell_q10": sell.get("q10"),
                "sell_q50": sell.get("q50"),
                "sell_q90": sell.get("q90"),
                "sell_expected_mae": sell.get("expected_mae"),
                "sell_expected_mfe": sell.get("expected_mfe"),
                "sell_effective_sample_size": sell.get("n_eff"),
                "sell_uncertainty": sell.get("uncertainty"),
                "sell_regime_alignment": dict((row.get("ev") or {}).get("sell") or {}).get("regime_alignment"),
                "sell_abstain_reasons": json.dumps(dict((row.get("ev") or {}).get("sell") or {}).get("abstain_reasons") or [], ensure_ascii=False),
                "top_matches_summary": json.dumps((buy.get("top_matches_summary") if chosen_side == "BUY" else sell.get("top_matches_summary")) or [], ensure_ascii=False),
                "missingness_summary": json.dumps(missingness, ensure_ascii=False),
                "freshness_summary": json.dumps(query.get("macro_freshness_summary") or {}, ensure_ascii=False),
            }
        )
    return rows


def _write_forecast_panel_artifacts(*, output_dir: str, run_id: str, signal_panel_payload: Any, progress_callback=None, total_trading_dates: int = 0, completed_trading_dates: int = 0, candidate_rows: int = 0, plans_count: int = 0, fills_count: int = 0) -> dict[str, Any]:
    rows = _forecast_rows(signal_panel_payload)
    if not rows:
        return {"row_count": 0, "csv_path": None, "parquet_path": None}
    try:
        import pandas as pd
    except Exception:
        return {"row_count": len(rows), "csv_path": None, "parquet_path": None, "write_error": "pandas_unavailable"}
    run_dir = Path(output_dir) / "research" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    csv_path = run_dir / "forecast_panel.csv"
    parquet_path = run_dir / "forecast_panel.parquet"
    _emit_progress(progress_callback, {
        "phase": "write_artifacts",
        "status": "running",
        "artifact_name": "forecast_panel_prepare",
        "artifact_index": 2,
        "artifact_total": 5,
        "artifact_rows": len(rows),
        "bytes_written": 0,
        "candidate_rows": candidate_rows,
        "plans_count": plans_count,
        "fills_count": fills_count,
        "total_trading_dates": total_trading_dates,
        "completed_trading_dates": completed_trading_dates,
    })
    frame = pd.DataFrame(rows)
    frame.to_csv(csv_path, index=False)
    _emit_progress(progress_callback, {
        "phase": "write_artifacts",
        "status": "running",
        "artifact_name": "forecast_panel_csv",
        "artifact_index": 3,
        "artifact_total": 5,
        "artifact_rows": len(rows),
        "artifact_bytes": csv_path.stat().st_size if csv_path.exists() else 0,
        "bytes_written": csv_path.stat().st_size if csv_path.exists() else 0,
        "candidate_rows": candidate_rows,
        "plans_count": plans_count,
        "fills_count": fills_count,
        "total_trading_dates": total_trading_dates,
        "completed_trading_dates": completed_trading_dates,
    })
    parquet_format = "parquet"
    try:
        frame.to_parquet(parquet_path, index=False)
    except Exception:
        parquet_path.write_text(json.dumps({"format": "json_fallback", "rows": rows}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        parquet_format = "json_fallback"
    _emit_progress(progress_callback, {
        "phase": "write_artifacts",
        "status": "running",
        "artifact_name": "forecast_panel_parquet",
        "artifact_index": 4,
        "artifact_total": 5,
        "artifact_rows": len(rows),
        "artifact_bytes": parquet_path.stat().st_size if parquet_path.exists() else 0,
        "bytes_written": sum(path.stat().st_size for path in [csv_path, parquet_path] if path.exists()),
        "candidate_rows": candidate_rows,
        "plans_count": plans_count,
        "fills_count": fills_count,
        "total_trading_dates": total_trading_dates,
        "completed_trading_dates": completed_trading_dates,
    })
    return {
        "row_count": len(rows),
        "csv_path": str(csv_path),
        "parquet_path": str(parquet_path),
        "parquet_format": parquet_format,
    }


def _reproducibility_payload(*, request: RunnerRequest, manifest, raw_diagnostics: dict | None, signal_panel_payload, validation_folds: dict | None) -> dict:
    metadata = dict(request.config.metadata or {})
    diagnostic_flag_keys = sorted(k for k in metadata if k.startswith("diagnostic_") or k in {"validation_summary_only", "diagnostics_lite"})
    validation_snapshot_ids = []
    for fold in (validation_folds or {}).get("folds") or []:
        artifact = (fold or {}).get("artifact") or {}
        for snapshot_id in artifact.get("snapshot_ids") or []:
            if snapshot_id not in validation_snapshot_ids:
                validation_snapshot_ids.append(snapshot_id)
    runtime_head_commit = getattr(manifest, "code_commit", None) or _git_commit_for_path()
    strategy_logic_commit = _git_commit_for_path("backtest_app") or runtime_head_commit
    git_provenance = collect_git_provenance()
    return {
        **git_provenance,
        "git_commit": runtime_head_commit,
        "git_commit_deprecated": True,
        "runtime_head_commit": runtime_head_commit,
        "strategy_logic_commit": strategy_logic_commit,
        "manifest": manifest.to_dict() if hasattr(manifest, "to_dict") else dict(manifest or {}),
        "exact_research_experiment_spec": request.config.research_spec.to_dict() if request.config.research_spec else None,
        "exact_metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        "metadata": metadata,
        "diagnostic_flags": {key: metadata.get(key) for key in diagnostic_flag_keys},
        "snapshot_ids": {
            "data_snapshot_id": getattr(manifest, "data_snapshot_id", None),
            "validation_snapshot_ids": validation_snapshot_ids,
        },
        "symbol_list": list(request.scenario.symbols),
        "window": {
            "start_date": request.scenario.start_date,
            "end_date": request.scenario.end_date,
        },
        "signal_panel": signal_panel_payload,
        "pipeline": ((raw_diagnostics or {}).get("pipeline") or {}),
    }


def load_historical(request: RunnerRequest, data_path: str | None, data_source: str, scenario_id: str | None, strategy_mode: str, progress_callback=None):
    if data_source == "local-db":
        cfg = LocalBacktestDbConfig.from_env()
        guard_backtest_local_only(cfg.url)
        session_factory = create_backtest_session_factory(cfg)
        loader = LocalPostgresLoader(session_factory, schema=cfg.schema)
        return loader.load_for_scenario(scenario_id=scenario_id or request.scenario.scenario_id, market=request.scenario.market, start_date=request.scenario.start_date, end_date=request.scenario.end_date, symbols=request.scenario.symbols, strategy_mode=strategy_mode, research_spec=request.config.research_spec, metadata=request.config.metadata, progress_callback=progress_callback)
    if not data_path:
        raise ValueError("data_path is required when data_source=json")
    return JsonHistoricalDataLoader().load(data_path)


def _date_str(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return str(value.isoformat())[:10]
    return str(value)[:10]


def _candidate_decision_date(candidate) -> str | None:
    return _date_str(candidate.reference_date) or _date_str(candidate.anchor_date)


def _emit_progress(progress_callback, payload: dict[str, Any]) -> None:
    if progress_callback:
        progress_callback(payload)


def _support_metadata_observed(raw_diagnostics: dict[str, Any] | None, signal_panel_payload: Any) -> dict[str, Any]:
    pipeline = dict(((raw_diagnostics or {}).get("pipeline") or {}))
    ev_config = dict(pipeline.get("ev_config") or {})
    observed = {
        "top_k": pipeline.get("top_k"),
        "kernel_temperature": ev_config.get("kernel_temperature"),
        "use_kernel_weighting": ev_config.get("use_kernel_weighting"),
        "min_effective_sample_size": ev_config.get("min_effective_sample_size"),
        "diagnostic_disable_ess_gate": ev_config.get("diagnostic_disable_ess_gate"),
    }
    gate_values = {
        bool(((row.get("decision_surface") or {}).get("gate_ablation") or {}).get("diagnostic_disable_ess_gate"))
        for row in list(signal_panel_payload or [])
        if isinstance(row, dict)
    }
    if gate_values:
        observed["diagnostic_disable_ess_gate_rows"] = sorted(gate_values)
    return observed


def _authoritative_summary_payload(
    *,
    request: RunnerRequest,
    raw_diagnostics: dict[str, Any] | None,
    signal_panel_payload: Any,
    selected_symbols: list[dict[str, Any]],
    plans: list[Any],
    fills: list[Any],
    reproducibility: dict[str, Any],
    forecast_panel_artifact: dict[str, Any],
    result_path: str | None,
) -> dict[str, Any]:
    panel = list(signal_panel_payload or [])
    abstain: dict[str, int] = {}
    buy_pass_count = 0
    sell_pass_count = 0
    n_eff_histogram: dict[str, int] = {}
    top1_weight_histogram: dict[str, int] = {}
    forecast_selected_count = 0
    forecast_selected_dates: set[str] = set()
    for row in panel:
        ds = row.get("decision_surface") or {}
        chosen_side = str(ds.get("chosen_side") or "ABSTAIN")
        if not bool(ds.get("abstain", False)) and chosen_side != "ABSTAIN":
            forecast_selected_count += 1
            if row.get("decision_date"):
                forecast_selected_dates.add(str(row.get("decision_date")))
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
                weight = float((matches[0] or {}).get("weight", 0.0) or 0.0)
                bucket = f"{weight:.1f}"
                top1_weight_histogram[bucket] = top1_weight_histogram.get(bucket, 0) + 1
    filled = [f for f in fills if str((f.get("fill_status") or "")).upper() in {"FULL", "PARTIAL"}]
    candidate_dates = sorted({str(item.get("decision_date")) for item in selected_symbols if item.get("decision_date")})
    return {
        "scenario_id": request.scenario.scenario_id,
        "window": {"start_date": request.scenario.start_date, "end_date": request.scenario.end_date},
        "symbols": list(request.scenario.symbols),
        "metadata": dict(request.config.metadata or {}),
        "authoritative": bool(reproducibility.get("authoritative", False)),
        "branch": reproducibility.get("branch"),
        "head_commit": reproducibility.get("head_commit"),
        "dirty_worktree": reproducibility.get("dirty_worktree"),
        "changed_tracked_files": reproducibility.get("changed_tracked_files", []),
        "diff_fingerprint": reproducibility.get("diff_fingerprint"),
        "forecast_selected_count": forecast_selected_count,
        "forecast_selected_dates": sorted(forecast_selected_dates),
        "forecast_viable": forecast_selected_count > 0,
        "candidate_count": len(selected_symbols),
        "candidate_dates": candidate_dates,
        "buy_pass_count": buy_pass_count,
        "sell_pass_count": sell_pass_count,
        "n_eff_histogram": dict(sorted(n_eff_histogram.items())),
        "top1_weight_histogram": dict(sorted(top1_weight_histogram.items())),
        "abstain_reason_histogram": dict(sorted(abstain.items())),
        "fills_count": len(filled),
        "trades_count": len(plans),
        "deploy_viable": len(filled) > 0 and len(plans) > 0,
        "forecast_panel": dict(forecast_panel_artifact or {}),
        "result_path": result_path,
        "support_metadata_observed": _support_metadata_observed(raw_diagnostics, signal_panel_payload),
    }


def _write_authoritative_summary_artifact(*, output_dir: str, payload: dict[str, Any]) -> str:
    path = Path(output_dir) / "authoritative_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(path)


def _candidate_groups(candidates, *, start_date: str, end_date: str):
    grouped = defaultdict(list)
    warmup = []
    for candidate in candidates:
        decision_date = _candidate_decision_date(candidate)
        if not decision_date:
            continue
        if decision_date < start_date or decision_date > end_date:
            warmup.append(candidate)
            continue
        grouped[decision_date].append(candidate)
    return dict(sorted(grouped.items())), warmup


def _scenario_trading_dates(*, bars_by_symbol: dict, start_date: str, end_date: str) -> list[str]:
    dates = sorted({str(bar.timestamp)[:10] for bars in bars_by_symbol.values() for bar in bars if start_date <= str(bar.timestamp)[:10] <= end_date})
    return dates


def _tuning_config():
    return {"MIN_TICK_GAP": 1, "ADAPTIVE_BASE_LEGS": 2, "ADAPTIVE_LEG_BOOST": 1.0, "MIN_TOTAL_SPREAD_PCT": 0.01, "ADAPTIVE_STRENGTH_SCALE": 0.1, "FIRST_LEG_BASE_PCT": 0.012, "FIRST_LEG_MIN_PCT": 0.006, "FIRST_LEG_MAX_PCT": 0.05, "FIRST_LEG_GAIN_WEIGHT": 0.6, "FIRST_LEG_ATR_WEIGHT": 0.5, "FIRST_LEG_REQ_FLOOR_PCT": 0.012, "MIN_FIRST_LEG_GAP_PCT": 0.03, "STRICT_MIN_FIRST_GAP": True, "ADAPTIVE_MAX_STEP_PCT": 0.06, "ADAPTIVE_FRAC_ALPHA": 1.25, "ADAPTIVE_GAIN_SCALE": 0.1, "MIN_LOT_QTY": 1}


def _close_positions_for_day(*, day: str, state: dict, bars_by_symbol: dict, config: BacktestConfig, force: bool = False, reason: str | None = None):
    realized = []
    for symbol, pos in list(state["open_positions"].items()):
        bars = bars_by_symbol.get(symbol, [])
        exit_bar = next((b for b in bars if str(b.timestamp)[:10] == day), None)
        if not exit_bar:
            continue
        if not force and day < pos["planned_exit_date"]:
            continue
        qty = max(float(pos["filled_quantity"] or 0.0), 0.0)
        if qty <= 0:
            state["reserved_capital"] = max(0.0, state["reserved_capital"] - float(pos["reserved_budget"]))
            del state["open_positions"][symbol]
            continue
        exit_price = float(exit_bar.close)
        entry_price = float(pos["entry_price"])
        pnl = (exit_price - entry_price) * qty if pos["side"] == Side.BUY.value else (entry_price - exit_price) * qty
        pnl -= qty * entry_price * ((float(config.fee_bps) + float(config.slippage_bps)) / 10000.0)
        state["cash"] += float(pos["reserved_budget"]) + pnl
        state["reserved_capital"] = max(0.0, state["reserved_capital"] - float(pos["reserved_budget"]))
        if pos.get("plan_ref") is not None:
            pos["plan_ref"].metadata["realized_exit_date"] = day
            if force:
                pos["plan_ref"].metadata["forced_liquidation"] = True
                pos["plan_ref"].metadata["forced_liquidation_reason"] = reason or "scenario_end"
        realized.append({"symbol": symbol, "exit_date": day, "pnl": pnl, "forced_liquidation": force, "reason": reason})
        del state["open_positions"][symbol]
    return realized


def _open_positions_market_value(*, day: str, state: dict, bars_by_symbol: dict):
    exposure = 0.0
    for symbol, pos in state["open_positions"].items():
        bar = next((b for b in bars_by_symbol.get(symbol, []) if str(b.timestamp)[:10] == day), None)
        mark = float(bar.close) if bar else float(pos["entry_price"])
        exposure += mark * float(pos["filled_quantity"] or 0.0)
    return exposure


def execute_daily_execution_loop(*, trading_dates: list[str], grouped_candidates: dict[str, list], bars_by_symbol: dict, config: BacktestConfig, market: str, strategy_mode: str, portfolio_cfg: PortfolioConfig, quote_policy_cfg: QuotePolicyConfig, tuning: dict, broker, initial_state: dict | None = None, progress_callback=None):
    state = dict(initial_state or {"cash": float(config.initial_capital), "reserved_capital": 0.0, "open_positions": {}, "turnover_used": 0})
    state["open_positions"] = dict(state.get("open_positions") or {})
    date_artifacts = []
    plans = []
    fills = []
    skipped = []
    selected_symbols = []
    portfolio_decisions_all = []
    total_trading_dates = len(trading_dates)
    if progress_callback:
        progress_callback({"phase": "daily_execution", "total_trading_dates": total_trading_dates, "completed_trading_dates": 0, "current_decision_date": None, "selected_count_so_far": 0, "plans_count_so_far": 0, "fills_count_so_far": 0})
    for idx, decision_date in enumerate(trading_dates, start=1):
        realized_today = _close_positions_for_day(day=decision_date, state=state, bars_by_symbol=bars_by_symbol, config=config)
        candidates = grouped_candidates.get(decision_date, [])
        pstate = PortfolioState(cash=state["cash"], reserved_capital=state["reserved_capital"], open_positions=dict(state["open_positions"]), turnover_used=state["turnover_used"])
        decisions = build_portfolio_decisions(candidates=candidates, initial_capital=config.initial_capital, cfg=portfolio_cfg, state=pstate) if candidates else []
        day_selected = []
        day_rejected = []
        for decision in decisions:
            portfolio_decisions_all.append(decision)
            candidate = decision.candidate
            if not decision.selected:
                day_rejected.append({"symbol": candidate.symbol, "reason": decision.kill_reason, "diagnostics": decision.diagnostics})
                skipped.append({"symbol": candidate.symbol, "code": "PORTFOLIO", "note": str(decision.kill_reason), "strategy_mode": strategy_mode, "portfolio_diagnostics": decision.diagnostics, "decision_date": decision_date})
                continue
            generated_at = datetime.fromisoformat(f"{decision_date}T00:00:00")
            policy_ab = compare_policy_ab(candidate, quote_policy_cfg)
            active_policy = policy_ab["quote_policy_v1"]
            plan, skip = build_order_plan_from_candidate(candidate, generated_at=generated_at, market=market, side=candidate.side_bias if strategy_mode in {"research_similarity_v1", "research_similarity_v2"} else Side.BUY, tuning=tuning, budget=max(0.0, decision.requested_budget), venue=ExecutionVenue.BACKTEST, rationale_prefix=f"execution:{strategy_mode}", quote_policy=active_policy)
            if not plan:
                day_rejected.append({"symbol": candidate.symbol, "reason": (skip or {}).get("code", "NO_PLAN"), "diagnostics": active_policy})
                continue
            plan.metadata["quote_policy_ab"] = policy_ab
            plan.metadata["decision_date"] = decision_date
            execution_date = str(plan.metadata.get("executable_from_date") or decision_date)
            bars = [bar for bar in bars_by_symbol.get(plan.symbol, []) if str(bar.timestamp)[:10] >= (execution_date if strategy_mode == "research_similarity_v2" else decision_date)]
            day_fills = broker.simulate_plan(plan, bars)
            plans.append(plan)
            fills.extend(day_fills)
            fill_rows = [f for f in day_fills if f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL}]
            filled_qty = sum(float(f.filled_quantity or 0.0) for f in fill_rows)
            avg_fill = (sum(float(f.average_fill_price or 0.0) * float(f.filled_quantity or 0.0) for f in fill_rows) / filled_qty) if filled_qty > 0 else 0.0
            if filled_qty > 0:
                horizon_days = int(candidate.expected_horizon_days or 5)
                first_fill_date = min(str(f.event_time)[:10] for f in fill_rows)
                bars_for_symbol = [b for b in bars_by_symbol.get(plan.symbol, []) if str(b.timestamp)[:10] >= first_fill_date]
                exit_idx = min(max(horizon_days, 1), max(len(bars_for_symbol) - 1, 0))
                planned_exit_date = str(bars_for_symbol[exit_idx].timestamp)[:10] if bars_for_symbol else first_fill_date
                reserved = float(decision.requested_budget)
                plan.metadata.update({"entry_date": first_fill_date, "first_fill_date": first_fill_date, "planned_exit_date": planned_exit_date, "realized_exit_date": None, "forced_liquidation": False, "forced_liquidation_reason": None})
                state["cash"] -= reserved
                state["reserved_capital"] += reserved
                state["open_positions"][plan.symbol] = {"side": plan.side.value, "entry_price": avg_fill or float(candidate.current_price or 0.0), "filled_quantity": filled_qty, "reserved_budget": reserved, "planned_exit_date": planned_exit_date, "plan_ref": plan}
                state["turnover_used"] += 1
                day_selected.append({"symbol": candidate.symbol, "side": decision.side.value, "requested_budget": decision.requested_budget, "size_multiplier": decision.size_multiplier, "policy_reason": active_policy.get("chosen_policy_reason"), "entry_date": first_fill_date, "first_fill_date": first_fill_date, "planned_exit_date": planned_exit_date})
                selected_symbols.append({"symbol": candidate.symbol, "side": decision.side.value, "size_multiplier": decision.size_multiplier, "expected_horizon_days": decision.expected_horizon_days, "decision_date": decision_date})
            else:
                day_rejected.append({"symbol": candidate.symbol, "reason": "no_fill", "diagnostics": active_policy})
        exposure = _open_positions_market_value(day=decision_date, state=state, bars_by_symbol=bars_by_symbol)
        date_artifacts.append({"decision_date": decision_date, "selected": day_selected, "rejected": day_rejected, "realized_today": realized_today, "cash": state["cash"], "reserved_capital": state["reserved_capital"], "exposure": exposure, "open_position_count": len(state["open_positions"]), "open_positions": sorted(state["open_positions"].keys())})
        if progress_callback:
            progress_callback({
                "phase": "daily_execution",
                "total_trading_dates": total_trading_dates,
                "completed_trading_dates": idx,
                "current_decision_date": decision_date,
                "selected_count_so_far": len(selected_symbols),
                "plans_count_so_far": len(plans),
                "fills_count_so_far": len(fills),
                "open_position_count": len(state["open_positions"]),
            })
    if trading_dates:
        forced = _close_positions_for_day(day=trading_dates[-1], state=state, bars_by_symbol=bars_by_symbol, config=config, force=True, reason="scenario_end")
        if date_artifacts:
            date_artifacts[-1]["realized_today"].extend(forced)
            date_artifacts[-1]["open_position_count"] = len(state["open_positions"])
            date_artifacts[-1]["open_positions"] = sorted(state["open_positions"].keys())
            date_artifacts[-1]["cash"] = state["cash"]
            date_artifacts[-1]["reserved_capital"] = state["reserved_capital"]
            date_artifacts[-1]["exposure"] = _open_positions_market_value(day=trading_dates[-1], state=state, bars_by_symbol=bars_by_symbol)
    return {"state": state, "date_artifacts": date_artifacts, "plans": plans, "fills": fills, "skipped": skipped, "selected_symbols": selected_symbols, "portfolio_decisions_all": portfolio_decisions_all}


def run_backtest(request: RunnerRequest, data_path: str | None, *, output_dir: str | None = None, save_json: bool = True, sql_db_url: str | None = None, data_source: str = "json", scenario_id: str | None = None, strategy_mode: str = "legacy_event_window", enable_validation: bool = True, validation_max_folds: int | None = None, validation_summary_only: bool = False, diagnostics_lite: bool = False, candidate_reuse_payload: dict | None = None, emit_timing_logs: bool = False, progress_callback=None) -> dict:
    total_timer = _stage_timer(emit_timing_logs, "total")
    load_timer = _stage_timer(emit_timing_logs, "load_bars")
    if progress_callback:
        progress_callback({"phase": "load_historical", "status": "running"})
    if candidate_reuse_payload is not None:
        historical = _history_from_reuse_payload(candidate_reuse_payload)
    else:
        historical = load_historical(request, data_path, data_source, scenario_id, strategy_mode, progress_callback=progress_callback)
    historical_metadata = getattr(historical, "metadata", {}) or {}
    if progress_callback:
        macro_history = historical_metadata.get("macro_history_by_date", {}) or {}
        macro_series_history = historical_metadata.get("macro_series_history", []) or []
        sector_map = historical_metadata.get("sector_map", {}) or {}
        progress_callback({
            "phase": "load_historical",
            "status": "running",
            "loaded_ohlcv_rows": sum(len(bars) for bars in (historical.bars_by_symbol or {}).values()),
            "loaded_macro_rows": len(macro_series_history) or sum(len(row or {}) for row in macro_history.values()),
            "loaded_sector_rows": len(sector_map),
        })
    load_timer(f"symbols={len(request.scenario.symbols)} reuse={candidate_reuse_payload is not None}")
    tuning = _tuning_config()
    portfolio_cfg = PortfolioConfig(top_n=int(request.config.metadata.get("portfolio_top_n", 5) or 5), risk_budget_fraction=float(request.config.metadata.get("portfolio_risk_budget_fraction", 0.95) or 0.95))
    quote_policy_cfg = QuotePolicyConfig(ev_threshold=float(request.config.metadata.get("quote_ev_threshold", 0.005)), uncertainty_cap=float(request.config.metadata.get("quote_uncertainty_cap", 0.12)), min_effective_sample_size=float(request.config.metadata.get("quote_min_effective_sample_size", 1.5)), min_fill_probability=float(request.config.metadata.get("quote_min_fill_probability", 0.10)))
    broker = SimulatedBroker(rules=SimulationRules(slippage_bps=request.config.slippage_bps, fee_bps=request.config.fee_bps, allow_partial_fills=request.config.allow_partial_fills))
    candidate_timer = _stage_timer(emit_timing_logs, "candidate_generation")
    if progress_callback:
        progress_callback({"phase": "candidate_grouping", "status": "running"})
    grouped_candidates, warmup_candidates = _candidate_groups(historical.candidates, start_date=request.scenario.start_date, end_date=request.scenario.end_date)
    trading_dates = candidate_reuse_payload.get("trading_dates") if candidate_reuse_payload else None
    trading_dates = trading_dates or _scenario_trading_dates(bars_by_symbol=historical.bars_by_symbol, start_date=request.scenario.start_date, end_date=request.scenario.end_date)
    candidate_timer(f"candidate_dates={len(grouped_candidates)} warmup={len(warmup_candidates)}")
    if progress_callback:
        raw_diagnostics = historical_metadata.get("diagnostics", {}) or {}
        event_record_batches = list(raw_diagnostics.get("event_records") or [])
        progress_callback({
            "phase": "candidate_grouping",
            "status": "running",
            "event_records_built": sum(len(batch or []) for batch in event_record_batches),
            "prototype_batches_built": len(event_record_batches),
            "candidate_rows": len(historical.candidates or []),
            "total_trading_dates": len(trading_dates),
        })
        progress_callback({"phase": "daily_execution", "status": "running", "total_trading_dates": len(trading_dates)})
    execution = execute_daily_execution_loop(trading_dates=trading_dates, grouped_candidates=grouped_candidates, bars_by_symbol=historical.bars_by_symbol, config=request.config, market=request.scenario.market, strategy_mode=strategy_mode, portfolio_cfg=portfolio_cfg, quote_policy_cfg=quote_policy_cfg, tuning=tuning, broker=broker, progress_callback=progress_callback)
    state = execution["state"]
    date_artifacts = execution["date_artifacts"]
    plans = execution["plans"]
    fills = execution["fills"]
    skipped = execution["skipped"]
    selected_symbols = execution["selected_symbols"]
    portfolio_decisions_all = execution["portfolio_decisions_all"]
    portfolio_paths = _portfolio_paths(date_artifacts)
    summary = build_summary(scenario_id=request.scenario.scenario_id, plans=plans, fills=fills, bars_by_symbol=historical.bars_by_symbol, date_artifacts=date_artifacts)
    historical_context = {"bars_by_symbol": historical.bars_by_symbol, "macro_history_by_date": historical_metadata.get("macro_history_by_date", {}), "macro_series_history": historical_metadata.get("macro_series_history", []), "sector_map": historical_metadata.get("sector_map", {}), "session_metadata_by_symbol": historical_metadata.get("session_metadata_by_symbol") or getattr(historical, "session_metadata_by_symbol", {}) or {}, "trading_dates": trading_dates}
    historical_metadata["bars_by_symbol"] = historical.bars_by_symbol
    historical_metadata["historical_context"] = historical_context
    manifest = ensure_manifest(request=request, data_source=data_source, historical_metadata=historical_metadata)
    raw_diagnostics = historical_metadata.get("diagnostics", {})
    diagnostics_payload = _diagnostics_lite_view(raw_diagnostics, grouped_candidates=grouped_candidates, warmup_candidates=warmup_candidates, trading_dates=trading_dates, plans=plans, fills=fills) if diagnostics_lite else raw_diagnostics
    signal_panel_payload = historical_metadata.get("signal_panel_artifact", [])
    forecast_panel_artifact = {"row_count": len(_forecast_rows(signal_panel_payload))}
    if diagnostics_lite:
        signal_panel_payload = {
            "row_count": len(signal_panel_payload),
            "decision_dates": len({str(r.get('decision_date')) for r in signal_panel_payload if isinstance(r, dict) and r.get('decision_date')}),
            "symbols": len({str(r.get('symbol')) for r in signal_panel_payload if isinstance(r, dict) and r.get('symbol')}),
        }
    validation_bootstrap_timer = _stage_timer(emit_timing_logs, "validation_bootstrap")
    if progress_callback:
        progress_callback({"phase": "summary_and_validation", "status": "running", "total_trading_dates": len(trading_dates), "completed_trading_dates": len(trading_dates)})
    bootstrap_validation_result = {
        "historical_context": historical_context,
        "bars_by_symbol": historical_context["bars_by_symbol"],
        "macro_history_by_date": historical_context["macro_history_by_date"],
        "macro_series_history": historical_context["macro_series_history"],
        "sector_map": historical_context["sector_map"],
        "session_metadata_by_symbol": historical_context["session_metadata_by_symbol"],
        "trading_dates": historical_context["trading_dates"],
        "portfolio": {"selected_symbols": selected_symbols, "decisions": [{"symbol": d.candidate.symbol, "selected": d.selected, "side": d.side.value, "size_multiplier": d.size_multiplier, "requested_budget": d.requested_budget, "expected_horizon_days": d.expected_horizon_days, "kill_reason": d.kill_reason, "diagnostics": d.diagnostics, "decision_date": _candidate_decision_date(d.candidate)} for d in portfolio_decisions_all], "date_artifacts": date_artifacts, **portfolio_paths},
        "plans": [p.to_dict() for p in plans],
        "fills": [f.to_dict() for f in fills],
        "diagnostics": diagnostics_payload,
        "artifacts": {"signal_panel": signal_panel_payload, "forecast_panel": forecast_panel_artifact, "warmup_candidates": [{"symbol": c.symbol, "decision_date": _candidate_decision_date(c)} for c in warmup_candidates], "historical_context": historical_context, "candidate_reuse": _policy_reuse_payload(historical=historical, grouped_candidates=grouped_candidates, warmup_candidates=warmup_candidates, trading_dates=trading_dates)},
    }
    validation_folds = run_fold_validation(request=request, data_path=data_path, data_source=data_source, scenario_id=scenario_id, strategy_mode=strategy_mode, runner_fn=run_backtest, mode="walk_forward", max_folds=validation_max_folds, summary_only=validation_summary_only, diagnostics_lite=diagnostics_lite, emit_timing_logs=emit_timing_logs, bootstrap_result=bootstrap_validation_result) if strategy_mode == "research_similarity_v2" and enable_validation else {"mode": "disabled", "folds": [], "aggregate": {}, "rejection_reasons": [], "train_artifacts": [], "test_artifacts": []}
    validation_bootstrap_timer(f"folds={len(validation_folds.get('folds') or [])}")
    reproducibility = _reproducibility_payload(request=request, manifest=manifest, raw_diagnostics=raw_diagnostics, signal_panel_payload=signal_panel_payload, validation_folds=validation_folds)
    sensitivity = [p.__dict__ for p in sensitivity_sweep(plans=plans, fills=fills, fee_grid=[0.0, request.config.fee_bps, request.config.fee_bps + 5.0], slippage_grid=[0.0, request.config.slippage_bps, request.config.slippage_bps + 5.0], total_symbols=len(request.scenario.symbols), bars_by_symbol=historical.bars_by_symbol)]
    quote_policy_sweep = {"ev_threshold": [0.003, quote_policy_cfg.ev_threshold, 0.010], "min_fill_probability": [0.05, quote_policy_cfg.min_fill_probability, 0.20], "uncertainty_caps": [0.08, quote_policy_cfg.uncertainty_cap, 0.16]}
    result = {"scenario": request.scenario.scenario_id, "strategy_mode": strategy_mode, "manifest": manifest.to_dict(), "historical_context": historical_context, "bars_by_symbol": historical_context["bars_by_symbol"], "macro_history_by_date": historical_context["macro_history_by_date"], "macro_series_history": historical_context["macro_series_history"], "sector_map": historical_context["sector_map"], "session_metadata_by_symbol": historical_context["session_metadata_by_symbol"], "trading_dates": historical_context["trading_dates"], "portfolio": {"selected_symbols": selected_symbols, "decisions": [{"symbol": d.candidate.symbol, "selected": d.selected, "side": d.side.value, "size_multiplier": d.size_multiplier, "requested_budget": d.requested_budget, "expected_horizon_days": d.expected_horizon_days, "kill_reason": d.kill_reason, "diagnostics": d.diagnostics, "decision_date": _candidate_decision_date(d.candidate)} for d in portfolio_decisions_all], "date_artifacts": date_artifacts, **portfolio_paths}, "plans": [p.to_dict() for p in plans], "fills": [f.to_dict() for f in fills], "summary": summary.__dict__, "diagnostics": {**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, "artifacts": {"signal_panel": signal_panel_payload, "forecast_panel": forecast_panel_artifact, "warmup_candidates": [{"symbol": c.symbol, "decision_date": _candidate_decision_date(c)} for c in warmup_candidates], "historical_context": historical_context, "candidate_reuse": _policy_reuse_payload(historical=historical, grouped_candidates=grouped_candidates, warmup_candidates=warmup_candidates, trading_dates=trading_dates), "reproducibility": reproducibility}, "validation": {"fold_engine": validation_folds, "sensitivity_sweep": sensitivity, "quote_policy_sweep": quote_policy_sweep, "coverage": summary.metadata.get("coverage", 0.0), "no_trade_ratio": summary.metadata.get("no_trade_ratio", 0.0)}, "skipped": skipped, "authoritative": reproducibility.get("authoritative")}
    if sql_db_url:
        snapshot_info = {"data_source": data_source, "strategy_mode": strategy_mode, "historical_metadata": historical_metadata, "date_artifacts": date_artifacts, "reproducibility": reproducibility}
        result["sql_run_id"] = SqlResultStore(sql_db_url, namespace="research").save_run(run_key=manifest.manifest_id(), scenario_id=request.scenario.scenario_id, strategy_id=request.scenario.strategy_id, strategy_mode=strategy_mode, market=request.scenario.market, data_source=data_source, config_version=request.scenario.strategy_version, label_version=str(request.config.metadata.get("label_version", "v1")), vector_version=str(request.config.metadata.get("vector_version", strategy_mode)), initial_capital=request.config.initial_capital, params={"scenario_params": request.scenario.params, "scenario_notes": request.scenario.notes, "config_metadata": request.config.metadata}, summary=summary.__dict__, diagnostics={**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, plans=plans, fills=fills, snapshot_info=snapshot_info, manifest=manifest.to_dict())
    if output_dir and save_json:
        authoritative_summary = _authoritative_summary_payload(
            request=request,
            raw_diagnostics=raw_diagnostics,
            signal_panel_payload=signal_panel_payload,
            selected_symbols=selected_symbols,
            plans=result["plans"],
            fills=result["fills"],
            reproducibility=reproducibility,
            forecast_panel_artifact=forecast_panel_artifact,
            result_path=None,
        )
        authoritative_summary_path = _write_authoritative_summary_artifact(output_dir=output_dir, payload=authoritative_summary)
        _emit_progress(progress_callback, {
            "phase": "write_artifacts",
            "status": "running",
            "artifact_name": "authoritative_summary_prewrite",
            "artifact_index": 1,
            "artifact_total": 5,
            "artifact_bytes": Path(authoritative_summary_path).stat().st_size if Path(authoritative_summary_path).exists() else 0,
            "bytes_written": Path(authoritative_summary_path).stat().st_size if Path(authoritative_summary_path).exists() else 0,
            "authoritative_summary_path": authoritative_summary_path,
            "candidate_rows": len(historical.candidates or []),
            "plans_count": len(plans),
            "fills_count": len(fills),
            "total_trading_dates": len(trading_dates),
            "completed_trading_dates": len(trading_dates),
        })
        write_timer = _stage_timer(emit_timing_logs, "write_artifacts")
        forecast_panel_artifact = _write_forecast_panel_artifacts(
            output_dir=output_dir,
            run_id=manifest.manifest_id(),
            signal_panel_payload=signal_panel_payload,
            progress_callback=progress_callback,
            total_trading_dates=len(trading_dates),
            completed_trading_dates=len(trading_dates),
            candidate_rows=len(historical.candidates or []),
            plans_count=len(plans),
            fills_count=len(fills),
        )
        result["artifacts"]["forecast_panel"] = forecast_panel_artifact
        authoritative_summary["forecast_panel"] = forecast_panel_artifact
        authoritative_summary_path = _write_authoritative_summary_artifact(output_dir=output_dir, payload=authoritative_summary)
        result["result_path"] = JsonResultStore(output_dir, namespace="research").save_run(run_id=manifest.manifest_id(), plans=plans, fills=fills, summary={**summary.__dict__, "diagnostics": {**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, "strategy_mode": strategy_mode}, diagnostics={"quote_policy_sweep": quote_policy_sweep, "portfolio": result["portfolio"], "signal_diagnostics": {**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, "reproducibility": reproducibility}, manifest=manifest.to_dict())
        authoritative_summary["result_path"] = result.get("result_path")
        authoritative_summary_path = _write_authoritative_summary_artifact(output_dir=output_dir, payload=authoritative_summary)
        write_timer(result.get("result_path") or "")
        try:
            result_bytes = Path(str(result.get("result_path"))).stat().st_size if result.get("result_path") else 0
        except Exception:
            result_bytes = 0
        _emit_progress(progress_callback, {
            "phase": "write_artifacts",
            "status": "running",
            "artifact_name": "result_json",
            "artifact_index": 5,
            "artifact_total": 5,
            "artifact_bytes": result_bytes,
            "bytes_written": result_bytes + (Path(authoritative_summary_path).stat().st_size if Path(authoritative_summary_path).exists() else 0),
            "candidate_rows": len(historical.candidates or []),
            "plans_count": len(plans),
            "fills_count": len(fills),
            "total_trading_dates": len(trading_dates),
            "completed_trading_dates": len(trading_dates),
            "result_path": result.get("result_path"),
            "authoritative_summary_path": authoritative_summary_path,
        })
    if progress_callback:
        progress_callback({"phase": "complete", "status": "ok", "total_trading_dates": len(trading_dates), "completed_trading_dates": len(trading_dates), "plans_count": len(plans), "fills_count": len(fills), "candidate_rows": len(historical.candidates or []), "result_path": result.get("result_path")})
    total_timer(f"plans={len(plans)} fills={len(fills)}")
    return result

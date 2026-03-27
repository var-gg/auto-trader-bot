from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime
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
    return SimpleNamespace(
        bars_by_symbol=payload.get("bars_by_symbol") or {},
        candidates=payload.get("candidates") or [],
        metadata=metadata,
    )


def _policy_reuse_payload(*, historical, grouped_candidates: dict[str, list], warmup_candidates: list, trading_dates: list[str]) -> dict:
    diagnostics = getattr(historical, "metadata", {}) or {}
    return {
        "bars_by_symbol": historical.bars_by_symbol,
        "candidates": list(getattr(historical, "candidates", []) or []),
        "metadata": dict(diagnostics),
        "signal_panel": diagnostics.get("signal_panel_artifact", []),
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


def _reproducibility_payload(*, request: RunnerRequest, manifest, raw_diagnostics: dict | None, signal_panel_payload, validation_folds: dict | None) -> dict:
    metadata = dict(request.config.metadata or {})
    diagnostic_flag_keys = sorted(k for k in metadata if k.startswith("diagnostic_") or k in {"validation_summary_only", "diagnostics_lite"})
    validation_snapshot_ids = []
    for fold in (validation_folds or {}).get("folds") or []:
        artifact = (fold or {}).get("artifact") or {}
        for snapshot_id in artifact.get("snapshot_ids") or []:
            if snapshot_id not in validation_snapshot_ids:
                validation_snapshot_ids.append(snapshot_id)
    return {
        "git_commit": getattr(manifest, "code_commit", None),
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


def load_historical(request: RunnerRequest, data_path: str | None, data_source: str, scenario_id: str | None, strategy_mode: str):
    if data_source == "local-db":
        cfg = LocalBacktestDbConfig.from_env()
        guard_backtest_local_only(cfg.url)
        session_factory = create_backtest_session_factory(cfg)
        loader = LocalPostgresLoader(session_factory, schema=cfg.schema)
        return loader.load_for_scenario(scenario_id=scenario_id or request.scenario.scenario_id, market=request.scenario.market, start_date=request.scenario.start_date, end_date=request.scenario.end_date, symbols=request.scenario.symbols, strategy_mode=strategy_mode, research_spec=request.config.research_spec, metadata=request.config.metadata)
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


def execute_daily_execution_loop(*, trading_dates: list[str], grouped_candidates: dict[str, list], bars_by_symbol: dict, config: BacktestConfig, market: str, strategy_mode: str, portfolio_cfg: PortfolioConfig, quote_policy_cfg: QuotePolicyConfig, tuning: dict, broker, initial_state: dict | None = None):
    state = dict(initial_state or {"cash": float(config.initial_capital), "reserved_capital": 0.0, "open_positions": {}, "turnover_used": 0})
    state["open_positions"] = dict(state.get("open_positions") or {})
    date_artifacts = []
    plans = []
    fills = []
    skipped = []
    selected_symbols = []
    portfolio_decisions_all = []
    for decision_date in trading_dates:
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


def run_backtest(request: RunnerRequest, data_path: str | None, *, output_dir: str | None = None, save_json: bool = True, sql_db_url: str | None = None, data_source: str = "json", scenario_id: str | None = None, strategy_mode: str = "legacy_event_window", enable_validation: bool = True, validation_max_folds: int | None = None, validation_summary_only: bool = False, diagnostics_lite: bool = False, candidate_reuse_payload: dict | None = None, emit_timing_logs: bool = False) -> dict:
    total_timer = _stage_timer(emit_timing_logs, "total")
    load_timer = _stage_timer(emit_timing_logs, "load_bars")
    if candidate_reuse_payload is not None:
        historical = _history_from_reuse_payload(candidate_reuse_payload)
    else:
        historical = load_historical(request, data_path, data_source, scenario_id, strategy_mode)
    load_timer(f"symbols={len(request.scenario.symbols)} reuse={candidate_reuse_payload is not None}")
    tuning = _tuning_config()
    portfolio_cfg = PortfolioConfig(top_n=int(request.config.metadata.get("portfolio_top_n", 5) or 5), risk_budget_fraction=float(request.config.metadata.get("portfolio_risk_budget_fraction", 0.95) or 0.95))
    quote_policy_cfg = QuotePolicyConfig(ev_threshold=float(request.config.metadata.get("quote_ev_threshold", 0.005)), uncertainty_cap=float(request.config.metadata.get("quote_uncertainty_cap", 0.12)), min_effective_sample_size=float(request.config.metadata.get("quote_min_effective_sample_size", 1.5)), min_fill_probability=float(request.config.metadata.get("quote_min_fill_probability", 0.10)))
    broker = SimulatedBroker(rules=SimulationRules(slippage_bps=request.config.slippage_bps, fee_bps=request.config.fee_bps, allow_partial_fills=request.config.allow_partial_fills))
    candidate_timer = _stage_timer(emit_timing_logs, "candidate_generation")
    grouped_candidates, warmup_candidates = _candidate_groups(historical.candidates, start_date=request.scenario.start_date, end_date=request.scenario.end_date)
    trading_dates = candidate_reuse_payload.get("trading_dates") if candidate_reuse_payload else None
    trading_dates = trading_dates or _scenario_trading_dates(bars_by_symbol=historical.bars_by_symbol, start_date=request.scenario.start_date, end_date=request.scenario.end_date)
    candidate_timer(f"candidate_dates={len(grouped_candidates)} warmup={len(warmup_candidates)}")
    execution = execute_daily_execution_loop(trading_dates=trading_dates, grouped_candidates=grouped_candidates, bars_by_symbol=historical.bars_by_symbol, config=request.config, market=request.scenario.market, strategy_mode=strategy_mode, portfolio_cfg=portfolio_cfg, quote_policy_cfg=quote_policy_cfg, tuning=tuning, broker=broker)
    state = execution["state"]
    date_artifacts = execution["date_artifacts"]
    plans = execution["plans"]
    fills = execution["fills"]
    skipped = execution["skipped"]
    selected_symbols = execution["selected_symbols"]
    portfolio_decisions_all = execution["portfolio_decisions_all"]
    summary = build_summary(scenario_id=request.scenario.scenario_id, plans=plans, fills=fills, bars_by_symbol=historical.bars_by_symbol, date_artifacts=date_artifacts)
    historical_metadata = getattr(historical, "metadata", {}) or {}
    historical_context = {"bars_by_symbol": historical.bars_by_symbol, "macro_history_by_date": historical_metadata.get("macro_history_by_date", {}), "sector_map": historical_metadata.get("sector_map", {}), "trading_dates": trading_dates}
    historical_metadata["bars_by_symbol"] = historical.bars_by_symbol
    historical_metadata["historical_context"] = historical_context
    manifest = ensure_manifest(request=request, data_source=data_source, historical_metadata=historical_metadata)
    raw_diagnostics = historical_metadata.get("diagnostics", {})
    diagnostics_payload = _diagnostics_lite_view(raw_diagnostics, grouped_candidates=grouped_candidates, warmup_candidates=warmup_candidates, trading_dates=trading_dates, plans=plans, fills=fills) if diagnostics_lite else raw_diagnostics
    signal_panel_payload = historical_metadata.get("signal_panel_artifact", [])
    if diagnostics_lite:
        signal_panel_payload = {
            "row_count": len(signal_panel_payload),
            "decision_dates": len({str(r.get('decision_date')) for r in signal_panel_payload if isinstance(r, dict) and r.get('decision_date')}),
            "symbols": len({str(r.get('symbol')) for r in signal_panel_payload if isinstance(r, dict) and r.get('symbol')}),
        }
    validation_bootstrap_timer = _stage_timer(emit_timing_logs, "validation_bootstrap")
    bootstrap_validation_result = {
        "historical_context": historical_context,
        "bars_by_symbol": historical_context["bars_by_symbol"],
        "macro_history_by_date": historical_context["macro_history_by_date"],
        "sector_map": historical_context["sector_map"],
        "trading_dates": historical_context["trading_dates"],
        "portfolio": {"selected_symbols": selected_symbols, "decisions": [{"symbol": d.candidate.symbol, "selected": d.selected, "side": d.side.value, "size_multiplier": d.size_multiplier, "requested_budget": d.requested_budget, "expected_horizon_days": d.expected_horizon_days, "kill_reason": d.kill_reason, "diagnostics": d.diagnostics, "decision_date": _candidate_decision_date(d.candidate)} for d in portfolio_decisions_all], "date_artifacts": date_artifacts},
        "plans": [p.to_dict() for p in plans],
        "fills": [f.to_dict() for f in fills],
        "diagnostics": diagnostics_payload,
        "artifacts": {"signal_panel": signal_panel_payload, "warmup_candidates": [{"symbol": c.symbol, "decision_date": _candidate_decision_date(c)} for c in warmup_candidates], "historical_context": historical_context, "candidate_reuse": _policy_reuse_payload(historical=historical, grouped_candidates=grouped_candidates, warmup_candidates=warmup_candidates, trading_dates=trading_dates)},
    }
    validation_folds = run_fold_validation(request=request, data_path=data_path, data_source=data_source, scenario_id=scenario_id, strategy_mode=strategy_mode, runner_fn=run_backtest, mode="walk_forward", max_folds=validation_max_folds, summary_only=validation_summary_only, diagnostics_lite=diagnostics_lite, emit_timing_logs=emit_timing_logs, bootstrap_result=bootstrap_validation_result) if strategy_mode == "research_similarity_v2" and enable_validation else {"mode": "disabled", "folds": [], "aggregate": {}, "rejection_reasons": [], "train_artifacts": [], "test_artifacts": []}
    validation_bootstrap_timer(f"folds={len(validation_folds.get('folds') or [])}")
    reproducibility = _reproducibility_payload(request=request, manifest=manifest, raw_diagnostics=raw_diagnostics, signal_panel_payload=signal_panel_payload, validation_folds=validation_folds)
    sensitivity = [p.__dict__ for p in sensitivity_sweep(plans=plans, fills=fills, fee_grid=[0.0, request.config.fee_bps, request.config.fee_bps + 5.0], slippage_grid=[0.0, request.config.slippage_bps, request.config.slippage_bps + 5.0], total_symbols=len(request.scenario.symbols), bars_by_symbol=historical.bars_by_symbol)]
    quote_policy_sweep = {"ev_threshold": [0.003, quote_policy_cfg.ev_threshold, 0.010], "min_fill_probability": [0.05, quote_policy_cfg.min_fill_probability, 0.20], "uncertainty_caps": [0.08, quote_policy_cfg.uncertainty_cap, 0.16]}
    result = {"scenario": request.scenario.scenario_id, "strategy_mode": strategy_mode, "manifest": manifest.to_dict(), "historical_context": historical_context, "bars_by_symbol": historical_context["bars_by_symbol"], "macro_history_by_date": historical_context["macro_history_by_date"], "sector_map": historical_context["sector_map"], "trading_dates": historical_context["trading_dates"], "portfolio": {"selected_symbols": selected_symbols, "decisions": [{"symbol": d.candidate.symbol, "selected": d.selected, "side": d.side.value, "size_multiplier": d.size_multiplier, "requested_budget": d.requested_budget, "expected_horizon_days": d.expected_horizon_days, "kill_reason": d.kill_reason, "diagnostics": d.diagnostics, "decision_date": _candidate_decision_date(d.candidate)} for d in portfolio_decisions_all], "date_artifacts": date_artifacts}, "plans": [p.to_dict() for p in plans], "fills": [f.to_dict() for f in fills], "summary": summary.__dict__, "diagnostics": {**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, "artifacts": {"signal_panel": signal_panel_payload, "warmup_candidates": [{"symbol": c.symbol, "decision_date": _candidate_decision_date(c)} for c in warmup_candidates], "historical_context": historical_context, "candidate_reuse": _policy_reuse_payload(historical=historical, grouped_candidates=grouped_candidates, warmup_candidates=warmup_candidates, trading_dates=trading_dates), "reproducibility": reproducibility}, "validation": {"fold_engine": validation_folds, "sensitivity_sweep": sensitivity, "quote_policy_sweep": quote_policy_sweep, "coverage": summary.metadata.get("coverage", 0.0), "no_trade_ratio": summary.metadata.get("no_trade_ratio", 0.0)}, "skipped": skipped}
    if sql_db_url:
        snapshot_info = {"data_source": data_source, "strategy_mode": strategy_mode, "historical_metadata": historical_metadata, "date_artifacts": date_artifacts, "reproducibility": reproducibility}
        result["sql_run_id"] = SqlResultStore(sql_db_url, namespace="research").save_run(run_key=manifest.manifest_id(), scenario_id=request.scenario.scenario_id, strategy_id=request.scenario.strategy_id, strategy_mode=strategy_mode, market=request.scenario.market, data_source=data_source, config_version=request.scenario.strategy_version, label_version=str(request.config.metadata.get("label_version", "v1")), vector_version=str(request.config.metadata.get("vector_version", strategy_mode)), initial_capital=request.config.initial_capital, params={"scenario_params": request.scenario.params, "scenario_notes": request.scenario.notes, "config_metadata": request.config.metadata}, summary=summary.__dict__, diagnostics={**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, plans=plans, fills=fills, snapshot_info=snapshot_info, manifest=manifest.to_dict())
    if output_dir and save_json:
        write_timer = _stage_timer(emit_timing_logs, "write_artifacts")
        result["result_path"] = JsonResultStore(output_dir, namespace="research").save_run(run_id=manifest.manifest_id(), plans=plans, fills=fills, summary={**summary.__dict__, "diagnostics": {**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, "strategy_mode": strategy_mode}, diagnostics={"quote_policy_sweep": quote_policy_sweep, "portfolio": result["portfolio"], "signal_diagnostics": {**(diagnostics_payload if isinstance(diagnostics_payload, dict) else {}), "reproducibility": reproducibility}, "reproducibility": reproducibility}, manifest=manifest.to_dict())
        write_timer(result.get("result_path") or "")
    total_timer(f"plans={len(plans)} fills={len(fills)}")
    return result

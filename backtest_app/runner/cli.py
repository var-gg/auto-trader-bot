from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, ResearchExperimentSpec, RunnerRequest
from backtest_app.research_runtime.runner import build_data_snapshot_id, ensure_manifest
from backtest_app.research_runtime.service import execute_research_backtest
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.portfolio import PortfolioConfig, PortfolioState, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab
from backtest_app.reporting.summary import build_summary
from backtest_app.results.store import JsonResultStore, SqlResultStore
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from backtest_app.validation import run_fold_validation, sensitivity_sweep
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, FillStatus, Side


STRATEGY_MODES = ["legacy_event_window", "research_similarity_v1", "research_similarity_v2"]


def _load_historical(request: RunnerRequest, data_path: str | None, data_source: str, scenario_id: str | None, strategy_mode: str):
    if data_source == "local-db":
        cfg = LocalBacktestDbConfig.from_env()
        guard_backtest_local_only(cfg.url)
        session_factory = create_backtest_session_factory(cfg)
        loader = LocalPostgresLoader(session_factory, schema=cfg.schema)
        return loader.load_for_scenario(scenario_id=scenario_id or request.scenario.scenario_id, market=request.scenario.market, start_date=request.scenario.start_date, end_date=request.scenario.end_date, symbols=request.scenario.symbols, strategy_mode=strategy_mode, research_spec=request.config.research_spec)
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


def _tuning_config():
    return {
        "MIN_TICK_GAP": 1,
        "ADAPTIVE_BASE_LEGS": 2,
        "ADAPTIVE_LEG_BOOST": 1.0,
        "MIN_TOTAL_SPREAD_PCT": 0.01,
        "ADAPTIVE_STRENGTH_SCALE": 0.1,
        "FIRST_LEG_BASE_PCT": 0.012,
        "FIRST_LEG_MIN_PCT": 0.006,
        "FIRST_LEG_MAX_PCT": 0.05,
        "FIRST_LEG_GAIN_WEIGHT": 0.6,
        "FIRST_LEG_ATR_WEIGHT": 0.5,
        "FIRST_LEG_REQ_FLOOR_PCT": 0.012,
        "MIN_FIRST_LEG_GAP_PCT": 0.03,
        "STRICT_MIN_FIRST_GAP": True,
        "ADAPTIVE_MAX_STEP_PCT": 0.06,
        "ADAPTIVE_FRAC_ALPHA": 1.25,
        "ADAPTIVE_GAIN_SCALE": 0.1,
        "MIN_LOT_QTY": 1,
    }


def _close_positions_for_day(*, day: str, state: dict, bars_by_symbol: dict, config: BacktestConfig):
    realized = []
    for symbol, pos in list(state["open_positions"].items()):
        bars = bars_by_symbol.get(symbol, [])
        exit_bar = next((b for b in bars if str(b.timestamp)[:10] == day), None)
        if not exit_bar:
            continue
        if day < pos["planned_exit_date"]:
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
        realized.append({"symbol": symbol, "exit_date": day, "pnl": pnl, "qty": qty, "side": pos["side"], "entry_date": pos.get("entry_date"), "first_fill_date": pos.get("first_fill_date"), "planned_exit_date": pos.get("planned_exit_date"), "realized_exit_date": day})
        del state["open_positions"][symbol]
    return realized


def _open_positions_market_value(*, day: str, state: dict, bars_by_symbol: dict):
    exposure = 0.0
    for symbol, pos in state["open_positions"].items():
        bar = next((b for b in bars_by_symbol.get(symbol, []) if str(b.timestamp)[:10] == day), None)
        mark = float(bar.close) if bar else float(pos["entry_price"])
        exposure += mark * float(pos["filled_quantity"] or 0.0)
    return exposure


def run_backtest(request: RunnerRequest, data_path: str | None, *, output_dir: str | None = None, save_json: bool = True, sql_db_url: str | None = None, data_source: str = "json", scenario_id: str | None = None, strategy_mode: str = "legacy_event_window", enable_validation: bool = True) -> dict:
    historical = _load_historical(request, data_path, data_source, scenario_id, strategy_mode)
    tuning = _tuning_config()
    portfolio_cfg = PortfolioConfig()
    quote_policy_cfg = QuotePolicyConfig(ev_threshold=float(request.config.metadata.get("quote_ev_threshold", 0.005)), uncertainty_cap=float(request.config.metadata.get("quote_uncertainty_cap", 0.12)), min_effective_sample_size=float(request.config.metadata.get("quote_min_effective_sample_size", 1.5)), min_fill_probability=float(request.config.metadata.get("quote_min_fill_probability", 0.10)))
    broker = SimulatedBroker(rules=SimulationRules(slippage_bps=request.config.slippage_bps, fee_bps=request.config.fee_bps, allow_partial_fills=request.config.allow_partial_fills))

    grouped_candidates, warmup_candidates = _candidate_groups(historical.candidates, start_date=request.scenario.start_date, end_date=request.scenario.end_date)
    all_dates = sorted(grouped_candidates.keys())
    state = {"cash": float(request.config.initial_capital), "reserved_capital": 0.0, "open_positions": {}, "turnover_used": 0}
    date_artifacts = []
    plans = []
    fills = []
    skipped = []
    selected_symbols = []
    portfolio_decisions_all = []

    for decision_date in all_dates:
        realized_today = _close_positions_for_day(day=decision_date, state=state, bars_by_symbol=historical.bars_by_symbol, config=request.config)
        candidates = grouped_candidates.get(decision_date, [])
        pstate = PortfolioState(cash=state["cash"], reserved_capital=state["reserved_capital"], open_positions=dict(state["open_positions"]), turnover_used=state["turnover_used"])
        decisions = build_portfolio_decisions(candidates=candidates, initial_capital=request.config.initial_capital, cfg=portfolio_cfg, state=pstate)
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
            plan, skip = build_order_plan_from_candidate(candidate, generated_at=generated_at, market=request.scenario.market, side=candidate.side_bias if strategy_mode in {"research_similarity_v1", "research_similarity_v2"} else Side.BUY, tuning=tuning, budget=max(0.0, decision.requested_budget), venue=ExecutionVenue.BACKTEST, rationale_prefix=f"{request.scenario.strategy_id}:{strategy_mode}", quote_policy=active_policy)
            if not plan:
                day_rejected.append({"symbol": candidate.symbol, "reason": (skip or {}).get("code", "NO_PLAN"), "diagnostics": active_policy})
                if skip:
                    skipped.append({"symbol": candidate.symbol, **skip, "strategy_mode": strategy_mode, "quote_policy_ab": policy_ab, "decision_date": decision_date})
                continue
            plan.metadata["quote_policy_ab"] = policy_ab
            plan.metadata["decision_date"] = decision_date
            execution_date = str(plan.metadata.get("executable_from_date") or decision_date)
            bars = [bar for bar in historical.bars_by_symbol.get(plan.symbol, []) if str(bar.timestamp)[:10] >= (execution_date if strategy_mode == "research_similarity_v2" else decision_date)]
            day_fills = broker.simulate_plan(plan, bars)
            plans.append(plan)
            fills.extend(day_fills)
            fill_rows = [f for f in day_fills if f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL}]
            filled_qty = sum(float(f.filled_quantity or 0.0) for f in fill_rows)
            avg_fill = (sum(float(f.average_fill_price or 0.0) * float(f.filled_quantity or 0.0) for f in fill_rows) / filled_qty) if filled_qty > 0 else 0.0
            if filled_qty > 0:
                horizon_days = int(candidate.expected_horizon_days or 5)
                first_fill_date = min(str(f.event_time)[:10] for f in fill_rows)
                bars_for_symbol = [b for b in historical.bars_by_symbol.get(plan.symbol, []) if str(b.timestamp)[:10] >= first_fill_date]
                exit_idx = min(max(horizon_days, 1), max(len(bars_for_symbol) - 1, 0))
                planned_exit_date = str(bars_for_symbol[exit_idx].timestamp)[:10] if bars_for_symbol else first_fill_date
                reserved = float(decision.requested_budget)
                plan.metadata["entry_date"] = first_fill_date
                plan.metadata["first_fill_date"] = first_fill_date
                plan.metadata["planned_exit_date"] = planned_exit_date
                plan.metadata.setdefault("realized_exit_date", None)
                state["cash"] -= reserved
                state["reserved_capital"] += reserved
                state["open_positions"][plan.symbol] = {"side": plan.side.value, "entry_price": avg_fill or float(candidate.current_price or 0.0), "filled_quantity": filled_qty, "reserved_budget": reserved, "planned_exit_date": planned_exit_date, "decision_date": decision_date, "entry_date": first_fill_date, "first_fill_date": first_fill_date, "plan_ref": plan}
                state["turnover_used"] += 1
                day_selected.append({"symbol": candidate.symbol, "side": decision.side.value, "requested_budget": decision.requested_budget, "size_multiplier": decision.size_multiplier, "policy_reason": active_policy.get("chosen_policy_reason"), "entry_date": first_fill_date, "first_fill_date": first_fill_date, "planned_exit_date": planned_exit_date})
                selected_symbols.append({"symbol": candidate.symbol, "side": decision.side.value, "size_multiplier": decision.size_multiplier, "expected_horizon_days": decision.expected_horizon_days, "decision_date": decision_date, "entry_date": first_fill_date, "first_fill_date": first_fill_date, "planned_exit_date": planned_exit_date})
            else:
                day_rejected.append({"symbol": candidate.symbol, "reason": "no_fill", "diagnostics": active_policy})

        exposure = _open_positions_market_value(day=decision_date, state=state, bars_by_symbol=historical.bars_by_symbol)
        date_artifacts.append({"decision_date": decision_date, "selected": day_selected, "rejected": day_rejected, "realized_today": realized_today, "cash": state["cash"], "reserved_capital": state["reserved_capital"], "exposure": exposure, "open_position_count": len(state["open_positions"]), "open_positions": sorted(state["open_positions"].keys())})

    summary = build_summary(scenario_id=request.scenario.scenario_id, plans=plans, fills=fills, bars_by_symbol=historical.bars_by_symbol, date_artifacts=date_artifacts)
    historical_metadata = getattr(historical, "metadata", {}) or {}
    historical_metadata["bars_by_symbol"] = historical.bars_by_symbol
    manifest = ensure_manifest(request=request, data_source=data_source, historical_metadata=historical_metadata)
    diagnostics = historical_metadata.get("diagnostics", {})
    validation_folds = run_fold_validation(request=request, data_path=data_path, data_source=data_source, scenario_id=scenario_id, strategy_mode=strategy_mode, runner_fn=run_backtest, mode="walk_forward") if strategy_mode == "research_similarity_v2" and enable_validation else {"mode": "disabled", "folds": [], "aggregate": {}, "rejection_reasons": [], "train_artifacts": [], "test_artifacts": []}
    sensitivity = [p.__dict__ for p in sensitivity_sweep(plans=plans, fills=fills, fee_grid=[0.0, request.config.fee_bps, request.config.fee_bps + 5.0], slippage_grid=[0.0, request.config.slippage_bps, request.config.slippage_bps + 5.0], total_symbols=len(request.scenario.symbols), bars_by_symbol=historical.bars_by_symbol)]
    quote_policy_sweep = {"ev_threshold": [0.003, quote_policy_cfg.ev_threshold, 0.010], "gap_grid": list(quote_policy_cfg.gap_grid), "size_grid": list(quote_policy_cfg.size_grid), "min_fill_probability": [0.05, quote_policy_cfg.min_fill_probability, 0.20], "uncertainty_caps": [0.08, quote_policy_cfg.uncertainty_cap, 0.16]}
    result = {
        "scenario": request.scenario.scenario_id,
        "strategy_mode": strategy_mode,
        "manifest": manifest.to_dict(),
        "portfolio": {
            "selected_symbols": selected_symbols,
            "decisions": [{"symbol": d.candidate.symbol, "selected": d.selected, "side": d.side.value, "size_multiplier": d.size_multiplier, "requested_budget": d.requested_budget, "expected_horizon_days": d.expected_horizon_days, "kill_reason": d.kill_reason, "abstain_reason": ((d.candidate.diagnostics.get("ev", {}).get("long", {}) if d.side == Side.BUY else d.candidate.diagnostics.get("ev", {}).get("short", {})).get("abstain_reasons", [])) if isinstance(d.candidate.diagnostics, dict) else [], "diagnostics": d.diagnostics, "decision_date": _candidate_decision_date(d.candidate)} for d in portfolio_decisions_all],
            "date_artifacts": date_artifacts,
            "cash_path": [{"decision_date": row["decision_date"], "cash": row["cash"]} for row in date_artifacts],
            "exposure_path": [{"decision_date": row["decision_date"], "exposure": row["exposure"]} for row in date_artifacts],
            "open_position_count_path": [{"decision_date": row["decision_date"], "open_position_count": row["open_position_count"]} for row in date_artifacts],
        },
        "plans": [p.to_dict() for p in plans],
        "fills": [f.to_dict() for f in fills],
        "summary": summary.__dict__,
        "diagnostics": diagnostics,
        "artifacts": {"signal_panel": historical_metadata.get("signal_panel_artifact", []), "warmup_candidates": [{"symbol": c.symbol, "decision_date": _candidate_decision_date(c)} for c in warmup_candidates]},
        "validation": {"fold_engine": validation_folds, "sensitivity_sweep": sensitivity, "quote_policy_sweep": quote_policy_sweep, "coverage": summary.metadata.get("coverage", 0.0), "no_trade_ratio": summary.metadata.get("no_trade_ratio", 0.0)},
        "skipped": skipped,
    }

    if sql_db_url:
        snapshot_info = {"data_source": data_source, "strategy_mode": strategy_mode, "historical_metadata": historical_metadata, "date_artifacts": date_artifacts}
        run_id = SqlResultStore(sql_db_url, namespace="research").save_run(run_key=manifest.manifest_id(), scenario_id=request.scenario.scenario_id, strategy_id=request.scenario.strategy_id, strategy_mode=strategy_mode, market=request.scenario.market, data_source=data_source, config_version=request.scenario.strategy_version, label_version=str(request.config.metadata.get("label_version", "v1")), vector_version=str(request.config.metadata.get("vector_version", strategy_mode)), initial_capital=request.config.initial_capital, params={"scenario_params": request.scenario.params, "scenario_notes": request.scenario.notes, "config_metadata": request.config.metadata}, summary=summary.__dict__, diagnostics=diagnostics, plans=plans, fills=fills, snapshot_info=snapshot_info, manifest=manifest.to_dict())
        result["sql_run_id"] = run_id

    if output_dir and save_json:
        result_path = JsonResultStore(output_dir, namespace="research").save_run(run_id=manifest.manifest_id(), plans=plans, fills=fills, summary={**summary.__dict__, "diagnostics": diagnostics, "strategy_mode": strategy_mode}, diagnostics={"quote_policy_sweep": quote_policy_sweep, "portfolio": result["portfolio"], "signal_diagnostics": diagnostics}, manifest=manifest.to_dict())
        result["result_path"] = result_path
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run backtest_app batch runner")
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--symbols", required=True, help="comma-separated")
    parser.add_argument("--data", default="", help="historical json fixture path")
    parser.add_argument("--data-source", choices=["json", "local-db"], default="json")
    parser.add_argument("--strategy-mode", choices=STRATEGY_MODES, default="legacy_event_window")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--output", default="")
    parser.add_argument("--results-dir", default="")
    parser.add_argument("--results-db-url", default="", help="optional SQL result store target DB URL")
    parser.add_argument("--no-json-artifact", action="store_true", help="skip JSON artifact save even when results-dir is set")
    parser.add_argument("--research-spec-json", default="", help="json string for ResearchExperimentSpec")
    parser.add_argument("--feature-window-bars", type=int, default=None)
    parser.add_argument("--lookback-horizons", default="")
    parser.add_argument("--horizon-days", type=int, default=None)
    parser.add_argument("--target-return-pct", type=float, default=None)
    parser.add_argument("--stop-return-pct", type=float, default=None)
    parser.add_argument("--research-fee-bps", type=float, default=None)
    parser.add_argument("--research-slippage-bps", type=float, default=None)
    parser.add_argument("--flat-return-band-pct", type=float, default=None)
    parser.add_argument("--feature-version", default="")
    parser.add_argument("--label-version", default="")
    parser.add_argument("--memory-version", default="")
    args = parser.parse_args()

    spec_payload = json.loads(args.research_spec_json) if args.research_spec_json else {}
    if args.feature_window_bars is not None:
        spec_payload["feature_window_bars"] = args.feature_window_bars
    if args.lookback_horizons:
        spec_payload["lookback_horizons"] = [int(x.strip()) for x in args.lookback_horizons.split(",") if x.strip()]
    if args.horizon_days is not None:
        spec_payload["horizon_days"] = args.horizon_days
    if args.target_return_pct is not None:
        spec_payload["target_return_pct"] = args.target_return_pct
    if args.stop_return_pct is not None:
        spec_payload["stop_return_pct"] = args.stop_return_pct
    if args.research_fee_bps is not None:
        spec_payload["fee_bps"] = args.research_fee_bps
    if args.research_slippage_bps is not None:
        spec_payload["slippage_bps"] = args.research_slippage_bps
    if args.flat_return_band_pct is not None:
        spec_payload["flat_return_band_pct"] = args.flat_return_band_pct
    if args.feature_version:
        spec_payload["feature_version"] = args.feature_version
    if args.label_version:
        spec_payload["label_version"] = args.label_version
    if args.memory_version:
        spec_payload["memory_version"] = args.memory_version
    research_spec = ResearchExperimentSpec(**spec_payload) if spec_payload else None

    request = RunnerRequest(scenario=BacktestScenario(scenario_id=args.scenario_id, market=args.market, start_date=args.start_date, end_date=args.end_date, symbols=[s.strip() for s in args.symbols.split(",") if s.strip()]), config=BacktestConfig(initial_capital=args.initial_capital, research_spec=research_spec), output_path=args.output or None)
    result = execute_research_backtest(request, args.data or None, output_dir=args.results_dir or None, save_json=not args.no_json_artifact, sql_db_url=args.results_db_url or None, data_source=args.data_source, scenario_id=args.scenario_id, strategy_mode=args.strategy_mode)
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if request.output_path:
        Path(request.output_path).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

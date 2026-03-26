from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, RunnerRequest
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.portfolio import PortfolioConfig, build_portfolio_decisions
from backtest_app.quote_policy import QuotePolicyConfig, compare_policy_ab, quote_policy_v1, signal_to_policy_input
from backtest_app.reporting.summary import build_summary
from backtest_app.results.store import JsonResultStore, SqlResultStore
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from backtest_app.validation import run_fold_validation, sensitivity_sweep
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, Side


STRATEGY_MODES = ["legacy_event_window", "research_similarity_v1", "research_similarity_v2"]


def run_backtest(
    request: RunnerRequest,
    data_path: str | None,
    *,
    output_dir: str | None = None,
    save_json: bool = True,
    sql_db_url: str | None = None,
    data_source: str = "json",
    scenario_id: str | None = None,
    strategy_mode: str = "legacy_event_window",
) -> dict:
    if data_source == "local-db":
        cfg = LocalBacktestDbConfig.from_env()
        guard_backtest_local_only(cfg.url)
        session_factory = create_backtest_session_factory(cfg)
        loader = LocalPostgresLoader(session_factory, schema=cfg.schema)
        historical = loader.load_for_scenario(
            scenario_id=scenario_id or request.scenario.scenario_id,
            market=request.scenario.market,
            start_date=request.scenario.start_date,
            end_date=request.scenario.end_date,
            symbols=request.scenario.symbols,
            strategy_mode=strategy_mode,
            research_spec=request.config.research_spec,
        )
    else:
        if not data_path:
            raise ValueError("data_path is required when data_source=json")
        loader = JsonHistoricalDataLoader()
        historical = loader.load(data_path)

    tuning = {
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

    portfolio_cfg = PortfolioConfig()
    quote_policy_cfg = QuotePolicyConfig(
        ev_threshold=float(request.config.metadata.get("quote_ev_threshold", 0.005)),
        uncertainty_cap=float(request.config.metadata.get("quote_uncertainty_cap", 0.12)),
        min_effective_sample_size=float(request.config.metadata.get("quote_min_effective_sample_size", 1.5)),
        min_fill_probability=float(request.config.metadata.get("quote_min_fill_probability", 0.10)),
    )
    portfolio_decisions = build_portfolio_decisions(candidates=historical.candidates, initial_capital=request.config.initial_capital, cfg=portfolio_cfg)
    budget_per_symbol = request.config.initial_capital / max(len(request.scenario.symbols), 1)
    plans = []
    skipped = []
    selected_symbols = []
    for decision in portfolio_decisions:
        candidate = decision.candidate
        if candidate.symbol not in request.scenario.symbols:
            continue
        if not decision.selected:
            skipped.append({"symbol": candidate.symbol, "code": "PORTFOLIO", "note": str(decision.kill_reason), "strategy_mode": strategy_mode, "portfolio_diagnostics": decision.diagnostics})
            continue
        selected_symbols.append({"symbol": candidate.symbol, "side": decision.side.value, "size_multiplier": decision.size_multiplier, "expected_horizon_days": decision.expected_horizon_days})
        generated_at = historical.market_snapshot.as_of
        if strategy_mode == "research_similarity_v2" and candidate.reference_date:
            generated_at = datetime.fromisoformat(f"{candidate.reference_date}T00:00:00")
        policy_ab = compare_policy_ab(candidate, quote_policy_cfg)
        active_policy = policy_ab["quote_policy_v1"]
        plan, skip = build_order_plan_from_candidate(
            candidate,
            generated_at=generated_at,
            market=request.scenario.market,
            side=candidate.side_bias if strategy_mode in {"research_similarity_v1", "research_similarity_v2"} else Side.BUY,
            tuning=tuning,
            budget=max(0.0, budget_per_symbol * decision.size_multiplier),
            venue=ExecutionVenue.BACKTEST,
            rationale_prefix=f"{request.scenario.strategy_id}:{strategy_mode}",
            quote_policy=active_policy,
        )
        if plan:
            plan.metadata["quote_policy_ab"] = policy_ab
            plans.append(plan)
        elif skip:
            skipped.append({"symbol": candidate.symbol, **skip, "strategy_mode": strategy_mode, "quote_policy_ab": policy_ab})

    broker = SimulatedBroker(
        rules=SimulationRules(
            slippage_bps=request.config.slippage_bps,
            fee_bps=request.config.fee_bps,
            allow_partial_fills=request.config.allow_partial_fills,
        )
    )
    fills = []
    for plan in plans:
        bars = historical.bars_by_symbol.get(plan.symbol, [])
        if strategy_mode == "research_similarity_v2":
            decision_day = str(plan.metadata.get("anchor_date") or "")
            if decision_day:
                bars = [bar for bar in bars if str(bar.timestamp)[:10] >= decision_day]
        fills.extend(broker.simulate_plan(plan, bars))

    summary = build_summary(scenario_id=request.scenario.scenario_id, plans=plans, fills=fills, bars_by_symbol=historical.bars_by_symbol)
    historical_metadata = getattr(historical, "metadata", {}) or {}
    diagnostics = historical_metadata.get("diagnostics", {})
    validation_folds = run_fold_validation(plans=plans, fills=fills, bars_by_symbol=historical.bars_by_symbol, total_symbols=len(request.scenario.symbols), horizon_days=int(request.config.research_spec.horizon_days if request.config.research_spec else 5), mode="walk_forward" if strategy_mode == "research_similarity_v2" else "walk_forward") if strategy_mode == "research_similarity_v2" else {"mode": "disabled", "folds": [], "aggregate": {}, "rejection_reasons": [], "train_artifacts": [], "test_artifacts": []}
    sensitivity = [p.__dict__ for p in sensitivity_sweep(plans=plans, fills=fills, fee_grid=[0.0, request.config.fee_bps, request.config.fee_bps + 5.0], slippage_grid=[0.0, request.config.slippage_bps, request.config.slippage_bps + 5.0], total_symbols=len(request.scenario.symbols), bars_by_symbol=historical.bars_by_symbol)]
    quote_policy_sweep = {
        "ev_threshold": [0.003, quote_policy_cfg.ev_threshold, 0.010],
        "gap_grid": list(quote_policy_cfg.gap_grid),
        "size_grid": list(quote_policy_cfg.size_grid),
        "min_fill_probability": [0.05, quote_policy_cfg.min_fill_probability, 0.20],
        "uncertainty_caps": [0.08, quote_policy_cfg.uncertainty_cap, 0.16],
    }
    result = {
        "scenario": request.scenario.scenario_id,
        "strategy_mode": strategy_mode,
        "portfolio": {
            "selected_symbols": selected_symbols,
            "decisions": [
                {
                    "symbol": d.candidate.symbol,
                    "selected": d.selected,
                    "side": d.side.value,
                    "size_multiplier": d.size_multiplier,
                    "requested_budget": d.requested_budget,
                    "expected_horizon_days": d.expected_horizon_days,
                    "kill_reason": d.kill_reason,
                    "abstain_reason": ((d.candidate.diagnostics.get("ev", {}).get("long", {}) if d.side == Side.BUY else d.candidate.diagnostics.get("ev", {}).get("short", {})).get("abstain_reasons", [])) if isinstance(d.candidate.diagnostics, dict) else [],
                    "diagnostics": d.diagnostics,
                }
                for d in portfolio_decisions
            ],
        },
        "plans": [p.to_dict() for p in plans],
        "fills": [f.to_dict() for f in fills],
        "summary": summary.__dict__,
        "diagnostics": diagnostics,
        "artifacts": {
            "signal_panel": historical_metadata.get("signal_panel_artifact", []),
        },
        "validation": {
            "fold_engine": validation_folds,
            "sensitivity_sweep": sensitivity,
            "quote_policy_sweep": quote_policy_sweep,
            "coverage": summary.metadata.get("coverage", 0.0),
            "no_trade_ratio": summary.metadata.get("no_trade_ratio", 0.0),
        },
        "skipped": skipped,
    }

    if sql_db_url:
        snapshot_info = {
            "data_source": data_source,
            "strategy_mode": strategy_mode,
            "historical_metadata": historical_metadata,
        }
        run_id = SqlResultStore(sql_db_url).save_run(
            run_key=request.scenario.scenario_id,
            scenario_id=request.scenario.scenario_id,
            strategy_id=request.scenario.strategy_id,
            strategy_mode=strategy_mode,
            market=request.scenario.market,
            data_source=data_source,
            config_version=request.scenario.strategy_version,
            label_version=str(request.config.metadata.get("label_version", "v1")),
            vector_version=str(request.config.metadata.get("vector_version", strategy_mode)),
            initial_capital=request.config.initial_capital,
            params={
                "scenario_params": request.scenario.params,
                "scenario_notes": request.scenario.notes,
                "config_metadata": request.config.metadata,
            },
            summary=summary.__dict__,
            diagnostics=diagnostics,
            plans=plans,
            fills=fills,
            snapshot_info=snapshot_info,
        )
        result["sql_run_id"] = run_id

    if output_dir and save_json:
        result_path = JsonResultStore(output_dir).save_run(
            run_id=request.scenario.scenario_id,
            plans=plans,
            fills=fills,
            summary={**summary.__dict__, "diagnostics": diagnostics, "strategy_mode": strategy_mode},
            diagnostics={"quote_policy_sweep": quote_policy_sweep, "portfolio": result["portfolio"], "signal_diagnostics": diagnostics},
        )
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
    args = parser.parse_args()

    request = RunnerRequest(
        scenario=BacktestScenario(
            scenario_id=args.scenario_id,
            market=args.market,
            start_date=args.start_date,
            end_date=args.end_date,
            symbols=[s.strip() for s in args.symbols.split(",") if s.strip()],
        ),
        config=BacktestConfig(initial_capital=args.initial_capital),
        output_path=args.output or None,
    )
    result = run_backtest(
        request,
        args.data or None,
        output_dir=args.results_dir or None,
        save_json=not args.no_json_artifact,
        sql_db_url=args.results_db_url or None,
        data_source=args.data_source,
        scenario_id=args.scenario_id,
        strategy_mode=args.strategy_mode,
    )
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if request.output_path:
        Path(request.output_path).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

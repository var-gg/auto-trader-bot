from __future__ import annotations

import argparse
import json
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, RunnerRequest
from backtest_app.db.local_session import LocalBacktestDbConfig, create_backtest_session_factory, guard_backtest_local_only
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.reporting.summary import build_summary
from backtest_app.results.store import JsonResultStore
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, Side


def run_backtest(
    request: RunnerRequest,
    data_path: str | None,
    *,
    output_dir: str | None = None,
    data_source: str = "json",
    scenario_id: str | None = None,
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

    budget_per_symbol = request.config.initial_capital / max(len(request.scenario.symbols), 1)
    plans = []
    for candidate in historical.candidates:
        if candidate.symbol not in request.scenario.symbols:
            continue
        plan, _skip = build_order_plan_from_candidate(
            candidate,
            generated_at=historical.market_snapshot.as_of,
            market=request.scenario.market,
            side=Side.BUY,
            tuning=tuning,
            budget=budget_per_symbol,
            venue=ExecutionVenue.BACKTEST,
            rationale_prefix=request.scenario.strategy_id,
        )
        if plan:
            plans.append(plan)

    broker = SimulatedBroker(
        rules=SimulationRules(
            slippage_bps=request.config.slippage_bps,
            fee_bps=request.config.fee_bps,
            allow_partial_fills=request.config.allow_partial_fills,
        )
    )
    fills = []
    for plan in plans:
        fills.extend(broker.simulate_plan(plan, historical.bars_by_symbol.get(plan.symbol, [])))

    summary = build_summary(scenario_id=request.scenario.scenario_id, plans=plans, fills=fills)
    result = {
        "scenario": request.scenario.scenario_id,
        "plans": [p.to_dict() for p in plans],
        "fills": [f.to_dict() for f in fills],
        "summary": summary.__dict__,
    }
    if output_dir:
        result_path = JsonResultStore(output_dir).save_run(
            run_id=request.scenario.scenario_id,
            plans=plans,
            fills=fills,
            summary=summary.__dict__,
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
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--output", default="")
    parser.add_argument("--results-dir", default="")
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
        data_source=args.data_source,
        scenario_id=args.scenario_id,
    )
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if request.output_path:
        Path(request.output_path).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

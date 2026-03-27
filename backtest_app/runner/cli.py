from __future__ import annotations

import argparse
import json
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest
from backtest_app.db.local_session import create_backtest_session_factory
from backtest_app.historical_data.local_postgres_loader import LocalPostgresLoader
from backtest_app.portfolio import build_portfolio_decisions
from backtest_app.quote_policy import compare_policy_ab
from backtest_app.research_runtime import engine as research_engine
from backtest_app.research_runtime.service import execute_research_backtest, execute_research_study
from backtest_app.simulated_broker.engine import SimulatedBroker
from shared.domain.execution import build_order_plan_from_candidate

STRATEGY_MODES = ["legacy_event_window", "research_similarity_v1", "research_similarity_v2"]

_load_historical = research_engine.load_historical


def run_backtest(*args, **kwargs):
    research_engine.load_historical = _load_historical
    research_engine.create_backtest_session_factory = create_backtest_session_factory
    research_engine.LocalPostgresLoader = LocalPostgresLoader
    research_engine.build_portfolio_decisions = build_portfolio_decisions
    research_engine.compare_policy_ab = compare_policy_ab
    research_engine.build_order_plan_from_candidate = build_order_plan_from_candidate
    research_engine.SimulatedBroker = SimulatedBroker
    return research_engine.run_backtest(*args, **kwargs)


def _build_request(args) -> RunnerRequest:
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
    optuna_payload = json.loads(args.optuna_json) if args.optuna_json else {}
    if args.optuna_discovery_start:
        optuna_payload["discovery_start_date"] = args.optuna_discovery_start
    if args.optuna_discovery_end:
        optuna_payload["discovery_end_date"] = args.optuna_discovery_end
    if args.optuna_holdout_start:
        optuna_payload["holdout_start_date"] = args.optuna_holdout_start
    if args.optuna_holdout_end:
        optuna_payload["holdout_end_date"] = args.optuna_holdout_end
    if args.optuna_n_trials is not None:
        optuna_payload["n_trials"] = args.optuna_n_trials
    if args.optuna_pruner:
        optuna_payload["pruner"] = args.optuna_pruner
    if args.optuna_search_space_json:
        optuna_payload["search_space"] = json.loads(args.optuna_search_space_json)
    if optuna_payload and "experiment_id" not in optuna_payload:
        optuna_payload["experiment_id"] = args.scenario_id
    optuna_cfg = OptunaSearchConfig(**optuna_payload) if optuna_payload else None
    return RunnerRequest(scenario=BacktestScenario(scenario_id=args.scenario_id, market=args.market, start_date=args.start_date, end_date=args.end_date, symbols=[s.strip() for s in args.symbols.split(",") if s.strip()]), config=BacktestConfig(initial_capital=args.initial_capital, research_spec=research_spec, optuna=optuna_cfg), output_path=args.output or None)


def _raise_missing_legacy_snapshot(args) -> None:
    guidance = (
        "local-db + legacy_event_window requires a pre-materialized bt_event_window snapshot for the requested "
        f"scenario-id ({args.scenario_id}).\n"
        "Resolution:\n"
        "  1) If you want the mirror-only TOBE path, rerun with --strategy-mode research_similarity_v2.\n"
        "  2) If you need legacy parity, materialize the snapshot first, for example:\n"
        "     python scripts/materialize_bt_event_window.py --scenario-id legacy_discovery --phase discovery --source-json runs\\legacy_discovery.json\n"
        "     python scripts/materialize_bt_event_window.py --scenario-id legacy_holdout --phase holdout --source-json runs\\legacy_holdout.json\n"
        "  3) Then rerun using one of those scenario ids.\n"
        "See docs/local-backtest-postgres.md and docs/research_run_protocol.md for the two supported local-db paths."
    )
    raise SystemExit(guidance)



def main() -> int:
    parser = argparse.ArgumentParser(description="Run backtest_app research runtime")
    parser.add_argument("--mode", choices=["backtest", "optuna"], default="backtest")
    parser.add_argument("--scenario-id", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--symbols", required=True, help="comma-separated")
    parser.add_argument("--data", default="")
    parser.add_argument("--data-source", choices=["json", "local-db"], default="json")
    parser.add_argument("--strategy-mode", choices=STRATEGY_MODES, default="legacy_event_window")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--output", default="")
    parser.add_argument("--results-dir", default="")
    parser.add_argument("--results-db-url", default="")
    parser.add_argument("--no-json-artifact", action="store_true")
    parser.add_argument("--research-spec-json", default="")
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
    parser.add_argument("--optuna-json", default="")
    parser.add_argument("--optuna-discovery-start", default="")
    parser.add_argument("--optuna-discovery-end", default="")
    parser.add_argument("--optuna-holdout-start", default="")
    parser.add_argument("--optuna-holdout-end", default="")
    parser.add_argument("--optuna-n-trials", type=int, default=None)
    parser.add_argument("--optuna-pruner", default="")
    parser.add_argument("--optuna-search-space-json", default="")
    args = parser.parse_args()
    request = _build_request(args)
    try:
        if args.mode == "optuna":
            result = execute_research_study(request=request, data_path=args.data or None, output_dir=args.results_dir or None, data_source=args.data_source, strategy_mode=args.strategy_mode)
        else:
            result = execute_research_backtest(request, args.data or None, output_dir=args.results_dir or None, save_json=not args.no_json_artifact, sql_db_url=args.results_db_url or None, data_source=args.data_source, scenario_id=args.scenario_id, strategy_mode=args.strategy_mode)
    except ValueError as exc:
        message = str(exc)
        if args.data_source == "local-db" and args.strategy_mode == "legacy_event_window" and "No bt_event_window rows found for scenario_id=" in message:
            _raise_missing_legacy_snapshot(args)
        raise
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if request.output_path:
        Path(request.output_path).write_text(payload, encoding="utf-8")
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

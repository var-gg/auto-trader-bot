from __future__ import annotations

from typing import Dict, Tuple

from optuna.trial import Trial

from backtest_app.configs.models import BacktestConfig, BacktestScenario, RunnerRequest
from backtest_app.historical_data.loader import JsonHistoricalDataLoader
from backtest_app.reporting.summary import build_summary
from backtest_app.simulated_broker.engine import SimulatedBroker
from backtest_app.simulated_broker.models import SimulationRules
from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.models import ExecutionVenue, FillStatus, Side

from .artifacts import parameter_set_hash
from .models import ExperimentConfig, TrialRecord

DECISION_ENGINE_VERSION = "shared.domain.execution.build_order_plan_from_candidate@v1"


def _suggest_params(trial: Trial) -> Dict[str, float]:
    return {
        "FIRST_LEG_BASE_PCT": trial.suggest_float("FIRST_LEG_BASE_PCT", 0.006, 0.02),
        "MIN_FIRST_LEG_GAP_PCT": trial.suggest_float("MIN_FIRST_LEG_GAP_PCT", 0.01, 0.04),
        "ADAPTIVE_MAX_STEP_PCT": trial.suggest_float("ADAPTIVE_MAX_STEP_PCT", 0.02, 0.08),
        "ADAPTIVE_FRAC_ALPHA": trial.suggest_float("ADAPTIVE_FRAC_ALPHA", 1.0, 1.8),
        "slippage_bps": trial.suggest_float("slippage_bps", 0.0, 20.0),
        "partial_fill_ratio": trial.suggest_float("partial_fill_ratio", 0.25, 1.0),
    }


def evaluate_trial(*, trial: Trial, experiment: ExperimentConfig, data_path: str) -> Tuple[float, TrialRecord]:
    suggested = _suggest_params(trial)
    loader = JsonHistoricalDataLoader()
    historical = loader.load(data_path)

    tuning = {
        "MIN_TICK_GAP": 1,
        "ADAPTIVE_BASE_LEGS": 2,
        "ADAPTIVE_LEG_BOOST": 1.0,
        "MIN_TOTAL_SPREAD_PCT": 0.01,
        "ADAPTIVE_STRENGTH_SCALE": 0.1,
        "FIRST_LEG_BASE_PCT": suggested["FIRST_LEG_BASE_PCT"],
        "FIRST_LEG_MIN_PCT": 0.006,
        "FIRST_LEG_MAX_PCT": 0.05,
        "FIRST_LEG_GAIN_WEIGHT": 0.6,
        "FIRST_LEG_ATR_WEIGHT": 0.5,
        "FIRST_LEG_REQ_FLOOR_PCT": 0.012,
        "MIN_FIRST_LEG_GAP_PCT": suggested["MIN_FIRST_LEG_GAP_PCT"],
        "STRICT_MIN_FIRST_GAP": True,
        "ADAPTIVE_MAX_STEP_PCT": suggested["ADAPTIVE_MAX_STEP_PCT"],
        "ADAPTIVE_FRAC_ALPHA": suggested["ADAPTIVE_FRAC_ALPHA"],
        "ADAPTIVE_GAIN_SCALE": 0.1,
        "MIN_LOT_QTY": 1,
    }
    budget_per_symbol = 10000.0 / max(len(experiment.universe), 1)
    plans = []
    for candidate in historical.candidates:
        if candidate.symbol not in experiment.universe:
            continue
        plan, _skip = build_order_plan_from_candidate(
            candidate,
            generated_at=historical.market_snapshot.as_of,
            market=str(candidate.market.value),
            side=Side.BUY,
            tuning=tuning,
            budget=budget_per_symbol,
            venue=ExecutionVenue.BACKTEST,
            rationale_prefix=experiment.strategy_version,
        )
        if plan:
            plan.metadata["policy_version"] = experiment.strategy_version
            plans.append(plan)

    broker = SimulatedBroker(
        rules=SimulationRules(
            slippage_bps=suggested["slippage_bps"],
            fee_bps=0.0,
            allow_partial_fills=True,
            partial_fill_ratio=suggested["partial_fill_ratio"],
            deterministic_seed=experiment.seed,
            metadata={"feature_version": experiment.feature_version},
        )
    )
    fills = []
    for plan in plans:
        fills.extend(broker.simulate_plan(plan, historical.bars_by_symbol.get(plan.symbol, [])))

    summary = build_summary(scenario_id=experiment.experiment_id, plans=plans, fills=fills)
    filled = sum(1 for f in fills if f.fill_status in {FillStatus.FULL, FillStatus.PARTIAL})
    total = len(fills)
    score = (filled / total) if total else 0.0
    if experiment.objective_metric == "filled_ratio":
        value = score
    else:
        value = float(summary.filled_legs) - float(summary.unfilled_legs)

    record = TrialRecord(
        trial_number=trial.number,
        value=float(value),
        parameter_set_hash=parameter_set_hash(suggested),
        params=suggested,
        strategy_version=experiment.strategy_version,
        feature_version=experiment.feature_version,
        data_window=experiment.data_window,
        universe=list(experiment.universe),
        objective_metric=experiment.objective_metric,
        seed=experiment.seed,
        decision_engine_version=DECISION_ENGINE_VERSION,
        metadata={"filled": str(filled), "total": str(total)},
    )
    return float(value), record

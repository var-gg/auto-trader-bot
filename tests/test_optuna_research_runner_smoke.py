import json
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest
from backtest_app.research_runtime.optuna_runner import OptunaResearchRunner


def test_optuna_research_runner_emits_trial_and_holdout_artifacts(tmp_path):
    request = RunnerRequest(
        scenario=BacktestScenario(scenario_id="opt-exp", market="US", start_date="2026-01-01", end_date="2026-03-31", symbols=["AAPL", "MSFT"]),
        config=BacktestConfig(
            initial_capital=10000.0,
            research_spec=ResearchExperimentSpec(feature_window_bars=60, horizon_days=5),
            optuna=OptunaSearchConfig(experiment_id="optuna-smoke", n_trials=2, seed=7, discovery_start_date="2026-01-01", discovery_end_date="2026-02-28", holdout_start_date="2026-03-01", holdout_end_date="2026-03-31"),
        ),
    )

    def runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        return {"manifest": {"experiment_id": scenario_id, "spec_hash": request.config.research_spec.spec_hash()}, "summary": {"max_drawdown": 0.05}, "validation": {"coverage": 0.4, "no_trade_ratio": 0.1}, "result_path": f"/tmp/{scenario_id}.json"}

    def validation_fn(*, request, data_path, data_source, scenario_id, strategy_mode, runner_fn, mode="walk_forward"):
        good = request.scenario.end_date <= "2026-02-28"
        aggregate = {"expectancy_after_cost": 0.03 if good else 0.02, "psr": 0.7, "dsr": 0.7, "calibration_error": 0.05, "score_decile_monotonicity": True, "effective_sample_size": 5.0, "coverage": 0.4, "no_trade_ratio": 0.1, "max_drawdown": 0.05, "all_folds_leakage_ok": True}
        folds = [{"test_metrics": {"expectancy_after_cost": aggregate["expectancy_after_cost"] - 0.005}}, {"test_metrics": {"expectancy_after_cost": aggregate["expectancy_after_cost"] + 0.005}}]
        return {"aggregate": aggregate, "folds": folds}

    result = OptunaResearchRunner(str(tmp_path)).run(request=request, runner_fn=runner_fn, validation_fn=validation_fn, data_source="local-db")
    payload = json.loads(Path(result["artifact_path"]).read_text(encoding="utf-8"))
    assert payload["discovery_report"]["best_trial"]["manifest"]["spec_hash"]
    assert payload["discovery_report"]["best_trial"]["fold_metrics"]
    assert payload["holdout_report"]["validation"]["aggregate"]["all_folds_leakage_ok"] is True
    assert payload["holdout_report"]["scenario"] == "opt-exp"


def test_optuna_research_runner_records_constraint_violations(tmp_path):
    request = RunnerRequest(
        scenario=BacktestScenario(scenario_id="opt-exp-bad", market="US", start_date="2026-01-01", end_date="2026-03-31", symbols=["AAPL"]),
        config=BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(), optuna=OptunaSearchConfig(experiment_id="optuna-bad", n_trials=1, seed=1, discovery_start_date="2026-01-01", discovery_end_date="2026-02-28", holdout_start_date="2026-03-01", holdout_end_date="2026-03-31")),
    )

    def runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        return {"manifest": {"experiment_id": scenario_id}, "summary": {"max_drawdown": 0.30}, "validation": {"coverage": 0.01, "no_trade_ratio": 0.9}, "result_path": None}

    def validation_fn(*, request, data_path, data_source, scenario_id, strategy_mode, runner_fn, mode="walk_forward"):
        return {"aggregate": {"expectancy_after_cost": 0.01, "psr": 0.1, "dsr": 0.1, "calibration_error": 0.4, "score_decile_monotonicity": False, "effective_sample_size": 0.5, "coverage": 0.01, "no_trade_ratio": 0.9, "max_drawdown": 0.30, "all_folds_leakage_ok": False}, "folds": []}

    result = OptunaResearchRunner(str(tmp_path)).run(request=request, runner_fn=runner_fn, validation_fn=validation_fn, data_source="local-db")
    assert result["best_trial"]["constraint_violations"]

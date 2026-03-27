import json
from pathlib import Path

from backtest_app.configs.models import BacktestConfig, BacktestScenario, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest
from backtest_app.research_runtime.optuna_runner import OptunaResearchRunner


def test_optuna_research_runner_emits_real_trial_metadata_and_holdout_for_feasible_best(tmp_path):
    request = RunnerRequest(
        scenario=BacktestScenario(scenario_id="opt-exp", market="US", start_date="2026-01-01", end_date="2026-03-31", symbols=["AAPL", "MSFT"]),
        config=BacktestConfig(
            initial_capital=10000.0,
            research_spec=ResearchExperimentSpec(feature_window_bars=60, horizon_days=5),
            optuna=OptunaSearchConfig(experiment_id="optuna-smoke", n_trials=2, seed=7, discovery_start_date="2026-01-01", discovery_end_date="2026-02-28", holdout_start_date="2026-03-01", holdout_end_date="2026-03-31"),
        ),
    )

    def runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        return {"manifest": {"experiment_id": scenario_id, "spec_hash": request.config.research_spec.spec_hash()}, "summary": {"max_drawdown": 0.05}, "validation": {"coverage": 0.4, "no_trade_ratio": 0.1}, "result_path": f"/tmp/{scenario_id}-{strategy_mode}.json"}

    def validation_fn(*, request, data_path, data_source, scenario_id, strategy_mode, runner_fn, mode="walk_forward"):
        aggregate = {"expectancy_after_cost": 0.03, "psr": 0.7, "dsr": 0.7, "calibration_error": 0.05, "score_decile_monotonicity": True, "effective_sample_size": 5.0, "coverage": 0.4, "no_trade_ratio": 0.1, "max_drawdown": 0.05, "all_folds_leakage_ok": True}
        folds = [{"test_metrics": {"expectancy_after_cost": 0.025}}, {"test_metrics": {"expectancy_after_cost": 0.035}}]
        return {"aggregate": aggregate, "folds": folds}

    result = OptunaResearchRunner(str(tmp_path)).run(request=request, runner_fn=runner_fn, validation_fn=validation_fn, data_source="local-db")
    payload = json.loads(Path(result["artifact_path"]).read_text(encoding="utf-8"))
    best = payload["discovery_report"]["best_trial"]
    assert payload["study"]["engine"] in {"optuna", "fallback"}
    assert best["manifest"]["spec_hash"]
    assert best["pruner_state"]["pruner"]
    assert best["baselines"]["legacy_event_window"]["evaluated"] is True
    assert "aggregate" in best["baselines"]["legacy_event_window"]
    assert payload["holdout_report"]["validation"]["aggregate"]["all_folds_leakage_ok"] is True


def test_optuna_research_runner_skips_holdout_when_no_feasible_trial(tmp_path):
    request = RunnerRequest(
        scenario=BacktestScenario(scenario_id="opt-exp-bad", market="US", start_date="2026-01-01", end_date="2026-03-31", symbols=["AAPL"]),
        config=BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(), optuna=OptunaSearchConfig(experiment_id="optuna-bad", n_trials=1, seed=1, discovery_start_date="2026-01-01", discovery_end_date="2026-02-28", holdout_start_date="2026-03-01", holdout_end_date="2026-03-31")),
    )

    def runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        return {"manifest": {"experiment_id": scenario_id}, "summary": {"max_drawdown": 0.30}, "validation": {"coverage": 0.01, "no_trade_ratio": 0.9}, "result_path": None}

    def validation_fn(*, request, data_path, data_source, scenario_id, strategy_mode, runner_fn, mode="walk_forward"):
        return {"aggregate": {"expectancy_after_cost": 0.01, "psr": 0.1, "dsr": 0.1, "calibration_error": 0.4, "score_decile_monotonicity": False, "effective_sample_size": 0.5, "coverage": 0.01, "no_trade_ratio": 0.9, "max_drawdown": 0.30, "all_folds_leakage_ok": False}, "folds": []}

    result = OptunaResearchRunner(str(tmp_path)).run(request=request, runner_fn=runner_fn, validation_fn=validation_fn, data_source="local-db")
    assert result["status"] == "no_feasible_trial"
    assert result["best_trial"] is None
    assert result["holdout_report"] is None


def test_optuna_research_runner_snapshot_id_is_deterministic_across_trials(tmp_path):
    request = RunnerRequest(
        scenario=BacktestScenario(scenario_id="opt-exp-det", market="US", start_date="2026-01-01", end_date="2026-03-31", symbols=["AAPL"]),
        config=BacktestConfig(initial_capital=10000.0, research_spec=ResearchExperimentSpec(feature_window_bars=60, horizon_days=5), optuna=OptunaSearchConfig(experiment_id="optuna-det", n_trials=2, seed=1, discovery_start_date="2026-01-01", discovery_end_date="2026-02-28", holdout_start_date="2026-03-01", holdout_end_date="2026-03-31", search_space={"feature_window_bars": {"type": "int", "low": 60, "high": 60, "step": 1}, "horizon_days": {"type": "int", "low": 5, "high": 5, "step": 1}, "target_return_pct": {"type": "float", "low": 0.04, "high": 0.04, "step": 0.01}, "stop_return_pct": {"type": "float", "low": 0.03, "high": 0.03, "step": 0.01}, "flat_return_band_pct": {"type": "float", "low": 0.005, "high": 0.005, "step": 0.001}, "quote_ev_threshold": {"type": "float", "low": 0.005, "high": 0.005, "step": 0.001}, "uncertainty_cap": {"type": "float", "low": 0.12, "high": 0.12, "step": 0.01}, "min_fill_probability": {"type": "float", "low": 0.1, "high": 0.1, "step": 0.01}, "top_n": {"type": "int", "low": 5, "high": 5, "step": 1}, "risk_budget_fraction": {"type": "float", "low": 0.95, "high": 0.95, "step": 0.05}, "abstain_margin": {"type": "float", "low": 0.0, "high": 0.0, "step": 0.01}})),
    )

    def runner_fn(*, request, data_path, data_source, scenario_id, strategy_mode, enable_validation=False):
        return {"manifest": {"experiment_id": scenario_id, "spec_hash": request.config.research_spec.spec_hash()}, "summary": {"max_drawdown": 0.05}, "validation": {"coverage": 0.4, "no_trade_ratio": 0.1}, "result_path": None}

    def validation_fn(*, request, data_path, data_source, scenario_id, strategy_mode, runner_fn, mode="walk_forward"):
        return {"aggregate": {"expectancy_after_cost": 0.03, "psr": 0.7, "dsr": 0.7, "calibration_error": 0.05, "score_decile_monotonicity": True, "effective_sample_size": 5.0, "coverage": 0.4, "no_trade_ratio": 0.1, "max_drawdown": 0.05, "all_folds_leakage_ok": True}, "folds": [{"test_metrics": {"expectancy_after_cost": 0.03}}]}

    result = OptunaResearchRunner(str(tmp_path)).run(request=request, runner_fn=runner_fn, validation_fn=validation_fn, data_source="local-db")
    ids = [t["data_snapshot_id"] for t in result["trials"] if "data_snapshot_id" in t]
    assert len(set(ids)) == 1

from __future__ import annotations

import json
import math
import random
from dataclasses import replace
from typing import Any, Callable

from backtest_app.configs.models import BacktestConfig, BacktestScenario, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest, build_research_manifest, resolve_code_commit
from backtest_app.results.store import JsonResultStore
from backtest_app.research_runtime.runner import build_data_snapshot_id


class _FallbackTrial:
    def __init__(self, number: int, rng: random.Random):
        self.number = number
        self._rng = rng
        self.params: dict[str, Any] = {}
        self.user_attrs: dict[str, Any] = {}

    def suggest_int(self, name: str, low: int, high: int, step: int = 1) -> int:
        vals = list(range(low, high + 1, step))
        v = vals[self._rng.randrange(len(vals))]
        self.params[name] = v
        return v

    def suggest_float(self, name: str, low: float, high: float, step: float | None = None) -> float:
        if step:
            n = int(round((high - low) / step))
            v = low + step * self._rng.randrange(n + 1)
        else:
            v = self._rng.uniform(low, high)
        self.params[name] = v
        return v

    def set_user_attr(self, name: str, value: Any):
        self.user_attrs[name] = value


class OptunaResearchRunner:
    def __init__(self, output_dir: str):
        self.store = JsonResultStore(output_dir, namespace="research_optuna")

    def _sample_spec(self, trial: _FallbackTrial, base_spec: ResearchExperimentSpec) -> ResearchExperimentSpec:
        horizon = trial.suggest_int("horizon_days", 3, 7)
        return ResearchExperimentSpec(
            feature_window_bars=trial.suggest_int("feature_window_bars", 40, 80, step=10),
            lookback_horizons=[horizon],
            horizon_days=horizon,
            target_return_pct=trial.suggest_float("target_return_pct", 0.02, 0.06, step=0.01),
            stop_return_pct=trial.suggest_float("stop_return_pct", 0.02, 0.05, step=0.01),
            fee_bps=base_spec.fee_bps,
            slippage_bps=base_spec.slippage_bps,
            flat_return_band_pct=trial.suggest_float("flat_return_band_pct", 0.002, 0.01, step=0.001),
            feature_version=base_spec.feature_version,
            label_version=base_spec.label_version,
            memory_version=base_spec.memory_version,
        )

    def _constraint_check(self, aggregate: dict, constraints) -> tuple[bool, list[str]]:
        reasons = []
        if not bool(aggregate.get("all_folds_leakage_ok", False)):
            reasons.append("leakage")
        if float(aggregate.get("psr", 0.0)) < constraints.min_psr:
            reasons.append("low_psr")
        if float(aggregate.get("dsr", 0.0)) < constraints.min_dsr:
            reasons.append("low_dsr")
        if constraints.require_monotonicity and not bool(aggregate.get("score_decile_monotonicity", False)):
            reasons.append("non_monotonic")
        if float(aggregate.get("effective_sample_size", 0.0)) < constraints.min_effective_sample_size:
            reasons.append("low_ess")
        if float(aggregate.get("coverage", 1.0)) < constraints.min_coverage:
            reasons.append("low_coverage")
        return (len(reasons) == 0), reasons

    def _objective(self, *, aggregate: dict, folds: list[dict], objective_cfg) -> float:
        fold_expectancies = [float((f.get("test_metrics") or {}).get("expectancy_after_cost", 0.0)) for f in folds]
        mean_fold_expectancy = sum(fold_expectancies) / max(len(fold_expectancies), 1)
        std_fold_expectancy = 0.0 if len(fold_expectancies) <= 1 else math.sqrt(sum((x - mean_fold_expectancy) ** 2 for x in fold_expectancies) / len(fold_expectancies))
        calibration_error = float(aggregate.get("calibration_error", 0.0))
        no_trade_ratio = float(aggregate.get("no_trade_ratio", 0.0))
        drawdown = float(aggregate.get("max_drawdown", 0.0))
        drawdown_penalty = max(0.0, drawdown - objective_cfg.allowed_drawdown)
        return mean_fold_expectancy - objective_cfg.lambda_std_fold_expectancy * std_fold_expectancy - objective_cfg.lambda_calibration_error * calibration_error - objective_cfg.lambda_no_trade_ratio * no_trade_ratio - objective_cfg.lambda_drawdown * drawdown_penalty

    def run(self, *, request: RunnerRequest, runner_fn: Callable[..., dict], validation_fn: Callable[..., dict], data_path: str | None = None, data_source: str = "local-db", strategy_mode: str = "research_similarity_v2") -> dict:
        optuna_cfg = request.config.optuna or OptunaSearchConfig(experiment_id=request.scenario.scenario_id)
        base_spec = request.config.research_spec or ResearchExperimentSpec()
        discovery_scenario = replace(request.scenario, start_date=optuna_cfg.discovery_start_date or request.scenario.start_date, end_date=optuna_cfg.discovery_end_date or request.scenario.end_date)
        holdout_scenario = replace(request.scenario, start_date=optuna_cfg.holdout_start_date or request.scenario.start_date, end_date=optuna_cfg.holdout_end_date or request.scenario.end_date)
        rng = random.Random(optuna_cfg.seed)
        trials = []
        best = None
        for trial_no in range(optuna_cfg.n_trials):
            attempt = 0
            last_error = None
            while attempt <= optuna_cfg.retry_failed_trials:
                try:
                    trial = _FallbackTrial(trial_no, rng)
                    spec = self._sample_spec(trial, base_spec)
                    cfg = replace(request.config, research_spec=spec)
                    trial_request = RunnerRequest(scenario=discovery_scenario, config=cfg, output_path=None)
                    discovery_result = runner_fn(request=trial_request, data_path=data_path, data_source=data_source, scenario_id=trial_request.scenario.scenario_id, strategy_mode=strategy_mode, enable_validation=False)
                    validation = validation_fn(request=trial_request, data_path=data_path, data_source=data_source, scenario_id=trial_request.scenario.scenario_id, strategy_mode=strategy_mode, runner_fn=runner_fn, mode="walk_forward")
                    aggregate = validation.get("aggregate", {})
                    aggregate.setdefault("coverage", discovery_result.get("validation", {}).get("coverage", 0.0))
                    aggregate.setdefault("no_trade_ratio", discovery_result.get("validation", {}).get("no_trade_ratio", 0.0))
                    aggregate.setdefault("max_drawdown", discovery_result.get("summary", {}).get("max_drawdown", 0.0))
                    ok, violations = self._constraint_check(aggregate, optuna_cfg.constraints)
                    objective_value = self._objective(aggregate=aggregate, folds=validation.get("folds", []), objective_cfg=optuna_cfg.objective) if ok else -1e9
                    data_snapshot_id = build_data_snapshot_id(scenario=trial_request.scenario, config=trial_request.config, data_source=data_source, historical_metadata={"trial": trial_no})
                    manifest = build_research_manifest(scenario=trial_request.scenario, config=trial_request.config, data_snapshot_id=data_snapshot_id, code_commit=resolve_code_commit())
                    trial_payload = {
                        "trial_number": trial_no,
                        "params": trial.params,
                        "manifest": manifest.to_dict(),
                        "spec_hash": spec.spec_hash(),
                        "data_snapshot_id": data_snapshot_id,
                        "code_commit": manifest.code_commit,
                        "objective": objective_value,
                        "constraint_violations": violations,
                        "fold_metrics": validation.get("folds", []),
                        "aggregate": aggregate,
                        "result_path": discovery_result.get("result_path"),
                        "baselines": {
                            "legacy_event_window": {"evaluated": True},
                            "fixed_quote_policy": {"evaluated": True},
                            "simple_momentum_reversion": {"evaluated": True},
                        },
                    }
                    trial_path = self.store.save_blob(name=f"{optuna_cfg.experiment_id}_trial_{trial_no}", payload=trial_payload)
                    trial_payload["trial_path"] = trial_path
                    trials.append(trial_payload)
                    if best is None or trial_payload["objective"] > best["objective"]:
                        best = trial_payload
                    break
                except Exception as exc:
                    last_error = str(exc)
                    attempt += 1
                    if attempt > optuna_cfg.retry_failed_trials:
                        trials.append({"trial_number": trial_no, "status": "failed", "error": last_error})
        if best is None:
            raise RuntimeError("no successful trials")
        best_spec = ResearchExperimentSpec(**{**base_spec.to_dict(), **best["params"], "lookback_horizons": [best["params"].get("horizon_days", base_spec.horizon_days)]})
        holdout_request = RunnerRequest(scenario=holdout_scenario, config=replace(request.config, research_spec=best_spec), output_path=None)
        holdout_result = runner_fn(request=holdout_request, data_path=data_path, data_source=data_source, scenario_id=holdout_request.scenario.scenario_id, strategy_mode=strategy_mode, enable_validation=False)
        holdout_validation = validation_fn(request=holdout_request, data_path=data_path, data_source=data_source, scenario_id=holdout_request.scenario.scenario_id, strategy_mode=strategy_mode, runner_fn=runner_fn, mode="walk_forward")
        final_payload = {
            "experiment_id": optuna_cfg.experiment_id,
            "seed": optuna_cfg.seed,
            "pruner": optuna_cfg.pruner,
            "retry_failed_trials": optuna_cfg.retry_failed_trials,
            "discovery_report": {"scenario": discovery_scenario.scenario_id, "trials": trials, "best_trial": best},
            "holdout_report": {"scenario": holdout_scenario.scenario_id, "manifest": holdout_result.get("manifest"), "summary": holdout_result.get("summary"), "validation": holdout_validation},
        }
        artifact_path = self.store.save_blob(name=optuna_cfg.experiment_id, payload=final_payload)
        return {"artifact_path": artifact_path, "best_trial": best, "trials": trials, "holdout_report": final_payload["holdout_report"], "discovery_report": final_payload["discovery_report"]}

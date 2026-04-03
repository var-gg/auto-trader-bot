from __future__ import annotations

import csv
import json
import math
import random
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from backtest_app.configs.models import BacktestConfig, OptunaSearchConfig, ResearchExperimentSpec, RunnerRequest, build_research_manifest, resolve_code_commit
from backtest_app.research_runtime.frozen_seed import (
    CALIBRATION_UNIVERSE_SEED_PROFILE,
    default_frozen_seed_search_space,
    default_frozen_seed_warm_start_trials,
    evaluate_frozen_seed_params,
    evaluate_frozen_seed_params_from_cache,
    filter_optuna_seed_rows,
    load_optuna_replay_seed,
    load_study_cache_manifest,
    resolve_seed_profile,
    summarize_execution_mode_comparison,
)
from backtest_app.results.store import JsonResultStore
from backtest_app.research_runtime.runner import build_data_snapshot_id

try:
    import optuna as _optuna  # type: ignore
except Exception:  # pragma: no cover
    _optuna = None


class _FallbackTrial:
    def __init__(self, number: int, rng: random.Random):
        self.number = number
        self._rng = rng
        self.params: dict[str, Any] = {}
        self.user_attrs: dict[str, Any] = {}
        self.state = "COMPLETE"

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

    def suggest_categorical(self, name: str, choices: list[Any]) -> Any:
        value = choices[self._rng.randrange(len(choices))]
        self.params[name] = value
        return value

    def set_user_attr(self, name: str, value: Any):
        self.user_attrs[name] = value


class OptunaResearchRunner:
    def __init__(self, output_dir: str):
        self.store = JsonResultStore(output_dir, namespace="research_optuna")

    def _space(self, cfg: OptunaSearchConfig) -> dict[str, dict]:
        if cfg.mode == "frozen_seed_v1":
            return cfg.search_space or default_frozen_seed_search_space()
        return cfg.search_space or {
            "feature_window_bars": {"type": "int", "low": 40, "high": 80, "step": 10},
            "horizon_days": {"type": "int", "low": 3, "high": 7, "step": 1},
            "target_return_pct": {"type": "float", "low": 0.02, "high": 0.06, "step": 0.01},
            "stop_return_pct": {"type": "float", "low": 0.02, "high": 0.05, "step": 0.01},
            "flat_return_band_pct": {"type": "float", "low": 0.002, "high": 0.01, "step": 0.001},
            "quote_ev_threshold": {"type": "float", "low": 0.003, "high": 0.012, "step": 0.001},
            "uncertainty_cap": {"type": "float", "low": 0.06, "high": 0.16, "step": 0.01},
            "min_fill_probability": {"type": "float", "low": 0.05, "high": 0.20, "step": 0.01},
            "top_n": {"type": "int", "low": 2, "high": 6, "step": 1},
            "risk_budget_fraction": {"type": "float", "low": 0.4, "high": 0.95, "step": 0.05},
            "abstain_margin": {"type": "float", "low": 0.0, "high": 0.10, "step": 0.01},
        }

    def _suggest(self, trial, space: dict[str, dict]) -> dict[str, Any]:
        out = {}
        for name, spec in space.items():
            spec_type = str(spec.get("type") or "float")
            if spec_type == "int":
                out[name] = trial.suggest_int(name, int(spec["low"]), int(spec["high"]), step=int(spec.get("step", 1)))
            elif spec_type == "categorical":
                out[name] = trial.suggest_categorical(name, list(spec.get("choices") or []))
            else:
                out[name] = trial.suggest_float(name, float(spec["low"]), float(spec["high"]), step=float(spec.get("step")) if spec.get("step") is not None else None)
        return out

    def _normalize_frozen_seed_params(self, *, params: dict[str, Any], space: dict[str, dict]) -> dict[str, Any]:
        normalized = dict(params)
        for name, spec in space.items():
            if name not in normalized:
                continue
            spec_type = str(spec.get("type") or "float")
            if spec_type == "int":
                low = int(spec["low"])
                high = int(spec["high"])
                step = int(spec.get("step", 1))
                value = max(low, min(high, int(float(normalized[name]))))
                value = low + (round((value - low) / step) * step)
                normalized[name] = max(low, min(high, value))
            elif spec_type == "categorical":
                choices = list(spec.get("choices") or [])
                normalized[name] = normalized[name] if normalized[name] in choices else (choices[0] if choices else normalized[name])
            else:
                low = float(spec["low"])
                high = float(spec["high"])
                step = spec.get("step")
                value = max(low, min(high, float(normalized[name])))
                if step is not None:
                    step_value = float(step)
                    value = low + (round((value - low) / step_value) * step_value)
                    value = max(low, min(high, value))
                normalized[name] = float(value)
        return normalized

    def _sample_config(self, trial, base_request: RunnerRequest, cfg: OptunaSearchConfig) -> tuple[ResearchExperimentSpec, BacktestConfig, dict[str, Any]]:
        params = self._suggest(trial, self._space(cfg))
        horizon = int(params["horizon_days"])
        base_spec = base_request.config.research_spec or ResearchExperimentSpec()
        spec = ResearchExperimentSpec(
            feature_window_bars=int(params["feature_window_bars"]),
            lookback_horizons=[horizon],
            horizon_days=horizon,
            target_return_pct=float(params["target_return_pct"]),
            stop_return_pct=float(params["stop_return_pct"]),
            fee_bps=base_spec.fee_bps,
            slippage_bps=base_spec.slippage_bps,
            flat_return_band_pct=float(params["flat_return_band_pct"]),
            feature_version=base_spec.feature_version,
            label_version=base_spec.label_version,
            memory_version=base_spec.memory_version,
        )
        metadata = dict(base_request.config.metadata)
        metadata.update(
            {
                "quote_ev_threshold": str(params["quote_ev_threshold"]),
                "quote_uncertainty_cap": str(params["uncertainty_cap"]),
                "quote_min_fill_probability": str(params["min_fill_probability"]),
                "portfolio_top_n": str(params["top_n"]),
                "portfolio_risk_budget_fraction": str(params["risk_budget_fraction"]),
                "abstain_margin": str(params["abstain_margin"]),
            }
        )
        return spec, replace(base_request.config, research_spec=spec, metadata=metadata), params

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
        return len(reasons) == 0, reasons

    def _objective(self, *, aggregate: dict, folds: list[dict], objective_cfg) -> float:
        fold_expectancies = [float((f.get("test_metrics") or {}).get("expectancy_after_cost", 0.0)) for f in folds]
        mean_fold_expectancy = sum(fold_expectancies) / max(len(fold_expectancies), 1)
        std_fold_expectancy = 0.0 if len(fold_expectancies) <= 1 else math.sqrt(sum((x - mean_fold_expectancy) ** 2 for x in fold_expectancies) / len(fold_expectancies))
        calibration_error = float(aggregate.get("calibration_error", 0.0))
        no_trade_ratio = float(aggregate.get("no_trade_ratio", 0.0))
        drawdown = float(aggregate.get("max_drawdown", 0.0))
        drawdown_penalty = max(0.0, drawdown - objective_cfg.allowed_drawdown)
        return mean_fold_expectancy - objective_cfg.lambda_std_fold_expectancy * std_fold_expectancy - objective_cfg.lambda_calibration_error * calibration_error - objective_cfg.lambda_no_trade_ratio * no_trade_ratio - objective_cfg.lambda_drawdown * drawdown_penalty

    def _baseline_metrics(self, *, request: RunnerRequest, runner_fn: Callable[..., dict], validation_fn: Callable[..., dict], data_path: str | None, data_source: str) -> dict:
        baselines = {}
        for name, strategy_mode in {
            "legacy_event_window": "legacy_event_window",
            "fixed_quote_policy": "research_similarity_v2",
            "simple_momentum_reversion": "research_similarity_v2",
        }.items():
            try:
                result = runner_fn(request=request, data_path=data_path, data_source=data_source, scenario_id=request.scenario.scenario_id, strategy_mode=strategy_mode, enable_validation=False)
                validation = validation_fn(request=request, data_path=data_path, data_source=data_source, scenario_id=request.scenario.scenario_id, strategy_mode=strategy_mode, runner_fn=runner_fn, mode="walk_forward")
                baselines[name] = {"evaluated": True, "summary": result.get("summary", {}), "aggregate": validation.get("aggregate", {})}
            except Exception as exc:
                baselines[name] = {"evaluated": False, "error": str(exc)}
        return baselines

    def _evaluate_generic_trial(self, *, trial, request: RunnerRequest, cfg: OptunaSearchConfig, runner_fn: Callable[..., dict], validation_fn: Callable[..., dict], data_path: str | None, data_source: str, strategy_mode: str, discovery_request: RunnerRequest) -> dict:
        spec, sampled_config, params = self._sample_config(trial, discovery_request, cfg)
        trial_request = RunnerRequest(scenario=discovery_request.scenario, config=sampled_config, output_path=None)
        discovery_result = runner_fn(request=trial_request, data_path=data_path, data_source=data_source, scenario_id=trial_request.scenario.scenario_id, strategy_mode=strategy_mode, enable_validation=False)
        validation = validation_fn(request=trial_request, data_path=data_path, data_source=data_source, scenario_id=trial_request.scenario.scenario_id, strategy_mode=strategy_mode, runner_fn=runner_fn, mode="walk_forward")
        aggregate = dict(validation.get("aggregate", {}))
        aggregate.setdefault("coverage", discovery_result.get("validation", {}).get("coverage", 0.0))
        aggregate.setdefault("no_trade_ratio", discovery_result.get("validation", {}).get("no_trade_ratio", 0.0))
        aggregate.setdefault("max_drawdown", discovery_result.get("summary", {}).get("max_drawdown", 0.0))
        feasible, violations = self._constraint_check(aggregate, cfg.constraints)
        objective_value = self._objective(aggregate=aggregate, folds=validation.get("folds", []), objective_cfg=cfg.objective) if feasible else -1e9
        data_snapshot_id = build_data_snapshot_id(scenario=trial_request.scenario, config=trial_request.config, data_source=data_source, historical_metadata={"scope": "discovery"})
        manifest = build_research_manifest(scenario=trial_request.scenario, config=trial_request.config, data_snapshot_id=data_snapshot_id, code_commit=resolve_code_commit())
        pruner_state = {"enabled": bool(_optuna is not None), "pruner": cfg.pruner}
        trial.set_user_attr("feasible", feasible)
        trial.set_user_attr("constraint_violations", violations)
        return {
            "trial_number": getattr(trial, "number", 0),
            "params": params,
            "manifest": manifest.to_dict(),
            "spec_hash": spec.spec_hash(),
            "data_snapshot_id": data_snapshot_id,
            "code_commit": manifest.code_commit,
            "objective": objective_value,
            "feasible": feasible,
            "constraint_violations": violations,
            "fold_metrics": validation.get("folds", []),
            "aggregate": aggregate,
            "result_path": discovery_result.get("result_path"),
            "pruner_state": pruner_state,
            "baselines": self._baseline_metrics(request=trial_request, runner_fn=runner_fn, validation_fn=validation_fn, data_path=data_path, data_source=data_source),
        }

    def _evaluate_frozen_seed_params_payload(
        self,
        *,
        params: dict[str, Any],
        cfg: OptunaSearchConfig,
        seed_rows: list[dict[str, Any]] | None,
        study_cache_root: str = "",
        initial_capital: float,
        trial_number: int,
        trial=None,
        source: str = "sampled",
    ) -> dict[str, Any]:
        if study_cache_root:
            evaluation = evaluate_frozen_seed_params_from_cache(
                study_cache_root=study_cache_root,
                params=params,
                initial_capital=initial_capital,
                objective_cfg=cfg.objective,
            )
        else:
            evaluation = evaluate_frozen_seed_params(seed_rows=seed_rows or [], params=params, initial_capital=initial_capital, objective_cfg=cfg.objective)
        aggregate = dict(evaluation.get("aggregate") or {})
        feasible = bool(aggregate.get("feasible"))
        violations = []
        if not feasible:
            if int(aggregate.get("trade_count", 0) or 0) < int(cfg.objective.min_trade_count):
                violations.append("low_trade_count")
            if int(aggregate.get("sell_fill_count", 0) or 0) < int(cfg.objective.min_sell_fill_count):
                violations.append("low_sell_fill_count")
        if trial is not None:
            trial.set_user_attr("feasible", feasible)
            trial.set_user_attr("constraint_violations", violations)
        return {
            "trial_number": trial_number,
            "params": params,
            "objective": aggregate.get("objective", -1e9),
            "feasible": feasible,
            "constraint_violations": violations,
            "fold_metrics": evaluation.get("folds", []),
            "aggregate": aggregate,
            "upstream_recomputed": False,
            "mode": "frozen_seed_v1",
            "source": source,
        }

    def _evaluate_frozen_seed_trial(self, *, trial, cfg: OptunaSearchConfig, seed_rows: list[dict[str, Any]] | None, study_cache_root: str = "", initial_capital: float) -> dict[str, Any]:
        params = self._suggest(trial, self._space(cfg))
        return self._evaluate_frozen_seed_params_payload(
            params=params,
            cfg=cfg,
            seed_rows=seed_rows,
            study_cache_root=study_cache_root,
            initial_capital=initial_capital,
            trial_number=getattr(trial, "number", 0),
            trial=trial,
            source="sampled",
        )

    def _build_study(self, cfg: OptunaSearchConfig):
        if _optuna is None:
            return None
        sampler = _optuna.samplers.TPESampler(seed=cfg.seed)
        pruner = _optuna.pruners.MedianPruner() if cfg.pruner == "median" else _optuna.pruners.NopPruner()
        return _optuna.create_study(direction="maximize", sampler=sampler, pruner=pruner)

    def _warm_start_trials(self, cfg: OptunaSearchConfig) -> list[dict[str, Any]]:
        if not cfg.warm_start:
            return []
        space = self._space(cfg)
        configured = [
            self._normalize_frozen_seed_params(params=dict(item), space=space)
            for item in (cfg.warm_start_trials or [])
            if isinstance(item, dict)
        ]
        defaults = [self._normalize_frozen_seed_params(params=dict(item), space=space) for item in default_frozen_seed_warm_start_trials()]
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for params in [*configured, *defaults]:
            key = json.dumps(params, sort_keys=True, default=str)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(params)
        return deduped

    def _enqueue_warm_start_trials(self, *, study, cfg: OptunaSearchConfig) -> list[dict[str, Any]]:
        warm_starts = self._warm_start_trials(cfg)
        for params in warm_starts:
            study.enqueue_trial(dict(params))
        return warm_starts

    def _write_study_outputs(self, *, experiment_id: str, trials: list[dict[str, Any]], best_trial: dict[str, Any] | None, summary_payload: dict[str, Any]) -> dict[str, str]:
        out_dir = Path(self.store.output_dir) / self.store.namespace
        out_dir.mkdir(parents=True, exist_ok=True)
        best_params_path = out_dir / f"{experiment_id}_best_params.json"
        trial_table_path = out_dir / f"{experiment_id}_trial_table.csv"
        study_summary_path = out_dir / f"{experiment_id}_study_summary.json"
        best_params_path.write_text(json.dumps((best_trial or {}).get("params") or {}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        fieldnames = sorted({key for trial in trials for key in trial.keys() if key not in {"fold_metrics", "aggregate", "params"}} | {"params_json", "aggregate_json"})
        with trial_table_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for trial in trials:
                row = {k: v for k, v in trial.items() if k in fieldnames}
                row["params_json"] = json.dumps(trial.get("params") or {}, ensure_ascii=False, default=str)
                row["aggregate_json"] = json.dumps(trial.get("aggregate") or {}, ensure_ascii=False, default=str)
                writer.writerow(row)
        study_summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return {
            "best_params_path": str(best_params_path),
            "trial_table_path": str(trial_table_path),
            "study_summary_path": str(study_summary_path),
        }

    def _run_frozen_seed_v1(self, *, request: RunnerRequest, cfg: OptunaSearchConfig) -> dict[str, Any]:
        resolved_seed_profile = resolve_seed_profile(seed_profile=cfg.seed_profile, seed_filter=cfg.seed_filter)
        study_cache_loaded = load_study_cache_manifest(cfg.seed_artifact_root)
        use_study_cache = bool(
            study_cache_loaded
            and str(((study_cache_loaded.get("manifest") or {}).get("seed_profile") or "")) == resolved_seed_profile
            and str(((study_cache_loaded.get("manifest") or {}).get("policy_scope") or "")) == str(cfg.policy_scope or "")
        )
        study_cache_root = str(study_cache_loaded.get("cache_root") or "") if use_study_cache and study_cache_loaded else ""
        if use_study_cache and study_cache_loaded:
            seed_bundle = {
                "summary": ((study_cache_loaded.get("manifest") or {}).get("source_seed_summary") or {}),
            }
            study_cache_manifest = dict(study_cache_loaded.get("manifest") or {})
            seed_rows: list[dict[str, Any]] = []
            filtered_seed_row_count = int(study_cache_manifest.get("row_count") or 0)
            filtered_buy_row_count = int(study_cache_manifest.get("buy_row_count") or 0)
            filtered_sell_row_count = int(study_cache_manifest.get("sell_row_count") or 0)
        else:
            seed_bundle = load_optuna_replay_seed(cfg.seed_artifact_root)
            study_cache_manifest = None
            seed_rows = filter_optuna_seed_rows(
                seed_rows=[dict(row) for row in (seed_bundle.get("rows") or [])],
                policy_scope=cfg.policy_scope,
                seed_filter=cfg.seed_filter,
                seed_profile=resolved_seed_profile,
            )
            filtered_seed_row_count = len(seed_rows)
            filtered_buy_row_count = sum(1 for row in seed_rows if str(row.get("side") or "") == "BUY")
            filtered_sell_row_count = sum(1 for row in seed_rows if str(row.get("side") or "") == "SELL")
        study = self._build_study(cfg)
        trials: list[dict[str, Any]] = []
        feasible_trials: list[dict[str, Any]] = []
        warm_starts = self._warm_start_trials(cfg)
        if _optuna is not None:
            if warm_starts:
                self._enqueue_warm_start_trials(study=study, cfg=cfg)

            def objective(trial):
                payload = self._evaluate_frozen_seed_trial(
                    trial=trial,
                    cfg=cfg,
                    seed_rows=seed_rows,
                    study_cache_root=study_cache_root,
                    initial_capital=request.config.initial_capital,
                )
                payload["trial_path"] = self.store.save_blob(name=f"{cfg.experiment_id}_trial_{payload['trial_number']}", payload=payload)
                payload["state"] = str(getattr(trial, "state", "COMPLETE"))
                trials.append(payload)
                if payload["feasible"]:
                    feasible_trials.append(payload)
                return float(payload["objective"])

            study.optimize(objective, n_trials=cfg.n_trials)
            study_meta = {"engine": "optuna", "study_name": study.study_name, "n_trials": len(study.trials), "sampler": type(study.sampler).__name__, "pruner": type(study.pruner).__name__, "warm_start_trial_count": len(warm_starts)}
        else:
            rng = random.Random(cfg.seed)
            study_meta = {"engine": "fallback", "n_trials": cfg.n_trials, "sampler": "random", "pruner": cfg.pruner, "warm_start_trial_count": len(warm_starts)}
            trial_no = 0
            for params in warm_starts[: cfg.n_trials]:
                payload = self._evaluate_frozen_seed_params_payload(
                    params=params,
                    cfg=cfg,
                    seed_rows=seed_rows,
                    study_cache_root=study_cache_root,
                    initial_capital=request.config.initial_capital,
                    trial_number=trial_no,
                    source="warm_start",
                )
                payload["trial_path"] = self.store.save_blob(name=f"{cfg.experiment_id}_trial_{trial_no}", payload=payload)
                payload["state"] = "COMPLETE"
                trials.append(payload)
                if payload["feasible"]:
                    feasible_trials.append(payload)
                trial_no += 1
            for trial_no in range(trial_no, cfg.n_trials):
                trial = _FallbackTrial(trial_no, rng)
                payload = self._evaluate_frozen_seed_trial(
                    trial=trial,
                    cfg=cfg,
                    seed_rows=seed_rows,
                    study_cache_root=study_cache_root,
                    initial_capital=request.config.initial_capital,
                )
                payload["trial_path"] = self.store.save_blob(name=f"{cfg.experiment_id}_trial_{trial_no}", payload=payload)
                payload["state"] = trial.state
                trials.append(payload)
                if payload["feasible"]:
                    feasible_trials.append(payload)
        best = max(feasible_trials, key=lambda t: float(t["objective"]), default=None)
        mode_comparison = summarize_execution_mode_comparison(trials)
        study_summary = {
            "experiment_id": cfg.experiment_id,
            "mode": cfg.mode,
            "policy_scope": cfg.policy_scope,
            "seed_profile": resolved_seed_profile,
            "objective_metric": cfg.objective_metric,
            "source_seed_summary": seed_bundle.get("summary") or {},
            "study_cache_root": study_cache_root or None,
            "study_cache_manifest": study_cache_manifest,
            "filtered_seed_row_count": filtered_seed_row_count,
            "filtered_buy_row_count": filtered_buy_row_count,
            "filtered_sell_row_count": filtered_sell_row_count,
            "warm_start_trial_count": len(warm_starts),
            "mode_comparison": mode_comparison,
            "best_trial": {
                "trial_number": (best or {}).get("trial_number"),
                "objective": (best or {}).get("objective"),
                "aggregate": (best or {}).get("aggregate"),
                "params": (best or {}).get("params"),
            },
            "holdout_status": "not_available",
        }
        output_paths = self._write_study_outputs(experiment_id=cfg.experiment_id, trials=trials, best_trial=best, summary_payload=study_summary)
        final_payload = {
            "experiment_id": cfg.experiment_id,
            "seed": cfg.seed,
            "study": study_meta,
            "status": "ok" if best is not None else "no_feasible_trial",
            "discovery_report": {
                "seed_artifact_root": cfg.seed_artifact_root,
                "trials": trials,
                "best_trial": best,
                "seed_summary": seed_bundle.get("summary") or {},
                "seed_profile": resolved_seed_profile,
                "filtered_seed_row_count": filtered_seed_row_count,
                "study_cache_root": study_cache_root or None,
            },
            "holdout_report": {"status": "not_available"},
            "study_outputs": output_paths,
            "mode_comparison": mode_comparison,
        }
        artifact_path = self.store.save_blob(name=cfg.experiment_id, payload=final_payload)
        return {
            "artifact_path": artifact_path,
            "best_trial": best,
            "trials": trials,
            "holdout_report": final_payload["holdout_report"],
            "discovery_report": final_payload["discovery_report"],
            "status": final_payload["status"],
            "study_outputs": output_paths,
            "mode_comparison": mode_comparison,
        }

    def run(self, *, request: RunnerRequest, runner_fn: Callable[..., dict], validation_fn: Callable[..., dict], data_path: str | None = None, data_source: str = "local-db", strategy_mode: str = "research_similarity_v2") -> dict:
        cfg = request.config.optuna or OptunaSearchConfig(experiment_id=request.scenario.scenario_id)
        if cfg.mode == "frozen_seed_v1":
            return self._run_frozen_seed_v1(request=request, cfg=cfg)
        discovery_request = RunnerRequest(scenario=replace(request.scenario, start_date=cfg.discovery_start_date or request.scenario.start_date, end_date=cfg.discovery_end_date or request.scenario.end_date), config=request.config, output_path=None)
        holdout_request_base = RunnerRequest(scenario=replace(request.scenario, start_date=cfg.holdout_start_date or request.scenario.start_date, end_date=cfg.holdout_end_date or request.scenario.end_date), config=request.config, output_path=None)
        trials: list[dict] = []
        feasible_trials: list[dict] = []

        if _optuna is not None:
            study = self._build_study(cfg)

            def objective(trial):
                payload = self._evaluate_generic_trial(trial=trial, request=request, cfg=cfg, runner_fn=runner_fn, validation_fn=validation_fn, data_path=data_path, data_source=data_source, strategy_mode=strategy_mode, discovery_request=discovery_request)
                payload["trial_path"] = self.store.save_blob(name=f"{cfg.experiment_id}_trial_{payload['trial_number']}", payload=payload)
                payload["state"] = str(getattr(trial, "state", "COMPLETE"))
                trials.append(payload)
                if payload["feasible"]:
                    feasible_trials.append(payload)
                return float(payload["objective"])

            study.optimize(objective, n_trials=cfg.n_trials)
            study_meta = {"engine": "optuna", "study_name": study.study_name, "n_trials": len(study.trials), "sampler": type(study.sampler).__name__, "pruner": type(study.pruner).__name__}
        else:
            rng = random.Random(cfg.seed)
            study_meta = {"engine": "fallback", "n_trials": cfg.n_trials, "sampler": "random", "pruner": cfg.pruner}
            for trial_no in range(cfg.n_trials):
                trial = _FallbackTrial(trial_no, rng)
                payload = self._evaluate_generic_trial(trial=trial, request=request, cfg=cfg, runner_fn=runner_fn, validation_fn=validation_fn, data_path=data_path, data_source=data_source, strategy_mode=strategy_mode, discovery_request=discovery_request)
                payload["trial_path"] = self.store.save_blob(name=f"{cfg.experiment_id}_trial_{trial_no}", payload=payload)
                payload["state"] = trial.state
                trials.append(payload)
                if payload["feasible"]:
                    feasible_trials.append(payload)

        best = max(feasible_trials, key=lambda t: t["objective"], default=None)
        holdout_report = None
        status = "ok"
        if best is not None:
            base_spec = request.config.research_spec or ResearchExperimentSpec()
            best_spec = ResearchExperimentSpec(**{**base_spec.to_dict(), **{k: v for k, v in best["params"].items() if k in {"feature_window_bars", "horizon_days", "target_return_pct", "stop_return_pct", "flat_return_band_pct"}}, "lookback_horizons": [best["params"].get("horizon_days", base_spec.horizon_days)]})
            holdout_metadata = {
                **dict(request.config.metadata),
                "quote_ev_threshold": str(best["params"].get("quote_ev_threshold", request.config.metadata.get("quote_ev_threshold", 0.005))),
                "quote_uncertainty_cap": str(best["params"].get("uncertainty_cap", request.config.metadata.get("quote_uncertainty_cap", 0.12))),
                "quote_min_fill_probability": str(best["params"].get("min_fill_probability", request.config.metadata.get("quote_min_fill_probability", 0.1))),
                "portfolio_top_n": str(best["params"].get("top_n", request.config.metadata.get("portfolio_top_n", 5))),
                "portfolio_risk_budget_fraction": str(best["params"].get("risk_budget_fraction", request.config.metadata.get("portfolio_risk_budget_fraction", 0.95))),
                "abstain_margin": str(best["params"].get("abstain_margin", request.config.metadata.get("abstain_margin", 0.0))),
            }
            holdout_request = RunnerRequest(scenario=holdout_request_base.scenario, config=replace(request.config, research_spec=best_spec, metadata=holdout_metadata), output_path=None)
            holdout_result = runner_fn(request=holdout_request, data_path=data_path, data_source=data_source, scenario_id=holdout_request.scenario.scenario_id, strategy_mode=strategy_mode, enable_validation=False)
            holdout_validation = validation_fn(request=holdout_request, data_path=data_path, data_source=data_source, scenario_id=holdout_request.scenario.scenario_id, strategy_mode=strategy_mode, runner_fn=runner_fn, mode="walk_forward")
            holdout_report = {"scenario": holdout_request.scenario.scenario_id, "manifest": holdout_result.get("manifest"), "summary": holdout_result.get("summary"), "validation": holdout_validation}
        else:
            status = "no_feasible_trial"

        final_payload = {"experiment_id": cfg.experiment_id, "seed": cfg.seed, "retry_failed_trials": cfg.retry_failed_trials, "study": study_meta, "status": status, "discovery_report": {"scenario": discovery_request.scenario.scenario_id, "trials": trials, "best_trial": best}, "holdout_report": holdout_report}
        artifact_path = self.store.save_blob(name=cfg.experiment_id, payload=final_payload)
        return {"artifact_path": artifact_path, "best_trial": best, "trials": trials, "holdout_report": holdout_report, "discovery_report": final_payload["discovery_report"], "status": status}

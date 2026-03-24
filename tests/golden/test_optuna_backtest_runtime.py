from __future__ import annotations

import json

from backtest_app.optuna.models import ExperimentConfig
from backtest_app.optuna.study_runner import OptunaStudyRunner


FIXTURE_PATH = r"A:\vargg-workspace\30_trading\auto-trader-bot\tests\fixtures\backtest_historical_fixture.json"


def test_optuna_runner_is_reproducible_for_same_seed(tmp_path):
    config = ExperimentConfig(
        experiment_id="optuna-seed-check",
        strategy_version="pm-core-v2",
        feature_version="features-basic-v1",
        data_window="2026-03-01:2026-03-24",
        universe=["NVDA", "AAPL"],
        objective_metric="filled_ratio",
        seed=7,
        n_trials=3,
        study_name="seed-check",
    )
    runner = OptunaStudyRunner(str(tmp_path))
    first = runner.run(config=config, data_path=FIXTURE_PATH)
    second = runner.run(config=config, data_path=FIXTURE_PATH)
    assert first["best_value"] == second["best_value"]
    assert first["best_trial"].parameter_set_hash == second["best_trial"].parameter_set_hash
    assert first["best_trial"].decision_engine_version == second["best_trial"].decision_engine_version


def test_optuna_artifact_tracks_decision_engine_version(tmp_path):
    config = ExperimentConfig(
        experiment_id="optuna-artifact-check",
        strategy_version="pm-core-v2",
        feature_version="features-basic-v1",
        data_window="2026-03-01:2026-03-24",
        universe=["NVDA", "AAPL"],
        objective_metric="filled_ratio",
        seed=11,
        n_trials=2,
        study_name="artifact-check",
    )
    result = OptunaStudyRunner(str(tmp_path)).run(config=config, data_path=FIXTURE_PATH)
    payload = json.loads((tmp_path / "optuna-artifact-check.json").read_text(encoding="utf-8"))
    assert payload["best_trial"]["decision_engine_version"].startswith("shared.domain.execution")
    assert payload["experiment"]["strategy_version"] == "pm-core-v2"
    assert result["artifact_path"].endswith("optuna-artifact-check.json")

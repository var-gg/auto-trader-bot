from __future__ import annotations

import random

import numpy as np
import optuna
from optuna.samplers import TPESampler

from .artifacts import JsonExperimentStore
from .models import ExperimentConfig, TrialRecord
from .objective import evaluate_trial


class OptunaStudyRunner:
    def __init__(self, artifact_dir: str):
        self.artifact_dir = artifact_dir

    def run(self, *, config: ExperimentConfig, data_path: str):
        random.seed(config.seed)
        np.random.seed(config.seed)
        sampler = TPESampler(seed=config.seed)
        study = optuna.create_study(direction=config.direction, study_name=config.study_name, sampler=sampler)
        records: list[TrialRecord] = []

        def objective(trial):
            value, record = evaluate_trial(trial=trial, experiment=config, data_path=data_path)
            records.append(record)
            return value

        study.optimize(objective, n_trials=config.n_trials)
        best_number = study.best_trial.number if study.best_trial else None
        best_record = next((r for r in records if r.trial_number == best_number), None)
        artifact_path = JsonExperimentStore(self.artifact_dir).save_study(
            config=config,
            trials=records,
            best_trial=best_record,
        )
        return {
            "study_name": study.study_name,
            "best_value": study.best_value,
            "best_trial": best_record,
            "artifact_path": artifact_path,
            "trial_count": len(records),
        }

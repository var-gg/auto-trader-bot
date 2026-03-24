from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class ExperimentConfig:
    experiment_id: str
    strategy_version: str
    feature_version: str
    data_window: str
    universe: List[str]
    objective_metric: str
    seed: int
    n_trials: int = 10
    study_name: str = "backtest_optuna"
    direction: str = "maximize"
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class TrialRecord:
    trial_number: int
    value: float
    parameter_set_hash: str
    params: Dict[str, float]
    strategy_version: str
    feature_version: str
    data_window: str
    universe: List[str]
    objective_metric: str
    seed: int
    decision_engine_version: str
    metadata: Dict[str, str] = field(default_factory=dict)

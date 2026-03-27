from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class ResearchExperimentSpec:
    feature_window_bars: int = 60
    lookback_horizons: List[int] = field(default_factory=lambda: [5])
    horizon_days: int = 5
    target_return_pct: float = 0.04
    stop_return_pct: float = 0.03
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    flat_return_band_pct: float = 0.005
    feature_version: str = "multiscale_v2"
    label_version: str = "event_outcome_v1"
    memory_version: str = "memory_asof_v1"

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    def spec_hash(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class ResearchRunManifest:
    experiment_id: str
    spec_hash: str
    data_snapshot_id: str
    code_commit: str
    calendar_convention: str
    execution_convention: str
    cost_model_version: str

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)

    def manifest_id(self) -> str:
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class OptunaConstraintConfig:
    min_psr: float = 0.55
    min_dsr: float = 0.55
    min_effective_sample_size: float = 1.0
    min_coverage: float = 0.05
    require_monotonicity: bool = True


@dataclass(frozen=True)
class OptunaObjectiveConfig:
    lambda_std_fold_expectancy: float = 0.25
    lambda_calibration_error: float = 0.20
    lambda_no_trade_ratio: float = 0.15
    lambda_drawdown: float = 0.20
    allowed_drawdown: float = 0.15


@dataclass(frozen=True)
class OptunaSearchConfig:
    experiment_id: str
    n_trials: int = 10
    seed: int = 42
    discovery_start_date: str = ""
    discovery_end_date: str = ""
    holdout_start_date: str = ""
    holdout_end_date: str = ""
    pruner: str = "median"
    retry_failed_trials: int = 1
    constraints: OptunaConstraintConfig = field(default_factory=OptunaConstraintConfig)
    objective: OptunaObjectiveConfig = field(default_factory=OptunaObjectiveConfig)


@dataclass(frozen=True)
class BacktestScenario:
    scenario_id: str
    market: str
    start_date: str
    end_date: str
    symbols: List[str]
    strategy_id: str = "pm_open"
    strategy_version: str = "v1"
    params: Dict[str, float] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float
    cash_buffer_ratio: float = 0.12
    slippage_bps: float = 0.0
    fee_bps: float = 0.0
    allow_partial_fills: bool = True
    metadata: Dict[str, str] = field(default_factory=dict)
    research_spec: Optional[ResearchExperimentSpec] = None
    manifest: Optional[ResearchRunManifest] = None
    optuna: Optional[OptunaSearchConfig] = None


@dataclass(frozen=True)
class RunnerRequest:
    scenario: BacktestScenario
    config: BacktestConfig
    output_path: Optional[str] = None


def resolve_code_commit() -> str:
    try:
        proc = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
        return proc.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def build_research_manifest(*, scenario: BacktestScenario, config: BacktestConfig, data_snapshot_id: str, calendar_convention: str = "TRADING_DAY", execution_convention: str = "EOD_T_SIGNAL__T1_OPEN_EXECUTION", cost_model_version: str = "cost_v1", code_commit: str | None = None) -> ResearchRunManifest:
    spec = config.research_spec or ResearchExperimentSpec()
    return ResearchRunManifest(experiment_id=scenario.scenario_id, spec_hash=spec.spec_hash(), data_snapshot_id=data_snapshot_id, code_commit=code_commit or resolve_code_commit(), calendar_convention=calendar_convention, execution_convention=execution_convention, cost_model_version=cost_model_version)

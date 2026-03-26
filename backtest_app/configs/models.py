from __future__ import annotations

import hashlib
import json
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


@dataclass(frozen=True)
class RunnerRequest:
    scenario: BacktestScenario
    config: BacktestConfig
    output_path: Optional[str] = None

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


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


@dataclass(frozen=True)
class RunnerRequest:
    scenario: BacktestScenario
    config: BacktestConfig
    output_path: Optional[str] = None

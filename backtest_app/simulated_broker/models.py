from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class SimulationRules:
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    allow_partial_fills: bool = True
    partial_fill_ratio: float = 0.5
    allow_gap_fill: bool = True
    session_cutoff_mode: str = "DAY"
    deterministic_seed: int = 0
    metadata: Dict[str, str] = field(default_factory=dict)

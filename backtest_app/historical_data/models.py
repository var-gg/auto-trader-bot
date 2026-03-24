from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from shared.domain.models import MarketSnapshot, SignalCandidate


@dataclass(frozen=True)
class HistoricalBar:
    symbol: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class HistoricalSlice:
    market_snapshot: MarketSnapshot
    bars_by_symbol: Dict[str, List[HistoricalBar]] = field(default_factory=dict)
    candidates: List[SignalCandidate] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional

from .base import DomainModel, JsonValue
from .enums import MarketCode, OutcomeLabel, Side


@dataclass(frozen=True)
class SignalCandidate(DomainModel):
    symbol: str
    ticker_id: Optional[int]
    market: MarketCode
    side_bias: Side
    signal_strength: float
    confidence: Optional[float] = None
    anchor_date: Optional[date] = None
    reference_date: Optional[date] = None
    current_price: Optional[float] = None
    atr_pct: Optional[float] = None
    target_return_pct: Optional[float] = None
    max_reverse_pct: Optional[float] = None
    expected_horizon_days: Optional[int] = None
    outcome_label: OutcomeLabel = OutcomeLabel.UNKNOWN
    reverse_breach_day: Optional[int] = None
    provenance: Dict[str, JsonValue] = field(default_factory=dict)
    diagnostics: Dict[str, JsonValue] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutionIntent(DomainModel):
    symbol: str
    ticker_id: Optional[int]
    side: Side
    intent_status: str
    rationale: str
    decision_time: datetime
    priority: Optional[float] = None
    requested_notional: Optional[float] = None
    requested_quantity: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, JsonValue] = field(default_factory=dict)

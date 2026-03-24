from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from .base import DomainModel, JsonValue
from .enums import MarketCode, RiskBias


@dataclass(frozen=True)
class MarketSnapshot(DomainModel):
    market: MarketCode
    as_of: datetime
    session_label: str
    is_open: bool
    reference_price: Optional[float] = None
    last_close_price: Optional[float] = None
    volatility_pct: Optional[float] = None
    macro_state: Dict[str, JsonValue] = field(default_factory=dict)
    news_state: Dict[str, JsonValue] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class StrategyContext(DomainModel):
    strategy_id: str
    strategy_version: str
    market: MarketCode
    venue: str
    decision_time: datetime
    account_equity: Optional[float] = None
    buying_power: Optional[float] = None
    cash_buffer_ratio: Optional[float] = None
    params: Dict[str, JsonValue] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class RiskPolicy(DomainModel):
    policy_id: str
    policy_version: str
    bias: RiskBias = RiskBias.NEUTRAL
    discount_multiplier: float = 1.0
    sell_markup_multiplier: float = 1.0
    max_symbol_weight: Optional[float] = None
    hard_stop_min: Optional[float] = None
    hard_stop_max: Optional[float] = None
    time_stop_minutes: Optional[int] = None
    allow_new_buys: bool = True
    blocked_symbols: List[str] = field(default_factory=list)
    reason: Optional[str] = None

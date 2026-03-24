from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from .base import DomainModel, JsonValue
from .enums import ExecutionVenue, FillStatus, OrderType, OutcomeLabel, Side


@dataclass(frozen=True)
class LadderLeg(DomainModel):
    leg_id: str
    side: Side
    order_type: OrderType
    quantity: int
    limit_price: Optional[float] = None
    price_offset_pct: Optional[float] = None
    rationale: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderPlan(DomainModel):
    plan_id: str
    symbol: str
    ticker_id: Optional[int]
    side: Side
    generated_at: datetime
    status: str
    rationale: str
    venue: ExecutionVenue = ExecutionVenue.LIVE
    requested_budget: Optional[float] = None
    requested_quantity: Optional[int] = None
    legs: List[LadderLeg] = field(default_factory=list)
    risk_notes: List[str] = field(default_factory=list)
    skip_reason: Optional[str] = None
    metadata: Dict[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class FillOutcome(DomainModel):
    plan_id: str
    leg_id: Optional[str]
    symbol: str
    side: Side
    fill_status: FillStatus
    venue: ExecutionVenue
    event_time: datetime
    requested_quantity: Optional[int] = None
    filled_quantity: Optional[int] = None
    requested_price: Optional[float] = None
    average_fill_price: Optional[float] = None
    slippage_bps: Optional[float] = None
    reject_code: Optional[str] = None
    reject_message: Optional[str] = None
    metadata: Dict[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class OutcomeRecord(DomainModel):
    symbol: str
    horizon_days: int
    label: OutcomeLabel
    entry_price: float
    exit_price: Optional[float] = None
    pnl_bps: Optional[float] = None
    evaluated_at: Optional[datetime] = None
    metadata: Dict[str, JsonValue] = field(default_factory=dict)

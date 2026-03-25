from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ResearchAnchor:
    symbol: str
    anchor_code: str
    reference_date: str
    anchor_date: Optional[str] = None
    side: str = "BUY"
    embedding: List[float] = field(default_factory=list)
    anchor_quality: float = 0.0
    regime_code: Optional[str] = None
    sector_code: Optional[str] = None
    liquidity_score: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PrototypeAnchor:
    prototype_id: str
    anchor_code: str
    side: str
    embedding: List[float]
    member_count: int
    representative_symbol: Optional[str] = None
    representative_date: Optional[str] = None
    anchor_quality: float = 0.0
    regime_code: Optional[str] = None
    sector_code: Optional[str] = None
    liquidity_score: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

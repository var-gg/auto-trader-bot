from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class DistributionEstimate:
    side: str
    p_target_first: float = 0.0
    p_stop_first: float = 0.0
    p_flat: float = 0.0
    expected_net_return: float = 0.0
    expected_mae: float = 0.0
    expected_mfe: float = 0.0
    q10_return: float = 0.0
    q50_return: float = 0.0
    q90_return: float = 0.0
    effective_sample_size: float = 0.0
    regime_alignment: float = 0.0
    uncertainty: float = 1.0
    lower_bound_return: float = 0.0
    upper_bound_return: float = 0.0
    utility: Dict[str, Any] = field(default_factory=dict)
    top_matches: List[Dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class DecisionSurface:
    buy: DistributionEstimate
    sell: DistributionEstimate
    chosen_side: str = "ABSTAIN"
    abstain: bool = True
    abstain_reasons: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EventOutcomeRecord:
    symbol: str
    event_date: str
    outcome_end_date: Optional[str]
    schema_version: str
    path_summary: Dict[str, Any] = field(default_factory=dict)
    side_outcomes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResearchAnchor:
    symbol: str
    anchor_code: str
    reference_date: str
    anchor_date: Optional[str] = None
    side: str = "BUY"
    embedding: List[float] = field(default_factory=list)
    shape_vector: List[float] = field(default_factory=list)
    ctx_vector: List[float] = field(default_factory=list)
    vector_version: Optional[str] = None
    embedding_model: Optional[str] = None
    vector_dim: Optional[int] = None
    anchor_quality: float = 0.0
    mae_pct: Optional[float] = None
    mfe_pct: Optional[float] = None
    days_to_hit: Optional[int] = None
    after_cost_return_pct: Optional[float] = None
    realized_return_pct: Optional[float] = None
    regime_code: Optional[str] = None
    sector_code: Optional[str] = None
    liquidity_score: Optional[float] = None
    prototype_id: Optional[str] = None
    prototype_membership: Dict[str, Any] = field(default_factory=dict)
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
    shape_vector: List[float] = field(default_factory=list)
    ctx_vector: List[float] = field(default_factory=list)
    vector_version: Optional[str] = None
    feature_version: Optional[str] = None
    embedding_model: Optional[str] = None
    vector_dim: Optional[int] = None
    anchor_quality: float = 0.0
    regime_code: Optional[str] = None
    sector_code: Optional[str] = None
    liquidity_score: Optional[float] = None
    support_count: int = 0
    decayed_support: float = 0.0
    mean_return_pct: Optional[float] = None
    median_return_pct: Optional[float] = None
    win_rate: Optional[float] = None
    mae_mean_pct: Optional[float] = None
    mfe_mean_pct: Optional[float] = None
    return_dispersion: Optional[float] = None
    uncertainty: Optional[float] = None
    freshness_days: Optional[float] = None
    liquidity_bucket: Optional[str] = None
    regime_bucket: Optional[str] = None
    sector_bucket: Optional[str] = None
    prototype_membership: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

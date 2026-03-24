from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from shared.domain.models import FillOutcome, FillStatus, OrderPlan


@dataclass(frozen=True)
class BacktestSummary:
    scenario_id: str
    total_plans: int
    total_legs: int
    filled_legs: int
    unfilled_legs: int
    symbols: List[str] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)


def build_summary(*, scenario_id: str, plans: Iterable[OrderPlan], fills: Iterable[FillOutcome]) -> BacktestSummary:
    plans = list(plans)
    fills = list(fills)
    return BacktestSummary(
        scenario_id=scenario_id,
        total_plans=len(plans),
        total_legs=sum(len(plan.legs) for plan in plans),
        filled_legs=sum(1 for fill in fills if fill.fill_status == FillStatus.FULL),
        unfilled_legs=sum(1 for fill in fills if fill.fill_status != FillStatus.FULL),
        symbols=sorted({plan.symbol for plan in plans}),
        metadata={"report": "basic"},
    )

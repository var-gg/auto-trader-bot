from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List

from shared.domain.models import FillOutcome, FillStatus, OrderPlan

from backtest_app.historical_data.models import HistoricalBar
from backtest_app.validation import compute_performance_metrics


@dataclass(frozen=True)
class BacktestSummary:
    scenario_id: str
    total_plans: int
    total_legs: int
    filled_legs: int
    unfilled_legs: int
    symbols: List[str] = field(default_factory=list)
    metadata: Dict[str, object] = field(default_factory=dict)


def build_summary(*, scenario_id: str, plans: Iterable[OrderPlan], fills: Iterable[FillOutcome], bars_by_symbol: Dict[str, List[HistoricalBar]] | None = None, date_artifacts: List[dict] | None = None) -> BacktestSummary:
    plans = list(plans)
    fills = list(fills)
    perf = compute_performance_metrics(plans=plans, fills=fills, bars_by_symbol=bars_by_symbol, total_symbols=len({plan.symbol for plan in plans}) or len(plans) or 1)
    date_artifacts = list(date_artifacts or [])
    return BacktestSummary(
        scenario_id=scenario_id,
        total_plans=len(plans),
        total_legs=sum(len(plan.legs) for plan in plans),
        filled_legs=sum(1 for fill in fills if fill.fill_status == FillStatus.FULL),
        unfilled_legs=sum(1 for fill in fills if fill.fill_status != FillStatus.FULL),
        symbols=sorted({plan.symbol for plan in plans}),
        metadata={
            "report": "validation",
            **perf,
            "raw_vs_calibrated_ev_lift": perf.get("raw_vs_calibrated_ev_lift", {}),
            "cash_path": [{"decision_date": row.get("decision_date"), "cash": row.get("cash")} for row in date_artifacts],
            "exposure_path": [{"decision_date": row.get("decision_date"), "exposure": row.get("exposure")} for row in date_artifacts],
            "open_position_count_path": [{"decision_date": row.get("decision_date"), "open_position_count": row.get("open_position_count")} for row in date_artifacts],
            "date_count": len(date_artifacts),
        },
    )

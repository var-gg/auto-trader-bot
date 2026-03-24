from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Tuple

from shared.domain.execution import build_order_plan_from_candidate
from shared.domain.execution.outcomes import label_outcome_from_pnl_bps
from shared.domain.models import ExecutionVenue, MarketCode, OutcomeLabel, Side, SignalCandidate

from .context import RunContext


@dataclass
class LivePlanningInput:
    market: str
    candidate: Dict[str, Any]
    tuning: Dict[str, Any]
    budget: float
    side: str = "BUY"
    generated_at: datetime | None = None
    rationale_prefix: str = "live-plan"
    venue: ExecutionVenue = ExecutionVenue.LIVE


class BuildOrderPlanCommand:
    """Live-side planning command that depends only on shared/domain.

    This is the planning seam for parity tests and future migration.
    """

    def execute(self, request: LivePlanningInput, ctx: RunContext):
        candidate = SignalCandidate(
            symbol=request.candidate["symbol"],
            ticker_id=request.candidate.get("ticker_id"),
            market=MarketCode(request.market),
            side_bias=Side(request.side),
            signal_strength=float(request.candidate.get("signal_strength") or request.candidate.get("signal_1d") or 0.0),
            current_price=float(request.candidate.get("current_price") or 0.0),
            atr_pct=float(request.candidate.get("atr_pct") or 0.0),
            outcome_label=OutcomeLabel(str(request.candidate.get("tb_label") or "UNKNOWN")) if request.candidate.get("tb_label") else OutcomeLabel.UNKNOWN,
            reverse_breach_day=request.candidate.get("reverse_breach_day"),
            provenance={
                "has_long_recommendation": request.candidate.get("has_long_recommendation", False),
                "policy_version": request.candidate.get("policy_version", "unknown"),
            },
            diagnostics={
                "iae_1_3": request.candidate.get("iae_1_3"),
                "source": request.candidate.get("source", "live-fixture"),
            },
            notes=list(request.candidate.get("notes", [])),
        )
        return build_order_plan_from_candidate(
            candidate,
            generated_at=request.generated_at or ctx.invoked_at,
            market=request.market,
            side=Side(request.side),
            tuning=request.tuning,
            budget=float(request.budget),
            venue=request.venue,
            rationale_prefix=request.rationale_prefix,
        )


class OutcomeLabelParityQuery:
    def execute(self, pnl_bps: float, ctx: RunContext):
        return label_outcome_from_pnl_bps(pnl_bps)

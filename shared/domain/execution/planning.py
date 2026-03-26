from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from shared.domain.models import ExecutionVenue, OrderPlan, Side, SignalCandidate
from .ladder import generate_pm_ladder, qty_from_budget, round_to_tick


def build_order_plan_from_candidate(
    candidate: SignalCandidate,
    *,
    generated_at: datetime,
    market: str,
    side: Side,
    tuning: Dict,
    budget: float,
    venue: ExecutionVenue = ExecutionVenue.BACKTEST,
    gain_pct: float = 0.0,
    rationale_prefix: str = "domain-plan",
    quote_policy: Dict | None = None,
) -> Tuple[OrderPlan | None, Dict[str, str] | None]:
    current_price = float(candidate.current_price or 0.0)
    if current_price <= 0 or budget <= 0:
        return None, {"code": "INVALID", "note": f"[{candidate.symbol}] invalid cur/budget"}
    atr_pct = float(candidate.atr_pct or 0.05)
    quote_policy = dict(quote_policy or {})
    required = float(quote_policy.get("buy_gap", max(0.012, 0.4 * atr_pct)) if side == Side.BUY else quote_policy.get("sell_gap", max(0.012, 0.4 * atr_pct)))
    first_limit_est = round_to_tick(current_price * (1.0 - required), market)
    qty_cap = qty_from_budget(first_limit_est, budget)
    if qty_cap <= 0:
        return None, {"code": "BUDGET", "note": f"[{candidate.symbol}] budget too small"}
    legs, desc = generate_pm_ladder(candidate, qty_cap, market, tuning, gain_pct=gain_pct, side=side)
    if not legs:
        return None, {"code": "NO_LEGS", "note": f"[{candidate.symbol}] no legs"}
    plan = OrderPlan(
        plan_id=f"{candidate.symbol.lower()}-{side.value.lower()}-{generated_at.strftime('%Y%m%d%H%M%S')}",
        symbol=candidate.symbol,
        ticker_id=candidate.ticker_id,
        side=side,
        generated_at=generated_at,
        status="READY",
        rationale=f"{rationale_prefix}: signal={candidate.signal_strength:.3f}, {desc}",
        venue=venue,
        requested_budget=budget,
        requested_quantity=sum(leg.quantity for leg in legs),
        legs=legs,
        metadata={
            "anchor_date": candidate.anchor_date.isoformat() if candidate.anchor_date else None,
            "reverse_breach_day": candidate.reverse_breach_day,
            "target_return_pct": candidate.target_return_pct,
            "max_reverse_pct": candidate.max_reverse_pct,
            "signal_strength": candidate.signal_strength,
            "strategy_side_bias": candidate.side_bias.value if hasattr(candidate.side_bias, 'value') else str(candidate.side_bias),
            "quote_policy": quote_policy,
        },
    )
    return plan, None

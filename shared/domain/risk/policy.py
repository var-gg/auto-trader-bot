from __future__ import annotations

from typing import Iterable, List

from shared.domain.models import LadderLeg, Side
from shared.domain.execution.ladder import round_to_tick


def apply_buy_risk_multiplier(legs: Iterable[LadderLeg], *, current_price: float, market: str, multiplier: float, tag: str = "risk") -> List[LadderLeg]:
    if multiplier <= 1.0 or current_price <= 0:
        return list(legs)
    out: List[LadderLeg] = []
    for leg in legs:
        lp = float(leg.limit_price or 0.0)
        if lp <= 0:
            out.append(leg)
            continue
        base_disc = max(0.0, min(0.95, 1.0 - (lp / current_price)))
        new_disc = max(0.0, min(0.95, base_disc * multiplier))
        new_lp = round_to_tick(current_price * (1.0 - new_disc), market)
        out.append(LadderLeg(**{**leg.to_dict(), "limit_price": new_lp, "metadata": {**leg.metadata, f"{tag}_base_discount": round(base_disc, 6), f"{tag}_adjusted_discount": round(new_disc, 6)}}))
    return out


def apply_sell_risk_multiplier(legs: Iterable[LadderLeg], *, current_price: float, market: str, multiplier: float, tag: str = "risk") -> List[LadderLeg]:
    if multiplier <= 1.0 or current_price <= 0:
        return list(legs)
    out: List[LadderLeg] = []
    for leg in legs:
        lp = float(leg.limit_price or 0.0)
        if lp <= 0:
            out.append(leg)
            continue
        base_markup = max(0.0, min(1.5, (lp / current_price) - 1.0))
        new_markup = max(0.0, min(1.8, base_markup * multiplier))
        new_lp = round_to_tick(current_price * (1.0 + new_markup), market)
        out.append(LadderLeg(**{**leg.to_dict(), "limit_price": new_lp, "metadata": {**leg.metadata, f"{tag}_base_markup": round(base_markup, 6), f"{tag}_adjusted_markup": round(new_markup, 6)}}))
    return out


def reverse_breach_triggered(*, current_holding_days: int, reverse_breach_day: int | None) -> bool:
    if reverse_breach_day is None:
        return False
    return int(current_holding_days) >= int(reverse_breach_day)

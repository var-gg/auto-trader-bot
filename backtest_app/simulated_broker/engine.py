from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Tuple

from shared.domain.models import ExecutionVenue, FillOutcome, FillStatus, OrderPlan

from backtest_app.historical_data.models import HistoricalBar
from .models import SimulationRules


def _event_time(ts: str) -> datetime:
    if ts and "T" in ts:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return datetime.now(timezone.utc)


@dataclass
class SimulatedBroker:
    rules: SimulationRules

    def _apply_costs(self, price: float, side: str) -> float:
        slip = float(self.rules.slippage_bps) / 10000.0
        if side == "BUY":
            return price * (1.0 + slip)
        return price * (1.0 - slip)

    def _fill_decision(self, *, requested_price: float | None, bar: HistoricalBar) -> Tuple[bool, str]:
        if requested_price is None:
            return True, "MARKET_SIM"
        if bar.low <= requested_price <= bar.high:
            return True, "TOUCHED_IN_BAR"
        if self.rules.allow_gap_fill and requested_price >= bar.open and requested_price <= bar.high:
            return True, "GAP_FILL_OPEN"
        return False, "NO_TOUCH"

    def simulate_plan(self, plan: OrderPlan, bars: Iterable[HistoricalBar]) -> List[FillOutcome]:
        bars = list(bars)
        if not bars:
            return [
                FillOutcome(
                    plan_id=plan.plan_id,
                    leg_id=None,
                    symbol=plan.symbol,
                    side=plan.side,
                    fill_status=FillStatus.UNFILLED,
                    venue=ExecutionVenue.BACKTEST,
                    event_time=datetime.now(timezone.utc),
                    requested_quantity=plan.requested_quantity,
                    requested_price=None,
                    reject_code="NO_DATA",
                    reject_message="No historical bars for simulation",
                    metadata={"rules": self.rules.metadata},
                )
            ]

        fills: List[FillOutcome] = []
        for leg in plan.legs:
            leg_filled = False
            for bar in bars:
                fillable, reason = self._fill_decision(requested_price=leg.limit_price, bar=bar)
                if not fillable:
                    continue
                leg_filled = True
                qty = int(leg.quantity)
                status = FillStatus.FULL
                if self.rules.allow_partial_fills and qty > 1 and 0.0 < self.rules.partial_fill_ratio < 1.0:
                    partial_qty = max(1, int(qty * self.rules.partial_fill_ratio))
                    if partial_qty < qty:
                        qty = partial_qty
                        status = FillStatus.PARTIAL
                fill_price = bar.open if (self.rules.allow_gap_fill and leg.limit_price is not None and leg.limit_price >= bar.open and leg.limit_price <= bar.high) else (leg.limit_price or bar.open)
                fill_price = self._apply_costs(float(fill_price), plan.side.value)
                fills.append(
                    FillOutcome(
                        plan_id=plan.plan_id,
                        leg_id=leg.leg_id,
                        symbol=plan.symbol,
                        side=plan.side,
                        fill_status=status,
                        venue=ExecutionVenue.BACKTEST,
                        event_time=_event_time(bar.timestamp),
                        requested_quantity=leg.quantity,
                        filled_quantity=qty,
                        requested_price=leg.limit_price,
                        average_fill_price=fill_price,
                        slippage_bps=self.rules.slippage_bps,
                        metadata={
                            "fill_reason": reason,
                            "fee_bps": self.rules.fee_bps,
                            "session_cutoff_mode": self.rules.session_cutoff_mode,
                        },
                    )
                )
                if self.rules.session_cutoff_mode == "FIRST_BAR_ONLY":
                    break
                if status == FillStatus.FULL:
                    break
            if not leg_filled:
                last_bar = bars[-1]
                fills.append(
                    FillOutcome(
                        plan_id=plan.plan_id,
                        leg_id=leg.leg_id,
                        symbol=plan.symbol,
                        side=plan.side,
                        fill_status=FillStatus.UNFILLED,
                        venue=ExecutionVenue.BACKTEST,
                        event_time=_event_time(last_bar.timestamp),
                        requested_quantity=leg.quantity,
                        filled_quantity=0,
                        requested_price=leg.limit_price,
                        reject_code="NO_TOUCH",
                        reject_message="Limit not reached before session cutoff",
                        metadata={"session_cutoff_mode": self.rules.session_cutoff_mode},
                    )
                )
        return fills

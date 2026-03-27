from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from backtest_app.historical_data.models import HistoricalBar
from shared.domain.models import FillOutcome, OrderPlan


class OrderAdapter(Protocol):
    def place_orders(self, plans: Sequence[OrderPlan]) -> list[dict]: ...


class StateAdapter(Protocol):
    def get_positions(self) -> list[dict]: ...
    def get_cash(self) -> float: ...
    def get_bars(self, *, symbols: Sequence[str], end_date: str, lookback_bars: int) -> dict[str, list[HistoricalBar]]: ...
    def get_macro(self, *, day: str) -> dict[str, float]: ...
    def get_sector_map(self, *, symbols: Sequence[str]) -> dict[str, str]: ...


class BrokerAdapter(Protocol):
    def submit(self, plan: OrderPlan) -> dict: ...
    def cancel(self, order_id: str) -> dict: ...
    def collect_fills(self, plans: Sequence[OrderPlan]) -> list[FillOutcome]: ...


class CalendarAdapter(Protocol):
    def is_open(self, market: str, day: str) -> bool: ...
    def next_session(self, market: str, day: str) -> str: ...


@dataclass
class LiveRuntimeAdapters:
    order_adapter: OrderAdapter
    state_adapter: StateAdapter
    broker_adapter: BrokerAdapter
    calendar_adapter: CalendarAdapter

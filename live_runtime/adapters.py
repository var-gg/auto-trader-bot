from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from shared.domain.models import OrderPlan


class OrderAdapter(Protocol):
    def place_orders(self, plans: Sequence[OrderPlan]) -> list[dict]: ...


class StateAdapter(Protocol):
    def get_positions(self) -> list[dict]: ...
    def get_cash(self) -> float: ...


class BrokerAdapter(Protocol):
    def submit(self, plan: OrderPlan) -> dict: ...
    def cancel(self, order_id: str) -> dict: ...


class CalendarAdapter(Protocol):
    def is_open(self, market: str, day: str) -> bool: ...
    def next_session(self, market: str, day: str) -> str: ...


@dataclass
class LiveRuntimeAdapters:
    order_adapter: OrderAdapter
    state_adapter: StateAdapter
    broker_adapter: BrokerAdapter
    calendar_adapter: CalendarAdapter

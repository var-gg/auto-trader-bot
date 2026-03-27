from __future__ import annotations

from dataclasses import dataclass

from .adapters import LiveRuntimeAdapters


@dataclass
class LiveRuntime:
    adapters: LiveRuntimeAdapters

    def run(self, *, market: str, day: str) -> dict:
        return {
            "market": market,
            "day": day,
            "market_open": self.adapters.calendar_adapter.is_open(market, day),
            "cash": self.adapters.state_adapter.get_cash(),
            "positions": self.adapters.state_adapter.get_positions(),
        }


def run_live_runtime(adapters: LiveRuntimeAdapters, *, market: str, day: str) -> dict:
    return LiveRuntime(adapters=adapters).run(market=market, day=day)

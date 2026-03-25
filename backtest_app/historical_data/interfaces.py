from __future__ import annotations

from typing import Protocol

from .models import HistoricalSlice


class HistoricalDataLoader(Protocol):
    def load(self, source: str) -> HistoricalSlice: ...

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from shared.domain.models import MarketSnapshot, SignalCandidate

from .features import compute_bar_features, compute_external_vector
from .models import HistoricalBar, HistoricalSlice


class JsonHistoricalDataLoader:
    """Fixture/file based loader for backtest runtime.

    No live DB/session dependency.
    Supplies canonical bars/candidates plus derived feature vectors.
    """

    def load(self, path: str) -> HistoricalSlice:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        snapshot = MarketSnapshot.from_dict(raw["market_snapshot"])
        bars_by_symbol = {
            symbol: [HistoricalBar(**bar) for bar in bars]
            for symbol, bars in raw.get("bars_by_symbol", {}).items()
        }
        features_by_symbol: Dict[str, Dict[str, float]] = {
            symbol: compute_bar_features(bars)
            for symbol, bars in bars_by_symbol.items()
        }
        external_payload = raw.get("external_factors", {})
        external_vector = compute_external_vector(external_payload) if external_payload else []
        candidates = []
        for item in raw.get("candidates", []):
            symbol = item.get("symbol")
            provenance = dict(item.get("provenance", {}))
            provenance["derived_bar_features"] = features_by_symbol.get(symbol, {})
            provenance["external_vector"] = external_vector
            item = {**item, "provenance": provenance}
            candidates.append(SignalCandidate.from_dict(item))
        return HistoricalSlice(
            market_snapshot=snapshot,
            bars_by_symbol=bars_by_symbol,
            candidates=candidates,
            metadata={**raw.get("metadata", {}), "external_vector_dim": str(len(external_vector))},
        )

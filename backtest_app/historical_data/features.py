from __future__ import annotations

from typing import Dict, Iterable, List

from .models import HistoricalBar


def compute_bar_features(bars: Iterable[HistoricalBar]) -> Dict[str, float]:
    bars = list(bars)
    if not bars:
        return {"return_1": 0.0, "range_pct": 0.0, "volume_mean": 0.0}
    first = bars[0]
    last = bars[-1]
    return_1 = (last.close - first.open) / first.open if first.open else 0.0
    highs = max(bar.high for bar in bars)
    lows = min(bar.low for bar in bars)
    closes = last.close or 0.0
    range_pct = ((highs - lows) / closes) if closes else 0.0
    volume_mean = sum(bar.volume for bar in bars) / len(bars)
    return {
        "return_1": float(return_1),
        "range_pct": float(range_pct),
        "volume_mean": float(volume_mean),
    }


def compute_external_vector(payload: Dict[str, float]) -> List[float]:
    ordered_keys = sorted(payload.keys())
    return [float(payload[k]) for k in ordered_keys]

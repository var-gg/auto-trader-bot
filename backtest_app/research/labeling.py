from __future__ import annotations

from dataclasses import dataclass, field
from math import exp
from typing import List, Optional

from backtest_app.historical_data.models import HistoricalBar


@dataclass(frozen=True)
class EventLabelingConfig:
    target_return_pct: float
    stop_return_pct: float
    horizon_days: int
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    flat_return_band_pct: float = 0.005
    earnings_proximity_days: int = 0
    event_blackout: bool = False


@dataclass(frozen=True)
class EventLabelResult:
    label: str
    days_to_hit: Optional[int]
    target_hit_day: Optional[int]
    stop_hit_day: Optional[int]
    horizon_close_day: Optional[int]
    mae_pct: float
    mfe_pct: float
    gross_return_pct: float
    realized_return_pct: float
    after_cost_return_pct: float
    quality_score: float
    ambiguous: bool = False
    no_trade: bool = False
    diagnostics: dict = field(default_factory=dict)

    @property
    def path_label(self) -> str:
        return self.label

    @property
    def side_labels(self) -> dict:
        buy = "NO_TRADE"
        sell = "NO_TRADE"
        if self.label in {"UP_FIRST", "HORIZON_UP"}:
            buy = self.label
        elif self.label in {"DOWN_FIRST", "HORIZON_DOWN"}:
            sell = self.label
        return {"BUY": buy, "SELL": sell}


def _safe_pct(x: float) -> float:
    return max(-1.0, min(1.0, float(x)))


def _quality_score(*, realized_return_pct: float, after_cost_return_pct: float, hit_speed: float, mfe: float, mae: float) -> float:
    pnl_component = 1.0 / (1.0 + exp(-8.0 * after_cost_return_pct))
    excursion_raw = 0.6 * max(0.0, mfe) + 0.4 * max(0.0, 1.0 + mae)
    excursion_component = max(0.0, min(1.0, excursion_raw))
    realized_component = 1.0 / (1.0 + exp(-6.0 * realized_return_pct))
    score = 0.35 * pnl_component + 0.25 * realized_component + 0.20 * excursion_component + 0.20 * hit_speed
    return max(0.0, min(1.0, score))


def label_event_window(bars: List[HistoricalBar], config: EventLabelingConfig) -> EventLabelResult:
    if not bars:
        return EventLabelResult(
            label="NO_TRADE",
            days_to_hit=None,
            target_hit_day=None,
            stop_hit_day=None,
            horizon_close_day=None,
            mae_pct=0.0,
            mfe_pct=0.0,
            gross_return_pct=0.0,
            realized_return_pct=0.0,
            after_cost_return_pct=0.0,
            quality_score=0.0,
            no_trade=True,
            diagnostics={"reason": "missing_bars"},
        )

    if config.event_blackout:
        return EventLabelResult(
            label="NO_TRADE",
            days_to_hit=None,
            target_hit_day=None,
            stop_hit_day=None,
            horizon_close_day=None,
            mae_pct=0.0,
            mfe_pct=0.0,
            gross_return_pct=0.0,
            realized_return_pct=0.0,
            after_cost_return_pct=0.0,
            quality_score=0.0,
            no_trade=True,
            diagnostics={"reason": "event_blackout", "earnings_proximity_days": config.earnings_proximity_days},
        )

    entry = float(bars[0].open)
    horizon = max(1, int(config.horizon_days))
    window = bars[:horizon]
    if entry <= 0.0:
        return EventLabelResult(
            label="NO_TRADE",
            days_to_hit=None,
            target_hit_day=None,
            stop_hit_day=None,
            horizon_close_day=None,
            mae_pct=0.0,
            mfe_pct=0.0,
            gross_return_pct=0.0,
            realized_return_pct=0.0,
            after_cost_return_pct=0.0,
            quality_score=0.0,
            no_trade=True,
            diagnostics={"reason": "invalid_entry"},
        )

    target_level = entry * (1.0 + float(config.target_return_pct))
    stop_level = entry * (1.0 - float(config.stop_return_pct))
    target_hit_day = None
    stop_hit_day = None
    mfe = 0.0
    mae = 0.0
    ambiguous = False

    for idx, bar in enumerate(window, start=1):
        high_ret = (float(bar.high) / entry) - 1.0
        low_ret = (float(bar.low) / entry) - 1.0
        mfe = max(mfe, high_ret)
        mae = min(mae, low_ret)
        target_hit = float(bar.high) >= target_level
        stop_hit = float(bar.low) <= stop_level
        if target_hit and target_hit_day is None:
            target_hit_day = idx
        if stop_hit and stop_hit_day is None:
            stop_hit_day = idx
        if target_hit and stop_hit and target_hit_day == stop_hit_day:
            ambiguous = True
            break
        if target_hit_day is not None or stop_hit_day is not None:
            break

    last_bar = window[-1]
    gross_return = (float(last_bar.close) / entry) - 1.0
    realized_return = gross_return
    cost_pct = (float(config.fee_bps) + float(config.slippage_bps)) / 10000.0
    after_cost_return = realized_return - cost_pct

    if ambiguous:
        label = "AMBIGUOUS"
        days_to_hit = target_hit_day or stop_hit_day
    elif target_hit_day is not None and (stop_hit_day is None or target_hit_day < stop_hit_day):
        label = "UP_FIRST"
        days_to_hit = target_hit_day
    elif stop_hit_day is not None and (target_hit_day is None or stop_hit_day < target_hit_day):
        label = "DOWN_FIRST"
        days_to_hit = stop_hit_day
    else:
        days_to_hit = None
        if abs(after_cost_return) <= float(config.flat_return_band_pct):
            label = "FLAT"
        elif after_cost_return > 0:
            label = "HORIZON_UP"
        elif after_cost_return < 0:
            label = "HORIZON_DOWN"
        else:
            label = "NO_TRADE"

    no_trade = label in {"FLAT", "NO_TRADE"}
    hit_speed = 0.0 if not days_to_hit else 1.0 - min(days_to_hit - 1, horizon - 1) / max(horizon - 1, 1)
    quality_score = _quality_score(realized_return_pct=realized_return, after_cost_return_pct=after_cost_return, hit_speed=hit_speed, mfe=mfe, mae=mae)

    return EventLabelResult(
        label=label,
        days_to_hit=days_to_hit,
        target_hit_day=target_hit_day,
        stop_hit_day=stop_hit_day,
        horizon_close_day=len(window),
        mae_pct=_safe_pct(mae),
        mfe_pct=_safe_pct(mfe),
        gross_return_pct=_safe_pct(gross_return),
        realized_return_pct=_safe_pct(realized_return),
        after_cost_return_pct=_safe_pct(after_cost_return),
        quality_score=quality_score,
        ambiguous=ambiguous,
        no_trade=no_trade,
        diagnostics={
            "entry_price": entry,
            "target_level": target_level,
            "stop_level": stop_level,
            "observed_days": len(window),
            "path_label": label,
            "side_labels": {"BUY": "NO_TRADE" if label in {"DOWN_FIRST", "HORIZON_DOWN", "FLAT", "NO_TRADE", "AMBIGUOUS"} else label, "SELL": "NO_TRADE" if label in {"UP_FIRST", "HORIZON_UP", "FLAT", "NO_TRADE", "AMBIGUOUS"} else label},
            "earnings_proximity_days": config.earnings_proximity_days,
            "event_blackout": config.event_blackout,
        },
    )

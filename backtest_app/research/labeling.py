from __future__ import annotations

from dataclasses import dataclass, field
from math import exp
from typing import Dict, List, Optional

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
class SideOutcome:
    side: str
    first_touch_label: str
    target_hit_day: Optional[int]
    stop_hit_day: Optional[int]
    horizon_return_pct: float
    after_cost_return_pct: float
    mae_pct: float
    mfe_pct: float
    close_return_d2_pct: float = 0.0
    close_return_d3_pct: float = 0.0
    resolved_by_d2: bool = False
    resolved_by_d3: bool = False
    ambiguous: bool = False
    flat: bool = False
    no_trade: bool = False


@dataclass(frozen=True)
class EventOutcomeRecord:
    schema_version: str
    path_label: str
    days_to_hit: Optional[int]
    horizon_close_day: Optional[int]
    gross_return_pct: float
    realized_return_pct: float
    quality_score: float
    ambiguous: bool = False
    flat: bool = False
    no_trade: bool = False
    buy: SideOutcome | None = None
    sell: SideOutcome | None = None
    path_summary: dict = field(default_factory=dict)
    diagnostics: dict = field(default_factory=dict)

    @property
    def side_payload(self) -> Dict[str, dict]:
        return {
            "BUY": as_side_payload(self.buy),
            "SELL": as_side_payload(self.sell),
        }


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
        return dict(self.diagnostics.get("side_labels", {}))


def as_side_payload(value: SideOutcome | None) -> dict:
    if value is None:
        return {}
    return {
        "side": value.side,
        "first_touch_label": value.first_touch_label,
        "target_hit_day": value.target_hit_day,
        "stop_hit_day": value.stop_hit_day,
        "horizon_return_pct": value.horizon_return_pct,
        "after_cost_return_pct": value.after_cost_return_pct,
        "mae_pct": value.mae_pct,
        "mfe_pct": value.mfe_pct,
        "close_return_d2_pct": value.close_return_d2_pct,
        "close_return_d3_pct": value.close_return_d3_pct,
        "resolved_by_d2": value.resolved_by_d2,
        "resolved_by_d3": value.resolved_by_d3,
        "ambiguous": value.ambiguous,
        "flat": value.flat,
        "no_trade": value.no_trade,
    }


def _safe_pct(x: float) -> float:
    return max(-1.0, min(1.0, float(x)))


def _quality_score(*, realized_return_pct: float, after_cost_return_pct: float, hit_speed: float, mfe: float, mae: float) -> float:
    pnl_component = 1.0 / (1.0 + exp(-8.0 * after_cost_return_pct))
    excursion_raw = 0.6 * max(0.0, mfe) + 0.4 * max(0.0, 1.0 + mae)
    excursion_component = max(0.0, min(1.0, excursion_raw))
    realized_component = 1.0 / (1.0 + exp(-6.0 * realized_return_pct))
    score = 0.35 * pnl_component + 0.25 * realized_component + 0.20 * excursion_component + 0.20 * hit_speed
    return max(0.0, min(1.0, score))


def _empty_event(reason: str) -> EventOutcomeRecord:
    zero = SideOutcome(side="BUY", first_touch_label="NO_TRADE", target_hit_day=None, stop_hit_day=None, horizon_return_pct=0.0, after_cost_return_pct=0.0, mae_pct=0.0, mfe_pct=0.0, no_trade=True)
    zero_sell = SideOutcome(side="SELL", first_touch_label="NO_TRADE", target_hit_day=None, stop_hit_day=None, horizon_return_pct=0.0, after_cost_return_pct=0.0, mae_pct=0.0, mfe_pct=0.0, no_trade=True)
    return EventOutcomeRecord(schema_version="event_outcome_v1", path_label="NO_TRADE", days_to_hit=None, horizon_close_day=None, gross_return_pct=0.0, realized_return_pct=0.0, quality_score=0.0, no_trade=True, buy=zero, sell=zero_sell, path_summary={"reason": reason}, diagnostics={"reason": reason, "side_labels": {"BUY": "NO_TRADE", "SELL": "NO_TRADE"}})


def build_event_outcome_record(bars: List[HistoricalBar], config: EventLabelingConfig) -> EventOutcomeRecord:
    if not bars:
        return _empty_event("missing_bars")
    if config.event_blackout:
        event = _empty_event("event_blackout")
        return EventOutcomeRecord(**{**event.__dict__, "diagnostics": {**event.diagnostics, "earnings_proximity_days": config.earnings_proximity_days, "event_blackout": True}})

    entry = float(bars[0].open)
    horizon = max(1, int(config.horizon_days))
    window = bars[:horizon]
    if entry <= 0.0:
        return _empty_event("invalid_entry")

    buy_target = entry * (1.0 + float(config.target_return_pct))
    buy_stop = entry * (1.0 - float(config.stop_return_pct))
    sell_target = entry * (1.0 - float(config.target_return_pct))
    sell_stop = entry * (1.0 + float(config.stop_return_pct))
    buy_target_hit_day = None
    buy_stop_hit_day = None
    sell_target_hit_day = None
    sell_stop_hit_day = None
    buy_mfe = 0.0
    buy_mae = 0.0
    sell_mfe = 0.0
    sell_mae = 0.0
    path_ambiguous = False

    for idx, bar in enumerate(window, start=1):
        high_ret = (float(bar.high) / entry) - 1.0
        low_ret = (float(bar.low) / entry) - 1.0
        buy_mfe = max(buy_mfe, high_ret)
        buy_mae = min(buy_mae, low_ret)
        sell_mfe = max(sell_mfe, -low_ret)
        sell_mae = min(sell_mae, -high_ret)

        buy_target_hit = float(bar.high) >= buy_target
        buy_stop_hit = float(bar.low) <= buy_stop
        sell_target_hit = float(bar.low) <= sell_target
        sell_stop_hit = float(bar.high) >= sell_stop
        if buy_target_hit and buy_target_hit_day is None:
            buy_target_hit_day = idx
        if buy_stop_hit and buy_stop_hit_day is None:
            buy_stop_hit_day = idx
        if sell_target_hit and sell_target_hit_day is None:
            sell_target_hit_day = idx
        if sell_stop_hit and sell_stop_hit_day is None:
            sell_stop_hit_day = idx
        if (buy_target_hit and buy_stop_hit) or (sell_target_hit and sell_stop_hit):
            path_ambiguous = True

    last_bar = window[-1]
    gross_return = (float(last_bar.close) / entry) - 1.0
    cost_pct = (float(config.fee_bps) + float(config.slippage_bps)) / 10000.0
    buy_after_cost = gross_return - cost_pct
    sell_horizon_return = -gross_return
    sell_after_cost = sell_horizon_return - cost_pct

    def _close_return(day_index: int, *, sign: float) -> float:
        if not window:
            return 0.0
        idx = min(max(day_index - 1, 0), len(window) - 1)
        close = float(window[idx].close)
        return _safe_pct(sign * (((close / entry) - 1.0) if entry > 0.0 else 0.0))

    def side_label(target_hit_day: Optional[int], stop_hit_day: Optional[int], after_cost_return: float, ambiguous: bool, positive_label: str, negative_label: str) -> tuple[str, Optional[int], bool, bool, bool]:
        if ambiguous and target_hit_day is not None and stop_hit_day is not None and target_hit_day == stop_hit_day:
            return "AMBIGUOUS", target_hit_day, True, False, False
        if target_hit_day is not None and (stop_hit_day is None or target_hit_day < stop_hit_day):
            return positive_label, target_hit_day, False, False, False
        if stop_hit_day is not None and (target_hit_day is None or stop_hit_day < target_hit_day):
            return negative_label, stop_hit_day, False, False, False
        if abs(after_cost_return) <= float(config.flat_return_band_pct):
            return "FLAT", None, False, True, False
        if after_cost_return > 0:
            return "HORIZON_UP", None, False, False, False
        if after_cost_return < 0:
            return "HORIZON_DOWN", None, False, False, False
        return "NO_TRADE", None, False, False, True

    buy_label, buy_days, buy_ambiguous, buy_flat, buy_no_trade = side_label(buy_target_hit_day, buy_stop_hit_day, buy_after_cost, path_ambiguous, "UP_FIRST", "DOWN_FIRST")
    sell_label, sell_days, sell_ambiguous, sell_flat, sell_no_trade = side_label(sell_target_hit_day, sell_stop_hit_day, sell_after_cost, path_ambiguous, "UP_FIRST", "DOWN_FIRST")

    buy = SideOutcome(
        side="BUY",
        first_touch_label=buy_label,
        target_hit_day=buy_target_hit_day,
        stop_hit_day=buy_stop_hit_day,
        horizon_return_pct=_safe_pct(gross_return),
        after_cost_return_pct=_safe_pct(buy_after_cost),
        mae_pct=_safe_pct(buy_mae),
        mfe_pct=_safe_pct(buy_mfe),
        close_return_d2_pct=_close_return(2, sign=1.0),
        close_return_d3_pct=_close_return(3, sign=1.0),
        resolved_by_d2=bool((buy_target_hit_day and buy_target_hit_day <= 2) or (buy_stop_hit_day and buy_stop_hit_day <= 2)),
        resolved_by_d3=bool((buy_target_hit_day and buy_target_hit_day <= 3) or (buy_stop_hit_day and buy_stop_hit_day <= 3)),
        ambiguous=buy_ambiguous,
        flat=buy_flat,
        no_trade=buy_no_trade,
    )
    sell = SideOutcome(
        side="SELL",
        first_touch_label=sell_label,
        target_hit_day=sell_target_hit_day,
        stop_hit_day=sell_stop_hit_day,
        horizon_return_pct=_safe_pct(sell_horizon_return),
        after_cost_return_pct=_safe_pct(sell_after_cost),
        mae_pct=_safe_pct(sell_mae),
        mfe_pct=_safe_pct(sell_mfe),
        close_return_d2_pct=_close_return(2, sign=-1.0),
        close_return_d3_pct=_close_return(3, sign=-1.0),
        resolved_by_d2=bool((sell_target_hit_day and sell_target_hit_day <= 2) or (sell_stop_hit_day and sell_stop_hit_day <= 2)),
        resolved_by_d3=bool((sell_target_hit_day and sell_target_hit_day <= 3) or (sell_stop_hit_day and sell_stop_hit_day <= 3)),
        ambiguous=sell_ambiguous,
        flat=sell_flat,
        no_trade=sell_no_trade,
    )

    path_label = "AMBIGUOUS" if path_ambiguous else buy_label
    days_to_hit = buy_days
    path_flat = buy_flat and sell_flat
    no_trade = buy_no_trade and sell_no_trade
    hit_speed = 0.0 if not days_to_hit else 1.0 - min(days_to_hit - 1, horizon - 1) / max(horizon - 1, 1)
    quality_score = _quality_score(realized_return_pct=gross_return, after_cost_return_pct=buy_after_cost, hit_speed=hit_speed, mfe=buy_mfe, mae=buy_mae)

    return EventOutcomeRecord(
        schema_version="event_outcome_v1",
        path_label=path_label,
        days_to_hit=days_to_hit,
        horizon_close_day=len(window),
        gross_return_pct=_safe_pct(gross_return),
        realized_return_pct=_safe_pct(gross_return),
        quality_score=quality_score,
        ambiguous=path_ambiguous,
        flat=path_flat,
        no_trade=no_trade,
        buy=buy,
        sell=sell,
        path_summary={"entry_price": entry, "target_level_buy": buy_target, "stop_level_buy": buy_stop, "target_level_sell": sell_target, "stop_level_sell": sell_stop, "observed_days": len(window)},
        diagnostics={"path_label": path_label, "side_labels": {"BUY": buy.first_touch_label, "SELL": sell.first_touch_label}, "earnings_proximity_days": config.earnings_proximity_days, "event_blackout": config.event_blackout},
    )


def label_event_window(bars: List[HistoricalBar], config: EventLabelingConfig) -> EventLabelResult:
    event = build_event_outcome_record(bars, config)
    buy = event.buy or SideOutcome(side="BUY", first_touch_label="NO_TRADE", target_hit_day=None, stop_hit_day=None, horizon_return_pct=0.0, after_cost_return_pct=0.0, mae_pct=0.0, mfe_pct=0.0, no_trade=True)
    return EventLabelResult(
        label=event.path_label,
        days_to_hit=event.days_to_hit,
        target_hit_day=buy.target_hit_day,
        stop_hit_day=buy.stop_hit_day,
        horizon_close_day=event.horizon_close_day,
        mae_pct=buy.mae_pct,
        mfe_pct=buy.mfe_pct,
        gross_return_pct=event.gross_return_pct,
        realized_return_pct=event.realized_return_pct,
        after_cost_return_pct=buy.after_cost_return_pct,
        quality_score=event.quality_score,
        ambiguous=event.ambiguous,
        no_trade=event.no_trade,
        diagnostics={**event.diagnostics, "path_summary": event.path_summary, "side_payload": event.side_payload},
    )

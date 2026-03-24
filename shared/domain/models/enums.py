from __future__ import annotations

from enum import Enum


class MarketCode(str, Enum):
    KR = "KR"
    US = "US"
    GLOBAL = "GLOBAL"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    AFTER_HOURS_06 = "AFTER_HOURS_06"
    AFTER_HOURS_07 = "AFTER_HOURS_07"


class IntentStatus(str, Enum):
    PROPOSED = "PROPOSED"
    READY = "READY"
    SKIPPED = "SKIPPED"
    BLOCKED = "BLOCKED"


class FillStatus(str, Enum):
    UNFILLED = "UNFILLED"
    PARTIAL = "PARTIAL"
    FULL = "FULL"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class OutcomeLabel(str, Enum):
    WIN = "WIN"
    LOSS = "LOSS"
    FLAT = "FLAT"
    UP_FIRST = "UP_FIRST"
    DOWN_FIRST = "DOWN_FIRST"
    TIMEOUT = "TIMEOUT"
    UNKNOWN = "UNKNOWN"


class ExecutionVenue(str, Enum):
    LIVE = "LIVE"
    BACKTEST = "BACKTEST"
    PAPER = "PAPER"


class RiskBias(str, Enum):
    RISK_OFF = "RISK_OFF"
    NEUTRAL = "NEUTRAL"
    RISK_ON = "RISK_ON"

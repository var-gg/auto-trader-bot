from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .models import HistoricalBar, SymbolSessionMetadata


@dataclass(frozen=True)
class ExchangeSessionConfig:
    exchange_code: str
    country_code: str
    exchange_tz: str
    session_close_local_time: str


EXCHANGE_SESSION_CONFIGS: dict[str, ExchangeSessionConfig] = {
    "KOE": ExchangeSessionConfig(
        exchange_code="KOE",
        country_code="KR",
        exchange_tz="Asia/Seoul",
        session_close_local_time="15:30",
    ),
    "NYQ": ExchangeSessionConfig(
        exchange_code="NYQ",
        country_code="US",
        exchange_tz="America/New_York",
        session_close_local_time="16:00",
    ),
    "NMS": ExchangeSessionConfig(
        exchange_code="NMS",
        country_code="US",
        exchange_tz="America/New_York",
        session_close_local_time="16:00",
    ),
    "NGM": ExchangeSessionConfig(
        exchange_code="NGM",
        country_code="US",
        exchange_tz="America/New_York",
        session_close_local_time="16:00",
    ),
    "BTS": ExchangeSessionConfig(
        exchange_code="BTS",
        country_code="US",
        exchange_tz="America/New_York",
        session_close_local_time="16:00",
    ),
}


def resolve_exchange_session_config(exchange_code: str | None) -> ExchangeSessionConfig | None:
    if not exchange_code:
        return None
    return EXCHANGE_SESSION_CONFIGS.get(str(exchange_code).strip().upper())


def build_symbol_session_metadata(*, symbol: str, exchange_code: str | None, country_code: str | None = None) -> SymbolSessionMetadata | None:
    cfg = resolve_exchange_session_config(exchange_code)
    if cfg is None:
        return None
    return SymbolSessionMetadata(
        symbol=str(symbol),
        exchange_code=cfg.exchange_code,
        country_code=str(country_code or cfg.country_code),
        exchange_tz=cfg.exchange_tz,
        session_close_local_time=cfg.session_close_local_time,
    )


def _session_date_from_timestamp(timestamp: str | datetime | date) -> date:
    if isinstance(timestamp, datetime):
        return timestamp.date()
    if isinstance(timestamp, date):
        return timestamp
    raw = str(timestamp)
    return datetime.fromisoformat(raw[:10]).date()


def derive_session_anchor_from_date(*, session_date_local: str | date, session_metadata: SymbolSessionMetadata) -> dict[str, Any]:
    local_date = session_date_local if isinstance(session_date_local, date) else datetime.fromisoformat(str(session_date_local)[:10]).date()
    close_time = time.fromisoformat(str(session_metadata.session_close_local_time))
    local_dt = datetime.combine(local_date, close_time, tzinfo=ZoneInfo(session_metadata.exchange_tz))
    utc_dt = local_dt.astimezone(timezone.utc)
    return {
        "exchange_code": session_metadata.exchange_code,
        "country_code": session_metadata.country_code,
        "exchange_tz": session_metadata.exchange_tz,
        "session_date_local": local_date.isoformat(),
        "session_close_ts_local": local_dt.isoformat(),
        "session_close_ts_utc": utc_dt.isoformat(),
        "feature_anchor_ts_utc": utc_dt.isoformat(),
    }


def derive_session_anchor_for_bar(*, bar: HistoricalBar, session_metadata: SymbolSessionMetadata) -> dict[str, Any]:
    return derive_session_anchor_from_date(
        session_date_local=_session_date_from_timestamp(bar.timestamp),
        session_metadata=session_metadata,
    )


def session_anchor_timestamp_utc(*, session_date_local: str | date, session_metadata: SymbolSessionMetadata) -> datetime:
    anchor = derive_session_anchor_from_date(session_date_local=session_date_local, session_metadata=session_metadata)
    return datetime.fromisoformat(str(anchor["feature_anchor_ts_utc"]))


def session_metadata_to_dict(session_metadata: SymbolSessionMetadata | Mapping[str, Any] | None) -> dict[str, Any]:
    if session_metadata is None:
        return {}
    if isinstance(session_metadata, SymbolSessionMetadata):
        return asdict(session_metadata)
    return {
        "symbol": str(session_metadata.get("symbol") or ""),
        "exchange_code": str(session_metadata.get("exchange_code") or ""),
        "country_code": session_metadata.get("country_code"),
        "exchange_tz": str(session_metadata.get("exchange_tz") or ""),
        "session_close_local_time": str(session_metadata.get("session_close_local_time") or ""),
    }

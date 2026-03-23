from __future__ import annotations
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
ET  = ZoneInfo("America/New_York")

def market_now_kst(market: str) -> datetime:
    return datetime.now(tz=KST)

def get_session_times_kst(market: str, ref: datetime | None = None) -> tuple[datetime, datetime]:
    """
    오늘(현지 기준) 정규장 세션의 KST 변환된 (start, close) 반환.
    - KR: 09:00~15:30 Asia/Seoul
    - US: 09:30~16:00 America/New_York (DST 자동)
    """
    now_kst = ref.astimezone(KST) if ref else datetime.now(KST)

    if market == "KR":
        start = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
        close = now_kst.replace(hour=15, minute=30, second=0, microsecond=0)
        return start, close

    # US: 현지 날짜 기준 세션을 ET에서 구성 후 KST로 변환
    now_et = now_kst.astimezone(ET)
    start_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_et = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    # ET→KST 변환
    return start_et.astimezone(KST), close_et.astimezone(KST)

def is_near_close(market: str, minutes: int, now_kst: datetime | None = None) -> bool:
    now = now_kst or datetime.now(KST)
    _, close = get_session_times_kst(market, now)
    return 0 <= (close - now).total_seconds() <= minutes * 60

def seconds_to_close(market: str, now_kst: datetime | None = None) -> int:
    now = now_kst or datetime.now(KST)
    _, close = get_session_times_kst(market, now)
    return int((close - now).total_seconds())

def is_within_first_hour(market: str, now_kst: datetime | None = None) -> bool:
    """개장 후 1시간 내인지 체크"""
    now = now_kst or datetime.now(KST)
    start, _ = get_session_times_kst(market, now)
    elapsed = (now - start).total_seconds()
    return 0 <= elapsed <= 3600  # 1시간 = 3600초

def is_before_last_hour(market: str, now_kst: datetime | None = None) -> bool:
    """폐장 전 1시간 내인지 체크"""
    now = now_kst or datetime.now(KST)
    _, close = get_session_times_kst(market, now)
    remaining = (close - now).total_seconds()
    return 0 <= remaining <= 3600  # 1시간 = 3600초

def is_after_market_close(market: str, now_kst: datetime | None = None) -> bool:
    """폐장 후인지 체크"""
    now = now_kst or datetime.now(KST)
    _, close = get_session_times_kst(market, now)
    return now > close

def is_kr_after_hours_regular(now_kst: datetime | None = None) -> bool:
    """국장 장후 시간외 거래 (15:30~16:00, 06)"""
    now = now_kst or datetime.now(KST)
    after_close_start = now.replace(hour=15, minute=30, second=0, microsecond=0)
    after_close_end = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return after_close_start <= now < after_close_end

def is_kr_after_hours_single(now_kst: datetime | None = None) -> bool:
    """국장 시간외 단일가 거래 (16:00~18:00, 07)"""
    now = now_kst or datetime.now(KST)
    single_start = now.replace(hour=16, minute=0, second=0, microsecond=0)
    single_end = now.replace(hour=18, minute=0, second=0, microsecond=0)
    return single_start <= now < single_end

def is_us_after_market(now_kst: datetime | None = None) -> bool:
    """미장 애프터마켓 (16:00~20:00 ET)"""
    now = now_kst or datetime.now(KST)
    now_et = now.astimezone(ET)
    
    # 미장 정규 종료: 16:00 ET
    # 애프터마켓: 16:00~20:00 ET
    regular_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    after_close = now_et.replace(hour=20, minute=0, second=0, microsecond=0)
    
    return regular_close <= now_et < after_close
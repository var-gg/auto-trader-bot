# app/shared/models/market_holiday.py
from datetime import date
from sqlalchemy import Column, Integer, String, Date, Boolean, UniqueConstraint
from app.core.db import Base
from app.core.config import DB_SCHEMA

class MarketHoliday(Base):
    """
    마켓 휴일 정보를 저장하는 테이블
    - Finnhub Market Holiday API를 통해 수집
    - 거래소별 휴일 정보를 관리
    """
    __tablename__ = "market_holiday"
    __table_args__ = (
        UniqueConstraint("exchange", "at_date", name="uq_market_holiday_exchange_date"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment="휴일 ID")
    
    # 거래소 정보 (API 응답의 exchange 필드)
    exchange = Column(String(10), nullable=False, index=True, comment="거래소 코드 (예: US, KR, JP)")
    timezone = Column(String(50), nullable=True, comment="거래소 타임존 (예: America/New_York)")
    
    # 휴일 정보 (API 응답의 data 배열 필드들)
    at_date = Column(Date, nullable=False, index=True, comment="휴일 날짜 (atDate)")
    event_name = Column(String(100), nullable=False, comment="휴일명 (eventName)")
    trading_hour = Column(String(20), nullable=True, comment="거래 시간 (tradingHour, 빈값이면 완전휴일)")
    
    # 계산된 필드
    is_open = Column(Boolean, nullable=False, default=False, comment="해당 날짜 거래소 개장 여부 (tradingHour 기반)")

    def __repr__(self):
        return f"<MarketHoliday(exchange={self.exchange}, date={self.at_date}, event={self.event_name})>"

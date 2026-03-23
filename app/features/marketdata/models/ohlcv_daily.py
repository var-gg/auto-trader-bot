# app/features/marketdata/models/ohlcv_daily.py
from sqlalchemy import (
    Column, BigInteger, Date, DateTime, Float, String, Boolean,
    UniqueConstraint, JSON
)
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

class OhlcvDaily(Base):
    __tablename__ = "ohlcv_daily"
    __table_args__ = (
        UniqueConstraint("ticker_id", "trade_date", name="uq_ohlcv_daily_ticker_date"),
        {"schema": DB_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="일봉 ID")
    ticker_id = Column(BigInteger, index=True, nullable=False, comment="티커 ID")  # shared.ticker.id (논리 FK)
    trade_date = Column(Date, index=True, nullable=False, comment="거래일")

    open = Column(Float, comment="시가")
    high = Column(Float, comment="고가")
    low = Column(Float, comment="저가")
    close = Column(Float, comment="종가")
    volume = Column(BigInteger, comment="거래량")

    # 장마감 완료 캔들인지 여부 (당일 미마감이면 False)
    is_final = Column(Boolean, default=True, nullable=False, comment="장마감 완료 여부")

    # 원천 정보
    source = Column(String(32), default="KIS", nullable=False, comment="데이터 소스")
    source_symbol = Column(String(32), comment="원천 심볼")
    source_exchange = Column(String(16), comment="원천 거래소")
    source_payload = Column(JSON, comment="원천 응답 데이터")

    ingested_at = Column(DateTime(timezone=True), server_default=func.now(), comment="수집일시")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), comment="수정일시")

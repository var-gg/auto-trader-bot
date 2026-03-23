# app/features/yahoo_finance/models/yahoo_index_timeseries.py

from sqlalchemy import Column, Integer, Numeric, Date, ForeignKey, PrimaryKeyConstraint, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class YahooIndexTimeseries(Base):
    """야후 파이낸스 지수/환율 시계열 데이터
    
    일별 지수 가격 및 환율 데이터를 저장합니다.
    FRED의 macro_timeseries와 유사하지만, 야후 파이낸스 데이터 전용입니다.
    """
    __tablename__ = "yahoo_index_timeseries"
    __table_args__ = (
        PrimaryKeyConstraint("series_id", "d"),
        {"schema": DB_SCHEMA},
    )
    
    series_id = Column(
        Integer,
        ForeignKey(f"{DB_SCHEMA}.yahoo_index_series.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="시리즈 ID (yahoo_index_series.id)"
    )
    d = Column(Date, nullable=False, index=True, comment="날짜")
    v = Column(Numeric(20, 8), comment="값 (종가 또는 환율)")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="생성일시")
    
    # Relationship
    series = relationship("YahooIndexSeries", back_populates="timeseries")
    
    def __repr__(self):
        return f"<YahooIndexTimeseries(series_id={self.series_id}, d={self.d}, v={self.v})>"


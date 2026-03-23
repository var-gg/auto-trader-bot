from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, Date, Numeric, String, UniqueConstraint
from sqlalchemy.orm import relationship
from app.core.db import Base
from app.core.config import DB_SCHEMA


class DividendHistory(Base):
    """배당 이력"""
    __tablename__ = "dividend_history"
    __table_args__ = (
        UniqueConstraint("ticker_id", "payment_date", name="uq_dividend_history_ticker_date"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment="배당 이력 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id"), nullable=False, index=True, comment="티커 ID")
    
    # 배당 정보
    dividend_per_share = Column(Numeric(10, 4), nullable=False, comment="주당 배당금")
    dividend_yield = Column(Float, nullable=True, comment="배당률 (%)")
    payment_date = Column(Date, nullable=False, comment="배당 지급일")
    currency = Column(String(3), nullable=False, default="USD", comment="통화 코드")
    
    # 메타데이터
    created_at = Column(DateTime, nullable=False, comment="생성 일시")
    
    # 관계 설정
    ticker = relationship("Ticker", back_populates="dividend_histories")

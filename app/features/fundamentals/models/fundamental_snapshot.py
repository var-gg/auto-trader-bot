from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship
from app.core.db import Base
from app.core.config import DB_SCHEMA


class FundamentalSnapshot(Base):
    """기업 재무 요약 정보 스냅샷"""
    __tablename__ = "fundamental_snapshot"
    __table_args__ = (
        UniqueConstraint("ticker_id", name="uq_fundamental_snapshot_ticker"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment="스냅샷 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id"), nullable=False, index=True, comment="티커 ID")
    
    # 재무 지표
    per = Column(Float, comment="PER (주가수익비율)")
    pbr = Column(Float, comment="PBR (주가순자산비율)")
    dividend_yield = Column(Float, comment="배당률 (%)")
    market_cap = Column(Numeric(20, 2), comment="시가총액")
    debt_ratio = Column(Float, comment="부채비율 (%)")
    
    # 메타데이터
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, comment="업데이트 일시 (UTC)")
    
    # 관계 설정
    ticker = relationship("Ticker", back_populates="fundamental_snapshots")

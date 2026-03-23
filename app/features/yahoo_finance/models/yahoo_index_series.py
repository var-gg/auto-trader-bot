# app/features/yahoo_finance/models/yahoo_index_series.py

from sqlalchemy import Column, Integer, String, Boolean, DateTime, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class YahooIndexSeries(Base):
    """야후 파이낸스 지수/환율 시리즈 메타데이터
    
    지수(^GSPC, ^KS11 등) 및 환율(KRW=X 등)의 메타 정보를 저장합니다.
    FRED의 macro_series와 유사하지만, 야후 파이낸스 데이터 전용입니다.
    """
    __tablename__ = "yahoo_index_series"
    __table_args__ = (
        UniqueConstraint("code", name="uq_yahoo_series_code"),
        {"schema": DB_SCHEMA},
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="시리즈 ID")
    code = Column(String(50), nullable=False, comment="심볼 코드 (예: ^GSPC, ^KS11, KRW=X)")
    name = Column(String(255), comment="지수/환율 이름")
    provider = Column(String(50), comment="데이터 제공자 (yahoo_finance)")
    freq = Column(String(20), comment="데이터 빈도 (daily)")
    unit = Column(String(50), comment="단위")
    active = Column(Boolean, default=True, comment="활성화 여부")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="수정일시")
    
    # Relationship
    timeseries = relationship(
        "YahooIndexTimeseries",
        back_populates="series",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return f"<YahooIndexSeries(id={self.id}, code={self.code}, name={self.name})>"


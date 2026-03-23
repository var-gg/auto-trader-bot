from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from app.core.db import Base
from app.core.config import DB_SCHEMA

# 관계 대상 모델을 레지스트리에 선등록 (런타임 import 순서 이슈 방지)
from app.features.fundamentals.models.fundamental_snapshot import FundamentalSnapshot  # noqa: F401
from app.features.fundamentals.models.dividend_history import DividendHistory  # noqa: F401
from app.features.recommendation.models.analyst_recommendation import AnalystRecommendation  # noqa: F401

class Ticker(Base):
    __tablename__ = "ticker"
    __table_args__ = (
        UniqueConstraint("symbol", "exchange", name="uq_ticker_symbol_exchange"),
        {"schema": DB_SCHEMA},
    )

    # ✅ 이제 id가 PK
    id = Column(Integer, primary_key=True, autoincrement=True, index=True, comment="티커 ID")

    # 기존 PK였던 symbol → 일반 컬럼
    symbol = Column(String, nullable=False, index=True, comment="종목 심볼")   # NVDA, 005930.KQ
    exchange = Column(String, nullable=False, index=True, comment="거래소") # NASDAQ, KOSPI
    country = Column(String, nullable=False, comment="국가 코드")              # US, KR
    type = Column(String, nullable=False, comment="종목 유형")                 # stock / etf
    
    # 관계 설정
    fundamental_snapshots = relationship("FundamentalSnapshot", back_populates="ticker")
    dividend_histories = relationship("DividendHistory", back_populates="ticker")
    analyst_recommendations = relationship("AnalystRecommendation", back_populates="ticker")

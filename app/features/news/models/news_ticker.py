from sqlalchemy import Column, DateTime, Integer, ForeignKey, Float, String, UniqueConstraint
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

class NewsTicker(Base):
    __tablename__ = "news_ticker"
    __table_args__ = (
        UniqueConstraint("news_id", "ticker_id", name="uq_news_ticker__news_ticker"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="뉴스 티커 매핑 ID")
    news_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.news.id", ondelete="CASCADE"), nullable=False, comment="뉴스 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"), nullable=False, comment="티커 ID")

    confidence = Column(Float, nullable=True, comment="신뢰도")
    method = Column(String(50), nullable=True, comment="매핑 방법")
    candidate_pool_size = Column(Integer, nullable=True, comment="후보 풀 크기")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

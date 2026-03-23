# app/features/news/models/news_exchange.py
from sqlalchemy import Column, Integer, ForeignKey, Float, DateTime, String, UniqueConstraint
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

# ✅ FK 타겟을 메타데이터에 확실히 올리기 위해 강제 import (중요)
from app.features.news.models.news import News  # noqa: F401

class NewsExchange(Base):
    __tablename__ = "news_exchange"
    __table_args__ = (
        UniqueConstraint("news_id", "exchange_code", name="uq_news_exchange__news_exchange"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="뉴스 거래소 매핑 ID")
    news_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.news.id", ondelete="CASCADE"), nullable=False, comment="뉴스 ID")
    exchange_code = Column(String(10), nullable=False, comment="거래소 코드 (예: NMS, NYQ, TSE)")
    confidence = Column(Float, nullable=True, comment="신뢰도")
    method = Column(String(50), nullable=True, comment="매핑 방법")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

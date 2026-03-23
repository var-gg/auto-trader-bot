# app/features/news/models/news_summary.py
from sqlalchemy import Column, Integer, ForeignKey, Text, DateTime, String, UniqueConstraint, Index
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

class NewsSummary(Base):
    __tablename__ = "news_summary"
    __table_args__ = (
        UniqueConstraint("news_id", "lang", name="uq_news_summary_news_lang"),  # 🔸 유니크
        Index("ix_news_summary_news_id", "news_id"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="뉴스 요약 ID")
    news_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.news.id", ondelete="CASCADE"), nullable=False, comment="뉴스 ID")
    lang = Column(String(10), default="ko", nullable=False, comment="언어 코드")
    title_localized = Column(String(300), nullable=True, comment="현지화된 제목")
    summary_text = Column(Text, nullable=False, comment="요약 텍스트")
    model = Column(String(100), nullable=True, comment="사용된 모델명")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

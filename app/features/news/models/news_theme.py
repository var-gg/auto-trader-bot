# app/features/news/models/news_theme.py
from sqlalchemy import Column, Integer, ForeignKey, Float, DateTime, String, UniqueConstraint
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA

# ✅ FK 타겟을 메타데이터에 확실히 올리기 위해 강제 import (중요)
from app.shared.models.theme import Theme  # noqa: F401  <-- 이 줄이 핵심
from app.features.news.models.news import News  # noqa: F401 (news FK도 안전하게)

class NewsTheme(Base):
    __tablename__ = "news_theme"
    __table_args__ = (
        UniqueConstraint("news_id", "theme_id", name="uq_news_theme__news_theme"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="뉴스 테마 매핑 ID")
    news_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.news.id", ondelete="CASCADE"), nullable=False, comment="뉴스 ID")
    theme_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.theme.id", ondelete="CASCADE"), nullable=False, comment="테마 ID")
    confidence = Column(Float, nullable=True, comment="신뢰도")
    method = Column(String(50), nullable=True, comment="매핑 방법")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

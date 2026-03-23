# app/features/news/models/news.py
import enum
from sqlalchemy import Column, Integer, String, Text, DateTime, Date, Boolean, Float, Index
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ENUM as PGEnum  # <- 핵심
from app.core.db import Base
from app.core.config import DB_SCHEMA

class NewsStatus(enum.Enum):
    RAW = "RAW"
    FILTERED_POSITIVE = "FILTERED_POS"
    FILTERED_NEGATIVE = "FILTERED_NEG"
    ANALYZED = "ANALYZED"

class ContentStatus(enum.Enum):
    NONE = "NONE"
    SUCCESS = "SUCCESS"
    FAILED_FINAL = "FAILED_FINAL"

class News(Base):
    __tablename__ = "news"
    __table_args__ = (
        Index("ix_news_published_date_kst", "published_date_kst"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="뉴스 ID")
    title = Column(String(500), nullable=False, comment="뉴스 제목")
    link = Column(String(1000), nullable=False, unique=True, comment="뉴스 링크")

    published_at = Column(DateTime(timezone=True), nullable=True, comment="발행 시간")
    published_date_kst = Column(Date, nullable=True, comment="발행일 (KST)")
    source = Column(String(100), nullable=True, comment="뉴스 소스")

    # ✅ Enum 라벨을 반드시 .value로 매핑
    status = Column(
        PGEnum(
            NewsStatus,
            name="newsstatus",
            schema=DB_SCHEMA,
            create_type=False,  # 타입은 이미 존재한다고 가정 (Alembic/SQL로 관리)
            values_callable=lambda x: [e.value for e in x],
            native_enum=True,
        ),
        default=NewsStatus.RAW,
        nullable=False,
        comment="뉴스 처리 상태"
    )

    is_finance_related = Column(Boolean, default=False, nullable=False, comment="금융 관련 여부")
    filter_score = Column(Float, nullable=True, comment="관련성 필터 점수")
    filter_model = Column(String(100), nullable=True, comment="필터링 모델명")
    filter_attempts = Column(Integer, default=0, nullable=False, comment="필터링 시도 횟수")

    content_status = Column(
        PGEnum(
            ContentStatus,
            name="contentstatus",
            schema=DB_SCHEMA,
            create_type=False,
            values_callable=lambda x: [e.value for e in x],
            native_enum=True,
        ),
        default=ContentStatus.NONE,
        nullable=False,
        comment="본문 수집 상태"
    )

    content_retry_count = Column(Integer, default=0, nullable=False, comment="본문 수집 재시도 횟수")
    content = Column(Text, nullable=True, comment="뉴스 본문")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False, comment="수정일시")

# app/features/news/models/kis_news.py

from sqlalchemy import Column, BigInteger, String, Text, DateTime, Index, ForeignKey, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class KisNews(Base):
    __tablename__ = "kis_news"
    __table_args__ = (
        Index("ux_kis_news_source", "source_type", "source_key", unique=True),
        Index("ix_kis_news_ticker_published", "ticker_id", "published_at"),
        CheckConstraint("source_type IN ('overseas', 'domestic')", name="ck_kis_news_source_type"),
        {"schema": DB_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="KIS 뉴스 ID")
    source_type = Column(String(20), nullable=False, comment="해외/국내 구분")
    source_key = Column(Text, nullable=False, comment="해외: news_key, 국내: cntt_usiq_srno")
    ticker_id = Column(
        BigInteger,
        ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="RESTRICT"),
        nullable=False,
        comment="티커 ID",
    )
    title = Column(Text, nullable=False, comment="뉴스 제목")
    published_at = Column(DateTime(timezone=True), nullable=False, comment="발행 시간 (KST 기준)")

    # 보조 메타
    publisher = Column(Text, nullable=True, comment="source(해외) / dorg(국내)")
    class_cd = Column(Text, nullable=True, comment="class_cd(해외) / news_lrdv_code(국내)")
    class_name = Column(Text, nullable=True, comment="class_name(해외)")
    nation_cd = Column(Text, nullable=True, comment="해외 nation_cd")
    exchange_cd = Column(Text, nullable=True, comment="해외 exchange_cd")
    symbol = Column(Text, nullable=True, comment="해외 symb (예: NVDA)")
    symbol_name = Column(Text, nullable=True, comment="해외 symb_name 한글명")
    kr_iscd = Column(Text, nullable=True, comment="국내 iscd1 (6자리 종목코드)")
    lang = Column(String(10), default="ko", comment="언어 코드")

    # 원문 보관
    raw_json = Column(JSONB, nullable=True, comment="원문 JSON")

    # 운영 필드
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False, comment="수정일시")


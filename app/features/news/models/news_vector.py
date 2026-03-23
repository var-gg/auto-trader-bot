# app/features/news/models/news_vector.py
from sqlalchemy import Column, Integer, ForeignKey, Text, DateTime, String, Float, Index, UniqueConstraint
from sqlalchemy.sql import func
from pgvector.sqlalchemy import Vector
from app.core.db import Base
from app.core.config import DB_SCHEMA

class NewsVector(Base):
    __tablename__ = "news_vector"
    __table_args__ = (
        UniqueConstraint("news_id", "model_name", name="uq_news_vector_news_model"),
        Index("ix_news_vector_news_id", "news_id"),
        Index("ix_news_vector_model_name", "model_name"),
        Index("ix_news_vector_created_at", "created_at"),
        {"schema": DB_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="뉴스 벡터 ID")
    news_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.news.id", ondelete="CASCADE"), nullable=False, comment="뉴스 ID")
    
    # 벡터 정보
    model_name = Column(String(100), nullable=False, comment="임베딩 모델명 (예: textembedding-gecko@003)")
    vector_dimension = Column(Integer, nullable=False, comment="벡터 차원수")
    embedding_vector = Column(Vector(3072), nullable=False, comment="임베딩 벡터 배열")
    
    # 메타데이터
    text_length = Column(Integer, nullable=True, comment="원본 텍스트 길이 (문자 수)")
    token_length = Column(Integer, nullable=True, comment="원본 텍스트 토큰 수 (자르기 전)")
    processing_time_ms = Column(Integer, nullable=True, comment="처리 시간 (밀리초)")
    
    # 상태 정보
    status = Column(String(20), default="SUCCESS", nullable=False, comment="처리 상태 (SUCCESS, FAILED)")
    error_message = Column(Text, nullable=True, comment="오류 메시지")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False, comment="수정일시")

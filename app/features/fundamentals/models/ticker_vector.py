# app/features/fundamentals/models/ticker_vector.py
from sqlalchemy import Column, Integer, ForeignKey, Text, DateTime, String, Index, UniqueConstraint
from sqlalchemy.sql import func
from pgvector.sqlalchemy import Vector
from app.core.db import Base
from app.core.config import DB_SCHEMA

class TickerVector(Base):
    """티커 벡터 모델 - 티커별 임베딩 벡터"""
    
    __tablename__ = "ticker_vector"
    __table_args__ = (
        UniqueConstraint("ticker_id", "model_name", name="uq_ticker_vector_ticker_model"),
        Index("ix_ticker_vector_ticker_id", "ticker_id"),
        Index("ix_ticker_vector_model_name", "model_name"),
        Index("ix_ticker_vector_created_at", "created_at"),
        {"schema": DB_SCHEMA},
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="티커 벡터 ID")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"), nullable=False, comment="티커 ID")
    
    # 벡터 정보
    model_name = Column(String(100), nullable=False, comment="임베딩 모델명")
    vector_dimension = Column(Integer, nullable=False, comment="벡터 차원수")
    embedding_vector = Column(Vector(3072), nullable=False, comment="임베딩 벡터 배열")
    
    # 소스 텍스트
    source_text = Column(Text, nullable=False, comment="임베딩 생성용 소스 텍스트")
    
    # 메타데이터
    text_length = Column(Integer, nullable=True, comment="원본 텍스트 길이 (문자 수)")
    token_length = Column(Integer, nullable=True, comment="원본 텍스트 토큰 수 (자르기 전)")
    processing_time_ms = Column(Integer, nullable=True, comment="처리 시간 (밀리초)")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False, comment="수정일시")
    
    def __repr__(self):
        return f"<TickerVector(id={self.id}, ticker_id={self.ticker_id}, model_name='{self.model_name}')>"
    
    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            "id": self.id,
            "ticker_id": self.ticker_id,
            "model_name": self.model_name,
            "vector_dimension": self.vector_dimension,
            "source_text": self.source_text,
            "text_length": self.text_length,
            "token_length": self.token_length,
            "processing_time_ms": self.processing_time_ms,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

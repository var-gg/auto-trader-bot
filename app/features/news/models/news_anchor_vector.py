# app/features/news/models/news_anchor_vector.py
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Index, UniqueConstraint
from sqlalchemy.sql import func
from pgvector.sqlalchemy import Vector
from app.core.db import Base
from app.core.config import DB_SCHEMA

class NewsAnchorVector(Base):
    """뉴스 앵커 벡터 모델 - 뉴스 분류를 위한 기준점 벡터들"""
    
    __tablename__ = "news_anchor_vector"
    __table_args__ = (
        UniqueConstraint("code", name="uq_news_anchor_vector_code"),
        Index("ix_news_anchor_vector_code", "code"),
        Index("ix_news_anchor_vector_model_name", "model_name"),
        Index("ix_news_anchor_vector_created_at", "created_at"),
        {"schema": DB_SCHEMA},
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="앵커 벡터 ID")
    code = Column(String(50), nullable=False, comment="앵커 코드 (예: MACRO, EARNINGS, POLICY)")
    name_ko = Column(String(100), nullable=True, comment="한글명")
    description = Column(Text, nullable=True, comment="설명")
    anchor_text = Column(Text, nullable=False, comment="앵커 문장 (임베딩 생성용)")
    
    # 벡터 정보
    model_name = Column(String(100), nullable=False, comment="임베딩 모델명")
    vector_dimension = Column(Integer, nullable=False, comment="벡터 차원수")
    embedding_vector = Column(Vector(3072), nullable=False, comment="임베딩 벡터 배열")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False, comment="수정일시")
    
    def __repr__(self):
        return f"<NewsAnchorVector(id={self.id}, code='{self.code}', name_ko='{self.name_ko}')>"
    
    def to_dict(self):
        """딕셔너리로 변환"""
        return {
            "id": self.id,
            "code": self.code,
            "name_ko": self.name_ko,
            "description": self.description,
            "anchor_text": self.anchor_text,
            "model_name": self.model_name,
            "vector_dimension": self.vector_dimension,
            # "embedding_vector": list(self.embedding_vector) if self.embedding_vector else [],  # 벡터는 응답에서 제외
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

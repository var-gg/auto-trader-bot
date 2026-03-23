# app/features/signals/models/similarity_analysis.py
from sqlalchemy import (
    Column, Integer, String, Numeric, DateTime
)
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class SimilarityAnalysis(Base):
    """
    유사도 분석 결과 테이블
    - 티커별 최신 유사 시그널 분석 결과 저장 (매번 덮어쓰기)
    - p_up, p_down: 상승/하락 확률
    - exp_up, exp_down: 상승/하락 시 기대 변동률
    - top_similarity: 가장 높은 유사도 점수
    """
    __tablename__ = "similarity_analysis"
    __table_args__ = {"schema": DB_SCHEMA}
    
    # 기본 정보 (PK)
    ticker_id = Column(
        Integer, 
        primary_key=True,
        comment="티커 ID (ticker 테이블 참조)"
    )
    ticker_name_ko = Column(String(255), nullable=True, comment="종목명 (한글)")
    exchange = Column(String(50), nullable=True, comment="거래소 코드")
    
    # 분석 결과
    p_up = Column(Numeric(10, 6), nullable=False, comment="상승 확률 (가중치 합)")
    p_down = Column(Numeric(10, 6), nullable=False, comment="하락 확률 (가중치 합)")
    exp_up = Column(Numeric(10, 6), nullable=False, comment="상승 시 기대 변동률")
    exp_down = Column(Numeric(10, 6), nullable=False, comment="하락 시 기대 변동률")
    top_similarity = Column(Numeric(10, 6), nullable=False, server_default="0", comment="가장 높은 유사도 점수 (TOP 1)")
    
    # 타임스탬프
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="수정일시")


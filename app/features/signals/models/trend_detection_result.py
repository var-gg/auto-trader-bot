# app/features/signals/models/trend_detection_result.py
from sqlalchemy import (
    Column, BigInteger, Integer, Date, Numeric, DateTime, 
    ForeignKey, UniqueConstraint, ARRAY, Float
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.db import Base
from app.core.config import DB_SCHEMA


class TrendDetectionResult(Base):
    """
    트렌드 탐지 결과 테이블
    - 탐지된 시그널 저장
    - 무차원 형태 벡터 포함 (유사도 검색용)
    """
    __tablename__ = "trend_detection_result"
    __table_args__ = (
        UniqueConstraint("ticker_id", "signal_date", "config_id", name="uq_result_ticker_date_config"),
        {"schema": DB_SCHEMA},
    )
    
    result_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="결과 ID")
    
    # 외래키
    ticker_id = Column(
        Integer, 
        nullable=False, 
        index=True,
        comment="티커 ID (ticker 테이블 참조)"
    )
    config_id = Column(
        Integer, 
        ForeignKey(f"{DB_SCHEMA}.trend_detection_config.config_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="설정 ID"
    )
    
    # 시그널 데이터
    signal_date = Column(Date, nullable=False, index=True, comment="시그널 발생 날짜")
    close = Column(Numeric(20, 8), comment="종가")
    change_7_24d = Column(Numeric(10, 6), comment="이후 7~24일 변동률")
    past_slope = Column(Numeric(10, 6), comment="직전 구간 기울기")
    past_std = Column(Numeric(10, 6), comment="직전 구간 표준편차")
    
    # 벡터 정보
    shape_vector = Column(ARRAY(Float), comment="무차원 형태 벡터 (2*m + 7 차원)")
    vector_dim = Column(Integer, comment="벡터 차원 수")
    vector_m = Column(Integer, comment="PAA 리샘플링 길이 m")
    prior_candles = Column(Integer, comment="시그널 이전 캔들 개수 (벡터 생성에 사용)")
    
    # 스코어 (추후 활용)
    signal_score = Column(Numeric(5, 3), comment="시그널 스코어 (추후 확장용)")
    
    # 타임스탬프
    detected_at = Column(DateTime(timezone=True), server_default=func.now(), comment="탐지일시")
    
    # 관계
    config = relationship("TrendDetectionConfig", foreign_keys=[config_id])


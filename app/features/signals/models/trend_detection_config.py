# app/features/signals/models/trend_detection_config.py
from sqlalchemy import (
    Column, Integer, String, Numeric, DateTime, UniqueConstraint, CheckConstraint
)
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class TrendDetectionConfig(Base):
    """
    트렌드 탐지 설정 테이블
    - 파라미터 조합별로 config_id 자동 생성
    - 동일한 파라미터 조합 + 버전은 재사용 (UNIQUE 제약)
    - 버전별로 다른 탐지/벡터 생성 알고리즘 사용 가능
    """
    __tablename__ = "trend_detection_config"
    __table_args__ = (
        UniqueConstraint(
            "direction", "lookback", "future_window", 
            "min_change", "max_reverse", "flatness_k", "atr_window", "version",
            name="uq_trend_config_params"
        ),
        CheckConstraint("direction IN ('UP', 'DOWN')", name="ck_direction"),
        {"schema": DB_SCHEMA},
    )
    
    config_id = Column(Integer, primary_key=True, autoincrement=True, comment="설정 ID")
    direction = Column(String(10), nullable=False, comment="시그널 방향 (UP/DOWN)")
    lookback = Column(Integer, nullable=False, comment="직전 구간 확인 기간")
    future_window = Column(Integer, nullable=False, comment="이후 구간 평가 기간")
    min_change = Column(Numeric(6, 4), nullable=False, comment="최소 변동률")
    max_reverse = Column(Numeric(6, 4), nullable=False, comment="반대 방향 최대 허용폭")
    flatness_k = Column(Numeric(6, 4), nullable=False, comment="평탄성 허용치 (ATR 배수)")
    atr_window = Column(Integer, default=7, nullable=False, comment="ATR 계산 윈도우")
    version = Column(String(10), default="v1", nullable=False, comment="알고리즘 버전 (탐지+벡터)")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="생성일시")


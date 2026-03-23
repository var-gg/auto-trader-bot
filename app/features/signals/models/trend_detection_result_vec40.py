# app/features/signals/models/trend_detection_result_vec40.py
from sqlalchemy import Column, Integer, Date, String, CheckConstraint, Index, ForeignKey
from sqlalchemy.types import UserDefinedType
from sqlalchemy.orm import relationship
from app.core.db import Base
from app.core.config import DB_SCHEMA


class Vector(UserDefinedType):
    """
    PostgreSQL의 custom vector type을 위한 SQLAlchemy 타입
    """
    cache_ok = True
    
    def get_col_spec(self):
        return "vector"
    
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            # 리스트를 PostgreSQL vector 형식으로 변환
            if isinstance(value, (list, tuple)):
                return "[" + ",".join(str(v) for v in value) + "]"
            return value
        return process
    
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            # PostgreSQL vector를 리스트로 변환
            if isinstance(value, str):
                # "[1.0,2.0,3.0]" 형식을 [1.0, 2.0, 3.0]로 변환
                value = value.strip("[]")
                return [float(v) for v in value.split(",")]
            return value
        return process


class TrendDetectionResultVec40(Base):
    """
    트렌드 탐지 결과 벡터40 테이블
    - 40차원 형태 벡터를 사용한 유사도 검색용
    - HNSW 인덱스를 통한 고속 벡터 유사도 검색
    """
    __tablename__ = "trend_detection_result_vec40"
    __table_args__ = (
        CheckConstraint(
            "direction IN ('UP', 'DOWN')",
            name="trend_detection_result_vec40_direction_check"
        ),
        Index(
            "idx_trend_vec40_hnsw",
            "shape_vector40",
            postgresql_using="hnsw",
            postgresql_ops={"shape_vector40": "vector_cosine_ops"}
        ),
        {"schema": DB_SCHEMA},
    )
    
    # 복합 primary key
    ticker_id = Column(
        Integer,
        ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="티커 ID (ticker 테이블 참조)"
    )
    signal_date = Column(
        Date,
        primary_key=True,
        nullable=False,
        comment="시그널 발생 날짜"
    )
    direction = Column(
        String(10),
        primary_key=True,
        nullable=False,
        comment="시그널 방향 (UP/DOWN)"
    )
    
    # 벡터 데이터
    shape_vector40 = Column(
        Vector,
        nullable=False,
        comment="40차원 형태 벡터"
    )
    
    # 관계 (필요시 추가)
    # ticker = relationship("Ticker", foreign_keys=[ticker_id])


# app/features/recommendation/models/analyst_recommendation.py

import enum
from sqlalchemy import (
    Column, Integer, String, Text, Numeric, DateTime, Boolean,
    ForeignKey, CheckConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ENUM as PGEnum
from app.core.db import Base
from app.core.config import DB_SCHEMA


class PositionType(enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class AnalystRecommendation(Base):
    __tablename__ = "analyst_recommendation"
    __table_args__ = (
        Index("idx_analyst_recommendation_valid_until", "valid_until"),
        Index("idx_analyst_recommendation_ticker_latest", "ticker_id", "is_latest"),
        CheckConstraint(
            "confidence_score >= 0.00 AND confidence_score <= 1.00",
            name="ck_confidence_score_range"
        ),
        CheckConstraint(
            "entry_price > 0",
            name="ck_entry_price_positive"
        ),
        CheckConstraint(
            "target_price > 0",
            name="ck_target_price_positive"
        ),
        CheckConstraint(
            "stop_price IS NULL OR stop_price > 0",
            name="ck_stop_price_positive"
        ),
        CheckConstraint(
            "analysis_price IS NULL OR analysis_price > 0",
            name="ck_analysis_price_positive"
        ),
        {"schema": DB_SCHEMA},
    )

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True, comment="추천 ID")

    # Foreign Key
    ticker_id = Column(
        Integer, 
        ForeignKey(f"{DB_SCHEMA}.ticker.id", ondelete="CASCADE"), 
        nullable=False, 
        index=True,
        comment="티커 ID"
    )

    # Position Information
    position_type = Column(
        PGEnum(
            PositionType,
            name="positiontype",
            schema=DB_SCHEMA,
            create_type=False,
            values_callable=lambda x: [e.value for e in x],
            native_enum=True,
        ),
        nullable=False,
        comment="포지션 타입 (LONG/SHORT)"
    )
    entry_price = Column(
        Numeric(15, 4), 
        nullable=False, 
        comment="진입가"
    )
    target_price = Column(
        Numeric(15, 4), 
        nullable=False, 
        comment="목표가"
    )
    stop_price = Column(
        Numeric(15, 4), 
        nullable=True, 
        comment="손절가"
    )
    analysis_price = Column(
        Numeric(15, 4), 
        nullable=True, 
        comment="분석 당시 최근가격"
    )

    # Validity & Reasoning
    valid_until = Column(
        DateTime(timezone=True), 
        nullable=False, 
        index=True,
        comment="유효기간"
    )
    reason = Column(
        Text, 
        nullable=False, 
        comment="추천 이유"
    )
    confidence_score = Column(
        Numeric(3, 2), 
        nullable=False, 
        comment="신뢰도 (0.00~1.00)"
    )

    # Latest Flag
    is_latest = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default="true",
        index=True,
        comment="최신 추천 여부 (티커별로 1개만 true)"
    )

    # Timestamps
    recommended_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        server_default=func.now(),
        index=True,
        comment="추천일시"
    )
    created_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        server_default=func.now(),
        comment="생성일시"
    )
    updated_at = Column(
        DateTime(timezone=True), 
        nullable=False, 
        server_default=func.now(),
        onupdate=func.now(),
        comment="수정일시"
    )

    # Relationships
    ticker = relationship("Ticker", back_populates="analyst_recommendations")

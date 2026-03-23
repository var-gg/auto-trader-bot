# app/features/trading_hybrid/models/trading_hybrid_models.py
from sqlalchemy import Column, String, Numeric, DateTime, Text, ForeignKey, Integer, Date, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.db import Base
from app.core.config import DB_SCHEMA


class LegActionLog(Base):
    """레그 액션 히스토리 (가격 조정, 취소 등)"""
    __tablename__ = "leg_action_log"
    __table_args__ = (
        Index("idx_leg_action_log_leg_id", "leg_id"),
        Index("idx_leg_action_log_created_at", "created_at"),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="액션 로그 ID")
    leg_id = Column(
        Integer,
        ForeignKey(f"{DB_SCHEMA}.order_leg.id", ondelete="CASCADE"),
        nullable=False,
        comment="레그 ID (FK)"
    )
    symbol = Column(Text, nullable=True, comment="종목 심볼")
    action = Column(
        Text,
        nullable=False,
        comment="액션 타입 (REPRICE_UP, CANCEL, REPLACE, SUSPEND_DOWN_SIGNAL, NEAR_CLOSE_TRIM 등)"
    )
    note = Column(Text, nullable=True, comment="액션 상세 내역")
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="생성일시"
    )

    # 관계 정의
    leg = relationship("OrderLeg", foreign_keys=[leg_id])


class DailySymbolBlock(Base):
    """일일 신규매수 차단 심볼 목록"""
    __tablename__ = "daily_symbol_block"
    __table_args__ = (
        Index("idx_daily_symbol_block_date", "block_date"),
        {"schema": DB_SCHEMA}
    )

    symbol = Column(Text, primary_key=True, nullable=False, comment="종목 심볼")
    block_date = Column(Date, primary_key=True, nullable=False, comment="차단 일자")
    reason = Column(
        Text,
        nullable=True,
        comment="차단 사유 (realized_loss, snapshot_delta 등)"
    )
    pnl_rate = Column(
        Numeric(10, 4),
        nullable=True,
        comment="손실률 (예: -0.0150 = -1.5%)"
    )
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="생성일시"
    )


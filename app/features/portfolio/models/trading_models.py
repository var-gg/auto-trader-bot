from sqlalchemy import Column, String, Numeric, DateTime, Text, ForeignKey, Integer, CheckConstraint, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from app.core.db import Base
from app.core.config import DB_SCHEMA

class OrderBatch(Base):
    """주문 배치 헤더"""
    __tablename__ = "order_batch"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(Integer, primary_key=True, autoincrement=True, comment="배치ID")
    asof_kst = Column(DateTime(timezone=True), nullable=False, comment="생성시각(KST)")
    mode = Column(Text, nullable=False, comment="배치구분(BUY/SELL)")
    currency = Column(Text, nullable=False, comment="계좌통화(예: USD)")
    available_cash = Column(Numeric(20, 2), nullable=False, comment="가용현금(참고값)")
    notes = Column(Text, nullable=True, comment="설계 메모(간단 코멘트)")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

    # 제약조건
    __table_args__ = (
        CheckConstraint("mode IN ('BUY','SELL')", name="ck_order_batch_mode"),
        {"schema": DB_SCHEMA}
    )

    # 관계 정의
    plans = relationship("OrderPlan", back_populates="batch", cascade="all, delete-orphan")


class OrderPlan(Base):
    """주문 플랜(심볼 단위) — 실행/제외 모두 저장"""
    __tablename__ = "order_plan"
    __table_args__ = (
        Index("ix_order_plan_batch_id", "batch_id"),
        Index("ix_order_plan_ticker_id", "ticker_id"),
        Index("uq_order_plan_batch_symbol_action", "batch_id", "symbol", "action", 
              postgresql_where=Column("decision") == "EXECUTE", unique=True),
        CheckConstraint("action IN ('BUY','SELL')", name="ck_order_plan_action"),
        CheckConstraint("decision IN ('EXECUTE','SKIP')", name="ck_order_plan_decision"),
        CheckConstraint("skip_code IN ('HOLD','CASH','EXPIRED','RISK','DUPLICATE','SPREAD','OTHER')", 
                       name="ck_order_plan_skip_code"),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="계획ID")
    batch_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.order_batch.id", ondelete="CASCADE"), 
                     nullable=False, comment="배치ID(FK)")
    ticker_id = Column(Integer, nullable=False, comment="티커ID")
    symbol = Column(Text, nullable=False, comment="심볼(예: MSFT)")
    action = Column(Text, nullable=False, comment="액션(BUY/SELL)")
    recommendation_id = Column(Integer, nullable=True, comment="추천서ID(참고)")
    note = Column(Text, nullable=True, comment="사유(2줄 이내 요약)")
    reverse_breach_day = Column(Integer, nullable=True, comment="역돌파 일자 (pm_best_signal 기반)")
    
    decision = Column(Text, nullable=False, comment="실행/제외")
    skip_code = Column(Text, nullable=True, comment="제외코드")
    skip_note = Column(Text, nullable=True, comment="제외사유")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

    # 관계 정의
    batch = relationship("OrderBatch", back_populates="plans")
    legs = relationship("OrderLeg", back_populates="plan", cascade="all, delete-orphan")


class OrderLeg(Base):
    """주문 레그(라인) — 실행 계획에만 존재(DECISION=EXECUTE)"""
    __tablename__ = "order_leg"
    __table_args__ = (
        Index("ix_order_leg_plan_id", "plan_id"),
        CheckConstraint("type IN ('LIMIT','MARKET','LOC')", name="ck_order_leg_type"),
        CheckConstraint("side IN ('BUY','SELL')", name="ck_order_leg_side"),
        CheckConstraint("quantity > 0", name="ck_order_leg_quantity_positive"),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="레그ID")
    plan_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.order_plan.id", ondelete="CASCADE"), 
                    nullable=False, comment="계획ID(FK)")
    type = Column(Text, nullable=False, comment="주문유형")
    side = Column(Text, nullable=False, comment="매수/매도")
    quantity = Column(Integer, nullable=False, comment="수량(정수, 1주단위)")
    limit_price = Column(Numeric(20, 4), nullable=True, comment="가격(LIMIT/LOC일 때)")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")

    # 관계 정의
    plan = relationship("OrderPlan", back_populates="legs")
    broker_orders = relationship("BrokerOrder", back_populates="leg", cascade="all, delete-orphan")


class BrokerOrder(Base):
    """브로커 전송 주문(헤더) — KIS 송신 스냅샷/상태"""
    __tablename__ = "broker_order"
    __table_args__ = (
        Index("ix_broker_order_leg_id", "leg_id"),
        Index("ix_broker_order_order_number", "order_number"),
        CheckConstraint("status IN ('SUBMITTED','ACCEPTED','PARTIAL','FILLED','CANCELLED','REJECTED')", 
                       name="ck_broker_order_status"),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="브로커주문ID")
    leg_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.order_leg.id", ondelete="CASCADE"), 
                   nullable=True, comment="레그ID(FK) - 직접 주문시 NULL 가능")
    order_number = Column(Text, nullable=True, comment="주문번호")
    routing_org_code = Column(Text, nullable=True, comment="거래소 전송 조직번호 (국내/해외 공용)")
    payload = Column(JSONB, nullable=True, comment="전송페이로드 스냅샷")
    status = Column(Text, nullable=False, comment="상태")
    submitted_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="전송시각")
    completed_at = Column(DateTime(timezone=True), nullable=True, comment="완료시각")
    reject_code = Column(Text, nullable=True, comment="거절코드")
    reject_message = Column(Text, nullable=True, comment="거절사유")

    # 관계 정의
    leg = relationship("OrderLeg", back_populates="broker_orders")
    fills = relationship("OrderFill", back_populates="broker_order", cascade="all, delete-orphan")


class OrderFill(Base):
    """체결(라인) — 부분체결 다건"""
    __tablename__ = "order_fill"
    __table_args__ = (
        Index("ix_order_fill_broker_order_id", "broker_order_id"),
        CheckConstraint("fill_status IN ('UNFILLED','PARTIAL','FULL','CANCELLED','REJECTED')", 
                       name="ck_order_fill_status"),
        {"schema": DB_SCHEMA}
    )

    id = Column(Integer, primary_key=True, autoincrement=True, comment="체결ID")
    broker_order_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.broker_order.id", ondelete="CASCADE"), 
                            nullable=False, comment="브로커주문ID(FK)")
    fill_qty = Column(Integer, nullable=False, comment="체결수량")
    fill_price = Column(Numeric(20, 4), nullable=False, comment="체결가격")
    fee = Column(Numeric(20, 4), nullable=True, comment="수수료")
    liquidity_flag = Column(Text, nullable=True, comment="유동성플래그(maker/taker 등)")
    fill_status = Column(Text, nullable=False, comment="체결상태(UNFILLED/PARTIAL/FULL/CANCELLED/REJECTED)")
    filled_at = Column(DateTime(timezone=True), nullable=False, comment="체결시각")

    # 관계 정의
    broker_order = relationship("BrokerOrder", back_populates="fills")

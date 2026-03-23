from sqlalchemy import Column, String, Numeric, DateTime, Text, ForeignKey, PrimaryKeyConstraint, Integer, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID
from app.core.db import Base
from app.core.config import DB_SCHEMA
import enum

class MarketType(enum.Enum):
    """시장 구분"""
    KR = "KR"      # 국내
    OVRS = "OVRS"  # 해외

class AccountSnapshot(Base):
    """계좌 스냅샷 - 앱에서 보는 요약 계좌 정보 (예수금·평가액·손익 등)"""
    __tablename__ = "account_snapshot"
    __table_args__ = {"schema": DB_SCHEMA}

    snapshot_id = Column(BigInteger, primary_key=True, autoincrement=True, comment="스냅샷 고유 ID")
    asof_kst = Column(DateTime(timezone=True), nullable=False, comment="스냅샷 기준 시각 (KST 기준)")
    market = Column(String(10), nullable=False, comment="시장 구분 (KR/OVRS)")
    account_uid = Column(Text, nullable=False, comment="계좌 식별자 (CANO-ACNT_PRDT_CD 조합)")
    base_ccy = Column(Text, nullable=False, comment="계좌 기준 통화 (KRW 또는 USD 등)")
    cash_balance_ccy = Column(Numeric(20, 8), comment="예수금 (출금 가능 현금)")
    buying_power_ccy = Column(Numeric(20, 8), comment="매수가능금액 (가용자금, 미수 미포함 기준)")
    total_market_value_ccy = Column(Numeric(20, 8), comment="총 보유 종목 평가금액 (유가증권 평가액)")
    total_equity_ccy = Column(Numeric(20, 8), comment="총자산금액 (현금 + 평가금액 등)")
    pnl_amount_ccy = Column(Numeric(20, 8), comment="총평가손익금액")
    pnl_rate = Column(Numeric(10, 4), comment="총평가손익률(%)")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시 (서버 입력 기준)")
    
    # 관계 정의
    positions = relationship("PositionSnapshot", back_populates="account_snapshot", cascade="all, delete-orphan")

class PositionSnapshot(Base):
    """보유 종목 스냅샷 - 보유 종목 리스트 (수량·평단·현재가·손익 등)"""
    __tablename__ = "position_snapshot"
    __table_args__ = {"schema": DB_SCHEMA}

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="보유 종목 스냅샷 고유 ID")
    snapshot_id = Column(BigInteger, ForeignKey(f"{DB_SCHEMA}.account_snapshot.snapshot_id"), nullable=False, comment="계좌 스냅샷 ID")
    ticker_id = Column(Integer, nullable=True, comment="내부 종목 ID (티커 마스터 FK)")
    symbol = Column(Text, nullable=True, comment="종목심볼 또는 상품번호 (pdno)")
    exchange_code = Column(Text, nullable=False, comment="거래소 코드 (KOE, NASD, NYSE 등)")
    position_ccy = Column(Text, nullable=False, comment="포지션 통화 (KRW, USD, HKD, …)")
    qty = Column(Numeric(20, 8), nullable=False, comment="보유수량 (체결 기준)")
    orderable_qty = Column(Numeric(20, 8), comment="주문가능수량")
    avg_cost_ccy = Column(Numeric(20, 8), comment="매입평균단가")
    last_price_ccy = Column(Numeric(20, 8), comment="현재가")
    market_value_ccy = Column(Numeric(20, 8), comment="평가금액 (현재가 × 보유수량)")
    unrealized_pnl_ccy = Column(Numeric(20, 8), comment="평가손익금액")
    pnl_rate = Column(Numeric(10, 4), comment="평가손익률(%)")
    fx_krw_per_ccy = Column(Numeric(20, 8), nullable=True, comment="환율 (1통화당 원화가) — 해외 전용")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시 (서버 입력 기준)")
    
    # 관계 정의
    account_snapshot = relationship("AccountSnapshot", back_populates="positions")

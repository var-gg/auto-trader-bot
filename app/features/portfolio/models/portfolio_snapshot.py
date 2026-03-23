from sqlalchemy import Column, String, Numeric, DateTime, Text, ForeignKey, PrimaryKeyConstraint, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB
from app.core.db import Base
from app.core.config import DB_SCHEMA

class KISPortfolioSnapshot(Base):
    """KIS 포트폴리오 스냅샷"""
    __tablename__ = "kis_portfolio_snapshot"
    __table_args__ = {"schema": DB_SCHEMA}

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True, comment="스냅샷 ID")
    provider = Column(Text, nullable=False, comment="제공사 (KIS 등)")
    account_id = Column(Text, nullable=False, comment="계좌 식별자")
    venue_scope = Column(Text, nullable=False, comment="시장 범위 (US/KR/ALL)")
    asof_kst = Column(DateTime(timezone=True), nullable=False, comment="조회 기준 시각 (KST)")
    raw_json = Column(JSONB, nullable=False, comment="API 응답 원본 JSON")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    
    # 관계 정의
    positions = relationship("KISPositionExecbasis", back_populates="snapshot", cascade="all, delete-orphan")
    currencies = relationship("KISCurrencySummary", back_populates="snapshot", cascade="all, delete-orphan")
    account_totals = relationship("KISAccountTotals", back_populates="snapshot", uselist=False, cascade="all, delete-orphan")

class KISPositionExecbasis(Base):
    """종목별 체결기준 현재 잔고"""
    __tablename__ = "kis_position_execbasis"
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id", "symbol"),
        {"schema": DB_SCHEMA}
    )

    snapshot_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.kis_portfolio_snapshot.snapshot_id"), nullable=False, comment="스냅샷 ID")
    symbol = Column(Text, nullable=False, comment="종목코드 (티커)")
    name = Column(Text, comment="종목명")
    exchange_code = Column(Text, comment="거래소 코드 (NYSE/NASD 등)")
    ticker_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.ticker.id"), nullable=True, comment="티커 ID (핀헙 기반)")
    ccy = Column(Text, comment="통화코드 (USD/HKD/...)")
    pos_qty_exec = Column(Numeric(20, 8), comment="체결기준 현재 보유수량")
    orderable_qty = Column(Numeric(20, 8), comment="주문가능수량")
    avg_cost_ccy = Column(Numeric(20, 8), comment="평균매입단가 (통화 기준)")
    last_price_ccy = Column(Numeric(20, 8), comment="현재가 (통화 기준)")
    purchase_amt_ccy = Column(Numeric(20, 8), comment="매입금액 (통화 기준)")
    eval_amt_ccy = Column(Numeric(20, 8), comment="평가금액 (통화 기준)")
    pnl_ccy = Column(Numeric(20, 8), comment="평가손익금액 (통화 기준)")
    pnl_rate = Column(Numeric(10, 4), comment="평가손익율")
    fx_rate_krw_per_ccy = Column(Numeric(20, 8), comment="환율 (KRW/통화)")
    std_pdno = Column(Text, comment="표준상품번호 (ISIN 등)")
    product_type_cd = Column(Text, comment="상품유형코드")
    security_type = Column(Text, comment="유가증권구분명")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    
    # 관계 정의
    snapshot = relationship("KISPortfolioSnapshot", back_populates="positions")

class KISCurrencySummary(Base):
    """통화별 요약"""
    __tablename__ = "kis_currency_summary"
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id", "ccy"),
        {"schema": DB_SCHEMA}
    )

    snapshot_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.kis_portfolio_snapshot.snapshot_id"), nullable=False, comment="스냅샷 ID")
    ccy = Column(Text, nullable=False, comment="통화코드 (USD/HKD/JPY…)")
    buy_amt_ccy_sum = Column(Numeric(20, 8), comment="외화매수금액합계")
    sell_amt_ccy_sum = Column(Numeric(20, 8), comment="외화매도금액합계")
    cash_ccy = Column(Numeric(20, 8), comment="외화예수금액")
    locked_margin_ccy = Column(Numeric(20, 8), comment="외화매수증거금")
    other_margin_ccy = Column(Numeric(20, 8), comment="외화기타증거금")
    withdrawable_ccy = Column(Numeric(20, 8), comment="출금가능 외화금액")
    portfolio_eval_ccy = Column(Numeric(20, 8), comment="통화별 평가총액")
    first_fx_krw_per_ccy = Column(Numeric(20, 8), comment="최초고시환율")
    local_custody_flag = Column(Text, comment="현지보관통화여부")
    nextday_withdrawable_ccy = Column(Numeric(20, 8), comment="익일출금가능 외화금액")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    
    # 관계 정의
    snapshot = relationship("KISPortfolioSnapshot", back_populates="currencies")

class KISAccountTotals(Base):
    """계좌 전체 요약"""
    __tablename__ = "kis_account_totals"
    __table_args__ = {"schema": DB_SCHEMA}

    snapshot_id = Column(Integer, ForeignKey(f"{DB_SCHEMA}.kis_portfolio_snapshot.snapshot_id"), primary_key=True, comment="스냅샷 ID")
    total_assets_krw = Column(Numeric(20, 8), comment="총자산금액 (KRW)")
    total_deposit_krw = Column(Numeric(20, 8), comment="총예수금액 (KRW)")
    withdrawable_total_krw = Column(Numeric(20, 8), comment="인출가능총금액 (KRW)")
    usable_fx_total_ccy = Column(Numeric(20, 8), comment="외화사용가능금액합계")
    locked_margin_krw = Column(Numeric(20, 8), comment="매수증거금 총액 (KRW)")
    unsettled_buy_krw = Column(Numeric(20, 8), comment="미결제매수금액합계 (KRW)")
    unsettled_sell_krw = Column(Numeric(20, 8), comment="미결제매도금액합계 (KRW)")
    fx_balance_total_ccy = Column(Numeric(20, 8), comment="총외화잔고합계")
    total_eval_pnl_krw = Column(Numeric(20, 8), comment="총평가손익금액 (KRW)")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, comment="생성일시")
    
    # 관계 정의
    snapshot = relationship("KISPortfolioSnapshot", back_populates="account_totals")

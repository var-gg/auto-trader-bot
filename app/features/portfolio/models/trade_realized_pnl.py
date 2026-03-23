# app/features/portfolio/models/trade_realized_pnl.py

from sqlalchemy import Column, Integer, String, Date, Numeric, Text, TIMESTAMP
from sqlalchemy.sql import func
from app.core.db import Base


class TradeRealizedPnl(Base):
    """일자별 종목 손익 테이블 모델
    
    KIS API에서 조회한 해외주식과 국내주식의 실현손익 데이터를 통합 저장하는 테이블입니다.
    일자별, 시장별, 종목별로 매매 손익 정보를 관리합니다.
    """
    
    __tablename__ = "trade_realized_pnl"
    __table_args__ = {
        'schema': 'trading'
    }
    
    # 기본 키
    id = Column(Integer, primary_key=True, autoincrement=True, comment="내부 PK (auto)")
    
    # 기본 정보
    trade_date = Column(Date, nullable=False, comment="매매일자")
    market_type = Column(String(10), nullable=False, comment="시장구분: 'KR' | 'US'")
    exchange_code = Column(String(10), comment="거래소 코드 (e.g. KRX, NASDAQ)")
    symbol = Column(String(20), nullable=False, comment="종목코드 (e.g. 005930, AAPL)")
    instrument_name = Column(Text, nullable=False, comment="종목명")
    currency_code = Column(String(10), nullable=False, comment="통화코드 (KRW, USD, etc.)")
    
    # 매수 정보
    buy_qty = Column(Numeric(18, 4), comment="매수수량")
    buy_price = Column(Numeric(18, 4), comment="매입단가")
    buy_amount = Column(Numeric(18, 4), comment="매수금액")
    
    # 매도 정보
    sell_qty = Column(Numeric(18, 4), comment="매도수량")
    sell_price = Column(Numeric(18, 4), comment="매도가격")
    sell_amount = Column(Numeric(18, 4), comment="매도금액")
    
    # 손익 정보
    realized_pnl = Column(Numeric(18, 4), nullable=False, comment="실현손익금액")
    pnl_rate = Column(Numeric(9, 4), comment="손익률 (%)")
    
    # 비용 정보
    fee = Column(Numeric(18, 4), comment="수수료")
    tax = Column(Numeric(18, 4), comment="제세금")
    interest = Column(Numeric(18, 4), comment="대출이자 (국내만)")
    exchange_rate = Column(Numeric(18, 6), comment="환율 (해외만)")
    
    # 기타
    note = Column(Text, comment="비고 또는 원본 참조용")
    
    # 타임스탬프
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), comment="생성일시")
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), comment="수정일시")
    
    def __repr__(self):
        return f"<TradeRealizedPnl(id={self.id}, trade_date={self.trade_date}, market_type={self.market_type}, symbol={self.symbol})>"

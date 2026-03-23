# app/features/marketdata/services/us_price_detail_ingestor.py
from __future__ import annotations
from typing import Any, Dict, List
from sqlalchemy.orm import Session

from app.core.kis_client import KISClient
from app.features.marketdata.services.us_price_detail_parser import parse_kis_price_detail_payload
from app.features.marketdata.repositories.us_market_repository import USMarketRepository
from app.shared.models.ticker import Ticker

class USPriceDetailIngestor:
    """
    🇺🇸 미국주식 전용 현재가상세 API를 통한 실시간 데이터 수집
    - NMS(NASDAQ), NYQ(NYSE) 거래소만 지원
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = KISClient(db)
        self.repo = USMarketRepository(db)

    def _load_tickers(self, pairs: List[Dict[str, str]]) -> List[Ticker]:
        """
        미국주식 티커만 조회 (NMS, NYQ 거래소만)
        pairs 예: [{"symbol":"AAPL","exchange":"NMS"}, ...]
        """
        symbols = [p["symbol"] for p in pairs]
        exchanges = [p["exchange"] for p in pairs]
        q = self.db.query(Ticker).filter(
            Ticker.symbol.in_(symbols),
            Ticker.exchange.in_(exchanges),
            Ticker.exchange.in_(["NMS", "NYQ"])  # 미국주식 거래소만
        )
        tk_map = {(t.symbol, t.exchange): t for t in q.all()}
        return [tk_map.get((p["symbol"], p["exchange"])) for p in pairs if tk_map.get((p["symbol"], p["exchange"]))]

    def sync_price_detail_for_ticker_id(self, ticker_id: int) -> Dict[str, Any]:
        """
        🇺🇸 지정된 단일 미국주식 티커 ID에 대해 현재가상세 데이터 수집
        - 실시간 현재가 정보를 기존 일봉 테이블에 upsert
        - 수집된 데이터를 JSON 형태로 반환
        """
        import logging
        from datetime import date
        
        logger = logging.getLogger(__name__)
        
        logger.info(f"Starting US price detail sync for ticker_id: {ticker_id}")
        
        # 미국주식 티커 정보 조회
        ticker = self.db.query(Ticker).filter(
            Ticker.id == ticker_id,
            Ticker.exchange.in_(["NMS", "NYQ"])  # 미국주식 거래소만
        ).first()
        
        if not ticker:
            logger.warning(f"US stock ticker ID {ticker_id} not found in database")
            return {
                "status": "error",
                "message": f"US stock ticker ID {ticker_id} not found",
                "ticker_id": ticker_id,
                "upserted": 0
            }
        
        # 미국주식 거래소 확인
        if ticker.exchange not in ["NMS", "NYQ"]:
            logger.warning(f"Non-US exchange: {ticker.exchange} for ticker {ticker.symbol}")
            return {
                "status": "error",
                "message": f"Non-US exchange: {ticker.exchange}",
                "ticker_id": ticker_id,
                "upserted": 0
            }
        
        try:
            logger.info(f"Fetching US price detail for {ticker.symbol}:{ticker.exchange} (ticker_id: {ticker_id})")
            
            # 미국주식 현재가상세 API 호출
            payload = self.client.price_detail(symbol=ticker.symbol, exchange=ticker.exchange)
            logger.debug(f"KIS API response received for {ticker.symbol}:{ticker.exchange}")
            
            # 응답 파싱
            parsed_data = parse_kis_price_detail_payload(payload, ticker.symbol, ticker.exchange, ticker.id)
            
            if parsed_data is None:
                logger.warning(f"No valid price data found for {ticker.symbol}:{ticker.exchange}")
                return {
                    "status": "warning",
                    "message": "No valid price data found",
                    "ticker_id": ticker_id,
                    "upserted": 0,
                    "raw_response": payload
                }
            
            # 데이터베이스에 저장
            upserted_count = self.repo.upsert_daily_rows([parsed_data])
            
            logger.info(f"Successfully upserted {upserted_count} rows for {ticker.symbol}:{ticker.exchange}")
            
            return {
                "status": "success",
                "message": f"Successfully synced price detail for {ticker.symbol}:{ticker.exchange}",
                "ticker_id": ticker_id,
                "symbol": ticker.symbol,
                "exchange": ticker.exchange,
                "upserted": upserted_count,
                "data": parsed_data,
                "raw_response": payload
            }
            
        except Exception as e:
            logger.error(f"Error syncing US price detail for ticker_id {ticker_id}: {e}")
            return {
                "status": "error",
                "message": f"Error syncing price detail: {str(e)}",
                "ticker_id": ticker_id,
                "upserted": 0
            }

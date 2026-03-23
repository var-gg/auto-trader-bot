# app/features/marketdata/services/kr_price_detail_ingestor.py
from __future__ import annotations
from typing import Any, Dict, List
from sqlalchemy.orm import Session

from app.core.kis_client import KISClient
from app.features.marketdata.services.kr_price_detail_parser import parse_kis_kr_price_detail_payload
from app.features.marketdata.repositories.us_market_repository import USMarketRepository
from app.shared.models.ticker import Ticker
from app.features.marketdata.models.ohlcv_daily import OhlcvDaily
from sqlalchemy import desc

class KRPriceDetailIngestor:
    """
    🇰🇷 국내주식 전용 현재가 시세 API를 통한 실시간 데이터 수집
    
    KIS 국내주식현재가 시세 API (FHKST01010100) 사용
    - 실시간 현재가 정보 수집
    - 기존 일봉 테이블에 upsert 처리
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = KISClient(db)
        self.repo = USMarketRepository(db)

    def _load_tickers(self, pairs: List[Dict[str, str]]) -> List[Ticker]:
        """
        국내주식 티커만 조회 (KOE 거래소만)
        pairs 예: [{"symbol":"005930","exchange":"KOE"}, ...]
        """
        if not pairs:
            return []
        
        # 국내주식 티커만 조회 (KOE 거래소)
        symbols = [pair["symbol"] for pair in pairs]
        tickers = (
            self.db.query(Ticker)
            .filter(Ticker.symbol.in_(symbols))
            .filter(Ticker.exchange == "KOE")  # 국내주식만
            .all()
        )
        
        return tickers

    def sync_price_detail_for_ticker_id(self, ticker_id: int) -> Dict[str, Any]:
        """
        단일 티커 ID의 국내주식 현재가 데이터 수집
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            Dict: 수집 결과
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"Starting KR price detail sync for ticker_id: {ticker_id}")
            
            # 티커 정보 조회
            ticker = self.db.query(Ticker).filter(Ticker.id == ticker_id).first()
            if not ticker:
                return {
                    "status": "error",
                    "message": f"Ticker ID {ticker_id} not found",
                    "data": None
                }
            
            # 국내주식이 아닌 경우 오류
            if ticker.exchange != "KOE":
                return {
                    "status": "error",
                    "message": f"Ticker {ticker.symbol}:{ticker.exchange} is not a Korean stock",
                    "data": None
                }
            
            logger.info(f"Processing ticker: {ticker.symbol}:{ticker.exchange}")
            
            # KIS 국내주식현재가 시세 API 호출
            api_response = self.client.kr_current_price(ticker.symbol)
            
            if not api_response:
                return {
                    "status": "error",
                    "message": f"Failed to get API response for {ticker.symbol}",
                    "data": None
                }
            
            # 응답 파싱
            parsed_data = parse_kis_kr_price_detail_payload(
                api_response, 
                ticker.symbol, 
                ticker.exchange, 
                ticker_id
            )
            
            if not parsed_data:
                return {
                    "status": "error",
                    "message": f"Failed to parse API response for {ticker.symbol}",
                    "data": None
                }
            
            # 데이터베이스에 저장
            upserted_count = self.repo.upsert_daily_rows([parsed_data])
            
            logger.info(f"Successfully synced KR price detail for {ticker.symbol}:{ticker.exchange}")
            
            return {
                "status": "success",
                "message": f"Successfully synced data for {ticker.symbol}:{ticker.exchange}",
                "data": parsed_data
            }
            
        except Exception as e:
            logger.error(f"Error syncing KR price detail for ticker_id {ticker_id}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error syncing data: {str(e)}",
                "data": None
            }

    def sync_price_detail_for_ticker_ids(self, ticker_ids: List[int]) -> Dict[str, Any]:
        """
        여러 티커 ID들의 국내주식 현재가 데이터 수집
        
        Args:
            ticker_ids: 티커 ID 목록
            
        Returns:
            Dict: 수집 결과 요약
        """
        import logging
        logger = logging.getLogger(__name__)
        
        results = {}
        successful_count = 0
        failed_count = 0
        
        for ticker_id in ticker_ids:
            result = self.sync_price_detail_for_ticker_id(ticker_id)
            results[ticker_id] = result
            
            if result["status"] == "success":
                successful_count += 1
            else:
                failed_count += 1
        
        return {
            "status": "completed",
            "total_tickers": len(ticker_ids),
            "successful": successful_count,
            "failed": failed_count,
            "results": results
        }

    def get_price_detail(self, ticker_id: int) -> Dict[str, Any]:
        """
        데이터베이스에서 티커의 최신 현재가 정보를 조회합니다.
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            Dict: 현재가 정보 (close_price, trade_date 등)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            # 데이터베이스에서 해당 티커의 최신 일봉 데이터 조회
            latest_price = (
                self.db.query(OhlcvDaily)
                .filter(OhlcvDaily.ticker_id == ticker_id)
                .order_by(desc(OhlcvDaily.trade_date))
                .first()
            )
            
            if not latest_price:
                logger.warning(f"No price data found for ticker_id: {ticker_id}")
                return {}
            
            # 현재가 정보 반환
            price_data = {
                "close_price": latest_price.close,
                "open_price": latest_price.open,
                "high_price": latest_price.high,
                "low_price": latest_price.low,
                "volume": latest_price.volume,
                "trade_date": latest_price.trade_date.isoformat() if latest_price.trade_date else None,
                "is_final": latest_price.is_final,
                "source": latest_price.source,
                "ingested_at": latest_price.ingested_at.isoformat() if latest_price.ingested_at else None
            }
            
            logger.debug(f"Retrieved price data for ticker_id {ticker_id}: close={latest_price.close}")
            return price_data
            
        except Exception as e:
            logger.error(f"Error retrieving price detail for ticker_id {ticker_id}: {str(e)}")
            return {}
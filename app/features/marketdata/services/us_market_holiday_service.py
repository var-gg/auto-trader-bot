# app/features/marketdata/services/us_market_holiday_service.py
from datetime import datetime, date
from typing import List, Dict, Any
from sqlalchemy.orm import Session

from app.core.finnhub_client import FinnhubClient
from app.features.marketdata.repositories.us_market_holiday_repository import USMarketHolidayRepository

class USMarketHolidayService:
    """미국 마켓 휴일 관리 서비스"""
    
    def __init__(self, db: Session):
        self.db = db
        self.client = FinnhubClient()
        self.repo = USMarketHolidayRepository(db)
    
    def sync_holidays_for_exchange(self, exchange: str) -> Dict[str, Any]:
        """
        특정 거래소의 휴일 정보를 Finnhub에서 조회하여 DB에 저장
        - exchange: 거래소 코드 (예: US, KR, JP)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Starting holiday sync for exchange: {exchange}")
        
        try:
            # Finnhub에서 휴일 정보 조회
            api_response = self.client._make_request("stock/market-holiday", {"exchange": exchange})
            holidays_data = api_response.get("data", [])
            timezone = api_response.get("timezone", "")
            
            logger.info(f"Retrieved {len(holidays_data)} holidays from Finnhub for {exchange}, timezone: {timezone}")
            
            if not holidays_data:
                logger.warning(f"No holiday data received for exchange: {exchange}")
                return {"status": "warning", "message": f"No holiday data for {exchange}", "upserted": 0}
            
            # 데이터 변환 및 정리
            processed_holidays = self._process_holiday_data(holidays_data, exchange, timezone)
            logger.info(f"Processed {len(processed_holidays)} holidays for {exchange}")
            
            # DB에 upsert
            upserted_count = self.repo.upsert_holidays(processed_holidays)
            logger.info(f"Upserted {upserted_count} holidays for {exchange}")
            
            return {
                "status": "success",
                "exchange": exchange,
                "timezone": timezone,
                "raw_count": len(holidays_data),
                "processed_count": len(processed_holidays),
                "upserted": upserted_count
            }
            
        except Exception as e:
            logger.error(f"Error syncing holidays for {exchange}: {e}")
            return {
                "status": "error",
                "exchange": exchange,
                "error": str(e),
                "upserted": 0
            }
    
    def sync_holidays_for_multiple_exchanges(self, exchanges: List[str] = None) -> Dict[str, Any]:
        """
        여러 거래소의 휴일 정보를 한번에 동기화
        - exchanges: 거래소 코드 리스트 (기본값: ["US", "KR", "JP"])
        """
        import logging
        logger = logging.getLogger(__name__)
        
        if exchanges is None:
            exchanges = ["US", "KR", "JP"]
        
        logger.info(f"Starting bulk holiday sync for exchanges: {exchanges}")
        
        results = {}
        total_upserted = 0
        
        for exchange in exchanges:
            result = self.sync_holidays_for_exchange(exchange)
            results[exchange] = result
            if result.get("status") == "success":
                total_upserted += result.get("upserted", 0)
        
        successful_exchanges = [ex for ex, res in results.items() if res.get("status") == "success"]
        failed_exchanges = [ex for ex, res in results.items() if res.get("status") == "error"]
        
        logger.info(f"Bulk sync completed - Successful: {len(successful_exchanges)}, Failed: {len(failed_exchanges)}")
        
        return {
            "status": "completed",
            "total_exchanges": len(exchanges),
            "successful_exchanges": successful_exchanges,
            "failed_exchanges": failed_exchanges,
            "total_upserted": total_upserted,
            "details": results
        }
    
    def _process_holiday_data(self, holidays_data: List[Dict[str, Any]], exchange: str, timezone: str) -> List[Dict[str, Any]]:
        """
        Finnhub 휴일 데이터를 DB 저장 형식으로 변환
        실제 API 응답 구조: {"eventName": "Christmas", "atDate": "2023-12-25", "tradingHour": ""}
        """
        processed = []
        
        for holiday in holidays_data:
            try:
                # 날짜 파싱 (atDate 필드 사용)
                holiday_date_str = holiday.get("atDate", "")
                if not holiday_date_str:
                    continue
                
                # 날짜 형식 변환 (YYYY-MM-DD)
                holiday_date = datetime.strptime(holiday_date_str, "%Y-%m-%d").date()
                
                # 휴일명 추출 (eventName 필드 사용)
                event_name = holiday.get("eventName", "Unknown Holiday")
                
                # 거래 시간 추출 (tradingHour 필드 사용)
                trading_hour = holiday.get("tradingHour", "")
                
                # 거래소 개장 여부 판단 (tradingHour 필드로 판단)
                # tradingHour가 비어있으면 완전 휴일 (is_open = False)
                # tradingHour가 있으면 부분 개장 (is_open = True)
                is_open = bool(trading_hour.strip())
                
                processed_holiday = {
                    "exchange": exchange,
                    "timezone": timezone,
                    "at_date": holiday_date,
                    "event_name": event_name,
                    "trading_hour": trading_hour,
                    "is_open": is_open
                }
                
                processed.append(processed_holiday)
                
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Error processing holiday data: {holiday}, error: {e}")
                continue
        
        return processed

    def is_market_closed_now(self) -> bool:
        """
        현재 날짜가 휴장인지 판별 (뉴욕 시간 기준)
        - 주말 (토요일, 일요일) 체크
        - 완전휴장 (부분개장 제외) 체크
        """
        from datetime import datetime
        import pytz
        
        # 뉴욕 시간 기준 현재 날짜
        ny_tz = pytz.timezone('America/New_York')
        ny_now = datetime.now(ny_tz)
        today_ny = ny_now.date()
        
        # 1. 주말 체크 (토요일=5, 일요일=6)
        if today_ny.weekday() >= 5:  # 토요일(5) 또는 일요일(6)
            return True
        
        # 2. 완전휴장 체크 (부분개장 제외)
        holidays = self.repo.get_holidays_by_exchange("US", today_ny, today_ny)
        
        for holiday in holidays:
            # 완전휴장인 경우 (is_open = False)
            if holiday.at_date == today_ny and not holiday.is_open:
                return True
        
        # 주말도 아니고 완전휴장도 아니면 거래일
        return False
    
    def get_holidays_by_exchange(self, exchange: str, start_date: date = None, end_date: date = None) -> List:
        """거래소별 휴일 조회 (Repository 메서드 래핑)"""
        return self.repo.get_holidays_by_exchange(exchange, start_date, end_date)

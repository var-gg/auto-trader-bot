# app/features/marketdata/repositories/us_market_holiday_repository.py
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.shared.models.market_holiday import MarketHoliday

class USMarketHolidayRepository:
    """미국 마켓 휴일 데이터 관리 Repository"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def upsert_holidays(self, holidays_data: List[Dict[str, Any]]) -> int:
        """
        마켓 휴일 데이터를 upsert
        - holidays_data: [{"exchange": "US", "timezone": "America/New_York", "at_date": "2024-01-01", "event_name": "New Year's Day", "trading_hour": "", "is_open": False}, ...]
        """
        if not holidays_data:
            return 0
        
        # PostgreSQL UPSERT 사용
        stmt = insert(MarketHoliday).values(holidays_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['exchange', 'at_date'],
            set_={
                'timezone': stmt.excluded.timezone,
                'event_name': stmt.excluded.event_name,
                'trading_hour': stmt.excluded.trading_hour,
                'is_open': stmt.excluded.is_open
            }
        )
        
        result = self.db.execute(stmt)
        self.db.commit()
        return len(holidays_data)
    
    def get_holidays_by_exchange(self, exchange: str, start_date=None, end_date=None):
        """
        거래소별 휴일 조회
        - exchange: 거래소 코드
        - start_date, end_date: 날짜 범위 (선택사항)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            logger.info(f"get_holidays_by_exchange 시작 - exchange: {exchange}, start_date: {start_date}, end_date: {end_date}")
            
            logger.debug("DB 쿼리 생성 시작")
            query = self.db.query(MarketHoliday).filter(MarketHoliday.exchange == exchange)
            
            if start_date:
                logger.debug(f"start_date 필터 추가: {start_date}")
                query = query.filter(MarketHoliday.at_date >= start_date)
            if end_date:
                logger.debug(f"end_date 필터 추가: {end_date}")
                query = query.filter(MarketHoliday.at_date <= end_date)
            
            logger.debug("쿼리 실행 시작 (order_by + all())")
            # statement_timeout은 DB 엔진 레벨에서 설정됨 (10초)
            result = query.order_by(MarketHoliday.at_date).all()
            logger.info(f"쿼리 실행 완료 - 결과 수: {len(result)}")
            
            return result
            
        except Exception as e:
            logger.error(f"get_holidays_by_exchange 오류: {e}", exc_info=True)
            raise

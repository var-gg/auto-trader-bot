# app/features/premarket/repositories/position_maturity_repository.py
"""
포지션 만기 체크 Repository
- 마지막 BUY 체결 시점 조회
- 보유 영업일 수 계산
- future 값 기반 만기 판단
"""
from typing import Optional, Dict
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import text, func, and_

from app.features.premarket.repositories.optuna_repository import OptunaRepository
from app.features.marketdata.models.ohlcv_daily import OhlcvDaily


class PositionMaturityRepository:
    """포지션 만기 체크"""
    
    def __init__(self, db: Session):
        self.db = db
        self.optuna_repo = OptunaRepository(db)
    
    def get_future_days(self) -> int:
        """
        최신 promoted 설정의 future 값 조회
        
        Returns:
            future 값 (영업일 수, 기본 12)
        """
        config = self.optuna_repo.get_latest_promoted_config()
        if config and config.future:
            return int(config.future)
        return 12  # 기본값
    
    def get_reverse_breach_day_from_last_buy_plan(self, ticker_id: int) -> Optional[int]:
        """
        마지막 BUY 플랜의 reverse_breach_day 조회
        
        Args:
            ticker_id: 티커 ID
        
        Returns:
            reverse_breach_day (int) 또는 None
        """
        sql = """
        SELECT op.reverse_breach_day
        FROM trading.order_plan op
        JOIN trading.order_leg ol ON ol.plan_id = op.id
        JOIN trading.broker_order bo ON bo.leg_id = ol.id
        JOIN trading.order_fill of ON of.broker_order_id = bo.id
        WHERE op.ticker_id = :ticker_id
          AND op.action = 'BUY'
          AND ol.side = 'BUY'
          AND of.fill_status IN ('PARTIAL', 'FULL')
        ORDER BY of.filled_at DESC
        LIMIT 1
        """
        result = self.db.execute(text(sql), {"ticker_id": ticker_id}).fetchone()
        
        if result and result.reverse_breach_day is not None:
            return int(result.reverse_breach_day)
        
        return None
    
    def get_last_buy_fill_date(self, ticker_id: int) -> Optional[date]:
        """
        마지막 BUY 체결 시점 조회
        
        Args:
            ticker_id: 티커 ID
        
        Returns:
            마지막 체결일 (date) 또는 None
        """
        sql = """
        SELECT MAX(of.filled_at) as last_filled_at
        FROM trading.order_fill of
        JOIN trading.broker_order bo ON of.broker_order_id = bo.id
        JOIN trading.order_leg ol ON bo.leg_id = ol.id
        JOIN trading.order_plan op ON ol.plan_id = op.id
        WHERE op.ticker_id = :ticker_id
          AND ol.side = 'BUY'
          AND of.fill_status IN ('PARTIAL', 'FULL')
        """
        result = self.db.execute(text(sql), {"ticker_id": ticker_id}).fetchone()
        
        if result and result.last_filled_at:
            filled_at = result.last_filled_at
            # datetime → date 변환
            if isinstance(filled_at, datetime):
                return filled_at.date()
            return filled_at
        
        return None
    
    def count_business_days_held(self, ticker_id: int, from_date: date) -> int:
        """
        체결일 이후 영업일 수 계산
        
        Args:
            ticker_id: 티커 ID
            from_date: 시작일 (체결일)
        
        Returns:
            영업일 수 (is_final=true인 일봉 개수)
        """
        count = self.db.query(func.count(OhlcvDaily.id)).filter(
            OhlcvDaily.ticker_id == ticker_id,
            OhlcvDaily.trade_date > from_date,  # 체결일 이후
            OhlcvDaily.is_final == True
        ).scalar()
        
        return int(count or 0)
    
    def check_position_maturity(self, ticker_id: int, symbol: str) -> Dict:
        """
        포지션 만기 체크 (reverse_breach_day 우선 사용)
        
        Args:
            ticker_id: 티커 ID
            symbol: 심볼
        
        Returns:
            {
                "is_matured": bool,
                "future_days": int,  # 실제 사용된 만기 일수 (reverse_breach_day or future)
                "business_days_held": int,
                "last_buy_date": date or None,
                "remaining_days": int,
                "maturity_source": str  # "reverse_breach_day" or "future" or "none"
            }
        """
        # ✅ 1순위: 마지막 BUY 플랜의 reverse_breach_day
        reverse_breach_day = self.get_reverse_breach_day_from_last_buy_plan(ticker_id)
        
        # 2순위: 최신 promoted 설정의 future
        config_future = self.get_future_days()
        
        # 실제 만기 일수 결정
        if reverse_breach_day is not None:
            future_days = reverse_breach_day
            maturity_source = "reverse_breach_day"
        else:
            future_days = config_future
            maturity_source = "future"
        
        last_buy_date = self.get_last_buy_fill_date(ticker_id)
        
        if not last_buy_date:
            # 체결 이력 없음 (오류 상황)
            return {
                "is_matured": False,
                "future_days": future_days,
                "business_days_held": 0,
                "last_buy_date": None,
                "remaining_days": future_days,
                "maturity_source": "none",
                "note": "체결이력없음"
            }
        
        business_days_held = self.count_business_days_held(ticker_id, last_buy_date)
        is_matured = business_days_held >= future_days
        remaining_days = max(0, future_days - business_days_held)
        
        return {
            "is_matured": is_matured,
            "future_days": future_days,
            "business_days_held": business_days_held,
            "last_buy_date": last_buy_date,
            "remaining_days": remaining_days,
            "maturity_source": maturity_source,
            "note": f"만기{'도달' if is_matured else '미도달'} (출처: {maturity_source})"
        }


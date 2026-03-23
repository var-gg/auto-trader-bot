# app/features/signals/repositories/signal_repository.py
from __future__ import annotations
from datetime import date
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from app.features.marketdata.models.ohlcv_daily import OhlcvDaily


class SignalRepository:
    """
    시그널 탐지를 위한 일봉 데이터 조회 Repository
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_final_daily_data(
        self,
        ticker_id: int,
        limit: Optional[int] = None,
        order_desc: bool = True
    ) -> List[OhlcvDaily]:
        """
        특정 티커의 is_final=True인 일봉 데이터 조회
        
        Args:
            ticker_id: 티커 ID
            limit: 조회할 최대 개수 (None이면 전체)
            order_desc: True면 최신순(내림차순), False면 과거순(오름차순)
            
        Returns:
            일봉 데이터 리스트
        """
        query = self.db.query(OhlcvDaily).filter(
            and_(
                OhlcvDaily.ticker_id == ticker_id,
                OhlcvDaily.is_final == True
            )
        )
        
        if order_desc:
            query = query.order_by(OhlcvDaily.trade_date.desc())
        else:
            query = query.order_by(OhlcvDaily.trade_date.asc())
        
        if limit is not None:
            query = query.limit(limit)
        
        return query.all()
    
    def count_final_daily_data(self, ticker_id: int) -> int:
        """
        특정 티커의 is_final=True인 일봉 데이터 개수 조회
        
        Args:
            ticker_id: 티커 ID
            
        Returns:
            데이터 개수
        """
        return self.db.query(func.count(OhlcvDaily.id)).filter(
            and_(
                OhlcvDaily.ticker_id == ticker_id,
                OhlcvDaily.is_final == True
            )
        ).scalar() or 0
    
    def get_min_trade_date(self, ticker_id: int, is_final: Optional[bool] = None) -> Optional[date]:
        """
        특정 티커의 가장 과거 거래일 조회
        
        Args:
            ticker_id: 티커 ID
            is_final: None이면 전체, True/False면 필터링
            
        Returns:
            가장 과거 날짜 (데이터가 없으면 None)
        """
        query = self.db.query(func.min(OhlcvDaily.trade_date)).filter(
            OhlcvDaily.ticker_id == ticker_id
        )
        
        if is_final is not None:
            query = query.filter(OhlcvDaily.is_final == is_final)
        
        return query.scalar()
    
    def get_max_trade_date(self, ticker_id: int, is_final: Optional[bool] = None) -> Optional[date]:
        """
        특정 티커의 가장 최근 거래일 조회
        
        Args:
            ticker_id: 티커 ID
            is_final: None이면 전체, True/False면 필터링
            
        Returns:
            가장 최근 날짜 (데이터가 없으면 None)
        """
        query = self.db.query(func.max(OhlcvDaily.trade_date)).filter(
            OhlcvDaily.ticker_id == ticker_id
        )
        
        if is_final is not None:
            query = query.filter(OhlcvDaily.is_final == is_final)
        
        return query.scalar()
    
    def get_daily_data_as_dict(
        self,
        ticker_id: int,
        limit: Optional[int] = None,
        order_desc: bool = False
    ) -> List[Dict[str, Any]]:
        """
        특정 티커의 is_final=True인 일봉 데이터를 딕셔너리 형태로 조회
        
        Args:
            ticker_id: 티커 ID
            limit: 조회할 최대 개수 (None이면 전체)
            order_desc: True면 최신순, False면 과거순
            
        Returns:
            일봉 데이터 딕셔너리 리스트
        """
        records = self.get_final_daily_data(ticker_id, limit, order_desc)
        
        return [
            {
                "date": rec.trade_date,
                "open": rec.open,
                "high": rec.high,
                "low": rec.low,
                "close": rec.close,
                "volume": rec.volume,
            }
            for rec in records
        ]


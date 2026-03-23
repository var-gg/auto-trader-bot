# app/features/yahoo_finance/repositories/yahoo_index_repository.py

from sqlalchemy.orm import Session
from sqlalchemy import select, and_, desc
from app.features.yahoo_finance.models.yahoo_index_series import YahooIndexSeries
from app.features.yahoo_finance.models.yahoo_index_timeseries import YahooIndexTimeseries
from typing import List, Optional, Dict, Any
from datetime import date
import logging

logger = logging.getLogger(__name__)


class YahooIndexRepository:
    """야후 파이낸스 지수/환율 데이터 저장소"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_or_create_series(
        self,
        code: str,
        name: Optional[str] = None,
        provider: str = "yahoo_finance",
        freq: str = "daily",
        unit: Optional[str] = None
    ) -> YahooIndexSeries:
        """시리즈 조회 또는 생성
        
        Args:
            code: 심볼 코드
            name: 지수/환율 이름
            provider: 데이터 제공자
            freq: 데이터 빈도
            unit: 단위
            
        Returns:
            YahooIndexSeries: 시리즈 객체
        """
        series = self.db.query(YahooIndexSeries).filter(
            YahooIndexSeries.code == code
        ).first()
        
        if not series:
            series = YahooIndexSeries(
                code=code,
                name=name or code,
                provider=provider,
                freq=freq,
                unit=unit,
                active=True
            )
            self.db.add(series)
            self.db.flush()
            logger.info(f"새로운 시리즈 생성: {code}")
        
        return series
    
    def upsert_timeseries_data(
        self,
        series_id: int,
        data: List[Dict[str, Any]]
    ) -> tuple[int, int]:
        """시계열 데이터 삽입/업데이트
        
        Args:
            series_id: 시리즈 ID
            data: 데이터 배열 [{"date": date, "value": float}, ...]
            
        Returns:
            tuple[int, int]: (삽입 개수, 업데이트 개수)
        """
        inserted_count = 0
        updated_count = 0
        
        for item in data:
            d = item["date"]
            v = item["value"]
            
            existing = self.db.query(YahooIndexTimeseries).filter(
                and_(
                    YahooIndexTimeseries.series_id == series_id,
                    YahooIndexTimeseries.d == d
                )
            ).first()
            
            if existing:
                if existing.v != v:
                    existing.v = v
                    updated_count += 1
            else:
                timeseries = YahooIndexTimeseries(
                    series_id=series_id,
                    d=d,
                    v=v
                )
                self.db.add(timeseries)
                inserted_count += 1
        
        self.db.flush()
        return inserted_count, updated_count
    
    def get_timeseries_data(
        self,
        code: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Optional[tuple[YahooIndexSeries, List[YahooIndexTimeseries]]]:
        """시계열 데이터 조회
        
        Args:
            code: 심볼 코드
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            Optional[tuple]: (시리즈, 데이터 리스트) 또는 None
        """
        series = self.db.query(YahooIndexSeries).filter(
            YahooIndexSeries.code == code
        ).first()
        
        if not series:
            return None
        
        query = self.db.query(YahooIndexTimeseries).filter(
            YahooIndexTimeseries.series_id == series.id
        )
        
        if start_date:
            query = query.filter(YahooIndexTimeseries.d >= start_date)
        
        if end_date:
            query = query.filter(YahooIndexTimeseries.d <= end_date)
        
        data = query.order_by(YahooIndexTimeseries.d.asc()).all()
        
        return series, data
    
    def get_latest_date(self, code: str) -> Optional[date]:
        """특정 심볼의 최신 데이터 날짜 조회
        
        Args:
            code: 심볼 코드
            
        Returns:
            Optional[date]: 최신 날짜 또는 None
        """
        series = self.db.query(YahooIndexSeries).filter(
            YahooIndexSeries.code == code
        ).first()
        
        if not series:
            return None
        
        latest = self.db.query(YahooIndexTimeseries).filter(
            YahooIndexTimeseries.series_id == series.id
        ).order_by(desc(YahooIndexTimeseries.d)).first()
        
        return latest.d if latest else None
    
    def get_all_series(self) -> List[YahooIndexSeries]:
        """모든 시리즈 조회
        
        Returns:
            List[YahooIndexSeries]: 시리즈 리스트
        """
        return self.db.query(YahooIndexSeries).filter(
            YahooIndexSeries.active == True
        ).all()


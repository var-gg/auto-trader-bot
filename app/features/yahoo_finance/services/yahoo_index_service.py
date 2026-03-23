# app/features/yahoo_finance/services/yahoo_index_service.py

from sqlalchemy.orm import Session
from app.features.yahoo_finance.repositories.yahoo_index_repository import YahooIndexRepository
from app.features.yahoo_finance.models.yahoo_finance_models import (
    YahooIndexIngestRequest,
    YahooIndexIngestResponse,
    YahooIndexIngestResult,
    YahooIndexQueryRequest,
    YahooIndexQueryResponse,
    YahooIndexDataPoint
)
from typing import List, Dict, Any
from datetime import datetime, date
import logging
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)


class YahooIndexService:
    """야후 파이낸스 지수/환율 데이터 수집 서비스"""
    
    # 수집 대상 심볼 (고정)
    TARGET_SYMBOLS = {
        "^GSPC": {"name": "S&P 500", "unit": "points"},
        "^KS200": {"name": "KOSPI200", "unit": "points"},
        "KRW=X": {"name": "USD/KRW", "unit": "KRW"}
    }
    
    def __init__(self, db: Session):
        self.db = db
        self.repository = YahooIndexRepository(db)
    
    async def ingest_data(self, request: YahooIndexIngestRequest) -> YahooIndexIngestResponse:
        """지수/환율 데이터 수집 및 저장
        
        Args:
            request: 데이터 수집 요청 (period만 포함)
            
        Returns:
            YahooIndexIngestResponse: 수집 결과
        """
        logger.info(f"야후 파이낸스 데이터 수집 시작 - period: {request.period}")
        
        results = []
        successful_count = 0
        failed_count = 0
        
        for symbol, info in self.TARGET_SYMBOLS.items():
            try:
                logger.info(f"심볼 '{symbol}' 데이터 수집 중...")
                
                # yfinance로 데이터 조회
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=request.period, interval="1d")
                
                if hist.empty:
                    logger.warning(f"심볼 '{symbol}'의 데이터가 없습니다")
                    results.append(YahooIndexIngestResult(
                        symbol=symbol,
                        success=False,
                        data_count=0,
                        inserted_count=0,
                        updated_count=0,
                        error="No data available"
                    ))
                    failed_count += 1
                    continue
                
                # 시리즈 조회/생성
                series = self.repository.get_or_create_series(
                    code=symbol,
                    name=info["name"],
                    provider="yahoo_finance",
                    freq="daily",
                    unit=info["unit"]
                )
                
                # 데이터 변환 (Close 가격만 사용)
                data = []
                for idx, row in hist.iterrows():
                    if pd.notna(row['Close']):
                        date_obj = idx.date() if isinstance(idx, pd.Timestamp) else idx
                        data.append({
                            "date": date_obj,
                            "value": float(row['Close'])
                        })
                
                # DB에 저장
                inserted_count, updated_count = self.repository.upsert_timeseries_data(
                    series_id=series.id,
                    data=data
                )
                
                self.db.commit()
                
                logger.info(
                    f"심볼 '{symbol}' 수집 완료 - "
                    f"조회: {len(data)}건, 삽입: {inserted_count}건, 업데이트: {updated_count}건"
                )
                
                results.append(YahooIndexIngestResult(
                    symbol=symbol,
                    success=True,
                    data_count=len(data),
                    inserted_count=inserted_count,
                    updated_count=updated_count,
                    error=None
                ))
                successful_count += 1
                
            except Exception as e:
                self.db.rollback()
                logger.error(f"심볼 '{symbol}' 수집 중 오류 발생: {str(e)}")
                results.append(YahooIndexIngestResult(
                    symbol=symbol,
                    success=False,
                    data_count=0,
                    inserted_count=0,
                    updated_count=0,
                    error=str(e)
                ))
                failed_count += 1
        
        all_success = failed_count == 0
        
        logger.info(
            f"야후 파이낸스 데이터 수집 완료 - "
            f"전체: {len(self.TARGET_SYMBOLS)}개, 성공: {successful_count}개, 실패: {failed_count}개"
        )
        
        return YahooIndexIngestResponse(
            success=all_success,
            period=request.period,
            total_symbols=len(self.TARGET_SYMBOLS),
            successful_symbols=successful_count,
            failed_symbols=failed_count,
            results=results,
            error=None if all_success else f"{failed_count}개 심볼 수집 실패"
        )
    
    async def query_data(self, request: YahooIndexQueryRequest) -> YahooIndexQueryResponse:
        """지수/환율 데이터 조회
        
        Args:
            request: 데이터 조회 요청
            
        Returns:
            YahooIndexQueryResponse: 조회 결과
        """
        try:
            logger.info(
                f"야후 파이낸스 데이터 조회 - "
                f"심볼: {request.symbol}, 기간: {request.start_date} ~ {request.end_date}"
            )
            
            result = self.repository.get_timeseries_data(
                code=request.symbol,
                start_date=request.start_date,
                end_date=request.end_date
            )
            
            if not result:
                return YahooIndexQueryResponse(
                    success=False,
                    symbol=request.symbol,
                    name=None,
                    data_count=0,
                    data=[],
                    error=f"Symbol '{request.symbol}' not found in database"
                )
            
            series, timeseries_data = result
            
            data_points = [
                YahooIndexDataPoint(d=ts.d, value=ts.v)
                for ts in timeseries_data
            ]
            
            logger.info(
                f"야후 파이낸스 데이터 조회 완료 - "
                f"심볼: {request.symbol}, 데이터: {len(data_points)}건"
            )
            
            return YahooIndexQueryResponse(
                success=True,
                symbol=request.symbol,
                name=series.name,
                data_count=len(data_points),
                data=data_points,
                error=None
            )
            
        except Exception as e:
            logger.error(f"야후 파이낸스 데이터 조회 중 오류 발생: {str(e)}")
            return YahooIndexQueryResponse(
                success=False,
                symbol=request.symbol,
                name=None,
                data_count=0,
                data=[],
                error=str(e)
            )


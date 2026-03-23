# app/features/yahoo_finance_test/services/yahoo_finance_test_service.py

from typing import Dict, Any, List
from sqlalchemy.orm import Session
from app.features.yahoo_finance_test.models.yahoo_finance_test_models import (
    YFDailyPriceRequest,
    YFDailyPriceResponse,
    YFDailyPriceData,
    YFMultiSymbolRequest,
    YFMultiSymbolResponse,
    YFServiceInfo
)
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class YahooFinanceTestService:
    """야후 파이낸스 API 테스트 서비스
    
    야후 파이낸스 API를 테스트하기 위한 서비스입니다.
    지수 및 환율 데이터를 조회하고 결과를 반환합니다.
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    async def get_daily_price(self, request: YFDailyPriceRequest) -> YFDailyPriceResponse:
        """일봉 데이터 조회
        
        야후 파이낸스 API를 통해 지정된 심볼의 일봉 데이터를 조회합니다.
        
        Args:
            request: 일봉 데이터 조회 요청 파라미터
            
        Returns:
            YFDailyPriceResponse: 일봉 데이터 응답
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"야후 파이낸스 일봉 데이터 조회 요청 - 심볼: {request.symbol}, 기간: {request.period}, 간격: {request.interval}")
            
            # yfinance를 통해 데이터 조회
            ticker = yf.Ticker(request.symbol)
            hist = ticker.history(period=request.period, interval=request.interval)
            
            if hist.empty:
                logger.warning(f"조회 결과가 없습니다 - 심볼: {request.symbol}")
                return YFDailyPriceResponse(
                    success=False,
                    symbol=request.symbol,
                    period=request.period,
                    interval=request.interval,
                    data=None,
                    metadata=None,
                    error=f"No data found for symbol: {request.symbol}",
                    raw_response=None
                )
            
            # 데이터 변환
            price_data = []
            for idx, row in hist.iterrows():
                # pandas Timestamp를 문자열로 변환
                date_str = idx.strftime('%Y-%m-%d') if isinstance(idx, pd.Timestamp) else str(idx)
                
                price_data.append(YFDailyPriceData(
                    date=date_str,
                    open=float(row['Open']) if pd.notna(row['Open']) else None,
                    high=float(row['High']) if pd.notna(row['High']) else None,
                    low=float(row['Low']) if pd.notna(row['Low']) else None,
                    close=float(row['Close']) if pd.notna(row['Close']) else None,
                    adj_close=float(row['Close']) if pd.notna(row['Close']) else None,  # yfinance는 기본적으로 조정 종가 제공
                    volume=int(row['Volume']) if pd.notna(row['Volume']) else None
                ))
            
            # 메타데이터 생성
            ticker_info = {}
            try:
                info = ticker.info
                ticker_info = {
                    "symbol": request.symbol,
                    "shortName": info.get("shortName", ""),
                    "longName": info.get("longName", ""),
                    "currency": info.get("currency", ""),
                    "exchangeName": info.get("exchangeName", ""),
                    "timezone": info.get("timeZone", "")
                }
            except Exception as e:
                logger.warning(f"메타데이터 조회 실패: {str(e)}")
                ticker_info = {"symbol": request.symbol}
            
            # 원본 응답 데이터 저장 (DataFrame을 dict로 변환)
            raw_response = {
                "history": hist.to_dict(orient='index'),
                "info": ticker_info
            }
            
            # 날짜를 문자열로 변환
            raw_response_serializable = {
                "history": {str(k): v for k, v in raw_response["history"].items()},
                "info": ticker_info
            }
            
            logger.info(f"야후 파이낸스 일봉 데이터 조회 완료 - 심볼: {request.symbol}, 데이터 개수: {len(price_data)}")
            
            return YFDailyPriceResponse(
                success=True,
                symbol=request.symbol,
                period=request.period,
                interval=request.interval,
                data=price_data,
                metadata=ticker_info,
                error=None,
                raw_response=raw_response_serializable
            )
            
        except Exception as e:
            logger.error(f"야후 파이낸스 일봉 데이터 조회 중 오류 발생: {str(e)}")
            return YFDailyPriceResponse(
                success=False,
                symbol=request.symbol,
                period=request.period,
                interval=request.interval,
                data=None,
                metadata=None,
                error=str(e),
                raw_response=None
            )
    
    async def get_multi_symbol_data(self, request: YFMultiSymbolRequest) -> YFMultiSymbolResponse:
        """다중 심볼 데이터 조회
        
        여러 심볼의 일봉 데이터를 한 번에 조회합니다.
        
        Args:
            request: 다중 심볼 조회 요청 파라미터
            
        Returns:
            YFMultiSymbolResponse: 다중 심볼 조회 응답
            
        Raises:
            Exception: API 호출 중 오류 발생시
        """
        try:
            logger.info(f"야후 파이낸스 다중 심볼 조회 요청 - 심볼: {request.symbols}, 기간: {request.period}, 간격: {request.interval}")
            
            results = {}
            all_success = True
            
            for symbol in request.symbols:
                symbol_request = YFDailyPriceRequest(
                    symbol=symbol,
                    period=request.period,
                    interval=request.interval
                )
                
                result = await self.get_daily_price(symbol_request)
                results[symbol] = result
                
                if not result.success:
                    all_success = False
            
            logger.info(f"야후 파이낸스 다중 심볼 조회 완료 - 전체 성공: {all_success}")
            
            return YFMultiSymbolResponse(
                success=all_success,
                period=request.period,
                interval=request.interval,
                results=results,
                error=None if all_success else "일부 심볼 조회 실패"
            )
            
        except Exception as e:
            logger.error(f"야후 파이낸스 다중 심볼 조회 중 오류 발생: {str(e)}")
            return YFMultiSymbolResponse(
                success=False,
                period=request.period,
                interval=request.interval,
                results={},
                error=str(e)
            )
    
    def get_service_info(self) -> YFServiceInfo:
        """서비스 정보 반환
        
        야후 파이낸스 테스트 서비스의 기본 정보를 반환합니다.
        
        Returns:
            YFServiceInfo: 서비스 정보
        """
        return YFServiceInfo(
            service_name="Yahoo Finance Test Service",
            description="야후 파이낸스 API 테스트 서비스 - 지수 및 환율 데이터 조회",
            supported_symbols=[
                {"symbol": "^GSPC", "name": "S&P 500", "description": "S&P 500 Index"},
                {"symbol": "^KS11", "name": "KOSPI", "description": "Korea Stock Price Index"},
                {"symbol": "^KS200", "name": "KOSPI200", "description": "Korea Stock Price Index 200"},
                {"symbol": "KRW=X", "name": "USD/KRW", "description": "US Dollar / Korean Won Exchange Rate"},
                {"symbol": "^DJI", "name": "Dow Jones", "description": "Dow Jones Industrial Average"},
                {"symbol": "^IXIC", "name": "NASDAQ", "description": "NASDAQ Composite"},
                {"symbol": "^N225", "name": "Nikkei 225", "description": "Nikkei 225 Index"},
                {"symbol": "^HSI", "name": "Hang Seng", "description": "Hang Seng Index"}
            ],
            supported_periods=[
                "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
            ],
            supported_intervals=[
                "1d", "5d", "1wk", "1mo", "3mo"
            ],
            version="1.0.0"
        )


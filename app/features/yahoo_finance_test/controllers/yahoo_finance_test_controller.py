# app/features/yahoo_finance_test/controllers/yahoo_finance_test_controller.py

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Dict, Any
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.yahoo_finance_test.models.yahoo_finance_test_models import (
    YFDailyPriceRequest,
    YFDailyPriceResponse,
    YFMultiSymbolRequest,
    YFMultiSymbolResponse,
    YFServiceInfo
)
from app.features.yahoo_finance_test.services.yahoo_finance_test_service import YahooFinanceTestService
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/yahoo-finance-test", tags=["Yahoo Finance Test"])


@router.get("/daily-price", response_model=YFDailyPriceResponse, summary="일봉 데이터 조회 (GET)")
async def get_daily_price(
    symbol: str = Query(default="^GSPC", description="심볼 코드 (^GSPC: S&P 500, ^KS11: KOSPI, ^KS200: KOSPI200, KRW=X: USD/KRW)"),
    period: str = Query(default="1mo", description="조회 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"),
    interval: str = Query(default="1d", description="데이터 간격 (1d: 일봉, 5d: 5일봉, 1wk: 주봉, 1mo: 월봉, 3mo: 분기봉)"),
    db: Session = Depends(get_db)
):
    """
    일봉 데이터 조회 (GET 방식)
    
    야후 파이낸스 API를 통해 지정된 심볼의 일봉 데이터를 조회합니다.
    GET 방식으로 쿼리 파라미터를 사용하여 데이터를 조회합니다.
    
    **지원 심볼:**
    - ^GSPC: S&P 500 Index
    - ^KS11: KOSPI (Korea Stock Price Index)
    - ^KS200: KOSPI200
    - KRW=X: USD/KRW Exchange Rate
    - ^DJI: Dow Jones Industrial Average
    - ^IXIC: NASDAQ Composite
    - ^N225: Nikkei 225
    - ^HSI: Hang Seng Index
    
    **조회 기간:**
    - 1d: 1일
    - 5d: 5일
    - 1mo: 1개월 (기본값)
    - 3mo: 3개월
    - 6mo: 6개월
    - 1y: 1년
    - 2y: 2년
    - 5y: 5년
    - 10y: 10년
    - ytd: 연초부터 현재까지
    - max: 전체 기간
    
    **데이터 간격:**
    - 1d: 일봉 (기본값)
    - 5d: 5일봉
    - 1wk: 주봉
    - 1mo: 월봉
    - 3mo: 분기봉
    
    **응답 데이터:**
    - success: 성공 여부
    - symbol: 조회한 심볼
    - period: 조회 기간
    - interval: 데이터 간격
    - data: 일봉 데이터 배열 (날짜, 시가, 고가, 저가, 종가, 조정종가, 거래량)
    - metadata: 메타데이터 (심볼 정보, 통화, 거래소 등)
    - raw_response: 원본 응답 데이터
    
    **사용 예시:**
    - S&P 500 최근 1개월: /yahoo-finance-test/daily-price?symbol=^GSPC&period=1mo
    - KOSPI 최근 3개월: /yahoo-finance-test/daily-price?symbol=^KS11&period=3mo
    - USD/KRW 최근 1년: /yahoo-finance-test/daily-price?symbol=KRW=X&period=1y
    """
    try:
        logger.info(f"야후 파이낸스 일봉 데이터 조회 (GET) - 심볼: {symbol}, 기간: {period}, 간격: {interval}")
        
        request = YFDailyPriceRequest(
            symbol=symbol,
            period=period,
            interval=interval
        )
        
        service = YahooFinanceTestService(db)
        response = await service.get_daily_price(request)
        
        if response.success:
            logger.info(f"야후 파이낸스 일봉 데이터 조회 완료 - 심볼: {symbol}, 데이터 개수: {len(response.data) if response.data else 0}")
        else:
            logger.warning(f"야후 파이낸스 일봉 데이터 조회 실패 - 심볼: {symbol}, 에러: {response.error}")
        
        return response
        
    except Exception as e:
        logger.error(f"야후 파이낸스 일봉 데이터 조회 (GET) 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.post("/daily-price", response_model=YFDailyPriceResponse, summary="일봉 데이터 조회 (POST)")
async def post_daily_price(
    request: YFDailyPriceRequest = Body(...),
    db: Session = Depends(get_db)
):
    """
    일봉 데이터 조회 (POST 방식)
    
    야후 파이낸스 API를 통해 지정된 심볼의 일봉 데이터를 조회합니다.
    POST 방식으로 JSON 바디를 사용하여 데이터를 조회합니다.
    
    **요청 예시:**
    ```json
    {
        "symbol": "^GSPC",
        "period": "1mo",
        "interval": "1d"
    }
    ```
    
    **지원 심볼:**
    - ^GSPC: S&P 500 Index
    - ^KS11: KOSPI (Korea Stock Price Index)
    - ^KS200: KOSPI200
    - KRW=X: USD/KRW Exchange Rate
    
    **조회 기간:** 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    
    **데이터 간격:** 1d, 5d, 1wk, 1mo, 3mo
    """
    try:
        logger.info(f"야후 파이낸스 일봉 데이터 조회 (POST) - 심볼: {request.symbol}, 기간: {request.period}, 간격: {request.interval}")
        
        service = YahooFinanceTestService(db)
        response = await service.get_daily_price(request)
        
        if response.success:
            logger.info(f"야후 파이낸스 일봉 데이터 조회 완료 - 심볼: {request.symbol}, 데이터 개수: {len(response.data) if response.data else 0}")
        else:
            logger.warning(f"야후 파이낸스 일봉 데이터 조회 실패 - 심볼: {request.symbol}, 에러: {response.error}")
        
        return response
        
    except Exception as e:
        logger.error(f"야후 파이낸스 일봉 데이터 조회 (POST) 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.post("/multi-symbol", response_model=YFMultiSymbolResponse, summary="다중 심볼 데이터 조회")
async def get_multi_symbol_data(
    request: YFMultiSymbolRequest = Body(...),
    db: Session = Depends(get_db)
):
    """
    다중 심볼 데이터 조회
    
    여러 심볼의 일봉 데이터를 한 번에 조회합니다.
    
    **요청 예시:**
    ```json
    {
        "symbols": ["^GSPC", "^KS11", "KRW=X"],
        "period": "1mo",
        "interval": "1d"
    }
    ```
    
    **응답 데이터:**
    - success: 전체 성공 여부
    - period: 조회 기간
    - interval: 데이터 간격
    - results: 심볼별 조회 결과 딕셔너리
    
    **사용 예시:**
    - S&P 500, KOSPI, USD/KRW 동시 조회
    - 여러 지수를 비교하기 위한 데이터 조회
    """
    try:
        logger.info(f"야후 파이낸스 다중 심볼 조회 - 심볼: {request.symbols}, 기간: {request.period}, 간격: {request.interval}")
        
        service = YahooFinanceTestService(db)
        response = await service.get_multi_symbol_data(request)
        
        logger.info(f"야후 파이낸스 다중 심볼 조회 완료 - 전체 성공: {response.success}")
        
        return response
        
    except Exception as e:
        logger.error(f"야후 파이낸스 다중 심볼 조회 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"다중 심볼 조회 중 오류가 발생했습니다: {str(e)}")


@router.get("/info", response_model=YFServiceInfo, summary="야후 파이낸스 테스트 서비스 정보")
async def get_service_info(db: Session = Depends(get_db)):
    """
    야후 파이낸스 테스트 서비스 정보
    
    야후 파이낸스 테스트 서비스의 기본 정보와 지원 기능을 반환합니다.
    서비스 이름, 설명, 지원 심볼, 지원 기간, 지원 간격, 버전 정보를 확인할 수 있습니다.
    """
    try:
        service = YahooFinanceTestService(db)
        return service.get_service_info()
        
    except Exception as e:
        logger.error(f"서비스 정보 조회 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail="서비스 정보 조회 중 오류가 발생했습니다.")


@router.get("/health", summary="야후 파이낸스 테스트 서비스 상태 확인")
async def health_check():
    """
    야후 파이낸스 테스트 서비스 상태 확인
    
    야후 파이낸스 테스트 서비스의 현재 상태를 확인합니다.
    서비스가 정상적으로 동작하는지 체크할 수 있습니다.
    """
    return {
        "status": "healthy",
        "service": "yahoo-finance-test",
        "description": "야후 파이낸스 API 테스트 서비스"
    }


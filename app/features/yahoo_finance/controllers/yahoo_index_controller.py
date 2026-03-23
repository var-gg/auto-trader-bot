# app/features/yahoo_finance/controllers/yahoo_index_controller.py

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from sqlalchemy.orm import Session
from app.core.db import get_db
from app.features.yahoo_finance.models.yahoo_finance_models import (
    YahooIndexIngestRequest,
    YahooIndexIngestResponse,
    YahooIndexQueryRequest,
    YahooIndexQueryResponse
)
from app.features.yahoo_finance.services.yahoo_index_service import YahooIndexService
from datetime import date
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/yahoo-finance", tags=["Yahoo Finance"])


@router.post("/ingest", response_model=YahooIndexIngestResponse, summary="지수/환율 데이터 수집")
async def ingest_data(
    period: str = Query(default="1mo", description="조회 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"),
    db: Session = Depends(get_db)
):
    """
    지수/환율 데이터 수집 및 DB 저장
    
    **고정 심볼 (3개):**
    - ^GSPC: S&P 500
    - ^KS200: KOSPI200
    - KRW=X: USD/KRW 환율
    
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
    
    **동작:**
    1. 야후 파이낸스 API를 통해 3개 심볼의 일봉 데이터 조회
    2. 각 심볼의 Close 가격을 DB에 저장
    3. 기존 데이터가 있으면 업데이트, 없으면 새로 삽입
    
    **응답:**
    - success: 전체 성공 여부
    - total_symbols: 처리한 심볼 개수 (3)
    - successful_symbols: 성공한 심볼 개수
    - failed_symbols: 실패한 심볼 개수
    - results: 심볼별 수집 결과 (조회 건수, 삽입 건수, 업데이트 건수)
    
    **사용 예시:**
    - 최근 1개월: /yahoo-finance/ingest?period=1mo
    - 최근 1년: /yahoo-finance/ingest?period=1y
    - 전체 기간: /yahoo-finance/ingest?period=max
    """
    try:
        logger.info(f"야후 파이낸스 데이터 수집 요청 - period: {period}")
        
        request = YahooIndexIngestRequest(period=period)
        
        service = YahooIndexService(db)
        response = await service.ingest_data(request)
        
        if response.success:
            logger.info(
                f"야후 파이낸스 데이터 수집 완료 - "
                f"성공: {response.successful_symbols}/{response.total_symbols}"
            )
        else:
            logger.warning(
                f"야후 파이낸스 데이터 수집 일부 실패 - "
                f"성공: {response.successful_symbols}, 실패: {response.failed_symbols}"
            )
        
        return response
        
    except Exception as e:
        logger.error(f"야후 파이낸스 데이터 수집 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"데이터 수집 중 오류가 발생했습니다: {str(e)}")


@router.get("/query", response_model=YahooIndexQueryResponse, summary="지수/환율 데이터 조회")
async def query_data(
    symbol: str = Query(..., description="심볼 코드 (^GSPC, ^KS200, KRW=X)"),
    start_date: Optional[date] = Query(None, description="시작 날짜 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="종료 날짜 (YYYY-MM-DD)"),
    db: Session = Depends(get_db)
):
    """
    지수/환율 데이터 조회
    
    DB에 저장된 지수/환율 데이터를 조회합니다.
    
    **지원 심볼:**
    - ^GSPC: S&P 500
    - ^KS200: KOSPI200
    - KRW=X: USD/KRW 환율
    
    **조회 옵션:**
    - start_date와 end_date를 모두 생략하면 전체 데이터 조회
    - start_date만 입력하면 해당 날짜 이후 데이터 조회
    - end_date만 입력하면 해당 날짜 이전 데이터 조회
    - 둘 다 입력하면 해당 기간 데이터 조회
    
    **응답:**
    - success: 성공 여부
    - symbol: 조회한 심볼
    - name: 지수/환율 이름
    - data_count: 조회된 데이터 개수
    - data: 데이터 배열 (날짜, 값)
    
    **사용 예시:**
    - S&P 500 전체: /yahoo-finance/query?symbol=^GSPC
    - KOSPI200 2024년: /yahoo-finance/query?symbol=^KS200&start_date=2024-01-01&end_date=2024-12-31
    - USD/KRW 최근 30일: /yahoo-finance/query?symbol=KRW=X&start_date=2024-09-01
    """
    try:
        logger.info(
            f"야후 파이낸스 데이터 조회 - "
            f"심볼: {symbol}, 기간: {start_date} ~ {end_date}"
        )
        
        request = YahooIndexQueryRequest(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        service = YahooIndexService(db)
        response = await service.query_data(request)
        
        if response.success:
            logger.info(
                f"야후 파이낸스 데이터 조회 완료 - "
                f"심볼: {symbol}, 데이터: {response.data_count}건"
            )
        else:
            logger.warning(f"야후 파이낸스 데이터 조회 실패 - 심볼: {symbol}, 에러: {response.error}")
        
        return response
        
    except Exception as e:
        logger.error(f"야후 파이낸스 데이터 조회 중 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"데이터 조회 중 오류가 발생했습니다: {str(e)}")


@router.get("/health", summary="야후 파이낸스 서비스 상태 확인")
async def health_check():
    """
    야후 파이낸스 서비스 상태 확인
    
    야후 파이낸스 서비스의 현재 상태를 확인합니다.
    서비스가 정상적으로 동작하는지 체크할 수 있습니다.
    """
    return {
        "status": "healthy",
        "service": "yahoo-finance",
        "description": "야후 파이낸스 지수/환율 데이터 수집 서비스",
        "target_symbols": ["^GSPC", "^KS200", "KRW=X"]
    }


# app/features/news/controllers/kis_news_controller.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Optional, Any, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.core.db import get_db
from app.core.kis_client import KISClient
from app.features.news.repositories.kis_news_repository import KisNewsRepository
from app.features.news.services.kis_news_service import KisNewsService
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

KST = ZoneInfo("Asia/Seoul")


# ========== Request Models ==========

class KisNewsIngestRequest(BaseModel):
    """KIS 뉴스 적재 요청"""
    date: Optional[str] = Field(None, description="날짜 (YYYYMMDD 형식, 미지정 시 현재)")
    time: Optional[str] = Field(None, description="시각 (HHMMSS 형식, 미지정 시 현재)")


# ========== Response Models ==========

class KisNewsIngestResponse(BaseModel):
    """KIS 뉴스 적재 응답"""
    success: int
    skipped: int
    errors: int
    error_details: List[dict] = []


class KisNewsItemResponse(BaseModel):
    """KIS 뉴스 아이템 응답"""
    id: int
    source_type: str
    source_key: str
    ticker_id: int
    title: str
    published_at: datetime
    publisher: Optional[str]
    class_cd: Optional[str]
    class_name: Optional[str]
    nation_cd: Optional[str]
    exchange_cd: Optional[str]
    symbol: Optional[str]
    symbol_name: Optional[str]
    kr_iscd: Optional[str]
    lang: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ========== Helper Functions ==========

def get_current_kis_datetime() -> tuple[str, str]:
    """현재 KST 날짜/시간을 KIS 포맷으로 반환"""
    now = datetime.now(KST)
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")
    return date_str, time_str


# ========== Endpoints ==========

@router.post("/kis-news/ingest/overseas", response_model=KisNewsIngestResponse)
def ingest_overseas_kis_news(
    request: KisNewsIngestRequest = KisNewsIngestRequest(),
    db: Session = Depends(get_db),
):
    """
    해외 KIS 뉴스 적재
    - date, time을 지정하지 않으면 현재 시각 기준으로 조회
    """
    try:
        # 날짜/시간 처리
        date_str, time_str = request.date, request.time
        if not date_str or not time_str:
            date_str, time_str = get_current_kis_datetime()

        # KIS API 호출
        kis = KISClient(db)
        kis_response = kis.overseas_news_test(
            INFO_GB="t",
            CLASS_CD="04",
            NATION_CD="US",
            EXCHANGE_CD="",
            SYMB="",
            DATA_DT=date_str,
            DATA_TM=time_str,
            CTS="",
        )

        # 응답 파싱
        if not kis_response or kis_response.get("rt_cd") != "0":
            raise HTTPException(
                status_code=400,
                detail=f"KIS API error: {kis_response.get('msg1') if kis_response else 'No response'}",
            )

        # 해외 뉴스는 outblock1에 있음 (KIS API 응답 구조)
        outblock1 = kis_response.get("outblock1", [])
        if not outblock1:
            return KisNewsIngestResponse(success=0, skipped=0, errors=0, error_details=[])

        # 뉴스 적재
        repo = KisNewsRepository(db)
        service = KisNewsService(repo)
        result = service.ingest_overseas_news(outblock1)

        return KisNewsIngestResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ingesting overseas KIS news: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/kis-news/ingest/domestic", response_model=KisNewsIngestResponse)
def ingest_domestic_kis_news(
    request: KisNewsIngestRequest = KisNewsIngestRequest(),
    db: Session = Depends(get_db),
):
    """
    국내 KIS 뉴스 적재
    - date, time을 지정하지 않으면 현재 시각 기준으로 조회
    """
    try:
        # 날짜/시간 처리
        date_str, time_str = request.date, request.time
        if not date_str or not time_str:
            date_str, time_str = get_current_kis_datetime()

        # KIS API 호출
        kis = KISClient(db)
        kis_response = kis.domestic_news_test(
            FID_NEWS_OFER_ENTP_CODE="",
            FID_COND_MRKT_CLS_CODE="",
            FID_INPUT_ISCD="",
            FID_TITL_CNTT="",
            FID_INPUT_DATE_1=date_str,
            FID_INPUT_HOUR_1=time_str,
            FID_RANK_SORT_CLS_CODE="",
            FID_INPUT_SRNO="",
        )

        # 응답 파싱
        if not kis_response or kis_response.get("rt_cd") != "0":
            raise HTTPException(
                status_code=400,
                detail=f"KIS API error: {kis_response.get('msg1') if kis_response else 'No response'}",
            )

        # 국내 뉴스는 output에 있음 (KIS API 응답 구조)
        output = kis_response.get("output", [])
        if not output:
            return KisNewsIngestResponse(success=0, skipped=0, errors=0, error_details=[])

        # 뉴스 적재
        repo = KisNewsRepository(db)
        service = KisNewsService(repo)
        result = service.ingest_domestic_news(output)

        return KisNewsIngestResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ingesting domestic KIS news: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kis-news/recent", response_model=List[KisNewsItemResponse])
def get_recent_kis_news(
    limit: int = Query(100, ge=1, le=500, description="조회할 뉴스 개수"),
    db: Session = Depends(get_db),
):
    """
    최근 KIS 뉴스 목록 조회
    """
    try:
        repo = KisNewsRepository(db)
        service = KisNewsService(repo)
        news_list = service.get_recent_news(limit)
        return [KisNewsItemResponse.from_orm(news) for news in news_list]
    except Exception as e:
        logger.error(f"Error getting recent KIS news: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kis-news/ticker/{ticker_id}", response_model=List[KisNewsItemResponse])
def get_kis_news_by_ticker(
    ticker_id: int,
    limit: int = Query(50, ge=1, le=200, description="조회할 뉴스 개수"),
    db: Session = Depends(get_db),
):
    """
    특정 티커의 KIS 뉴스 조회
    """
    try:
        repo = KisNewsRepository(db)
        service = KisNewsService(repo)
        news_list = service.get_news_by_ticker(ticker_id, limit)
        return [KisNewsItemResponse.from_orm(news) for news in news_list]
    except Exception as e:
        logger.error(f"Error getting KIS news by ticker: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


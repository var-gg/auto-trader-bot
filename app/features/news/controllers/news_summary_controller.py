from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List

from app.core.db import get_db
from app.features.news.services.news_summary_service import NewsSummaryService

router = APIRouter(prefix="/news", tags=["news"])


class NewsSummaryItem(BaseModel):
    """뉴스 요약 항목 (프롬프트용 간소화)"""
    id: int = Field(..., description="뉴스 ID")
    summary_text: str = Field(..., description="뉴스 요약 텍스트")
    published_date_kst: Optional[str] = Field(None, description="발행일 (KST, ISO 형식)")


class NewsSummaryResponse(BaseModel):
    """뉴스 요약 응답 (직접 매핑 우선 할당)"""
    ticker: Dict[str, Any] = Field(..., description="티커 정보")
    news_summaries: List[NewsSummaryItem] = Field(..., description="뉴스 요약 목록")
    news_counts: Dict[str, int] = Field(..., description="뉴스 개수 정보 (직접매핑, 테마매핑, 총개수)")
    returned_count: int = Field(..., description="실제 반환된 뉴스 개수")
    limit_requested: int = Field(..., description="요청된 최대 개수")
    allocation: Dict[str, int] = Field(..., description="할당 정보 (직접매핑 한도, 테마매핑 한도, 실제 직접매핑 개수, 실제 테마매핑 개수)")




@router.get(
    "/summary/prompt/{ticker_id}",
    summary="티커별 뉴스 요약 프롬프트 조회",
    description="지정된 티커와 관련된 뉴스 요약을 애널리스트 AI 프롬프트 자료용으로 최신 발행 기준으로 조회합니다. 관련 뉴스는 직접 매핑과 테마 매핑(confidence=1.0)을 통해 결정됩니다.",
    response_description="티커 정보와 관련 뉴스 요약 목록을 프롬프트 자료용으로 반환합니다."
)
async def get_news_summary_by_ticker_id(
    ticker_id: int,
    limit: int = Query(10, ge=5, le=20, description="반환할 뉴스 개수 (5-20개)"),
    db: Session = Depends(get_db)
):
    """
    티커 ID로 뉴스 요약 프롬프트 자료 조회
    
    관련 뉴스 정의:
    1. 뉴스와 티커가 직접 매핑된 경우 (news_ticker 기준)
    2. 뉴스와 테마가 매핑되어 있고, 해당 테마의 confidence가 1.0이며, 
       그 테마가 해당 티커와 매핑된 경우 (news_theme + ticker_theme 기준)
    """
    service = NewsSummaryService(db)
    result = service.get_news_summary_for_ticker(ticker_id, limit)
    
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    
    return NewsSummaryResponse(**result)



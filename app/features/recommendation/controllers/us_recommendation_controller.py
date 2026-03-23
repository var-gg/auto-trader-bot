# app/features/recommendation/controllers/us_recommendation_controller.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.params import Query as QueryParam
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field

from app.core.db import get_db
from app.features.recommendation.repositories.recommendation_repository import RecommendationRepository
from app.features.recommendation.services.us_recommendation_service import UsRecommendationService

router = APIRouter(prefix="/recommendations", tags=["[미국주식] recommendations"])


# Pydantic 모델들
class CandidateTickersResponse(BaseModel):
    """추천 후보 티커 목록 응답 모델"""
    candidate_tickers: List[dict]
    total_count: int
    days_back: int
    generated_at: str
    criteria: dict




class TickerEligibilityResponse(BaseModel):
    """티커 적격성 검증 응답 모델"""
    ticker_id: int
    is_eligible: bool
    days_back: int
    checked_at: str
    message: str
    ticker_info: Optional[dict] = None


@router.get(
    "/candidates",
    response_model=CandidateTickersResponse,
    summary="추천 후보 티커 조회",
    description="""
    추천 후보가 될 수 있는 티커 목록을 조회합니다.
    
    **조건:**
    1. 지정된 기간(1~5일) 동안 발행된 뉴스 중
    2. 뉴스티커 직접 매핑이 존재하는 티커 (confidence >= 0.5)
    3. 해당 뉴스 생성일 이후 추천이 생성되지 않은 티커만
    
    **사용 예시:**
    - `GET /recommendations/candidates?days_back=3` (3일 전 뉴스 참조)
    """,
    response_description="추천 후보 티커 목록과 메타데이터"
)
def get_candidate_tickers(
    days_back: int = QueryParam(3, ge=1, le=5, description="현재일 기준 몇 일 전까지의 뉴스를 참조할지"),
    db: Session = Depends(get_db)
):
    """추천 후보 티커 목록을 조회합니다."""
    try:
        repository = RecommendationRepository(db)
        service = UsRecommendationService(repository)
        
        result = service.get_candidate_tickers(days_back, "US")
        return CandidateTickersResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"추천 후보 조회 중 오류가 발생했습니다: {str(e)}")






@router.get(
    "/validate/{ticker_id}",
    response_model=TickerEligibilityResponse,
    summary="티커 추천 적격성 검증",
    description="특정 티커가 추천 후보가 될 수 있는지 검증합니다.",
    response_description="티커 적격성 검증 결과"
)
def validate_ticker_eligibility(
    ticker_id: int,
    days_back: int = QueryParam(3, ge=1, le=5, description="참조할 뉴스 기간 (일수)"),
    db: Session = Depends(get_db)
):
    """특정 티커의 추천 적격성을 검증합니다."""
    try:
        repository = RecommendationRepository(db)
        service = UsRecommendationService(repository)
        
        result = service.validate_ticker_eligibility(ticker_id, days_back, "US")
        return TickerEligibilityResponse(**result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"티커 적격성 검증 중 오류가 발생했습니다: {str(e)}")

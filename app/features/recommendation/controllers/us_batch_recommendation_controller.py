# app/features/recommendation/controllers/us_batch_recommendation_controller.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional, Dict, Any

from app.core.db import get_db
from app.features.recommendation.services.us_batch_recommendation_service import UsBatchRecommendationService

router = APIRouter(prefix="/recommendations", tags=["[미국주식] recommendations"])


class RecommendationResult(BaseModel):
    """단일 추천 결과"""
    success: bool = Field(..., description="처리 성공 여부")
    ticker_id: int = Field(..., description="티커 ID")
    ticker_symbol: str = Field(..., description="티커 심볼")
    ticker_exchange: str = Field(..., description="티커 거래소")
    recommendation_id: Optional[int] = Field(None, description="생성된 추천 ID (성공 시)")
    position_type: Optional[str] = Field(None, description="포지션 타입 (성공 시)")
    entry_price: Optional[float] = Field(None, description="진입가 (성공 시)")
    target_price: Optional[float] = Field(None, description="목표가 (성공 시)")
    stop_price: Optional[float] = Field(None, description="손절가 (성공 시)")
    confidence_score: Optional[float] = Field(None, description="신뢰도 (성공 시)")
    is_latest: Optional[bool] = Field(None, description="최신 추천 여부 (성공 시)")
    generated_at: Optional[str] = Field(None, description="생성일시 (성공 시)")
    error: Optional[str] = Field(None, description="오류 메시지 (실패 시)")


class MarketDataSyncResult(BaseModel):
    """마켓데이터 동기화 결과"""
    success: bool = Field(..., description="동기화 성공 여부")
    message: str = Field(..., description="동기화 메시지")
    counts: Dict[str, int] = Field(default={}, description="티커별 동기화된 데이터 수")
    total_upserted: int = Field(default=0, description="총 동기화된 데이터 수")


class BatchRecommendationResponse(BaseModel):
    """배치 추천 생성 응답"""
    status: str = Field(..., description="처리 상태 (completed/skipped/error)")
    reason: str = Field(..., description="상태 이유")
    message: str = Field(..., description="상태 메시지")
    generated_at: str = Field(..., description="처리 완료일시 (ISO8601 UTC)")
    total_candidates: int = Field(..., description="총 후보 티커 수")
    processed: int = Field(..., description="처리된 티커 수")
    successful: int = Field(..., description="성공한 추천 수")
    failed: int = Field(..., description="실패한 추천 수")
    market_data_sync: Optional[MarketDataSyncResult] = Field(None, description="마켓데이터 동기화 결과")
    recommendations: List[RecommendationResult] = Field(default=[], description="성공한 추천 목록")
    errors: List[RecommendationResult] = Field(default=[], description="실패한 추천 목록")
    error: Optional[str] = Field(None, description="전체 처리 오류 (실패 시)")


@router.post(
    "/batch-generate",
    response_model=BatchRecommendationResponse,
    summary="배치 추천 생성 (비동기) ★★★",
    description="""
    추천 후보 티커들에 대해 배치로 AI 추천을 생성합니다 (asyncio 기반).
    
    **처리 과정:**
    1. 휴장 여부 확인: `/marketdata/is-market-closed` 서비스 활용
    2. 후보 티커 조회: `/recommendations/candidates` 서비스 활용 (days_back=1 고정)
    2.5. 마켓데이터 동기화: `/marketdata/sync/daily` 서비스 활용 (days=1 고정)
    3. 비동기 병렬 처리: asyncio.gather를 사용한 진짜 네트워크 병렬 처리
    4. 결과 집계: 성공/실패 통계와 상세 결과 반환
    
    **비동기 병렬 처리 방식:**
    - 후보 티커를 10개씩 배치로 분할
    - 각 배치 내에서 asyncio.gather로 동시 GPT API 호출
    - Cloud Run vCPU=1 환경에 최적화된 네트워크 I/O 병렬 처리
    
    **성능 최적화:**
    - ThreadPoolExecutor 대신 asyncio 기반 진짜 비동기 처리
    - GPT API 호출이 네트워크 I/O 바운드이므로 vCPU=1에서도 효율적
    - 동시에 여러 GPT 요청을 처리하여 전체 처리 시간 단축
    
    **휴장 처리:**
    - 현재 휴장 중이면 처리 중단하고 skipped 상태 반환
    
    **사용 예시:**
    - `POST /recommendations/batch-generate` (인자 없음)
    """,
    response_description="배치 처리 결과와 생성된 추천 목록"
)
async def generate_batch_recommendations_async(
    db: Session = Depends(get_db)
):
    """배치 추천 생성을 실행합니다 (비동기)."""
    try:
        service = UsBatchRecommendationService(db)
        result = await service.generate_batch_recommendations_async()
        
        # Pydantic 모델로 변환
        return BatchRecommendationResponse(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"배치 추천 생성 중 오류가 발생했습니다: {str(e)}"
        )

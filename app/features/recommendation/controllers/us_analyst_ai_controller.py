# app/features/recommendation/controllers/us_analyst_ai_controller.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Optional

from app.core.db import get_db
from app.features.recommendation.services.us_analyst_ai_service import UsAnalystAIService
from app.shared.models.ticker import Ticker

router = APIRouter(prefix="/recommendations", tags=["[미국주식] recommendations"])


class AnalystRecommendationResponse(BaseModel):
    """애널리스트 AI 추천 응답 모델"""
    recommendation_id: int = Field(..., description="추천 ID")
    ticker_id: int = Field(..., description="티커 ID")
    ticker_symbol: str = Field(..., description="티커 심볼")
    ticker_exchange: str = Field(..., description="거래소")
    position_type: str = Field(..., description="포지션 타입 (LONG/SHORT)")
    entry_price: float = Field(..., description="진입가")
    target_price: float = Field(..., description="목표가")
    stop_price: Optional[float] = Field(None, description="손절가")
    analysis_price: Optional[float] = Field(None, description="분석 당시 최근가격")
    valid_until: str = Field(..., description="유효기간 (ISO 형식)")
    reason: str = Field(..., description="추천 이유")
    confidence_score: float = Field(..., description="신뢰도 (0.0~1.0)")
    is_latest: bool = Field(..., description="최신 추천 여부")
    generated_at: str = Field(..., description="생성 시간 (ISO 형식)")


@router.post(
    "/ai-analysis/{ticker_id}",
    response_model=AnalystRecommendationResponse,
    summary="애널리스트 AI 분석 및 추천 생성",
    description="""
    특정 티커에 대해 애널리스트 AI를 통해 분석하고 추천을 생성하여 데이터베이스에 저장합니다.
    
    **처리 과정:**
    1. 티커 존재 여부 확인: 데이터베이스에서 티커 ID 검증
    2. 다중 데이터 소스 수집:
       - 어닝 데이터: /earnings/analyst/{ticker_id} 서비스 활용
       - 뉴스 요약: /news/summary/prompt/{ticker_id} 서비스 활용 (limit=10 고정)
       - 매크로 스냅샷: /macro/prompt/snapshot 서비스 활용
       - 마켓 데이터: /marketdata/prompt/ticker/{ticker_id} 서비스 활용 (days=50 고정)
       - 펀더멘털: /fundamentals/prompt/{ticker_id} 서비스 활용
    3. 현재 시간 정보 추가: KST 기준 현재 시간
    4. GPT API 호출: 수집된 데이터를 바탕으로 애널리스트 AI가 종합 분석 수행
    5. 추천 데이터 검증: GPT 응답의 유효성 및 필수 필드 검증
    6. 데이터베이스 저장: 생성된 추천을 analyst_recommendation 테이블에 저장
    7. 응답 데이터 구성: 저장된 추천 정보를 응답 모델에 맞게 변환
    
    **데이터 소스:**
    - 어닝 데이터: /earnings/analyst/{ticker_id}
    - 뉴스 요약: /news/summary/prompt/{ticker_id} (limit=10 고정)
    - 매크로 스냅샷: /macro/prompt/snapshot
    - 마켓 데이터: /marketdata/prompt/ticker/{ticker_id} (days=50 고정)
    - 펀더멘털: /fundamentals/prompt/{ticker_id}
    
    **AI 모델:** GPT 모델을 사용하여 종합적인 분석 수행
    **저장 테이블:** analyst_recommendation
    
    **사용 예시:**
    - `POST /recommendations/ai-analysis/123` (티커 ID 123에 대한 AI 분석 및 추천 생성)
    """,
    response_description="생성된 애널리스트 AI 추천 정보"
)
def generate_ai_analysis(
    ticker_id: int,
    db: Session = Depends(get_db)
):
    """애널리스트 AI를 통해 분석하고 추천을 생성합니다."""
    try:
        # 티커 존재 여부 확인
        ticker = db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise HTTPException(
                status_code=404, 
                detail=f"티커 ID {ticker_id}를 찾을 수 없습니다."
            )
        
        # AI 분석 서비스 호출 (동기 버전 사용)
        service = UsAnalystAIService(db)
        result = service.generate_analyst_recommendation(ticker_id)
        
        return AnalystRecommendationResponse(
            recommendation_id=result["recommendation_id"],
            ticker_id=result["ticker_id"],
            ticker_symbol=result["ticker_symbol"],
            ticker_exchange=result["ticker_exchange"],
            position_type=result["position_type"],
            entry_price=result["entry_price"],
            target_price=result["target_price"],
            stop_price=result.get("stop_price"),
            analysis_price=result.get("analysis_price"),
            valid_until=result["valid_until"],
            reason=result["reason"],
            confidence_score=result["confidence_score"],
            is_latest=result.get("is_latest", True),
            generated_at=result["generated_at"]
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"AI 분석 중 오류가 발생했습니다: {str(e)}"
        )

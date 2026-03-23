# app/features/recommendation/controllers/kr_analyst_prompt_controller.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.recommendation.services.kr_analyst_ai_service import KrAnalystAIService, ANALYST_RECOMMENDATION_SCHEMA
from app.shared.models.ticker import Ticker

router = APIRouter(prefix="/recommendations/kr", tags=["[국내주식] recommendations"])


@router.get(
    "/analyst-prompt/{ticker_id}",
    response_class=PlainTextResponse,
    summary="애널리스트 AI 프롬프트 생성",
    description="""
    특정 티커에 대한 애널리스트 AI 프롬프트를 생성합니다 (국내주식).
    
    **처리 과정:**
    1. 티커 존재 여부 확인: 데이터베이스에서 티커 ID 검증
    2. 다중 데이터 소스 수집:
       - 어닝 데이터: /earnings/analyst/{ticker_id} 서비스 활용
       - 뉴스 요약: /news/summary/prompt/{ticker_id} 서비스 활용 (limit=10 고정)
       - 마켓 데이터: /marketdata/prompt/ticker/{ticker_id} 서비스 활용 (days=50 고정)
       - 펀더멘털: /fundamentals/prompt/{ticker_id} 서비스 활용
    3. 현재 시간 정보 추가: KST 기준 현재 시간
    4. 프롬프트 템플릿 구성: 수집된 데이터를 JSON 형태로 구조화
    5. 최종 프롬프트 생성: 시스템 메시지 + 컨텍스트 데이터 + 정책 가이드라인 + 출력 형식
    
    **데이터 소스:**
    - 어닝 데이터: /earnings/analyst/{ticker_id}
    - 뉴스 요약: /news/summary/prompt/{ticker_id} (limit=10 고정)
    - 마켓 데이터: /marketdata/prompt/ticker/{ticker_id} (days=50 고정)
    - 펀더멘털: /fundamentals/prompt/{ticker_id}
    - 현재 시간: KST 기준
    
    **미국주식과의 차이점:**
    - 매크로 스냅샷 제외 (FRED 수치는 국내주식과 관계없음)
    - 단위를 원(원)으로 변경
    - 국내주식 특화 프롬프트 구성
    
    **프롬프트 구성:**
    1. 시스템 메시지 (애널리스트 역할 정의)
    2. 컨텍스트 데이터 (JSON 형태)
    3. 정책 가이드라인
    4. 출력 형식 (JSON 스키마)
    
    **사용 예시:**
    - `GET /recommendations/kr/analyst-prompt/123` (티커 ID 123에 대한 프롬프트 생성)
    """,
    response_description="생성된 애널리스트 AI 프롬프트 텍스트 (줄바꿈 포함)"
)
def generate_analyst_prompt(
    ticker_id: int,
    db: Session = Depends(get_db)
):
    """애널리스트 AI용 프롬프트를 생성합니다 (국내주식)."""
    try:
        # 티커 존재 여부 확인
        ticker = db.query(Ticker).filter(Ticker.id == ticker_id).first()
        if not ticker:
            raise HTTPException(
                status_code=404, 
                detail=f"티커 ID {ticker_id}를 찾을 수 없습니다."
            )
        
        # 프롬프트 생성 서비스 호출 (KrAnalystAIService 사용)
        service = KrAnalystAIService(db)
        prompt_text = service.generate_analyst_prompt_only(ticker_id)
        
        # [OUTPUT] 섹션과 스키마 추가
        import json
        schema_text = json.dumps(ANALYST_RECOMMENDATION_SCHEMA, ensure_ascii=False, indent=2)
        full_prompt = f"{prompt_text}\n\n[OUTPUT]\n{schema_text}"
        
        # PlainTextResponse로 프롬프트 텍스트 직접 반환
        return PlainTextResponse(
            content=full_prompt,
            media_type="text/plain; charset=utf-8"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"프롬프트 생성 중 오류가 발생했습니다: {str(e)}"
        )

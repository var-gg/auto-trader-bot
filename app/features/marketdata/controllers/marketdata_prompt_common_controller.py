# app/features/marketdata/controllers/marketdata_prompt_common_controller.py
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, Any
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.features.marketdata.services.marketdata_prompt_common_service import MarketdataPromptCommonService
from app.shared.models.ticker import Ticker

router = APIRouter(prefix="/marketdata/prompt", tags=["[미국주식/국내주식] marketdata"])

@router.get(
    "/ticker/{ticker_id}",
    summary="티커 기반 프롬프트 생성",
    description="""
    지정된 티커 ID의 주가데이터와 기술지표를 포함한 AI 분석용 프롬프트를 생성합니다.
    미국주식과 국내주식 모두 지원합니다.
    
    **처리 과정:**
    1. 티커 존재 여부 확인: 데이터베이스에서 티커 ID가 존재하는지 검증
    2. 거래소 확인: 미국주식 또는 국내주식 여부를 자동으로 판별
    3. 일봉 데이터 조회: 지정된 기간(days)의 최근 주가 데이터를 ohlcv_daily 테이블에서 조회
    4. 기술지표 계산: 
       - 이동평균선 (MA20, MA50)
       - RSI (14일 기준)
       - 가격 변동률 계산
    5. 데이터 패키징: 
       - 현재가 정보
       - 최근 거래일별 OHLCV 데이터
       - 기술지표 요약 정보
    6. 프롬프트 구성: AI 분석에 필요한 형태로 데이터를 구조화하여 반환
    
    **포함 데이터:**
    - 현재가 (current_price)
    - 최근 거래일별 OHLCV (recent_closes)
    - 기술지표 요약 (technical_summary):
      - MA20, MA50
      - RSI14 및 과매수/과매도 상태
      - 가격 변동률
    - 거래소 정보 (exchange)
    
    **데이터 소스:** ohlcv_daily 테이블
    **기술지표:** MA, RSI 계산
    **지원 거래소:** 미국주식 (NMS, NYQ, NAS 등), 국내주식 (KOE)
    **용도:** 애널리스트 AI 분석용 데이터 패키징
    **기간 범위:** 1~252일 (기본 21일)
    
    **사용 예시:**
    - 미국주식: `GET /marketdata/prompt/ticker/123?days=50`
    - 국내주식: `GET /marketdata/prompt/ticker/456?days=30`
    """,
    response_description="생성된 프롬프트 데이터와 티커 정보를 반환합니다."
)
def prompt_by_ticker(
    ticker_id: int,
    days: int = Query(21, ge=1, le=252, description="프롬프트에 포함할 거래일 수 (최근 N일, 기본 21일, 최대 252일)"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    # 티커 존재 여부 확인
    ticker = db.query(Ticker).filter(Ticker.id == ticker_id).first()
    if not ticker:
        raise HTTPException(status_code=404, detail=f"티커 ID {ticker_id}를 찾을 수 없습니다.")
    
    # 공용 프롬프트 서비스 호출
    service = MarketdataPromptCommonService(db)
    marketdata = service.build_ticker_prompt(ticker_id, days)
    
    return {
        "marketdata": marketdata
    }

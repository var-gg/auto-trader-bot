from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List

from app.core.db import get_db
from app.features.fundamentals.services.us_fundamental_service import FundamentalService

router = APIRouter(prefix="/fundamentals", tags=["[미국주식] fundamentals"])


class SyncSymbolRequest(BaseModel):
    """특정 심볼 펀더멘털 동기화 요청"""
    symbol: str = Field(..., examples=["AAPL"], description="종목 심볼 (예: AAPL, MSFT)")
    exchange: str = Field(..., examples=["NMS"], description="거래소 코드 (예: NMS, NYQ)")


class SyncSymbolResponse(BaseModel):
    """특정 심볼 펀더멘털 동기화 응답"""
    ticker_id: int = Field(..., description="티커 ID")
    symbol: str = Field(..., description="종목 심볼")
    exchange: str = Field(..., description="거래소 코드")
    fundamental_snapshot: Optional[Dict[str, Any]] = Field(None, description="펀더멘털 스냅샷 데이터")
    dividend_histories_added: int = Field(0, description="추가된 배당 이력 건수")
    error: Optional[str] = Field(None, description="에러 메시지")


class SyncAllResponse(BaseModel):
    """전체 펀더멘털 동기화 응답"""
    total_tickers: int = Field(..., description="전체 티커 수")
    processed: int = Field(..., description="처리된 티커 수")
    stale_for_finnhub: int = Field(..., description="Finnhub 업데이트가 필요한 티커 수")
    errors: List[str] = Field(..., description="에러 목록")
    results: List[Dict[str, Any]] = Field(..., description="처리 결과 목록")
    start_time: str = Field(..., description="시작 시간 (ISO 형식)")
    end_time: Optional[str] = Field(None, description="완료 시간 (ISO 형식)")
    estimated_seconds: float = Field(..., description="시작 시점 추정 총 소요(초)")
    notes: Optional[str] = Field(None, description="추가 메모/설명")


class PromptDataResponse(BaseModel):
    """펀더멘털 프롬프트 데이터 응답"""
    ticker: Dict[str, Any] = Field(..., description="티커 정보")
    fundamentals: Dict[str, Any] = Field(..., description="펀더멘털 데이터")
    dividend_history: List[Dict[str, Any]] = Field(..., description="배당 이력 (최근 5개)")


@router.post(
    "/sync/symbol",
    summary="특정 심볼 펀더멘털 동기화",
    description="""
    지정된 종목의 펀더멘털 데이터를 Finnhub API를 통해 동기화합니다.
    
    **처리 과정:**
    1. 티커 정보 조회: symbol과 exchange로 ticker_id 확인
    2. Finnhub API 호출: 펀더멘털 데이터와 배당 이력 수집
    3. 데이터 파싱: PER, PBR, 시가총액, 배당이력 등 추출
    4. DB 저장: 펀더멘털 스냅샷은 upsert, 배당이력은 중복 제거 후 추가
    
    **수집 데이터:**
    - 펀더멘털 스냅샷: PER, PBR, 시가총액, 배당률
    - 배당이력: 최근 5년간의 배당 지급 이력
    
    **처리 방식:**
    - 펀더멘털 스냅샷: upsert 방식으로 업데이트
    - 배당이력: 중복되지 않는 새로운 레코드만 추가
    
    **지원 대상:**
    - 미국 주식 (NYSE, NASDAQ 거래소)
    - Finnhub API에서 지원하는 모든 미국 상장 기업
    """,
    response_description="동기화된 펀더멘털 데이터와 추가된 배당 이력 건수를 반환합니다."
)
async def sync_fundamentals_by_symbol(
    request: SyncSymbolRequest,
    db: Session = Depends(get_db)
):
    service = FundamentalService(db)
    result = service.sync_fundamentals_by_symbol(request.symbol, request.exchange)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return SyncSymbolResponse(**result)


@router.post(
    "/sync/all",
    summary="전체 펀더멘털 동기화 (최적화) ★★★",
    description="""
    NYQ/NMS 거래소의 모든 미국 주식 펀더멘털을 빠르게 동기화합니다.
    
    **처리 과정:**
    1. 대상 티커 조회: NYQ/NMS 거래소의 모든 미국 주식
    2. 스마트 필터링: 오늘 이미 업데이트된 스냅샷은 Finnhub 호출 생략
    3. 펀더멘털 수집: Finnhub API를 통한 PER, PBR, 시가총액 데이터
    4. 배당이력 수집: yfinance 벌크 다운로드로 성능 최적화
    5. DB 저장: 벌크 인서트로 대량 데이터 처리 가속화
    
    **성능 최적화:**
    - 오늘 업데이트된 스냅샷은 Finnhub API 호출 생략
    - 배당이력은 yfinance 벌크 다운로드 + DB 벌크 인서트
    - 중복 데이터 처리 최소화로 전체 처리 시간 단축
    
    **수집 데이터:**
    - 펀더멘털 스냅샷: PER, PBR, 시가총액, 배당률
    - 배당이력: 최근 5년간의 배당 지급 이력
    
    **지원 대상:**
    - NYQ (NYSE), NMS (NASDAQ) 거래소의 모든 미국 주식
    - Finnhub API와 yfinance에서 지원하는 모든 종목
    """,
    response_description="진행 상황, 처리 결과, 에러 목록과 추정 소요 시간을 반환합니다."
)
async def sync_fundamentals_all(
    db: Session = Depends(get_db)
):
    service = FundamentalService(db)
    result = service.sync_fundamentals_all()
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return SyncAllResponse(**result)


@router.get(
    "/prompt/{ticker_id}",
    summary="펀더멘털 프롬프트 데이터 조회",
    description="""
    지정된 티커의 펀더멘털 데이터를 애널리스트 AI 프롬프트용으로 조회합니다.
    
    **처리 과정:**
    1. 티커 정보 조회: ticker_id로 기본 정보 확인
    2. 펀더멘털 데이터 조회: DB에서 최신 스냅샷 데이터 추출
    3. 배당이력 조회: 최근 5개 배당 지급 이력 수집
    4. 데이터 구조화: AI 프롬프트에 최적화된 JSON 형태로 변환
    
    **조회 데이터:**
    - 티커 정보: symbol, exchange, 종목명 등
    - 펀더멘털 스냅샷: PER, PBR, 시가총액, 배당률
    - 배당이력: 최근 5개 배당 지급 정보 (지급일, 금액, 배당률)
    
    **사용 목적:**
    - 애널리스트 AI 서비스의 프롬프트 데이터 제공
    - 투자 분석을 위한 펀더멘털 지표 요약
    - 배당 정책 분석을 위한 최근 배당 이력 제공
    
    **지원 대상:**
    - 모든 미국 주식 (NYSE, NASDAQ 거래소)
    - 펀더멘털 데이터가 수집된 모든 종목
    """,
    response_description="티커 정보, 펀더멘털 데이터, 배당 이력(최근 5개)을 포함한 프롬프트용 데이터를 반환합니다."
)
async def get_fundamental_prompt_data(
    ticker_id: int,
    db: Session = Depends(get_db)
):
    service = FundamentalService(db)
    result = service.get_fundamental_prompt_data(ticker_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return PromptDataResponse(**result)

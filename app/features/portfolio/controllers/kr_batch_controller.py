from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging

from app.core.db import get_db
from app.features.portfolio.services.kr_buy_order_ai_service import KrBuyOrderAIService
from app.features.portfolio.services.kr_sell_order_ai_service import KrSellOrderAIService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portfolio/kr", tags=["[국내주식] portfolio"])


class BuyOrderBatchResponse(BaseModel):
    """국내주식 매수주문 배치 생성 및 실행 응답 모델"""
    batch_id: Optional[int] = None
    asof_kst: str
    mode: str
    currency: str
    available_cash: float
    notes: str
    plans_count: int
    skipped_count: int
    executed_orders: List[Dict[str, Any]]
    is_market_closed: bool
    generated_at: str
    message: Optional[str] = None


class SellOrderBatchResponse(BaseModel):
    """국내주식 매도주문 배치 생성 및 실행 응답 모델"""
    batch_id: Optional[int] = None
    asof_kst: str
    mode: str
    currency: str
    available_cash: float
    notes: str
    plans_count: int
    skipped_count: int
    executed_orders: List[Dict[str, Any]]
    is_market_closed: bool
    generated_at: str
    message: Optional[str] = None


@router.post(
    "/buy-order-batch",
    response_model=BuyOrderBatchResponse,
    summary="국내주식 매수주문 배치 AI 생성 및 즉시 실행 ★★★",
    description="""
    AI를 통해 국내주식 매수주문 배치를 생성하고 즉시 실행합니다.
    
    **처리 과정:**
    0. **휴장 확인**: `/marketdata/is-market-closed` 서비스로 휴장 여부 확인
    1. **자산 스냅샷 갱신**: `/asset-snapshots/collect/kr` API를 통해 국내 자산 스냅샷 수집
    2. **프롬프트 생성**: 최신 자산 스냅샷 + 유효한 LONG 추천서들 (country='KR'만)
    3. **GPT AI 호출**: 매수주문 설계자 AI가 주문 배치 생성
    4. **데이터베이스 저장**: OrderBatch → OrderPlan → OrderLeg 구조로 저장
    5. **주문 즉시 실행**: 실제 KIS API를 통해 국내주식 주문 실행
    6. **결과 반환**: 생성된 배치 정보 및 실행 결과
    
    **휴장 처리:**
    - 휴장 시: 전체 로직 skip, `is_market_closed: true` 반환
    - 개장 시: 정상적으로 매수주문 배치 생성 및 실행
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 KR 스냅샷
    - 추천서: analyst_recommendation 테이블의 유효기간 내 LONG 추천서 (country='KR', is_latest=True)
    - 현재가: /marketdata/kr/sync/price-detail/{ticker_id} API를 통한 실시간 가격 조회
    
    **AI 모델:** GPT를 사용하여 국내주식 매수주문 배치 설계
    **저장 테이블:** order_batch, order_plan, order_leg, broker_order
    
    **국내주식 특화 요소:**
    - 호가단위 준수 (1원, 5원, 10원, 50원, 100원, 500원, 1000원)
    - 거래시간 고려 (정규장 09:00-15:30, 시간외단일가 16:00-18:00)
    - 상한가/하한가 30% 변동폭 제한
    - LIMIT/LOC 주문 유형만 지원
    
    **특징:**
    - **스마트 휴장 감지**: 주말/공휴일 자동 감지하여 불필요한 처리 방지
    - **실시간 포트폴리오 갱신**: 매번 KIS API를 통해 최신 잔고 조회
    - **지능형 주문 설계**: GPT AI를 통한 체계적인 매수 전략 수립
    - **실행 계획(EXECUTE)과 제외 계획(SKIP) 모두 저장**
    - **각 계획은 여러 레그(LIMIT/LOC)로 구성 가능**
    - **체계적인 주문 추적 및 관리 지원**
    - **실제 KIS API 주문 실행 및 BrokerOrder 테이블 연동**
    
    **사용 예시:**
    - `POST /portfolio/kr/buy-order-batch` (국내주식 매수주문 배치 AI 생성 + 즉시 실행)
    """,
    response_description="생성된 국내주식 매수주문 배치 정보 및 실행 결과"
)
def generate_kr_buy_order_batch(
    db: Session = Depends(get_db)
):
    """AI를 통해 국내주식 매수주문 배치를 생성하고 즉시 실행합니다."""
    try:
        kr_buy_order_ai_service = KrBuyOrderAIService(db)
        result = kr_buy_order_ai_service.generate_buy_order_batch()
        
        return BuyOrderBatchResponse(
            batch_id=result["batch_id"],
            asof_kst=result["asof_kst"],
            mode=result["mode"],
            currency=result["currency"],
            available_cash=result["available_cash"],
            notes=result["notes"],
            plans_count=result["plans_count"],
            skipped_count=result["skipped_count"],
            executed_orders=result["executed_orders"],
            is_market_closed=result["is_market_closed"],
            generated_at=result["generated_at"],
            message=result.get("message")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 매수주문 배치 생성 중 오류가 발생했습니다: {str(e)}"
        )


@router.post(
    "/sell-order-batch",
    response_model=SellOrderBatchResponse,
    summary="국내주식 매도주문 배치 AI 생성 및 즉시 실행 ★★★",
    description="""
    AI를 통해 국내주식 매도주문 배치를 생성하고 즉시 실행합니다.
    
    **처리 과정:**
    0. **휴장 확인**: `/marketdata/is-market-closed` 서비스로 휴장 여부 확인
    1. **자산 스냅샷 갱신**: `/asset-snapshots/collect/kr` API를 통해 국내 자산 스냅샷 수집
    2. **프롬프트 생성**: 현재 보유종목의 최근 추천서 기반 매도 프롬프트 (country='KR'만)
    3. **GPT AI 호출**: 매도주문 설계자 AI가 주문 배치 생성
    4. **데이터베이스 저장**: OrderBatch → OrderPlan → OrderLeg 구조로 저장
    5. **주문 즉시 실행**: 실제 KIS API를 통해 국내주식 주문 실행
    6. **결과 반환**: 생성된 배치 정보 및 실행 결과
    
    **휴장 처리:**
    - 휴장 시: 전체 로직 skip, `is_market_closed: true` 반환
    - 개장 시: 정상적으로 매도주문 배치 생성 및 실행
    
    **데이터 소스:**
    - 자산 스냅샷: asset_snapshot + position_snapshot 테이블의 최신 KR 스냅샷
    - 추천서: analyst_recommendation 테이블의 현재 보유종목 최근 추천서 (country='KR', is_latest=True)
    - 현재가: /marketdata/kr/sync/price-detail/{ticker_id} API를 통한 실시간 가격 조회
    
    **AI 모델:** GPT를 사용하여 국내주식 매도주문 배치 설계
    **저장 테이블:** order_batch, order_plan, order_leg, broker_order
    
    **국내주식 특화 요소:**
    - 호가단위 준수 (1원, 5원, 10원, 50원, 100원, 500원, 1000원)
    - 거래시간 고려 (정규장 09:00-15:30, 시간외단일가 16:00-18:00)
    - 상한가/하한가 30% 변동폭 제한
    - LIMIT/LOC 주문 유형만 지원
    
    **특징:**
    - **스마트 휴장 감지**: 주말/공휴일 자동 감지하여 불필요한 처리 방지
    - **실시간 포트폴리오 갱신**: 매번 KIS API를 통해 최신 잔고 조회
    - **지능형 주문 설계**: GPT AI를 통한 체계적인 매도 전략 수립
    - **실행 계획(EXECUTE)과 제외 계획(SKIP) 모두 저장**
    - **각 계획은 여러 레그(LIMIT/LOC)로 구성 가능**
    - **체계적인 주문 추적 및 관리 지원**
    - **실제 KIS API 주문 실행 및 BrokerOrder 테이블 연동**
    
    **사용 예시:**
    - `POST /portfolio/kr/sell-order-batch` (국내주식 매도주문 배치 AI 생성 + 즉시 실행)
    """,
    response_description="생성된 국내주식 매도주문 배치 정보 및 실행 결과"
)
def generate_kr_sell_order_batch(
    db: Session = Depends(get_db)
):
    """AI를 통해 국내주식 매도주문 배치를 생성하고 즉시 실행합니다."""
    try:
        kr_sell_order_ai_service = KrSellOrderAIService(db)
        result = kr_sell_order_ai_service.generate_sell_order_batch()
        
        return SellOrderBatchResponse(
            batch_id=result["batch_id"],
            asof_kst=result["asof_kst"],
            mode=result["mode"],
            currency=result["currency"],
            available_cash=result["available_cash"],
            notes=result["notes"],
            plans_count=result["plans_count"],
            skipped_count=result["skipped_count"],
            executed_orders=result["executed_orders"],
            is_market_closed=result["is_market_closed"],
            generated_at=result["generated_at"],
            message=result.get("message")
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"국내주식 매도주문 배치 생성 중 오류가 발생했습니다: {str(e)}"
        )
